package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt"
	lru "github.com/hashicorp/golang-lru"
	m3oauth "github.com/m3o/services/pkg/auth"
	publicapi "github.com/m3o/services/publicapi/proto"
	v1 "github.com/m3o/services/v1/proto"
	authpb "github.com/micro/micro/v3/proto/auth"
	"github.com/micro/micro/v3/service"
	"github.com/micro/micro/v3/service/api"
	"github.com/micro/micro/v3/service/auth"
	"github.com/micro/micro/v3/service/client"
	"github.com/micro/micro/v3/service/context/metadata"
	"github.com/micro/micro/v3/service/errors"
	"github.com/micro/micro/v3/service/events"
	"github.com/micro/micro/v3/service/logger"
	log "github.com/micro/micro/v3/service/logger"
	"github.com/micro/micro/v3/service/registry"
	"github.com/micro/micro/v3/service/server"
	"github.com/micro/micro/v3/service/store"
)

type V1 struct {
	papi        publicapi.PublicapiService
	accsvc      authpb.AccountsService
	keyRecCache *expiringLRUCache
}

type cacheValueWrapper struct {
	value   interface{}
	expires time.Time
}

// expiringLRUCache works much like the LRUCache but adds an expiry to each entry
// implementation is naive and has many edge cases because we don't actively expire things but just check
// on the `Get()` call. Good enough for our purposes until we implement a redis cache
type expiringLRUCache struct {
	*lru.Cache
	ttl time.Duration
}

func (c *expiringLRUCache) Add(key, value interface{}) bool {
	return c.Cache.Add(key, &cacheValueWrapper{value: value, expires: time.Now().Add(c.ttl)})
}

func (c *expiringLRUCache) Get(key interface{}) (interface{}, bool) {
	val, ok := c.Cache.Get(key)
	if !ok {
		return nil, false
	}
	wrappedVal := val.(*cacheValueWrapper)
	if wrappedVal.hasExpired() {
		// We don't delete the entry here because we'd have to introduce locking (doing a get then delete)
		//
		return nil, false
	}
	return wrappedVal.value, true
}

func (c cacheValueWrapper) hasExpired() bool {
	return time.Now().After(c.expires)
}

const (
	storePrefixHashedKey = "hashed"
	storePrefixUserID    = "user"
	storePrefixKeyID     = "key"

	lruCacheTTL = 5 * time.Minute
)

var (
	errUnauthorized = errors.Unauthorized("v1", "Unauthorized")
	errInternal     = errors.InternalServerError("v1", "Error processing request")
)

func NewHandler(srv *service.Service) *V1 {
	c, err := lru.New(10000) // TODO how big should this be?
	if err != nil {
		log.Fatalf("Failed to create LRU cache %s", err)
	}
	keyRecCache := expiringLRUCache{Cache: c, ttl: lruCacheTTL}
	v1 := &V1{
		papi:        publicapi.NewPublicapiService("publicapi", srv.Client()),
		accsvc:      authpb.NewAccountsService("auth", srv.Client()),
		keyRecCache: &keyRecCache,
	}
	go v1.consumeEvents()
	return v1
}

func (v1 *V1) writeAPIRecord(rec *apiKeyRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	// store under hashed key for API usage
	if err := store.Write(&store.Record{
		Key:   fmt.Sprintf("%s:%s", storePrefixHashedKey, rec.ApiKey),
		Value: b,
	}); err != nil {
		return err
	}

	// store under the user ID for retrieval on dashboard
	if err := store.Write(&store.Record{
		Key:   fmt.Sprintf("%s:%s:%s:%s", storePrefixUserID, rec.Namespace, rec.UserID, rec.ApiKey),
		Value: b,
	}); err != nil {
		return err
	}

	// store under the key's ID for deletion
	if err := store.Write(&store.Record{
		Key:   fmt.Sprintf("%s:%s:%s:%s", storePrefixKeyID, rec.Namespace, rec.UserID, rec.ID),
		Value: b,
	}); err != nil {
		return err
	}

	v1.keyRecCache.Add(rec.ApiKey, rec)
	return nil
}

func (v1 *V1) deleteAPIRecord(rec *apiKeyRecord) error {
	// store under hashed key for API usage
	if err := store.Delete(fmt.Sprintf("%s:%s", storePrefixHashedKey, rec.ApiKey)); err != nil {
		return err
	}

	// store under the user ID for retrieval on dashboard
	if err := store.Delete(fmt.Sprintf("%s:%s:%s:%s", storePrefixUserID, rec.Namespace, rec.UserID, rec.ApiKey)); err != nil {
		return err
	}

	// store under the key's ID for deletion
	if err := store.Delete(fmt.Sprintf("%s:%s:%s:%s", storePrefixKeyID, rec.Namespace, rec.UserID, rec.ID)); err != nil {
		return err
	}
	v1.keyRecCache.Remove(rec.ApiKey)

	return nil
}

func readAPIRecordByKeyID(ns, user, keyID string) (*apiKeyRecord, error) {
	recs, err := store.Read(fmt.Sprintf("%s:%s:%s:%s", storePrefixKeyID, ns, user, keyID))
	if err != nil {
		return nil, err
	}

	rec := recs[0]
	keyRec := &apiKeyRecord{}
	if err := json.Unmarshal(rec.Value, keyRec); err != nil {
		return nil, err
	}
	return keyRec, nil
}

func hashSecret(s string) (string, error) {
	h := sha256.New()
	h.Write([]byte(s))
	h.Sum(nil)
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// checkRequestedScopes returns true if account has sufficient privileges for them to generate the requestedScopes.
func (v1 *V1) checkRequestedScopes(ctx context.Context, requestedScopes []string) bool {
	allowedScopes, err := v1.listAPIs()
	if err != nil {
		// fail closed
		return false
	}
	// add wildcard
	allowedScopes = append(allowedScopes, "*")
	for _, requested := range requestedScopes {
		found := false
		for _, allowed := range allowedScopes {
			if allowed == requested {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (v1 *V1) readAPIRecordByAPIKey(authz string) (string, *apiKeyRecord, error) {
	if len(authz) == 0 || !strings.HasPrefix(authz, "Bearer ") {
		return "", nil, errUnauthorized
	}

	// do lookup on hash of key
	key := authz[7:]
	hashed, err := hashSecret(key)
	if err != nil {
		return "", nil, errUnauthorized
	}

	cached, ok := v1.keyRecCache.Get(hashed)
	if ok {
		return key, cached.(*apiKeyRecord), nil
	}

	recs, err := store.Read(fmt.Sprintf("%s:%s", storePrefixHashedKey, hashed))
	if err != nil {
		if err != store.ErrNotFound {
			log.Errorf("Error while looking up api key %s", err)
		}
		// not found == invalid (or even revoked)
		log.Infof("Authz not found %+v", hashed)
		return "", nil, errUnauthorized
	}
	// rehydrate
	apiRec := apiKeyRecord{}
	if err := json.Unmarshal(recs[0].Value, &apiRec); err != nil {
		log.Errorf("Error while rehydrating api key record %s", err)
		return "", nil, errUnauthorized
	}
	v1.keyRecCache.Add(hashed, &apiRec)

	return key, &apiRec, nil
}

func verifyCallAllowed(apiRec *apiKeyRecord, reqURL string) error {
	// checks
	// We do 2 types of checks
	// 1. Has the key been explicitly blocked? Happens if it's exhausted it's money for example
	// 2. Do the scopes of the token allow them to call the requested API? The name of the scopes correspond to the service

	if apiRec.Status == keyStatusBlocked {
		return errBlocked(apiRec.StatusMessage)
	}

	for _, s := range apiRec.Scopes {
		if s == "*" {
			// they can call anything they like
			return nil
		}
		if strings.HasPrefix(reqURL, fmt.Sprintf("/v1/%s/", s)) {
			// match
			return nil
		}
	}
	// TODO better error please
	return errBlocked("Insufficient privileges")

}

func errBlocked(msg string) error {
	return errors.Forbidden("v1.blocked", fmt.Sprintf("Request blocked. %s", msg))
}

func (v1 *V1) refreshToken(apiRec *apiKeyRecord, key string) error {
	// do we need to refresh the token?
	tok, _, err := new(jwt.Parser).ParseUnverified(apiRec.Token, jwt.MapClaims{})
	if err != nil {
		log.Errorf("Error parsing existing jwt %s", err)
		return errUnauthorized
	}
	if claims, ok := tok.Claims.(jwt.MapClaims); ok {
		if !claims.VerifyExpiresAt(time.Now().Unix(), true) {
			// needs a refresh
			tok, err := auth.Token(
				auth.WithCredentials(apiRec.AccID, key),
				auth.WithTokenIssuer(apiRec.Namespace),
				auth.WithExpiry(1*time.Hour))
			if err != nil {
				log.Errorf("Error refreshing token %s", err)
				return errInternal
			}
			apiRec.Token = tok.AccessToken
			if err := v1.writeAPIRecord(apiRec); err != nil {
				log.Errorf("Error updating API record %s", err)
				return errInternal
			}
		}
	} else {
		log.Errorf("Error parsing existing jwt claims %s", err)
		return errUnauthorized
	}
	return nil
}

func (v1 *V1) getRequestedService(reqURL string) (string, string, []*registry.Service, error) {
	trimmedPath := strings.TrimPrefix(reqURL, "/v1/")
	parts := strings.Split(trimmedPath, "/")
	if len(parts) < 2 {
		// can't work out service and method
		return "", "", nil, errors.NotFound("v1", "")
	}

	service := parts[0]
	svcs, err := registry.GetService(service)
	if err != nil {
		if err == registry.ErrNotFound {
			return "", "", nil, errors.NotFound("v1", "No such API")
		}
		log.Errorf("Error looking up service %s", err)
		return "", "", nil, errInternal
	}
	endpoint := ""
	if len(parts) == 2 {
		// /v1/helloworld/call -> helloworld Helloworld.Call
		endpoint = fmt.Sprintf("%s.%s", strings.Title(parts[0]), strings.Title(parts[1]))
	} else {
		// /v1/hello/world/call -> hello World.Call
		endpoint = fmt.Sprintf("%s.%s", strings.Title(parts[1]), strings.Title(parts[2]))
	}

	return service, endpoint, svcs, nil
}

func parseContentType(ct string) string {
	if idx := strings.IndexRune(ct, ';'); idx >= 0 {
		ct = ct[:idx]
	}
	if len(ct) == 0 {
		ct = "application/json"
	}
	return ct
}

// Endpoint is a catch all for endpoints
func (v1 *V1) Endpoint(ctx context.Context, stream server.Stream) (retErr error) {
	// check api key
	defer stream.Close()

	md, ok := metadata.FromContext(ctx)
	if !ok {
		return errUnauthorized
	}

	key, apiRec, err := v1.readAPIRecordByAPIKey(md["Authorization"])
	if err != nil {
		return err
	}

	reqURL, ok := md.Get("url")
	if !ok {
		log.Errorf("Requested URL not found")
		return errInternal
	}

	// reqURL likely contains the query params
	u, err := url.Parse(reqURL)
	if err == nil {
		// only set the path not the params
		reqURL = u.Path
	}

	if err := verifyCallAllowed(apiRec, reqURL); err != nil {
		return err
	}

	if err := v1.refreshToken(apiRec, key); err != nil {
		return err
	}
	// set the auth
	ctx = metadata.Set(ctx, "Authorization", fmt.Sprintf("Bearer %s", apiRec.Token))

	// assume application/json for now
	ct := "application/json"
	service, endpoint, svcs, err := v1.getRequestedService(reqURL)
	if err != nil {
		return err
	}

	if isStream(endpoint, svcs) {
		return serveStream(ctx, stream, service, endpoint, svcs, apiRec)
	}

	// forward the request
	var payload json.RawMessage
	if err := stream.Recv(&payload); err != nil {
		log.Errorf("Error receiving from stream %s", err)
		return errInternal
	}

	// because we have a query we want to merge params and payload
	if u != nil && len(u.RawQuery) > 0 {
		payload = mergeURLPayload(ctx, md, u, payload)
	}

	request := client.DefaultClient.NewRequest(
		service,
		endpoint,
		&payload,
		client.WithContentType(ct),
	)
	// create request/response
	var response json.RawMessage
	// make the call
	if err := client.Call(ctx, request, &response); err != nil {
		return err
	}
	go publishEndpointEvent(reqURL, service, endpoint, apiRec)

	stream.Send(response)
	return nil

}

// mergePayload will attempt to generate a new payload including query params
func mergeURLPayload(ctx context.Context, md metadata.Metadata, u *url.URL, payload json.RawMessage) json.RawMessage {
	method, ok := md.Get("Method")
	if !ok {
		method = "POST"
	}
	// generate a new http request
	req, err := http.NewRequestWithContext(
		ctx,
		method,
		u.String(),
		bytes.NewReader(payload),
	)

	if err != nil {
		return payload
	}

	// attempt to parse out the params into the payload
	b, err := api.RequestPayload(req)
	if err != nil {
		return payload
	}

	return json.RawMessage(b)
}

func publishEndpointEvent(reqURL, apiName, endpointName string, apiRec *apiKeyRecord) {
	if err := events.Publish("v1api", v1.Event{Type: "Request",
		Request: &v1.RequestEvent{
			UserId:       apiRec.UserID,
			Namespace:    apiRec.Namespace,
			ApiKeyId:     apiRec.ID,
			Url:          reqURL,
			ApiName:      apiName,
			EndpointName: endpointName,
		}}); err != nil {
		log.Errorf("Error publishing event %s", err)
	}
}

func (v1 *V1) listAPIs() ([]string, error) {
	rsp, err := v1.papi.List(context.Background(), &publicapi.ListRequest{}, client.WithAuthToken())
	if err != nil {
		return nil, err
	}

	ret := make([]string, len(rsp.Apis))
	for i, v := range rsp.Apis {
		ret[i] = v.Name
	}
	return ret, nil
}

func (v1 *V1) DeleteCustomer(ctx context.Context, request *v1.DeleteCustomerRequest, response *v1.DeleteCustomerResponse) error {
	if _, err := m3oauth.VerifyMicroAdmin(ctx, "v1.DeleteCustomer"); err != nil {
		return err
	}

	if len(request.Id) == 0 {
		return errors.BadRequest("v1.DeleteCustomer", "Missing ID")
	}

	if err := v1.deleteCustomer(ctx, request.Id); err != nil {
		log.Errorf("Error deleting customer %s", err)
		return err
	}
	return nil
}

func (v1 *V1) deleteCustomer(ctx context.Context, userID string) error {
	// delete all their keys
	keys, err := listKeysForUser("micro", userID)
	if err != nil && err != store.ErrNotFound {
		return err
	}
	for _, k := range keys {
		if err := v1.deleteKey(ctx, k); err != nil {
			logger.Errorf("Error deleting key %s", err)
			return err
		}
	}
	return nil
}
