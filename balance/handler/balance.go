package handler

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	pb "github.com/m3o/services/balance/proto"
	ns "github.com/m3o/services/namespaces/proto"
	m3oauth "github.com/m3o/services/pkg/auth"
	publicapi "github.com/m3o/services/publicapi/proto"
	stripe "github.com/m3o/services/stripe/proto"
	v1 "github.com/m3o/services/v1/proto"
	"github.com/micro/micro/v3/service"
	"github.com/micro/micro/v3/service/client"
	"github.com/micro/micro/v3/service/config"
	"github.com/micro/micro/v3/service/errors"
	"github.com/micro/micro/v3/service/events"
	"github.com/micro/micro/v3/service/logger"
	log "github.com/micro/micro/v3/service/logger"
	"github.com/micro/micro/v3/service/store"
)

const (
	prefixCounter         = "balance-service/counter"
	microNamespace        = "micro"
	prefixStoreByCustomer = "adjByCust"
)

type counter struct {
	sync.RWMutex
	redisClient *redis.Client
}

func (c *counter) incr(ctx context.Context, userID, path string, delta int64) (int64, error) {
	return c.redisClient.IncrBy(ctx, fmt.Sprintf("%s:%s:%s", prefixCounter, userID, path), delta).Result()
}

func (c *counter) decr(ctx context.Context, userID, path string, delta int64) (int64, error) {
	return c.redisClient.DecrBy(ctx, fmt.Sprintf("%s:%s:%s", prefixCounter, userID, path), delta).Result()
}

func (c *counter) read(ctx context.Context, userID, path string) (int64, error) {
	ret, err := c.redisClient.Get(ctx, fmt.Sprintf("%s:%s:%s", prefixCounter, userID, path)).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return ret, err
}

func (c *counter) reset(ctx context.Context, userID, path string) error {
	return c.redisClient.Set(ctx, fmt.Sprintf("%s:%s:%s", prefixCounter, userID, path), 0, 0).Err()
}

func (c *counter) deleteUser(ctx context.Context, userID string) error {
	keys, err := c.redisClient.Keys(ctx, fmt.Sprintf("%s:%s:*", prefixCounter, userID)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	if err := c.redisClient.Del(ctx, keys...).Err(); err != nil && err != redis.Nil {
		return err
	}

	return nil
}

type publicAPICacheEntry struct {
	api     *publicapi.PublicAPI
	created time.Time
}

type publicAPICache struct {
	sync.RWMutex
	cache  map[string]*publicAPICacheEntry
	pubSvc publicapi.PublicapiService
	ttl    time.Duration
}

// Adjustment represents a balance adjustment (not including normal API usage). e.g. funds being added, promo codes, manual adjustment for customer service etc
type Adjustment struct {
	ID         string
	Created    time.Time
	Amount     int64  // positive is credit, negative is debit
	Reference  string // reference description
	Visible    bool   // should this be visible to the customer? If false, it only displays to admins
	CustomerID string
	ActionedBy string // who made the adjustment
	Meta       map[string]string
}

func (p *publicAPICache) get(ctx context.Context, name string) (*publicapi.PublicAPI, error) {
	// check the cache
	p.RLock()
	cached := p.cache[name]
	p.RUnlock()
	if cached != nil && cached.created.Add(p.ttl).After(time.Now()) {
		return cached.api, nil
	}
	rsp, err := p.pubSvc.Get(ctx, &publicapi.GetRequest{Name: name}, client.WithAuthToken())
	if err != nil {
		return nil, err
	}
	p.Lock()
	p.cache[name] = &publicAPICacheEntry{api: rsp.Api, created: time.Now()}
	p.Unlock()
	return rsp.Api, nil
}

type Balance struct {
	c         *counter // counts the balance. Balance is expressed in 1/10,000ths of a cent which allows us to price in fractions e.g. a request costs 0.0001 cents or 10,000 requests for 1 cent
	v1Svc     v1.V1Service
	pubSvc    *publicAPICache
	nsSvc     ns.NamespacesService
	stripeSvc stripe.StripeService
}

func NewHandler(svc *service.Service) *Balance {
	redisConfig := struct {
		Address  string
		User     string
		Password string
	}{}
	val, err := config.Get("micro.balance.redis")
	if err != nil {
		log.Fatalf("No redis config found %s", err)
	}
	if err := val.Scan(&redisConfig); err != nil {
		log.Fatalf("Error parsing redis config %s", err)
	}
	if len(redisConfig.Password) == 0 || len(redisConfig.User) == 0 || len(redisConfig.Password) == 0 {
		log.Fatalf("Missing redis config %s", err)
	}
	rc := redis.NewClient(&redis.Options{
		Addr:     redisConfig.Address,
		Username: redisConfig.User,
		Password: redisConfig.Password,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: false,
		},
	})
	b := &Balance{
		c:     &counter{redisClient: rc},
		v1Svc: v1.NewV1Service("v1", svc.Client()),
		pubSvc: &publicAPICache{
			pubSvc: publicapi.NewPublicapiService("publicapi", svc.Client()),
			cache:  map[string]*publicAPICacheEntry{},
			ttl:    5 * time.Minute,
		},
		nsSvc:     ns.NewNamespacesService("namespaces", svc.Client()),
		stripeSvc: stripe.NewStripeService("stripe", svc.Client()),
	}
	go b.consumeEvents()
	return b
}

func (b Balance) Increment(ctx context.Context, request *pb.IncrementRequest, response *pb.IncrementResponse) error {
	// increment counter
	acc, err := m3oauth.VerifyMicroAdmin(ctx, "balance.Increment")
	if err != nil {
		return err
	}
	if len(request.Reference) == 0 {
		return errors.BadRequest("balance.Increment", "Missing reference")
	}

	// TODO idempotency
	// increment the balance
	currBal, err := b.c.incr(ctx, request.CustomerId, "$balance$", request.Delta)
	if err != nil {
		return err
	}
	response.NewBalance = currBal
	adj, err := storeAdjustment(acc.ID, request.Delta, request.CustomerId, request.Reference, request.Visible, nil)
	if err != nil {
		return err
	}

	evt := pb.Event{
		Type: pb.EventType_EventTypeIncrement,
		Adjustment: &pb.Adjustment{
			Id:        adj.ID,
			Created:   adj.Created.Unix(),
			Delta:     adj.Amount,
			Reference: adj.Reference,
			Meta:      adj.Meta,
		},
		CustomerId: adj.CustomerID,
	}
	if err := events.Publish(pb.EventsTopic, &evt); err != nil {
		logger.Errorf("Error publishing event %+v", evt)
	}

	if currBal < 0 {
		return nil
	}

	return nil
}

func storeAdjustment(actionedBy string, delta int64, customerID, reference string, visible bool, meta map[string]string) (*Adjustment, error) {

	// record it
	rec := &Adjustment{
		ID:         uuid.New().String(),
		Created:    time.Now(),
		Amount:     delta,
		Reference:  reference,
		Visible:    visible,
		CustomerID: customerID,
		ActionedBy: actionedBy,
		Meta:       meta,
	}
	adj, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}

	if err := store.Write(&store.Record{
		Key:   fmt.Sprintf("%s/%s/%s", prefixStoreByCustomer, customerID, rec.ID),
		Value: adj,
	}); err != nil {
		return nil, err
	}
	return rec, nil
}

func (b *Balance) Decrement(ctx context.Context, request *pb.DecrementRequest, response *pb.DecrementResponse) error {
	acc, err := m3oauth.VerifyMicroAdmin(ctx, "balance.Decrement")
	if err != nil {
		return err
	}
	if len(request.Reference) == 0 {
		return errors.BadRequest("balance.Decrement", "Missing reference")
	}
	// TODO idempotency
	// decrement the balance
	currBal, err := b.c.decr(ctx, request.CustomerId, "$balance$", request.Delta)
	if err != nil {
		return err
	}

	response.NewBalance = currBal
	adj, err := storeAdjustment(acc.ID, -request.Delta, request.CustomerId, request.Reference, request.Visible, nil)
	if err != nil {
		return err
	}

	evt := pb.Event{
		Type: pb.EventType_EventTypeDecrement,
		Adjustment: &pb.Adjustment{
			Id:        adj.ID,
			Created:   adj.Created.Unix(),
			Delta:     adj.Amount,
			Reference: adj.Reference,
			Meta:      adj.Meta,
		},
		CustomerId: adj.CustomerID,
	}
	if err := events.Publish(pb.EventsTopic, &evt); err != nil {
		logger.Errorf("Error publishing event %+v", evt)
	}

	if currBal > 0 {
		return nil
	}
	evt = pb.Event{
		Type:       pb.EventType_EventTypeZeroBalance,
		CustomerId: adj.CustomerID,
	}
	if err := events.Publish(pb.EventsTopic, &evt); err != nil {
		logger.Errorf("Error publishing event %+v", evt)
	}

	return nil
}

func (b *Balance) Current(ctx context.Context, request *pb.CurrentRequest, response *pb.CurrentResponse) error {
	acc, err := m3oauth.VerifyMicroCustomer(ctx, "balance.Current")
	if err != nil {
		return err
	}
	if len(request.CustomerId) == 0 {
		request.CustomerId = acc.ID
	}
	if acc.ID != request.CustomerId {
		// is this an admin?
		if _, err := m3oauth.VerifyMicroAdmin(ctx, "balance.Current"); err != nil {
			return err
		}
	}
	currBal, err := b.c.read(ctx, request.CustomerId, "$balance$")
	if err != nil && err != redis.Nil {
		log.Errorf("Error reading from counter %s", err)
		return errors.InternalServerError("balance.Current", "Error retrieving current balance")
	}
	response.CurrentBalance = currBal
	return nil
}

func (b *Balance) ListAdjustments(ctx context.Context, request *pb.ListAdjustmentsRequest, response *pb.ListAdjustmentsResponse) error {
	acc, err := m3oauth.VerifyMicroCustomer(ctx, "balance.ListAdjustments")
	if err != nil {
		// TODO check for micro admin
		return err
	}
	recs, err := store.Read(fmt.Sprintf("%s/%s/", prefixStoreByCustomer, acc.ID), store.ReadPrefix())
	if err != nil {
		return err
	}

	ret := []*pb.Adjustment{}
	for _, rec := range recs {
		var adj Adjustment
		if err := json.Unmarshal(rec.Value, &adj); err != nil {
			return err
		}
		if !adj.Visible {
			continue
		}
		ret = append(ret, &pb.Adjustment{
			Id:        adj.ID,
			Created:   adj.Created.Unix(),
			Delta:     adj.Amount,
			Reference: adj.Reference,
			Meta:      adj.Meta,
		})
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Created < ret[j].Created
	})
	response.Adjustments = ret
	return nil
}

func (b *Balance) deleteCustomer(ctx context.Context, userID string) error {
	if err := b.c.deleteUser(ctx, userID); err != nil {
		return err
	}
	recs, err := store.Read(fmt.Sprintf("%s/%s/", prefixStoreByCustomer, userID), store.ReadPrefix())
	if err != nil {
		return err
	}
	for _, rec := range recs {
		if err := store.Delete(rec.Key); err != nil {
			return err
		}
	}
	return nil

}

func (b *Balance) DeleteCustomer(ctx context.Context, request *pb.DeleteCustomerRequest, response *pb.DeleteCustomerResponse) error {
	if _, err := m3oauth.VerifyMicroAdmin(ctx, "balance.DeleteCustomer"); err != nil {
		return err
	}
	if len(request.UserId) == 0 {
		return errors.BadRequest("balance.DeleteCustomer", "Missing user ID")
	}

	if err := b.deleteCustomer(ctx, request.UserId); err != nil {
		logger.Errorf("Error deleting customer %s", err)
		return err
	}
	return nil
}
