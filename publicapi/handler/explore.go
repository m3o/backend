package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "github.com/m3o/services/publicapi/proto"
	"github.com/micro/micro/v3/service"
	"github.com/micro/micro/v3/service/errors"
	log "github.com/micro/micro/v3/service/logger"
	logger "github.com/micro/micro/v3/service/logger"
	model "github.com/micro/micro/v3/service/model"
	"github.com/micro/micro/v3/service/registry"
	"github.com/micro/micro/v3/service/store"
)

type Explore struct {
	sync.RWMutex
	apiCache    []*API
	regCache    map[string]*registry.Service
	lastUpdated time.Time
	trackSearch model.Model
}

type SearchCount struct {
	// search term itself
	Id    string `json:"id"`
	Count int64  `json:"count"`
}

// The API consisting of the public api def and the summary explore api info
type API struct {
	PublicAPI  *pb.PublicAPI
	ExploreAPI *pb.ExploreAPI
}

func marshalExploreAPI(ae *APIEntry, svc *registry.Service) *pb.ExploreAPI {
	eps := []*pb.Endpoint{}

	for _, ep := range svc.Endpoints {
		eps = append(eps, &pb.Endpoint{
			Name: ep.Name,
		})
	}

	return &pb.ExploreAPI{
		Name:        ae.Name,
		Description: ae.Description,
		Category:    ae.Category,
		Icon:        ae.Icon,
		Endpoints:   eps,
	}
}

func NewExploreAPIHandler(srv *service.Service) *Explore {
	e := &Explore{
		trackSearch: model.New(SearchCount{}, &model.Options{
			Key: "id",
		}),
	}
	// start the cache
	go e.runCache()
	return e
}

func (e *Explore) runCache() {
	// force list/cache
	if _, err := e.loadCache(); err != nil {
		log.Error("Failed to write cache: %v", err)
	}

	t := time.NewTicker(time.Minute)

	for _ = range t.C {
		if _, err := e.loadCache(); err != nil {
			log.Error("Failed to write cache: %v", err)
		}
	}
}

func (e *Explore) loadCache() ([]*API, error) {
	// get the current list of services
	svcs, err := registry.ListServices()
	if err != nil {
		return nil, err
	}

	regCache := map[string]*registry.Service{}
	apiCache := []*API{}

	// create a registry cache
	for _, s := range svcs {
		regCache[s.Name] = s
	}

	// read api entries from the store
	recs, err := store.Read(fmt.Sprintf(prefixName, ""), store.ReadPrefix())
	if err != nil {
		log.Errorf("Error reading from store %s", err)
		return nil, err
	}

	for _, v := range recs {
		var ae APIEntry
		if err := json.Unmarshal(v.Value, &ae); err != nil {
			return nil, err
		}

		// check against the registry
		svc, ok := e.regCache[ae.Name]
		if !ok {
			continue
		}

		apiCache = append(apiCache, &API{
			PublicAPI:  marshal(&ae),
			ExploreAPI: marshalExploreAPI(&ae, svc),
		})
	}

	e.Lock()
	e.regCache = regCache
	e.lastUpdated = time.Now()
	e.apiCache = apiCache
	e.Unlock()

	// return the api cache
	return apiCache, nil
}

// Index returns a summary description of all the apis
func (e *Explore) Index(ctx context.Context, request *pb.IndexRequest, response *pb.IndexResponse) error {
	apis, err := e.exploreList()
	if err != nil {
		return err
	}
	end := len(apis)

	if request.Limit > 0 {
		maxEnd := int(request.Limit + request.Offset)
		if maxEnd < end {
			end = maxEnd
		}
	}

	// offset surpasses the actual end of the slice
	if request.Offset > int64(end) {
		return nil
	}

	for _, api := range apis[request.Offset:end] {
		// return the explore api definition
		response.Apis = append(response.Apis, api.ExploreAPI)
	}

	return nil
}

func (e *Explore) exploreList() ([]*API, error) {
	e.RLock()
	cache := e.apiCache
	e.RUnlock()

	if len(cache) > 0 {
		return cache, nil
	}

	return e.loadCache()
}

func (e *Explore) recordSearch(searchTerm string) error {
	searchTerm = strings.ToLower(searchTerm)
	searchTerm = strings.Replace(searchTerm, " ", "-", -1)

	if searchTerm == "" {
		return fmt.Errorf("no search term to track")
	}
	oldTrack := []*SearchCount{}
	err := e.trackSearch.Read(model.QueryEquals("id", searchTerm), &oldTrack)
	if err != nil {
		return err
	}
	if len(oldTrack) == 0 {
		return e.trackSearch.Create(SearchCount{
			Id:    searchTerm,
			Count: 1,
		})
	}
	tr := oldTrack[0]
	tr.Count += 1
	return e.trackSearch.Update(tr)
}

// Search returns APIs based on the given search term
func (e *Explore) Search(ctx context.Context, request *pb.SearchRequest, response *pb.SearchResponse) error {
	go func() {
		if request.SearchTerm == "" {
			return
		}
		err := e.recordSearch(request.SearchTerm)
		if err != nil {
			logger.Error(err)
		}
	}()
	list, err := e.exploreList()
	if err != nil {
		return err
	}

	if request.SearchTerm == "" {
		for _, api := range list {
			if len(request.Category) > 0 && request.Category != api.ExploreAPI.Category {
				continue
			}
			response.Apis = append(response.Apis, api.ExploreAPI)
		}
		return nil
	}

	// lowercase the search term
	request.SearchTerm = strings.ToLower(request.SearchTerm)

	// Very rudimentary search result ranking
	// prioritize name and endoint name matches
	matchedName := []*pb.ExploreAPI{}
	matchedEndpoint := []*pb.ExploreAPI{}
	matchedOther := []*pb.ExploreAPI{}

	for _, api := range list {
		if len(request.Category) > 0 && request.Category != api.ExploreAPI.Category {
			continue
		}

		if strings.Contains(strings.ToLower(api.ExploreAPI.Name), request.SearchTerm) {
			matchedName = append(matchedName, api.ExploreAPI)
			continue
		}
		found := false
		for _, ep := range api.ExploreAPI.Endpoints {
			if strings.Contains(strings.ToLower(ep.Name), request.SearchTerm) {
				matchedEndpoint = append(matchedEndpoint, api.ExploreAPI)
				found = true
				break
			}
		}
		if found {
			continue
		}
		if strings.Contains(strings.ToLower(api.ExploreAPI.Description), request.SearchTerm) {
			matchedOther = append(matchedOther, api.ExploreAPI)
			continue
		}

	}
	response.Apis = append(response.Apis, matchedName...)
	response.Apis = append(response.Apis, matchedEndpoint...)
	response.Apis = append(response.Apis, matchedOther...)
	return nil
}

// API returns a single named API entry
func (e *Explore) API(ctx context.Context, request *pb.APIRequest, response *pb.APIResponse) error {
	if len(request.Name) == 0 {
		return errors.BadRequest("explore.API", "missing name")
	}

	list, err := e.exploreList()
	if err != nil {
		return err
	}

	// check the cache
	for _, api := range list {
		if api.ExploreAPI.Name == request.Name {
			response.Api = api.PublicAPI
			response.Summary = api.ExploreAPI
			return nil
		}
	}

	return errors.NotFound("explore.API", "not found")

}
