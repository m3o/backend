package handler

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	m3oauth "github.com/m3o/services/pkg/auth"
	pb "github.com/m3o/services/usage/proto"
	"github.com/micro/micro/v3/service"
	"github.com/micro/micro/v3/service/auth"
	"github.com/micro/micro/v3/service/config"
	"github.com/micro/micro/v3/service/errors"
	log "github.com/micro/micro/v3/service/logger"
	"github.com/micro/micro/v3/service/store"
	dbproto "github.com/micro/services/db/proto"
)

const (
	prefixCounter         = "usage-service/counter"
	prefixUsageByCustomer = "usageByCustomer" // customer ID / date
	counterTTL            = 48 * time.Hour
)

type counter struct {
	sync.RWMutex
	redisClient *redis.Client
}

func (c *counter) incr(ctx context.Context, userID, path string, delta int64, t time.Time) (int64, error) {
	t = t.UTC()
	key := fmt.Sprintf("%s:%s:%s:%s", prefixCounter, userID, t.Format("20060102"), path)
	pipe := c.redisClient.TxPipeline()
	incr := pipe.IncrBy(ctx, key, delta)
	pipe.Expire(ctx, key, counterTTL) // make sure we expire the counters
	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	return incr.Result()
}

func (c *counter) decr(ctx context.Context, userID, path string, delta int64, t time.Time) (int64, error) {
	t = t.UTC()
	key := fmt.Sprintf("%s:%s:%s:%s", prefixCounter, userID, t.Format("20060102"), path)
	pipe := c.redisClient.TxPipeline()
	decr := pipe.DecrBy(ctx, key, delta)
	pipe.Expire(ctx, key, counterTTL) // make sure we expire counters
	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	return decr.Result()
}

func (c *counter) read(ctx context.Context, userID, path string, t time.Time) (int64, error) {
	t = t.UTC()
	ret, err := c.redisClient.Get(ctx, fmt.Sprintf("%s:%s:%s:%s", prefixCounter, userID, t.Format("20060102"), path)).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return ret, err
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

type listEntry struct {
	Service string
	Count   int64
}

func (c *counter) listForUser(userID string, t time.Time) ([]listEntry, error) {
	ctx := context.Background()
	keyPrefix := fmt.Sprintf("%s:%s:%s:", prefixCounter, userID, t.Format("20060102"))
	sc := c.redisClient.Scan(ctx, 0, keyPrefix+"*", 0)
	if err := sc.Err(); err != nil {
		return nil, err
	}
	iter := sc.Iterator()
	res := []listEntry{}
	for {
		if !iter.Next(ctx) {
			break
		}
		key := iter.Val()
		i, err := c.redisClient.Get(ctx, key).Int64()
		if err != nil {
			return nil, err
		}
		res = append(res, listEntry{
			Service: strings.TrimPrefix(key, keyPrefix),
			Count:   i,
		})
	}
	return res, iter.Err()
}

type UsageSvc struct {
	c         *counter
	dbService dbproto.DbService
}

func NewHandler(svc *service.Service, dbService dbproto.DbService) *UsageSvc {
	redisConfig := struct {
		Address  string
		User     string
		Password string
	}{}
	val, err := config.Get("micro.usage.redis")
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
	p := &UsageSvc{
		c:         &counter{redisClient: rc},
		dbService: dbService,
	}
	go p.consumeEvents()
	return p
}

func (p *UsageSvc) Read(ctx context.Context, request *pb.ReadRequest, response *pb.ReadResponse) error {
	acc, ok := auth.AccountFromContext(ctx)
	if !ok {
		return errors.Unauthorized("usage.Read", "Unauthorized")
	}
	if len(request.CustomerId) == 0 {
		request.CustomerId = acc.ID
	}
	if acc.ID != request.CustomerId {
		_, err := m3oauth.VerifyMicroAdmin(ctx, "usage.Read")
		if err != nil {
			return err
		}
	}

	now := time.Now().UTC().Truncate(24 * time.Hour)
	liveEntries, err := p.c.listForUser(request.CustomerId, now)
	if err != nil {
		log.Errorf("Error retrieving usage %s", err)
		return errors.InternalServerError("usage.Read", "Error retrieving usage")
	}

	response.Usage = map[string]*pb.UsageHistory{}
	// add live data on top of historical
	keyPrefix := fmt.Sprintf("%s/%s/", prefixUsageByCustomer, request.CustomerId)
	recs, err := store.Read(keyPrefix, store.ReadPrefix())
	if err != nil {
		log.Errorf("Error querying historical data %s", err)
		return errors.InternalServerError("usage.Read", "Error retrieving usage")
	}

	addEntryToResponse := func(response *pb.ReadResponse, e listEntry, unixTime int64) {
		// detailed view includes data for individual endpoints
		if !request.Detail && strings.Contains(e.Service, "$") {
			return
		}
		use := response.Usage[e.Service]
		if use == nil {
			use = &pb.UsageHistory{
				ApiName: e.Service,
				Records: []*pb.UsageRecord{},
			}
		}
		use.Records = append(use.Records, &pb.UsageRecord{Date: unixTime, Requests: e.Count})
		response.Usage[e.Service] = use
	}

	// add to slices
	for _, rec := range recs {
		date := strings.TrimPrefix(rec.Key, keyPrefix)
		dateObj, err := time.Parse("20060102", date)
		if err != nil {
			log.Errorf("Error parsing date obj %s", err)
			return errors.InternalServerError("usage.Read", "Error retrieving usage")
		}
		var de dateEntry
		if err := json.Unmarshal(rec.Value, &de); err != nil {
			log.Errorf("Error parsing date obj %s", err)
			return errors.InternalServerError("usage.Read", "Error retrieving usage")
		}
		for _, e := range de.Entries {
			addEntryToResponse(response, e, dateObj.Unix())
		}
	}
	for _, e := range liveEntries {
		addEntryToResponse(response, e, now.Unix())
	}
	// sort slices
	for _, v := range response.Usage {
		sort.Slice(v.Records, func(i, j int) bool {
			if v.Records[i].Date == v.Records[j].Date {
				return v.Records[i].Requests < v.Records[j].Requests
			}
			return v.Records[i].Date < v.Records[j].Date
		})
	}
	// remove dupe
	for k, v := range response.Usage {
		lenRecs := len(v.Records)
		if lenRecs < 2 {
			continue
		}
		if v.Records[lenRecs-2].Date != v.Records[lenRecs-1].Date {
			continue
		}
		response.Usage[k].Records = append(v.Records[:lenRecs-2], v.Records[lenRecs-1])

	}
	return nil
}

type dateEntry struct {
	Entries []listEntry
}

func (p *UsageSvc) UsageCron() {
	defer func() {
		log.Infof("Usage sweep ended")
	}()
	log.Infof("Performing usage sweep")
	// loop through counters and persist
	ctx := context.Background()
	sc := p.c.redisClient.Scan(ctx, 0, prefixCounter+":*", 0)
	if err := sc.Err(); err != nil {
		log.Errorf("Error running redis scan %s", err)
		return
	}

	toPersist := map[string]map[string][]listEntry{} // userid->date->[]listEntry
	it := sc.Iterator()
	for {
		if !it.Next(ctx) {
			if err := it.Err(); err != nil {
				log.Errorf("Error during iteration %s", err)
			}
			break
		}

		key := it.Val()
		count, err := p.c.redisClient.Get(ctx, key).Int64()
		if err != nil {
			log.Errorf("Error retrieving value %s", err)
			return
		}
		parts := strings.Split(strings.TrimPrefix(key, prefixCounter+":"), ":")
		if len(parts) < 3 {
			log.Errorf("Unexpected number of components in key %s", key)
			continue
		}
		userID := parts[0]
		date := parts[1]
		service := parts[2]
		dates := toPersist[userID]
		if dates == nil {
			dates = map[string][]listEntry{}
			toPersist[userID] = dates
		}
		entries := dates[date]
		if entries == nil {
			entries = []listEntry{}
		}
		entries = append(entries, listEntry{
			Service: service,
			Count:   count,
		})
		dates[date] = entries
	}

	for userID, v := range toPersist {
		for date, entry := range v {
			de := dateEntry{
				Entries: entry,
			}
			b, err := json.Marshal(de)
			if err != nil {
				log.Errorf("Error marshalling entry %s", err)
				return
			}
			store.Write(&store.Record{
				Key:   fmt.Sprintf("%s/%s/%s", prefixUsageByCustomer, userID, date),
				Value: b,
			})
		}

	}

}

func (p *UsageSvc) Sweep(ctx context.Context, request *pb.SweepRequest, response *pb.SweepResponse) error {
	p.UsageCron()
	return nil
}

func (p *UsageSvc) deleteUser(ctx context.Context, userID string) error {
	if err := p.c.deleteUser(ctx, userID); err != nil {
		return err
	}

	recs, err := store.Read(fmt.Sprintf("%s/%s/", prefixUsageByCustomer, userID), store.ReadPrefix())
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

func (p *UsageSvc) DeleteCustomer(ctx context.Context, request *pb.DeleteCustomerRequest, response *pb.DeleteCustomerResponse) error {
	if _, err := m3oauth.VerifyMicroAdmin(ctx, "usage.DeleteCustomer"); err != nil {
		return err
	}

	if len(request.Id) == 0 {
		return errors.BadRequest("usage.DeleteCustomer", "Error deleting customer")
	}

	if err := p.deleteUser(ctx, request.Id); err != nil {
		log.Errorf("Error deleting customer %s", err)
		return err
	}
	return nil
}

func (p *UsageSvc) SaveEvent(ctx context.Context, request *pb.SaveEventRequest, response *pb.SaveEventResponse) error {
	if request.Event == nil {
		return fmt.Errorf("event not provided")
	}
	if request.Event.Table == "" {
		return fmt.Errorf("table not provided")
	}
	rec := request.Event.Record.AsMap()
	if request.Event.Id == "" {
		request.Event.Id = uuid.New().String()
	}
	rec["id"] = request.Event.Id
	rec["createdAt"] = time.Now().Unix()
	bs, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bs, request.Event.Record)
	if err != nil {
		return err
	}
	_, err = p.dbService.Create(ctx, &dbproto.CreateRequest{
		Table:  request.Event.Table,
		Record: request.Event.Record,
	})
	return err
}

func (p *UsageSvc) ListEvents(ctx context.Context, request *pb.ListEventsRequest, response *pb.ListEventsResponse) error {
	if request.Table == "" {
		return fmt.Errorf("no table provided")
	}
	resp, err := p.dbService.Read(ctx, &dbproto.ReadRequest{
		Table:   request.Table,
		Query:   "createdAt > 0",
		OrderBy: "createdAt",
		Order:   "desc",
	})
	if err != nil {
		return err
	}
	for _, v := range resp.Records {
		response.Events = append(response.Events, &pb.Event{
			Table:  request.Table,
			Record: v,
		})
	}
	return nil
}
