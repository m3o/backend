package handler

import (
	"context"
	"encoding/json"
	"log"

	balance "github.com/m3o/services/balance/proto"
	onboarding "github.com/m3o/services/onboarding/proto"
	pevents "github.com/m3o/services/pkg/events"
	"github.com/micro/micro/v3/service"
	"github.com/micro/micro/v3/service/client"
	"github.com/micro/micro/v3/service/config"
	mevents "github.com/micro/micro/v3/service/events"
	logger "github.com/micro/micro/v3/service/logger"
)

type Onboarding struct {
	balSvc       balance.BalanceService
	promoCredit  int64
	promoMessage string
}

func NewOnboarding(svc *service.Service) *Onboarding {
	cfg := conf{}
	v, err := config.Get("micro.onboarding")
	if err != nil {
		log.Fatalf("Failed to load config %s", err)
	}
	if err := v.Scan(&cfg); err != nil {
		log.Fatalf("Failed to load config %s", err)
	}
	if cfg.PromoCredit == 0 || len(cfg.PromoMessage) == 0 {
		log.Fatalf("Missing config")
	}
	o := &Onboarding{
		balSvc:       balance.NewBalanceService("balance", svc.Client()),
		promoCredit:  cfg.PromoCredit,
		promoMessage: cfg.PromoMessage,
	}
	o.consumeEvents()
	return o
}

func (o *Onboarding) consumeEvents() {
	go pevents.ProcessTopic(topic, "onboarding", o.processOnboardingEvents)
}

func (o *Onboarding) processOnboardingEvents(ev mevents.Event) error {
	var ve onboarding.Event
	if err := json.Unmarshal(ev.Payload, &ve); err != nil {
		logger.Errorf("Error unmarshalling onboarding event: $s", err)
		return nil
	}
	switch ve.Type {
	case "XXNOTUSEDXX": // TODO reinstate some signup related processing
		if err := o.processSignup(ve.NewSignup); err != nil {
			logger.Errorf("Error processing signup event %s", err)
			return err
		}
	default:
		logger.Infof("Skipping event %+v", ve)
	}
	return nil

}

func (o *Onboarding) processSignup(ev *onboarding.NewSignupEvent) error {
	// add a promo credit to their balance
	_, err := o.balSvc.Increment(context.Background(), &balance.IncrementRequest{
		CustomerId: ev.Id,
		Delta:      o.promoCredit,
		Visible:    true,
		Reference:  o.promoMessage,
	}, client.WithAuthToken())
	return err
}
