package handler

import (
	"log"

	balance "github.com/m3o/services/balance/proto"
	"github.com/micro/micro/v3/service"
	"github.com/micro/micro/v3/service/config"
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
	return o
}
