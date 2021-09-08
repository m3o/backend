package handler

import (
	"context"

	pb "github.com/m3o/services/mailchimp/proto"
	"github.com/m3o/services/pkg/auth"
	"github.com/micro/micro/v3/service"
	"github.com/micro/micro/v3/service/config"
	"github.com/micro/micro/v3/service/logger"
)

type Mailchimp struct {
	cfg *mailchimpConfig
}

type mailchimpConfig struct {
	MainListID string   `json:"main_list_id"`
	ApiKey     string   `json:"api_key"`
	BlockList  []string `json:"block_list"`
}

func New(serv *service.Service) *Mailchimp {
	val, err := config.Get("micro.mailchimp")
	if err != nil {
		logger.Fatalf("Error reading config %s", err)
	}
	cfg := &mailchimpConfig{}
	if err := val.Scan(cfg); err != nil {
		logger.Fatalf("Error reading config %s", err)
	}

	mc := &Mailchimp{
		cfg: cfg,
	}
	mc.consumeEvents()
	return mc
}

func (m *Mailchimp) AddCustomer(ctx context.Context, request *pb.AddCustomerRequest, response *pb.AddCustomerResponse) error {
	if _, err := auth.VerifyMicroAdmin(ctx, "mailchimp.AddCustomer"); err != nil {
		return err
	}
	return m.addCustomer(request.Email)
}

func (m *Mailchimp) DeleteCustomer(ctx context.Context, request *pb.DeleteCustomerRequest, response *pb.DeleteCustomerResponse) error {
	if _, err := auth.VerifyMicroAdmin(ctx, "mailchimp.DeleteCustomer"); err != nil {
		return err
	}
	return m.deleteCustomer(request.Email)
}
