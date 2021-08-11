package handler

import (
	"context"
	"encoding/json"

	pb "github.com/m3o/services/balance/proto"
	cpb "github.com/m3o/services/customers/proto"
	pevents "github.com/m3o/services/pkg/events"
	stripepb "github.com/m3o/services/stripe/proto"
	v1 "github.com/m3o/services/v1/proto"
	"github.com/micro/micro/v3/service/client"
	"github.com/micro/micro/v3/service/events"
	mevents "github.com/micro/micro/v3/service/events"
	"github.com/micro/micro/v3/service/logger"
)

const (
	msgInsufficientFunds = "Insufficient funds"
)

func (b *Balance) consumeEvents() {
	go pevents.ProcessTopic("v1api", "balance", b.processV1apiEvents)
	go pevents.ProcessTopic("stripe", "balance", b.processStripeEvents)
	go pevents.ProcessTopic("customers", "balance", b.processCustomerEvents)
}

func (b *Balance) processV1apiEvents(ev mevents.Event) error {
	ctx := context.Background()
	ve := &v1.Event{}
	if err := json.Unmarshal(ev.Payload, ve); err != nil {
		logger.Errorf("Error unmarshalling v1 event: $s", err)
		return nil
	}
	switch ve.Type {
	case "APIKeyCreate":
		if err := b.processAPIKeyCreated(ctx, ve.ApiKeyCreate); err != nil {
			logger.Errorf("Error processing API key created event %s", err)
			return err
		}
	case "Request":
		if err := b.processRequest(ctx, ve.Request); err != nil {
			logger.Errorf("Error processing request event %s", err)
			return err
		}
	default:
		logger.Infof("Skipped event %+v", ve)

	}
	return nil

}

func (b *Balance) processAPIKeyCreated(ctx context.Context, ac *v1.APIKeyCreateEvent) error {
	return nil
}

func (b *Balance) processRequest(ctx context.Context, rqe *v1.RequestEvent) error {
	apiName := rqe.ApiName
	rsp, err := b.pubSvc.get(ctx, apiName)
	if err != nil {
		logger.Errorf("Error looking up API %s", err)
		return err
	}

	methodName := rqe.EndpointName
	price, ok := rsp.Pricing[methodName]
	if !ok {
		logger.Warnf("Failed to find price for api call %s:%s", apiName, methodName)
		return nil
	}
	// decrement the balance
	currBal, err := b.c.decr(ctx, rqe.UserId, "$balance$", price)
	if err != nil {
		return err
	}

	if currBal > 0 {
		return nil
	}

	evt := pb.Event{
		Type:       pb.EventType_EventTypeZeroBalance,
		CustomerId: rqe.UserId,
	}
	if err := events.Publish(pb.EventsTopic, &evt); err != nil {
		logger.Errorf("Error publishing event %+v", evt)
	}

	return nil
}

func (b *Balance) processStripeEvents(ev mevents.Event) error {
	ctx := context.Background()
	ve := &stripepb.Event{}
	logger.Infof("Processing event %+v", ev)
	if err := json.Unmarshal(ev.Payload, ve); err != nil {
		logger.Errorf("Error unmarshalling stripe event: $s", err)
		return nil
	}
	switch ve.Type {
	case "ChargeSucceeded":
		if err := b.processChargeSucceeded(ctx, ve.ChargeSucceeded); err != nil {
			logger.Errorf("Error processing charge succeeded event %s", err)
			return err
		}
	default:
		logger.Infof("Unrecognised event %+v", ve)

	}
	return nil

}

func (b *Balance) processChargeSucceeded(ctx context.Context, ev *stripepb.ChargeSuceededEvent) error {
	// TODO if we return error and we have already incremented the counter then we double count so make this idempotent
	// safety first
	if ev == nil || ev.Amount == 0 {
		return nil
	}
	// add to balance
	// stripe event is in cents, multiply by 10000 to get the fraction that balance represents
	_, err := b.c.incr(ctx, ev.CustomerId, "$balance$", ev.Amount*10000)
	if err != nil {
		logger.Errorf("Error incrementing balance %s", err)
	}

	srsp, err := b.stripeSvc.GetPayment(ctx, &stripepb.GetPaymentRequest{Id: ev.ChargeId}, client.WithAuthToken())
	if err != nil {
		return err
	}

	adj, err := storeAdjustment(ev.CustomerId, ev.Amount*10000, ev.CustomerId, "Funds added", true, map[string]string{
		"receipt_url": srsp.Payment.ReceiptUrl,
	})
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

	return nil
}

func (b *Balance) processCustomerEvents(ev mevents.Event) error {
	ctx := context.Background()
	ce := &cpb.Event{}
	if err := json.Unmarshal(ev.Payload, ce); err != nil {
		logger.Errorf("Error unmarshalling customer event: $s", err)
		return nil
	}
	switch ce.Type {
	case cpb.EventType_EventTypeDeleted:
		if err := b.processCustomerDelete(ctx, ce); err != nil {
			logger.Errorf("Error processing request event %s", err)
			return err
		}
	default:
		logger.Infof("Skipping event %+v", ce)
	}
	return nil

}

func (b *Balance) processCustomerDelete(ctx context.Context, event *cpb.Event) error {
	// delete all their balances
	if err := b.deleteCustomer(ctx, event.Customer.Id); err != nil {
		logger.Errorf("Error deleting customer %s", err)
		return err
	}
	return nil
}
