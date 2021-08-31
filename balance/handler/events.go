package handler

import (
	"context"
	"encoding/json"

	pevents "github.com/m3o/services/pkg/events"
	eventspb "github.com/m3o/services/pkg/events/proto/customers"
	"github.com/m3o/services/pkg/events/proto/requests"
	stripeevents "github.com/m3o/services/pkg/events/proto/stripe"
	stripepb "github.com/m3o/services/stripe/proto"
	"github.com/micro/micro/v3/service/client"
	"github.com/micro/micro/v3/service/events"
	mevents "github.com/micro/micro/v3/service/events"
	"github.com/micro/micro/v3/service/logger"
)

func (b *Balance) consumeEvents() {
	go pevents.ProcessTopic(requests.Topic, "balance", b.processV1apiEvents)
	go pevents.ProcessTopic(stripeevents.Topic, "balance", b.processStripeEvents)
	go pevents.ProcessTopic(eventspb.Topic, "balance", b.processCustomerEvents)
}

func (b *Balance) processV1apiEvents(ev mevents.Event) error {
	ctx := context.Background()
	ve := &requests.Event{}
	if err := json.Unmarshal(ev.Payload, ve); err != nil {
		logger.Errorf("Error unmarshalling v1 event: $s", err)
		return nil
	}
	switch ve.Type {
	case requests.EventType_EventTypeRequest:
		if err := b.processRequest(ctx, ve.Request); err != nil {
			logger.Errorf("Error processing request event %s", err)
			return err
		}
	default:
		logger.Infof("Skipped event %+v", ve)

	}
	return nil

}

func (b *Balance) processRequest(ctx context.Context, rqe *requests.Request) error {
	apiName := rqe.ApiName
	rsp, err := b.pubSvc.get(ctx, apiName)
	if err != nil {
		logger.Errorf("Error looking up API %s", err)
		return err
	}

	methodName := rqe.EndpointName
	price, ok := rsp.Pricing[methodName]
	if !ok || price == 0 {
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

	evt := &eventspb.Event{
		Type: eventspb.EventType_EventTypeBalanceZero,
		Customer: &eventspb.Customer{
			Id: rqe.UserId,
		},
	}
	if err := events.Publish(eventspb.Topic, evt); err != nil {
		logger.Errorf("Error publishing event %+v", evt)
	}

	return nil
}

func (b *Balance) processStripeEvents(ev mevents.Event) error {
	ctx := context.Background()
	ve := &stripeevents.Event{}
	logger.Infof("Processing event %+v", ev)
	if err := json.Unmarshal(ev.Payload, ve); err != nil {
		logger.Errorf("Error unmarshalling stripe event: $s", err)
		return nil
	}
	switch ve.Type {
	case stripeevents.EventType_EventTypeChargeSucceeded:
		if err := b.processChargeSucceeded(ctx, ve.ChargeSucceeded); err != nil {
			logger.Errorf("Error processing charge succeeded event %s", err)
			return err
		}
	default:
		logger.Infof("Skipping event %+v", ve)

	}
	return nil

}

func (b *Balance) processChargeSucceeded(ctx context.Context, ev *stripeevents.ChargeSuceeded) error {
	// safety first
	if ev == nil || ev.Amount == 0 {
		return nil
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

	// add to balance. We do this LAST in case we error doing anything else and cause a double count
	// stripe event is in cents, multiply by 10000 to get the fraction that balance represents
	_, err = b.c.incr(ctx, ev.CustomerId, "$balance$", ev.Amount*10000)
	if err != nil {
		logger.Errorf("Error incrementing balance %s", err)
	}

	evt := &eventspb.Event{
		Type: eventspb.EventType_EventTypeBalanceIncrement,
		BalanceIncrement: &eventspb.BalanceIncrement{
			Amount:    adj.Amount,
			Type:      "topup",
			Reference: adj.Reference,
		},
		Customer: &eventspb.Customer{
			Id: adj.CustomerID,
		},
	}

	if err := events.Publish(eventspb.Topic, evt); err != nil {
		logger.Errorf("Error publishing event %+v", evt)
	}

	return nil
}

func (b *Balance) processCustomerEvents(ev mevents.Event) error {
	ctx := context.Background()
	ce := &eventspb.Event{}
	if err := json.Unmarshal(ev.Payload, ce); err != nil {
		logger.Errorf("Error unmarshalling customer event: $s", err)
		return nil
	}
	switch ce.Type {
	case eventspb.EventType_EventTypeDeleted:
		if err := b.processCustomerDelete(ctx, ce); err != nil {
			logger.Errorf("Error processing request event %s", err)
			return err
		}
	default:
		logger.Infof("Skipping event %+v", ce)
	}
	return nil

}

func (b *Balance) processCustomerDelete(ctx context.Context, event *eventspb.Event) error {
	// delete all their balances
	if err := b.deleteCustomer(ctx, event.Customer.Id); err != nil {
		logger.Errorf("Error deleting customer %s", err)
		return err
	}
	return nil
}
