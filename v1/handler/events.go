package handler

import (
	"context"
	"encoding/json"

	cpb "github.com/m3o/services/customers/proto"
	pevents "github.com/m3o/services/pkg/events"
	mevents "github.com/micro/micro/v3/service/events"
	"github.com/micro/micro/v3/service/logger"
)

func (v1 *V1) consumeEvents() {
	go pevents.ProcessTopic("customers", "v1api", v1.processCustomerEvents)
}

func (v1 *V1) processCustomerEvents(ev mevents.Event) error {
	ctx := context.Background()
	ce := &cpb.Event{}
	if err := json.Unmarshal(ev.Payload, ce); err != nil {
		logger.Errorf("Error unmarshalling customer event: $s", err)
		return nil
	}
	switch ce.Type {
	case cpb.EventType_EventTypeDeleted:
		if err := v1.processCustomerDelete(ctx, ce); err != nil {
			logger.Errorf("Error processing customer delete event %s", err)
			return err
		}
	case cpb.EventType_EventTypeBanned:
		if err := v1.processCustomerBan(ctx, ce); err != nil {
			logger.Errorf("Error processing customer banned event %s", err)
			return err
		}
	case cpb.EventType_EventTypeUnbanned:
		if err := v1.processCustomerUnban(ctx, ce); err != nil {
			logger.Errorf("Error processing customer unbanned event %s", err)
			return err
		}
	default:
		logger.Infof("Skipping event %+v", ce)
	}
	return nil

}

func (v1 *V1) processCustomerDelete(ctx context.Context, event *cpb.Event) error {
	return v1.deleteCustomer(ctx, event.Customer.Id)
}

func (v1 *V1) processCustomerBan(ctx context.Context, event *cpb.Event) error {
	// disable all their keys
	return v1.updateKeyStatus(ctx, "v1.ban", "micro", event.Customer.Id, "", keyStatusBlocked, "User has been banned")

}

func (v1 *V1) processCustomerUnban(ctx context.Context, event *cpb.Event) error {
	return v1.updateKeyStatus(ctx, "v1.unban", "micro", event.Customer.Id, "", keyStatusActive, "")

}
