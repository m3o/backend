package handler

import (
	"context"
	"encoding/json"

	customers "github.com/m3o/services/customers/proto"
	"github.com/m3o/services/pkg/events"
	customerspb "github.com/m3o/services/pkg/events/proto/customers"
	"github.com/m3o/services/pkg/events/proto/requests"
	"github.com/micro/micro/v3/service/client"
	merrors "github.com/micro/micro/v3/service/errors"
	mevents "github.com/micro/micro/v3/service/events"
	"github.com/micro/micro/v3/service/logger"
)

func (b *Mixpanel) consumeEvents() {
	go events.ProcessTopic(requests.Topic, "mixpanel", b.processV1APIEvent)
	go events.ProcessTopic(customerspb.Topic, "mixpanel", b.processCustomerEvent)
}

func (b *Mixpanel) processV1APIEvent(ev mevents.Event) error {
	ve := &requests.Event{}

	if err := json.Unmarshal(ev.Payload, ve); err != nil {
		logger.Errorf("Error unmarshalling v1 event, discarding: $s", err)
		return nil
	}

	customerID := ""
	switch ve.Type {
	case requests.EventType_EventTypeRequest:
		customerID = ve.Request.UserId
	default:
		logger.Infof("Event type for v1 not supported %s", ve.Type)
		return nil
	}

	ignore, err := b.ignoreCustomer(customerID)
	if ignore {
		return nil
	}
	if err != nil {
		return err
	}

	mEv := b.client.newMixpanelEvent(ev.Topic, ve.Type.String(), customerID, ev.ID, ev.Timestamp.Unix(), ve)
	if err := b.client.Track(mEv); err != nil {
		logger.Errorf("Error tracking event %s", err)
		return err
	}
	return nil

}

func (b *Mixpanel) ignoreCustomer(customerID string) (bool, error) {
	// should we ignore?
	rsp, err := b.custSvc.Read(context.Background(), &customers.ReadRequest{Id: customerID}, client.WithAuthToken())
	if err != nil {
		merr, ok := err.(*merrors.Error)
		if ok {
			if merr.Code == 404 || merr.Detail == "not found" {
				logger.Errorf("Failed to find customer %s", customerID)
				return true, nil
			}
		}
		logger.Errorf("Failed to read customer %s %s", customerID, err)
		return false, err
	}
	for _, i := range b.ignoreList {
		if rsp.Customer.Email == i {
			return true, nil
		}
	}
	return false, nil

}

func (b *Mixpanel) processCustomerEvent(ev mevents.Event) error {
	ve := &customerspb.Event{}
	if err := json.Unmarshal(ev.Payload, ve); err != nil {
		logger.Errorf("Error unmarshalling balance event, discarding: $s", err)
		return nil
	}
	customerID := ve.Customer.Id
	if len(ve.Customer.Email) == 0 {
		rsp, err := b.custSvc.Read(context.Background(), &customers.ReadRequest{Id: customerID}, client.WithAuthToken())
		if err != nil {
			merr, ok := err.(*merrors.Error)
			if ok {
				if merr.Code == 404 || merr.Detail == "not found" {
					logger.Warnf("Failed to find customer %s", customerID)
					return nil
				}
			}
			logger.Errorf("Failed to read customer %s %s", customerID, err)
			return err
		}
		ve.Customer.Email = rsp.Customer.Email
		ve.Customer.Status = rsp.Customer.Status
		ve.Customer.Created = rsp.Customer.Created
		ve.Customer.Updated = rsp.Customer.Updated

	}
	for _, i := range b.ignoreList {
		if ve.Customer.Email == i {
			return nil
		}
	}

	mEv := b.client.newMixpanelEvent(ev.Topic, ve.Type.String(), customerID, ev.ID, ev.Timestamp.Unix(), ve)
	if err := b.client.Track(mEv); err != nil {
		logger.Errorf("Error tracking event %s", err)
		return err
	}

	return nil
}
