package handler

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	pevents "github.com/m3o/services/pkg/events"
	"github.com/m3o/services/pkg/events/proto/customers"
	mevents "github.com/micro/micro/v3/service/events"
	"github.com/micro/micro/v3/service/logger"
)

const (
	mailchimpURL = "https://us8.api.mailchimp.com/3.0"
)

func (m *Mailchimp) consumeEvents() {
	go pevents.ProcessTopic(customers.Topic, "mailchimp", m.processCustomerEvents)
}

func (m *Mailchimp) processCustomerEvents(ev mevents.Event) error {
	ctx := context.Background()
	ve := &customers.Event{}
	if err := json.Unmarshal(ev.Payload, ve); err != nil {
		logger.Errorf("Error unmarshalling customer event: $s", err)
		return nil
	}
	switch ve.Type {
	case customers.EventType_EventTypeSignup:
		m.processCustomerSignup(ctx, ve)
	case customers.EventType_EventTypeDeleted:
		m.processCustomerDelete(ctx, ve)
	default:
		logger.Infof("Skipped event %+v", ve)

	}
	return nil

}

type MailchimpMember struct {
	Email  string   `json:"email_address"`
	Status string   `json:"status"`
	Tags   []string `json:"tags"`
}

func (m *Mailchimp) processCustomerSignup(ctx context.Context, ev *customers.Event) error {
	return m.addCustomer(ev.Customer.Email)
}

func (m *Mailchimp) addCustomer(email string) error {
	// sign them up to mailchimp
	// 'https://${dc}.api.mailchimp.com/3.0/lists/{list_id}/members?skip_merge_validation=<SOME_BOOLEAN_VALUE>' \
	//  --user "anystring:${apikey}"' \
	//  -d '{"email_address":"","email_type":"","status":"subscribed","merge_fields":{},"interests":{},"language":"","vip":false,"location":{"latitude":0,"longitude":0},"marketing_permissions":[],"ip_signup":"","timestamp_signup":"","ip_opt":"","timestamp_opt":"","tags":[]}'
	for _, v := range m.cfg.BlockList {
		if strings.EqualFold(v, email) {
			return nil
		}
	}
	mm := newMailchimpMember(email)
	b, _ := json.Marshal(mm)
	req, err := http.NewRequest("POST", mailchimpURL+"/lists/"+m.cfg.MainListID+"/members", bytes.NewBuffer(b))
	if err != nil {
		logger.Errorf("Error creating request %s", err)
		return err
	}
	req.SetBasicAuth("anystring", m.cfg.ApiKey)
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Errorf("Error POSTing to mailchimp %s", err)
		return err
	}
	defer rsp.Body.Close()
	return nil
}

func newMailchimpMember(email string) *MailchimpMember {
	return &MailchimpMember{
		Email:  email,
		Status: "subscribed",
		Tags:   []string{"customer"},
	}
}

func (m *Mailchimp) processCustomerDelete(ctx context.Context, ev *customers.Event) error {
	return m.deleteCustomer(ev.Customer.Email)
}

func (m *Mailchimp) deleteCustomer(email string) error {
	// delete from mailchimp
	// curl -X POST \
	//  https://us8.api.mailchimp.com/3.0/lists/{list_id}/members/{subscriber_hash}/actions/delete-permanent \
	//  --user "anystring:${apikey}"'
	hash := md5.Sum([]byte(strings.ToLower(email)))
	req, err := http.NewRequest("POST", mailchimpURL+"/lists/"+m.cfg.MainListID+"/members/"+fmt.Sprintf("%x", hash)+"/actions/delete-permanent", nil)
	if err != nil {
		logger.Errorf("Error creating request %s", err)
		return err
	}
	req.SetBasicAuth("anystring", m.cfg.ApiKey)
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Errorf("Error POSTing to mailchimp %s", err)
		return err
	}
	defer rsp.Body.Close()

	return nil
}
