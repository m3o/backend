package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	custpb "github.com/m3o/services/customers/proto"
	m3oauth "github.com/m3o/services/pkg/auth"
	custevents "github.com/m3o/services/pkg/events/proto/customers"
	stripeevents "github.com/m3o/services/pkg/events/proto/stripe"
	stripepb "github.com/m3o/services/stripe/proto"
	api "github.com/micro/micro/v3/proto/api"
	"github.com/micro/micro/v3/service"
	"github.com/micro/micro/v3/service/auth"
	"github.com/micro/micro/v3/service/client"
	"github.com/micro/micro/v3/service/config"
	"github.com/micro/micro/v3/service/context/metadata"
	"github.com/micro/micro/v3/service/errors"
	"github.com/micro/micro/v3/service/events"
	"github.com/micro/micro/v3/service/logger"
	log "github.com/micro/micro/v3/service/logger"
	"github.com/micro/micro/v3/service/store"
	"github.com/stripe/stripe-go/v71/webhook"

	"github.com/stripe/stripe-go/v71"
	stripeclient "github.com/stripe/stripe-go/v71/client"
)

const (
	prefixStripeID = "mappingByStripeID:%s"
	prefixM3OID    = "mappingByID:%s"
)

type WebhookResponse struct{}

type CustomerMapping struct {
	ID       string
	StripeID string
}

type Stripe struct {
	custSvc           custpb.CustomersService
	client            *stripeclient.API // stripe api client
	testClient        *stripeclient.API // stripe api client for test env
	successURL        string
	cancelURL         string
	signingSecret     string
	testSigningSecret string
}

func NewHandler(serv *service.Service) stripepb.StripeHandler {
	configObj := struct {
		ApiKey            string `json:"api_key"`
		SuccessURL        string `json:"success_url"`
		CancelURL         string `json:"cancel_url"`
		SigningSecret     string `json:"signing_secret"`
		TestAPIKey        string `json:"test_api_key"`
		TestSigningSecret string `json:"test_signing_secret"`
	}{}
	val, err := config.Get("micro.stripe")
	if err != nil {
		log.Warnf("Error getting config: %v", err)
	}
	if err := val.Scan(&configObj); err != nil {
		log.Fatalf("Error retrieving config %s", err)
	}

	if len(configObj.ApiKey) == 0 ||
		len(configObj.CancelURL) == 0 ||
		len(configObj.SuccessURL) == 0 ||
		len(configObj.SigningSecret) == 0 ||
		len(configObj.TestAPIKey) == 0 ||
		len(configObj.TestSigningSecret) == 0 {
		log.Fatalf("Missing required config: micro.stripe")
	}

	s := &Stripe{
		custSvc:           custpb.NewCustomersService("customers", serv.Client()),
		client:            stripeclient.New(configObj.ApiKey, nil),
		successURL:        configObj.SuccessURL,
		cancelURL:         configObj.CancelURL,
		signingSecret:     configObj.SigningSecret,
		testClient:        stripeclient.New(configObj.TestAPIKey, nil),
		testSigningSecret: configObj.TestSigningSecret,
	}
	s.consumeEvents()
	return s
}

func (s *Stripe) Webhook(ctx context.Context, req *api.Request, rsp *api.Response) error {
	md, ok := metadata.FromContext(ctx)
	if !ok {
		log.Errorf("Missing metadata from request")
		return errors.BadRequest("stripe.Webhook", "Missing headers")
	}
	isTest := false
	ev, err := webhook.ConstructEvent([]byte(req.Body), md["Stripe-Signature"], s.signingSecret)
	if err != nil {
		// try the test secret
		ev, err = webhook.ConstructEvent([]byte(req.Body), md["Stripe-Signature"], s.testSigningSecret)
		if err != nil {
			log.Errorf("Error verifying signature %s", err)
			return errors.BadRequest("stripe.Webhook", "Bad signature")
		}
		isTest = true
	}
	log.Infof("Received event %s:%s", ev.ID, ev.Type)
	switch ev.Type {
	case "customer.created":
		return s.customerCreated(ctx, &ev, isTest)
	case "charge.succeeded":
		return s.chargeSucceeded(ctx, &ev)
	case "payment_method.attached":
		return s.paymentMethodAttached(ctx, &ev)
	case "payment_method.detached":
		return s.paymentMethodDetached(ctx, &ev)
	default:
		log.Infof("Discarding event %s:%s", ev.ID, ev.Type)
	}
	return nil
}

func (s *Stripe) customerCreated(ctx context.Context, event *stripe.Event, isTest bool) error {
	// correlate customer based on email
	// store mapping stripe id to our id
	var cust stripe.Customer
	if err := json.Unmarshal(event.Data.Raw, &cust); err != nil {
		return err
	}
	// lookup customer on email

	// m3o.com emails are special case
	if isTest && !strings.HasSuffix(cust.Email, "@m3o.com") {
		// drop it
		log.Warnf("Received test event for non m3o.com email")
		return errors.BadRequest("stripe.Webhook", "Test env ")
	}

	rsp, err := s.custSvc.Read(ctx, &custpb.ReadRequest{Email: cust.Email}, client.WithAuthToken())
	if err != nil {
		// TODO check if not found error
		log.Errorf("Error looking up customer %s", cust.Email)
		return err
	}
	cm := CustomerMapping{
		ID:       rsp.Customer.Id,
		StripeID: cust.ID,
	}

	// persist it
	return s.storeMapping(&cm)
}

func (s *Stripe) storeMapping(cm *CustomerMapping) error {
	b, _ := json.Marshal(cm)
	// index on both stripe id and our id
	if err := store.Write(
		&store.Record{
			Key:   fmt.Sprintf(prefixM3OID, cm.ID),
			Value: b,
		},
	); err != nil {
		return err
	}
	return store.Write(
		&store.Record{
			Key:   fmt.Sprintf(prefixStripeID, cm.StripeID),
			Value: b,
		},
	)
}

func (s *Stripe) deleteMapping(cm *CustomerMapping) error {
	if err := store.Delete(fmt.Sprintf(prefixM3OID, cm.ID)); err != nil {
		return err
	}
	if err := store.Delete(fmt.Sprintf(prefixStripeID, cm.StripeID)); err != nil {
		return err
	}
	return nil
}

func (s *Stripe) chargeSucceeded(ctx context.Context, event *stripe.Event) error {
	var ch stripe.Charge
	if err := json.Unmarshal(event.Data.Raw, &ch); err != nil {
		return err
	}
	// lookup the customer
	recs, err := store.Read(fmt.Sprintf(prefixStripeID, ch.Customer.ID))
	if err != nil {
		if err == store.ErrNotFound {
			log.Errorf("Unrecognised customer for charge %s", ch.ID)
			return nil
		} else {
			log.Errorf("Error looking up customer for charge %s", ch.ID)
		}
		return err
	}
	var cm CustomerMapping
	if err := json.Unmarshal(recs[0].Value, &cm); err != nil {
		return err
	}

	if err := events.Publish(stripeevents.Topic, &stripeevents.Event{
		Type: stripeevents.EventType_EventTypeChargeSucceeded,
		ChargeSucceeded: &stripeevents.ChargeSuceeded{
			CustomerId: cm.ID,
			Currency:   string(ch.Currency), // TOOD
			Amount:     ch.Amount,
			ChargeId:   ch.ID,
		},
	}); err != nil {
		log.Errorf("Error publishing event %s", err)
		return err
	}
	log.Infof("Processing complete for %s", event.ID)
	return nil
}

func (s *Stripe) paymentMethodAttached(ctx context.Context, event *stripe.Event) error {
	var paymtMethod stripe.PaymentMethod
	if err := json.Unmarshal(event.Data.Raw, &paymtMethod); err != nil {
		return err
	}
	cm, err := mappingForStripeCustomer(paymtMethod.Customer.ID)
	if err != nil {
		logger.Errorf("Error looking up customer mapping %s", err)
		return err
	}

	evt := &custevents.Event{
		Type: custevents.EventType_EventTypeAddPaymentMethod,
		Customer: &custevents.Customer{
			Id: cm.ID,
		},
		AddPaymentMethod: &custevents.AddPaymentMethod{Id: paymtMethod.ID},
	}
	if err := events.Publish(custevents.Topic, evt); err != nil {
		logger.Errorf("Error publishing event %+v", err)
		return err
	}

	return nil
}

func (s *Stripe) paymentMethodDetached(ctx context.Context, event *stripe.Event) error {
	var paymtMethod stripe.PaymentMethod
	if err := json.Unmarshal(event.Data.Raw, &paymtMethod); err != nil {
		return err
	}
	custID := paymtMethod.Customer.ID

	if len(custID) == 0 {
		custID = event.GetPreviousValue("customer")
		if len(custID) == 0 {
			logger.Errorf("Unable to determine customer ID")
			return nil
		}
	}

	cm, err := mappingForStripeCustomer(custID)
	if err != nil {
		logger.Errorf("Error looking up customer mapping %s", err)
		return err
	}
	evt := &custevents.Event{
		Type: custevents.EventType_EventTypeDeletePaymentMethod,
		Customer: &custevents.Customer{
			Id: cm.ID,
		},
		DeletePaymentMethod: &custevents.DeletePaymentMethod{Id: paymtMethod.ID},
	}
	if err := events.Publish(custevents.Topic, evt); err != nil {
		logger.Errorf("Error publishing event %+v", err)
		return err
	}

	return nil
}

func (s *Stripe) CreateCheckoutSession(ctx context.Context, request *stripepb.CreateCheckoutSessionRequest, response *stripepb.CreateCheckoutSessionResponse) error {
	acc, ok := auth.AccountFromContext(ctx)
	if !ok {
		return errors.Unauthorized("stripe.CreateCheckoutSession", "Unauthorized")
	}
	if !request.SaveCard && request.Amount < 500 { // min spend
		return errors.BadRequest("stripe.CreateCheckoutSession", "Amount must be at least 500")
	}

	c := s.client
	if strings.HasSuffix(acc.Name, "@m3o.com") {
		c = s.testClient
	}

	params := &stripe.CheckoutSessionParams{
		PaymentMethodTypes: stripe.StringSlice([]string{
			"card",
		}),
		SuccessURL: stripe.String(s.successURL),
		CancelURL:  stripe.String(s.cancelURL),
	}

	if request.SaveCard {
		params.Mode = stripe.String(string(stripe.CheckoutSessionModeSetup))

	} else {
		params.Mode = stripe.String(string(stripe.CheckoutSessionModePayment))
		params.LineItems = []*stripe.CheckoutSessionLineItemParams{
			{
				PriceData: &stripe.CheckoutSessionLineItemPriceDataParams{
					Currency: stripe.String("usd"),
					ProductData: &stripe.CheckoutSessionLineItemPriceDataProductDataParams{
						Name: stripe.String("M3O credit"),
					},
					UnitAmount: stripe.Int64(request.Amount),
				},
				Quantity: stripe.Int64(1),
			}}
	}

	// lookup customer
	recs, err := store.Read(fmt.Sprintf(prefixM3OID, acc.ID))
	if err != nil && err != store.ErrNotFound {
		log.Errorf("Error looking up stripe customer %s", err)
		return errors.InternalServerError("stripe.CreateCheckoutSession", "Error creating checkout session")

	}
	if len(recs) == 0 {
		if !request.SaveCard {
			// use email from account
			// in payment mode stripe will auto create a customer object for you
			params.CustomerEmail = stripe.String(acc.Name)
		} else {
			// create a customer obj and attach
			cust, err := c.Customers.New(&stripe.CustomerParams{
				Email: stripe.String(acc.Name),
			})
			if err != nil {
				log.Errorf("Error creating stripe customer %s", err)
				return errors.InternalServerError("stripe.CreateCheckoutSession", "Error creating checkout session")
			}
			cm := CustomerMapping{
				ID:       acc.ID,
				StripeID: cust.ID,
			}
			if err := s.storeMapping(&cm); err != nil {
				log.Errorf("Error storing stripe customer mapping %s", err)
				return errors.InternalServerError("stripe.CreateCheckoutSession", "Error creating checkout session")
			}
			params.Customer = stripe.String(cust.ID)
		}
	} else {
		// use existing customer obj
		var cm CustomerMapping
		json.Unmarshal(recs[0].Value, &cm)
		params.Customer = stripe.String(cm.StripeID)
	}
	session, err := c.CheckoutSessions.New(params)
	if err != nil {
		return err
	}

	response.Id = session.ID
	return nil
}

func (s *Stripe) ListCards(ctx context.Context, request *stripepb.ListCardsRequest, response *stripepb.ListCardsResponse) error {
	acc, cm, err := mappingForCustomer(ctx, "stripe.ListCards")
	if err != nil {
		return err
	}

	c := s.client
	if strings.HasSuffix(acc.Name, "@m3o.com") {
		c = s.testClient
	}

	iter := c.PaymentMethods.List(&stripe.PaymentMethodListParams{
		Customer: stripe.String(cm.StripeID),
		Type:     stripe.String(string(stripe.PaymentMethodTypeCard)),
	})

	response.Cards = []*stripepb.Card{}
	for iter.Next() {
		pm := iter.PaymentMethod()
		response.Cards = append(response.Cards, &stripepb.Card{
			Id:       pm.ID,
			LastFour: pm.Card.Last4,
			Expires:  fmt.Sprintf("%02d/%d", pm.Card.ExpMonth, pm.Card.ExpYear),
		})
	}
	if iter.Err() != nil {
		return iter.Err()
	}
	return nil

}

func (s *Stripe) ChargeCard(ctx context.Context, request *stripepb.ChargeCardRequest, response *stripepb.ChargeCardResponse) error {
	acc, cm, err := mappingForCustomer(ctx, "stripe.ChargeCard")
	if err != nil {
		return err
	}
	c := s.client
	if strings.HasSuffix(acc.Name, "@m3o.com") {
		c = s.testClient
	}

	if err := s.ownsCard(c, cm, request.Id); err != nil {
		return errors.Forbidden("stripe.ChargeCard", "Card does not belong to user")
	}

	intent, err := c.PaymentIntents.New(&stripe.PaymentIntentParams{
		Params:        stripe.Params{},
		Amount:        stripe.Int64(request.Amount),
		Currency:      stripe.String(string(stripe.CurrencyUSD)),
		Customer:      stripe.String(cm.StripeID),
		Description:   stripe.String("M3O funds"),
		PaymentMethod: stripe.String(request.Id),
	})
	if err != nil {
		log.Errorf("Error setting up payment intent %s", err)
		return err
	}

	intent, err = c.PaymentIntents.Confirm(intent.ID, nil)
	if err != nil {
		log.Errorf("Error confirming payment intent %s", err)
		return err
	}
	if intent.Status != stripe.PaymentIntentStatusRequiresAction {
		return nil
	}
	response.ClientSecret = intent.ClientSecret

	return nil
}

func (s *Stripe) DeleteCard(ctx context.Context, request *stripepb.DeleteCardRequest, response *stripepb.DeleteCardResponse) error {
	acc, cm, err := mappingForCustomer(ctx, "stripe.DeleteCard")
	if err != nil {
		return err
	}
	c := s.client
	if strings.HasSuffix(acc.Name, "@m3o.com") {
		c = s.testClient
	}
	if err := s.ownsCard(c, cm, request.Id); err != nil {
		return errors.Forbidden("stripe.DeleteCard", "Card does not belong to user")
	}

	_, err = c.PaymentMethods.Detach(request.Id, nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *Stripe) ownsCard(c *stripeclient.API, cm *CustomerMapping, cardID string) error {
	pm, err := c.PaymentMethods.Get(cardID, nil)
	if err != nil {
		log.Errorf("Error loading payment method %s", err)
		return err
	}
	if cm.StripeID != pm.Customer.ID {
		log.Errorf("Card does not belong to this user %s. Card %s belongs to %s", cm.StripeID, cardID, pm.Customer.ID)
		return fmt.Errorf("card does not belong to user")
	}
	return nil
}

// mappingForCustomer returns the customer mapping for this context
func mappingForCustomer(ctx context.Context, method string) (*auth.Account, *CustomerMapping, error) {
	acc, ok := auth.AccountFromContext(ctx)
	if !ok {
		return nil, nil, errors.Unauthorized(method, "Unauthorized")
	}
	recs, err := store.Read(fmt.Sprintf(prefixM3OID, acc.ID))
	if err != nil {
		log.Errorf("Error looking up stripe customer %s", err)
		return nil, nil, err
	}

	var cm CustomerMapping
	json.Unmarshal(recs[0].Value, &cm)
	return acc, &cm, nil
}

// mappingForStripeCustomer returns the customer mapping for this stripe customer
func mappingForStripeCustomer(stripeID string) (*CustomerMapping, error) {
	recs, err := store.Read(fmt.Sprintf(prefixStripeID, stripeID))
	if err != nil {
		log.Errorf("Error looking up stripe customer %s", err)
		return nil, err
	}

	var cm CustomerMapping
	json.Unmarshal(recs[0].Value, &cm)
	return &cm, nil
}

func (s *Stripe) ListPayments(ctx context.Context, request *stripepb.ListPaymentsRequest, response *stripepb.ListPaymentsResponse) error {
	acc, cm, err := mappingForCustomer(ctx, "stripe.ListPayments")
	if err != nil {
		return err
	}

	c := s.client
	if strings.HasSuffix(acc.Name, "@m3o.com") {
		c = s.testClient
	}

	iter := c.Charges.List(&stripe.ChargeListParams{
		Customer: stripe.String(cm.StripeID),
	})
	response.Payments = []*stripepb.Payment{}
	for iter.Next() {
		c := iter.Charge()
		response.Payments = append(response.Payments, &stripepb.Payment{
			Id:         c.ID,
			Amount:     c.Amount,
			Currency:   string(c.Currency),
			Date:       c.Created,
			ReceiptUrl: c.ReceiptURL,
		})
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return nil
}

func (s *Stripe) GetPayment(ctx context.Context, request *stripepb.GetPaymentRequest, response *stripepb.GetPaymentResponse) error {
	// only for admins right now
	_, err := m3oauth.VerifyMicroAdmin(ctx, "stripe.GetPayment")
	if err != nil {
		return err
	}

	c, err := s.client.Charges.Get(request.Id, &stripe.ChargeParams{})
	if err != nil {
		// try with the test client
		c, err = s.testClient.Charges.Get(request.Id, &stripe.ChargeParams{})
		if err != nil {
			return err
		}
	}
	response.Payment = &stripepb.Payment{
		Id:         c.ID,
		Amount:     c.Amount,
		Currency:   string(c.Currency),
		Date:       c.Created,
		ReceiptUrl: c.ReceiptURL,
	}
	return nil
}
