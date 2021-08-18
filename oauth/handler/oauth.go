package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	githubapi "github.com/google/go-github/v38/github"
	"github.com/google/uuid"
	cproto "github.com/m3o/services/customers/proto"
	eproto "github.com/m3o/services/emails/proto"
	oauth "github.com/m3o/services/oauth/proto"
	onboarding "github.com/m3o/services/onboarding/proto"
	authproto "github.com/micro/micro/v3/proto/auth"
	"github.com/micro/micro/v3/service"
	"github.com/micro/micro/v3/service/auth"
	"github.com/micro/micro/v3/service/client"
	mevents "github.com/micro/micro/v3/service/events"
	"github.com/patrickmn/go-cache"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/github"
	"golang.org/x/oauth2/google"

	mconfig "github.com/micro/micro/v3/service/config"
	cont "github.com/micro/micro/v3/service/context"
	merrors "github.com/micro/micro/v3/service/errors"
	logger "github.com/micro/micro/v3/service/logger"
	model "github.com/micro/micro/v3/service/model"
)

var (
	oauthConfGl = &oauth2.Config{
		ClientID:     "",
		ClientSecret: "",
		RedirectURL:  "http://127.0.0.1:4200/google-login",
		Scopes:       []string{"https://www.googleapis.com/auth/userinfo.email"},
		Endpoint:     google.Endpoint,
	}
	oauthStateStringGl = ""

	oauthConfGithub = &oauth2.Config{
		ClientID:     "",
		ClientSecret: "",
		RedirectURL:  "http://127.0.0.1:4200/github-login",
		Scopes:       []string{"user:email"},
		Endpoint:     github.Endpoint,
	}
)

const (
	microNamespace   = "micro"
	internalErrorMsg = "An error occurred during onboarding. Contact #m3o-support at slack.m3o.com if the issue persists"
	topic            = "onboarding"
)

type googleConf struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RedirectURL  string `json:"redirect_url"`
}

type githubConf struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RedirectURL  string `json:"redirect_url"`
}

type oauthConf struct {
	Google googleConf `json:"google"`
	Github githubConf `json:"github"`
}

type Oauth struct {
	customerService cproto.CustomersService
	emailService    eproto.EmailsService
	auth            auth.Auth
	accounts        authproto.AccountsService
	config          oauthConf
	cache           *cache.Cache
	resetCode       model.Model
	track           model.Model
}

func NewOauth(srv *service.Service, auth auth.Auth) *Oauth {
	c := oauthConf{}
	val, err := mconfig.Get("micro.oauth")
	if err != nil {
		logger.Fatalf("Error getting config: %v", err)
	}
	err = val.Scan(&c)
	if err != nil {
		logger.Fatalf("Error scanning config: %v", err)
	}

	if c.Google.ClientSecret == "" {
		logger.Fatal("No google oauth client ID")
	}
	if c.Google.ClientSecret == "" {
		logger.Fatal("No google oauth client secret")
	}
	oauthConfGl.ClientID = c.Google.ClientID
	oauthConfGl.ClientSecret = c.Google.ClientSecret
	if c.Google.RedirectURL != "" {
		oauthConfGl.RedirectURL = c.Google.RedirectURL
	}

	if c.Github.ClientSecret == "" {
		logger.Fatal("No google oauth client ID")
	}
	if c.Github.ClientSecret == "" {
		logger.Fatal("No google oauth client secret")
	}
	oauthConfGithub.ClientID = c.Github.ClientID
	oauthConfGithub.ClientSecret = c.Github.ClientSecret
	if c.Github.RedirectURL != "" {
		oauthConfGithub.RedirectURL = c.Github.RedirectURL
	}

	s := &Oauth{
		customerService: cproto.NewCustomersService("customers", srv.Client()),
		auth:            auth,
		accounts:        authproto.NewAccountsService("auth", srv.Client()),
		config:          c,
		cache:           cache.New(1*time.Minute, 5*time.Minute),
	}
	return s
}

// GoogleOauthURL returns the url which kicks off the google oauth flow
func (e *Oauth) GoogleURL(ctx context.Context, req *oauth.GoogleURLRequest, rsp *oauth.GoogleURLResponse) error {
	URL, err := url.Parse(oauthConfGl.Endpoint.AuthURL)
	if err != nil {
		return err
	}

	parameters := url.Values{}
	parameters.Add("client_id", oauthConfGl.ClientID)
	parameters.Add("scope", strings.Join(oauthConfGl.Scopes, " "))
	parameters.Add("redirect_uri", oauthConfGl.RedirectURL)
	parameters.Add("response_type", "code")
	//parameters.Add("state", oauthStateString)
	URL.RawQuery = parameters.Encode()
	logger.Info(URL.String())
	url := URL.String()
	rsp.Url = url
	return nil
}

func (e *Oauth) GoogleLogin(ctx context.Context, req *oauth.GoogleLoginRequest, rsp *oauth.LoginResponse) error {
	state := req.State
	if state != oauthStateStringGl {
		return fmt.Errorf("invalid oauth state, expected " + oauthStateStringGl + ", got " + state + "\n")
	}

	code := req.Code

	if code == "" {
		reason := req.ErrorReason
		if reason == "user_denied" {
			return fmt.Errorf("user has denied permission")
		}
		return fmt.Errorf("code not found")
	}

	token, err := oauthConfGl.Exchange(oauth2.NoContext, code)
	if err != nil {
		return fmt.Errorf("failed exchange: %v", err)
	}

	logger.Info("Got token")
	resp, err := http.Get("https://www.googleapis.com/oauth2/v2/userinfo?access_token=" + url.QueryEscape(token.AccessToken))
	if err != nil {
		return fmt.Errorf("Get: " + err.Error() + "\n")
	}
	defer resp.Body.Close()

	response, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// TODO there must be a proper lib api for this
	gresp := map[string]interface{}{}
	err = json.Unmarshal(response, &gresp)
	if err != nil {
		return err
	}

	email, emailOk := gresp["email"].(string)
	if !emailOk {
		return fmt.Errorf("no email in oauth info")
	}

	readResp, err := e.customerService.Read(cont.DefaultContext, &cproto.ReadRequest{
		Email: email,
	}, client.WithAuthToken())
	if err != nil && (strings.Contains(err.Error(), "notfound") || strings.Contains(err.Error(), "not found")) {
		logger.Infof("Oauth registering %v", email)
		rsp.IsSignup = true
		return e.registerOauthUser(ctx, rsp, email)
	}
	if err != nil {
		return err
	}
	logger.Infof("Oauth logging in %v", email)
	return e.loginOauthUser(ctx, rsp, readResp.Customer.Id, email)
}

func (e *Oauth) GithubURL(ctx context.Context, req *oauth.GithubURLRequest, rsp *oauth.GithubURLResponse) error {
	URL, err := url.Parse(oauthConfGithub.Endpoint.AuthURL)
	if err != nil {
		return err
	}

	parameters := url.Values{}
	parameters.Add("client_id", oauthConfGithub.ClientID)
	parameters.Add("scope", strings.Join(oauthConfGithub.Scopes, " "))
	parameters.Add("redirect_uri", oauthConfGithub.RedirectURL)
	parameters.Add("response_type", "code")
	//parameters.Add("state", oauthStateString)
	URL.RawQuery = parameters.Encode()
	logger.Info(URL.String())
	url := URL.String()
	rsp.Url = url
	return nil
}

func (e *Oauth) GithubLogin(ctx context.Context, req *oauth.GithubLoginRequest, rsp *oauth.LoginResponse) error {
	state := req.State
	if state != oauthStateStringGl {
		return fmt.Errorf("invalid oauth state, expected " + oauthStateStringGl + ", got " + state + "\n")
	}

	code := req.Code

	if code == "" {
		reason := req.ErrorReason
		if reason == "user_denied" {
			return fmt.Errorf("user has denied permission")
		}
		return fmt.Errorf("code not found")
	}

	token, err := oauthConfGithub.Exchange(oauth2.NoContext, code)
	if err != nil {
		return fmt.Errorf("failed exchange: %v", err)
	}

	logger.Info("Got token", token)

	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token.AccessToken},
	)
	tc := oauth2.NewClient(ctx, ts)
	githubClient := githubapi.NewClient(tc)
	emails, _, err := githubClient.Users.ListEmails(ctx, nil)
	if err != nil {
		return err
	}
	email := ""
	for _, em := range emails {
		if em.Primary != nil && *em.Primary {
			email = *em.Email
		}
	}
	if email == "" {
		return fmt.Errorf("no github primary email found")
	}

	readResp, err := e.customerService.Read(ctx, &cproto.ReadRequest{
		Email: email,
	}, client.WithAuthToken())
	if err != nil && (strings.Contains(err.Error(), "notfound") || strings.Contains(err.Error(), "not found")) {
		logger.Infof("Oauth registering %v", email)
		rsp.IsSignup = true
		return e.registerOauthUser(ctx, rsp, email)
	}
	if err != nil {
		return err
	}
	logger.Infof("Oauth logging in %v", email)
	return e.loginOauthUser(ctx, rsp, readResp.Customer.Id, email)
}

func (e *Oauth) registerOauthUser(ctx context.Context, rsp *oauth.LoginResponse, email string) error {
	// create entry in customers service
	crsp, err := e.customerService.Create(ctx, &cproto.CreateRequest{Email: email}, client.WithAuthToken())
	if err != nil {
		logger.Error(err)
		return merrors.InternalServerError("oauth.registerOauthUser", internalErrorMsg)
	}

	if _, err := e.customerService.MarkVerified(ctx, &cproto.MarkVerifiedRequest{Email: email}, client.WithAuthToken()); err != nil {
		logger.Errorf("Error marking customer as verified: %v", err)
		return merrors.InternalServerError("oauth.registerOauthUser", internalErrorMsg)
	}

	secret := uuid.New().String()
	_, err = e.auth.Generate(crsp.Customer.Id,
		auth.WithScopes("customer"),
		auth.WithSecret(secret),
		auth.WithIssuer(microNamespace),
		auth.WithName(email),
		auth.WithType("customer"))
	if err != nil {
		logger.Errorf("Error generating token for %v: %v", crsp.Customer.Id, err)
		return merrors.InternalServerError("oauth.registerOauthUser", internalErrorMsg)
	}

	t, err := e.auth.Token(auth.WithCredentials(crsp.Customer.Id, secret), auth.WithTokenIssuer(microNamespace))
	if err != nil {
		logger.Errorf("Can't get token for %v: %v", crsp.Customer.Id, err)
		return merrors.InternalServerError("oauth.registerOauthUser", internalErrorMsg)
	}
	rsp.AuthToken = &oauth.AuthToken{
		AccessToken:  t.AccessToken,
		RefreshToken: t.RefreshToken,
		Expiry:       t.Expiry.Unix(),
		Created:      t.Created.Unix(),
	}
	rsp.CustomerID = crsp.Customer.Id
	rsp.Namespace = microNamespace
	if err := mevents.Publish(topic, &onboarding.Event{Type: "newSignup", NewSignup: &onboarding.NewSignupEvent{Email: email, Id: crsp.Customer.Id}}); err != nil {
		logger.Warnf("Error publishing %s", err)
	}
	return nil
}

func (e *Oauth) loginOauthUser(ctx context.Context, rsp *oauth.LoginResponse, id, email string) error {
	secret := uuid.New().String()
	_, err := e.accounts.ChangeSecret(ctx, &authproto.ChangeSecretRequest{
		Id:        email,
		NewSecret: secret,
		Options: &authproto.Options{
			Namespace: microNamespace,
		},
	}, client.WithAuthToken())
	if err != nil {
		return err
	}

	t, err := e.auth.Token(auth.WithCredentials(id, secret), auth.WithTokenIssuer(microNamespace))
	if err != nil {
		logger.Errorf("Can't get token for %v: %v", id, err)
		return merrors.InternalServerError("oauth.loginOauthUser", internalErrorMsg)
	}
	rsp.AuthToken = &oauth.AuthToken{
		AccessToken:  t.AccessToken,
		RefreshToken: t.RefreshToken,
		Expiry:       t.Expiry.Unix(),
		Created:      t.Created.Unix(),
	}
	rsp.CustomerID = id
	rsp.Namespace = microNamespace

	return nil
}
