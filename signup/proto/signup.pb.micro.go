// Code generated by protoc-gen-micro. DO NOT EDIT.
// source: proto/signup.proto

package go_micro_service_signup

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

import (
	context "context"
	api "github.com/micro/micro/v3/service/api"
	client "github.com/micro/micro/v3/service/client"
	server "github.com/micro/micro/v3/service/server"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// Reference imports to suppress errors if they are not otherwise used.
var _ api.Endpoint
var _ context.Context
var _ client.Option
var _ server.Option

// Api Endpoints for Signup service

func NewSignupEndpoints() []*api.Endpoint {
	return []*api.Endpoint{}
}

// Client API for Signup service

type SignupService interface {
	// Sends the verification email to the user
	SendVerificationEmail(ctx context.Context, in *SendVerificationEmailRequest, opts ...client.CallOption) (*SendVerificationEmailResponse, error)
	// Verify kicks off the process of verification
	Verify(ctx context.Context, in *VerifyRequest, opts ...client.CallOption) (*VerifyResponse, error)
	SetPaymentMethod(ctx context.Context, in *SetPaymentMethodRequest, opts ...client.CallOption) (*SetPaymentMethodResponse, error)
	HasPaymentMethod(ctx context.Context, in *HasPaymentMethodRequest, opts ...client.CallOption) (*HasPaymentMethodResponse, error)
	// Creates a subscription and an account
	CompleteSignup(ctx context.Context, in *CompleteSignupRequest, opts ...client.CallOption) (*CompleteSignupResponse, error)
	Recover(ctx context.Context, in *RecoverRequest, opts ...client.CallOption) (*RecoverResponse, error)
	ResetPassword(ctx context.Context, in *ResetPasswordRequest, opts ...client.CallOption) (*ResetPasswordResponse, error)
}

type signupService struct {
	c    client.Client
	name string
}

func NewSignupService(name string, c client.Client) SignupService {
	return &signupService{
		c:    c,
		name: name,
	}
}

func (c *signupService) SendVerificationEmail(ctx context.Context, in *SendVerificationEmailRequest, opts ...client.CallOption) (*SendVerificationEmailResponse, error) {
	req := c.c.NewRequest(c.name, "Signup.SendVerificationEmail", in)
	out := new(SendVerificationEmailResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *signupService) Verify(ctx context.Context, in *VerifyRequest, opts ...client.CallOption) (*VerifyResponse, error) {
	req := c.c.NewRequest(c.name, "Signup.Verify", in)
	out := new(VerifyResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *signupService) SetPaymentMethod(ctx context.Context, in *SetPaymentMethodRequest, opts ...client.CallOption) (*SetPaymentMethodResponse, error) {
	req := c.c.NewRequest(c.name, "Signup.SetPaymentMethod", in)
	out := new(SetPaymentMethodResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *signupService) HasPaymentMethod(ctx context.Context, in *HasPaymentMethodRequest, opts ...client.CallOption) (*HasPaymentMethodResponse, error) {
	req := c.c.NewRequest(c.name, "Signup.HasPaymentMethod", in)
	out := new(HasPaymentMethodResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *signupService) CompleteSignup(ctx context.Context, in *CompleteSignupRequest, opts ...client.CallOption) (*CompleteSignupResponse, error) {
	req := c.c.NewRequest(c.name, "Signup.CompleteSignup", in)
	out := new(CompleteSignupResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *signupService) Recover(ctx context.Context, in *RecoverRequest, opts ...client.CallOption) (*RecoverResponse, error) {
	req := c.c.NewRequest(c.name, "Signup.Recover", in)
	out := new(RecoverResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *signupService) ResetPassword(ctx context.Context, in *ResetPasswordRequest, opts ...client.CallOption) (*ResetPasswordResponse, error) {
	req := c.c.NewRequest(c.name, "Signup.ResetPassword", in)
	out := new(ResetPasswordResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for Signup service

type SignupHandler interface {
	// Sends the verification email to the user
	SendVerificationEmail(context.Context, *SendVerificationEmailRequest, *SendVerificationEmailResponse) error
	// Verify kicks off the process of verification
	Verify(context.Context, *VerifyRequest, *VerifyResponse) error
	SetPaymentMethod(context.Context, *SetPaymentMethodRequest, *SetPaymentMethodResponse) error
	HasPaymentMethod(context.Context, *HasPaymentMethodRequest, *HasPaymentMethodResponse) error
	// Creates a subscription and an account
	CompleteSignup(context.Context, *CompleteSignupRequest, *CompleteSignupResponse) error
	Recover(context.Context, *RecoverRequest, *RecoverResponse) error
	ResetPassword(context.Context, *ResetPasswordRequest, *ResetPasswordResponse) error
}

func RegisterSignupHandler(s server.Server, hdlr SignupHandler, opts ...server.HandlerOption) error {
	type signup interface {
		SendVerificationEmail(ctx context.Context, in *SendVerificationEmailRequest, out *SendVerificationEmailResponse) error
		Verify(ctx context.Context, in *VerifyRequest, out *VerifyResponse) error
		SetPaymentMethod(ctx context.Context, in *SetPaymentMethodRequest, out *SetPaymentMethodResponse) error
		HasPaymentMethod(ctx context.Context, in *HasPaymentMethodRequest, out *HasPaymentMethodResponse) error
		CompleteSignup(ctx context.Context, in *CompleteSignupRequest, out *CompleteSignupResponse) error
		Recover(ctx context.Context, in *RecoverRequest, out *RecoverResponse) error
		ResetPassword(ctx context.Context, in *ResetPasswordRequest, out *ResetPasswordResponse) error
	}
	type Signup struct {
		signup
	}
	h := &signupHandler{hdlr}
	return s.Handle(s.NewHandler(&Signup{h}, opts...))
}

type signupHandler struct {
	SignupHandler
}

func (h *signupHandler) SendVerificationEmail(ctx context.Context, in *SendVerificationEmailRequest, out *SendVerificationEmailResponse) error {
	return h.SignupHandler.SendVerificationEmail(ctx, in, out)
}

func (h *signupHandler) Verify(ctx context.Context, in *VerifyRequest, out *VerifyResponse) error {
	return h.SignupHandler.Verify(ctx, in, out)
}

func (h *signupHandler) SetPaymentMethod(ctx context.Context, in *SetPaymentMethodRequest, out *SetPaymentMethodResponse) error {
	return h.SignupHandler.SetPaymentMethod(ctx, in, out)
}

func (h *signupHandler) HasPaymentMethod(ctx context.Context, in *HasPaymentMethodRequest, out *HasPaymentMethodResponse) error {
	return h.SignupHandler.HasPaymentMethod(ctx, in, out)
}

func (h *signupHandler) CompleteSignup(ctx context.Context, in *CompleteSignupRequest, out *CompleteSignupResponse) error {
	return h.SignupHandler.CompleteSignup(ctx, in, out)
}

func (h *signupHandler) Recover(ctx context.Context, in *RecoverRequest, out *RecoverResponse) error {
	return h.SignupHandler.Recover(ctx, in, out)
}

func (h *signupHandler) ResetPassword(ctx context.Context, in *ResetPasswordRequest, out *ResetPasswordResponse) error {
	return h.SignupHandler.ResetPassword(ctx, in, out)
}
