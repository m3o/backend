package main

import (
	"github.com/m3o/services/mailchimp/handler"
	pb "github.com/m3o/services/mailchimp/proto"

	"github.com/micro/micro/v3/service"
	"github.com/micro/micro/v3/service/logger"
)

func main() {
	// Create service
	srv := service.New(
		service.Name("mailchimp"),
		service.Version("latest"),
	)

	// Register handler
	pb.RegisterMailchimpHandler(srv.Server(), handler.New(srv))

	// Run service
	if err := srv.Run(); err != nil {
		logger.Fatal(err)
	}
}
