package main

import (
	"github.com/m3o/services/endtoend/handler"
	"github.com/m3o/services/pkg/tracing"
	"github.com/micro/micro/plugin/prometheus/v3"
	"github.com/micro/micro/v3/service"
	"github.com/micro/micro/v3/service/logger"
	"github.com/micro/micro/v3/service/metrics"
	"github.com/robfig/cron/v3"
)

func main() {
	// Create service
	srv := service.New(
		service.Name("endtoend"),
		service.Version("latest"),
	)

	// Register handler
	e := handler.NewEndToEnd(srv)
	srv.Handle(e)

	c := cron.New()
	c.AddFunc("0/5 * * * *", e.CronCheck)
	c.Start()
	traceCloser := tracing.SetupOpentracing("endtoend")
	defer traceCloser.Close()
	// Set up a default metrics reporter (being careful not to clash with any that have already been set):
	if !metrics.IsSet() {
		prometheusReporter, err := prometheus.New()
		if err != nil {
			logger.Fatalf("Failed to configure prom %s", err)
		}
		metrics.SetDefaultMetricsReporter(prometheusReporter)
	}

	// Run service
	if err := srv.Run(); err != nil {
		logger.Fatal(err)
	}
}
