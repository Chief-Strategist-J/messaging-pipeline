package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"event-platform/ingestion-api/internal/config"
	"event-platform/ingestion-api/internal/constants"
	"event-platform/ingestion-api/internal/customprocessors"
	"event-platform/ingestion-api/internal/eventtypes"
	"event-platform/ingestion-api/internal/httpapi"
	"event-platform/ingestion-api/internal/ingest"
	"event-platform/ingestion-api/internal/observability"
)

const (
	readTimeout  = 30 * time.Second
	writeTimeout = 30 * time.Second
	idleTimeout  = 60 * time.Second
	shutdownWait = 10 * time.Second
)

func main() {
	go func() { log.Println("pprof:", http.ListenAndServe(":6060", nil)) }()

	cfg := config.Load()
	shutdownTracing := observability.InitTracing(cfg.OTLPEndpoint)
	defer shutdownTracing(context.Background())

	if err := eventtypes.LoadFromFile(cfg.EventTypesPath); err != nil {
		log.Fatalf(constants.ErrEventTypeMissing, err)
	}
	eventtypes.RegisterCustomProcessor(constants.CustomProcessorPurchase, customprocessors.PurchaseEnrichment)

	producer, err := ingest.NewKafkaProducer(cfg.KafkaBrokers, cfg.SchemaID)
	if err != nil {
		log.Fatalf(constants.ErrKafkaProducerInit, err)
	}
	defer producer.Close()
	deduper := ingest.NewRedisDeduper(cfg.RedisAddr)

	mux := http.NewServeMux()
	handler := httpapi.NewHandler(producer, deduper)
	mux.Handle(constants.RouteEvents, httpapi.WithTracing(httpapi.WithRateLimit(handler, cfg.MaxConcurrent)))
	mux.HandleFunc(constants.RouteHealth, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	go func() {
		log.Printf(constants.LogListening, cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf(constants.ErrServerError, err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), shutdownWait)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf(constants.ErrShutdownFailed, err)
	}
}
