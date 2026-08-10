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

	v1 "event-platform/ingestion-api/src/api/rest/v1"
	"event-platform/ingestion-api/src/features/events"
	"event-platform/ingestion-api/src/infra/adapters/kafka"
	"event-platform/ingestion-api/src/infra/adapters/redis"
	"event-platform/ingestion-api/src/infra/tracing"
	"event-platform/ingestion-api/src/shared/config"
	"event-platform/ingestion-api/src/shared/constants"
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
	shutdownTracing := tracing.InitTracing(cfg.OTLPEndpoint)
	defer shutdownTracing(context.Background())

	if err := events.FeatureLoadFromFile(cfg.EventTypesPath); err != nil {
		log.Fatalf(constants.ErrEventTypeMissing, err)
	}
	events.FeatureRegisterCustomProcessor(constants.CustomProcessorPurchase, events.FeaturePurchaseEnrichment)

	producer, err := kafka.NewKafkaProducer(cfg.KafkaBrokers, cfg.SchemaID)
	if err != nil {
		log.Fatalf(constants.ErrKafkaProducerInit, err)
	}
	defer producer.Close()
	deduper := redis.NewRedisDeduper(cfg.RedisAddr)

	handler := v1.NewHandler(producer, deduper)
	router := v1.NewRouter(handler, cfg.MaxConcurrent)

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      router,
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
