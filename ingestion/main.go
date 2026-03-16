package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := loadConfig()
	logger := log.New(os.Stdout, "ingestion ", log.LstdFlags|log.LUTC)
	metrics := &metricState{durationBuckets: make([]uint64, len(durationBucketBounds))}
	httpClient := &http.Client{Timeout: cfg.HTTPTimeout}
	producer := newKafkaProducer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.HTTPTimeout)
	defer producer.close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler(metrics, cfg.PollInterval))
	mux.HandleFunc("/metrics", metrics.handler)

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Printf("http server listening on %s", cfg.ListenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("http server failed: %v", err)
		}
	}()

	runIngestionLoop(ctx, cfg, metrics, logger, producer, httpClient)
	logger.Println("shutdown signal received")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("http server shutdown error: %v", err)
	}
}

func runIngestionLoop(ctx context.Context, cfg config, metrics *metricState, logger *log.Logger, producer *kafkaProducer, client *http.Client) {
	runID := 0
	run := func() {
		runID++
		start := time.Now()
		if err := runOnce(ctx, cfg, metrics, logger, producer, client, runID); err != nil {
			metrics.observeRun(time.Since(start), false)
			logger.Printf("run_id=%d status=error err=%q", runID, err)
			return
		}
		metrics.observeRun(time.Since(start), true)
		logger.Printf("run_id=%d status=ok", runID)
	}

	run()
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func runOnce(ctx context.Context, cfg config, metrics *metricState, logger *log.Logger, producer *kafkaProducer, client *http.Client, runID int) error {
	results, err := fetchOpenAQ(ctx, cfg, metrics, client)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		logger.Printf("run_id=%d status=ok records=0 detail=no results from OpenAQ", runID)
		return nil
	}
	metrics.observeAirQuality(results)

	publishFailures := 0
	fetchedAt := time.Now().UTC().Format(time.RFC3339)
	for _, result := range results {
		record := map[string]any{
			"source":     "openaq",
			"country":    cfg.Country,
			"fetched_at": fetchedAt,
			"result":     result,
		}
		if err := producer.publishWithRetry(ctx, metrics, record, cfg.MaxRetries, cfg.InitialBackoff); err != nil {
			publishFailures++
			logger.Printf("run_id=%d stage=publish status=error err=%q", runID, err)
		}
	}

	logger.Printf("run_id=%d status=processed records=%d publish_failures=%d", runID, len(results), publishFailures)
	if publishFailures > 0 {
		return fmt.Errorf("publish failed for %d/%d records", publishFailures, len(results))
	}
	return nil
}

func healthHandler(metrics *metricState, pollInterval time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		now := time.Now().Unix()
		lastSuccess, failures := metrics.getHealthSnapshot()
		if lastSuccess == 0 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("starting"))
			return
		}

		maxDelay := max(int64((2 * pollInterval).Seconds()), 120)

		if now-lastSuccess > maxDelay || failures > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("degraded"))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}
