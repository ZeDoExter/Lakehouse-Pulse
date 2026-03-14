package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type config struct {
	ListenAddr      string
	OpenAQURL       string
	OpenAQAPIKey    string
	OpenAQCountryID int
	OpenAQLimit     int
	OpenAQCity      string
	Country         string
	KafkaRESTURL    string
	KafkaTopic      string
	PollInterval    time.Duration
	HTTPTimeout     time.Duration
	MaxRetries      int
	InitialBackoff  time.Duration
	ShutdownTimeout time.Duration
}

type metricState struct {
	runTotal            prometheus.Counter
	runErrors           prometheus.Counter
	fetchErrors         prometheus.Counter
	publishTotal        prometheus.Counter
	publishErrors       prometheus.Counter
	runDuration         prometheus.Histogram
	lastSuccess         prometheus.Gauge
	consecutiveFailures prometheus.Gauge
	pm25Avg             prometheus.Gauge
	pm10Avg             prometheus.Gauge
	coAvg               prometheus.Gauge
	airQualityUpdatedAt prometheus.Gauge

	mu                 sync.Mutex
	lastSuccessUnix    int64
	currentConsecutive uint64
}

func newMetricState() *metricState {
	m := &metricState{
		runTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lakehouse_pulse_ingestion_runs_total",
			Help: "Total ingestion polling runs.",
		}),
		runErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lakehouse_pulse_ingestion_run_errors_total",
			Help: "Total ingestion runs that ended in error.",
		}),
		fetchErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lakehouse_pulse_ingestion_fetch_errors_total",
			Help: "Total OpenAQ fetch errors across retries.",
		}),
		publishTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lakehouse_pulse_ingestion_publish_total",
			Help: "Total successfully published Kafka records.",
		}),
		publishErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lakehouse_pulse_ingestion_publish_errors_total",
			Help: "Total Kafka publish errors across retries.",
		}),
		runDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "lakehouse_pulse_ingestion_run_duration_seconds",
			Help:    "Duration of ingestion runs in seconds.",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lakehouse_pulse_ingestion_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last successful run.",
		}),
		consecutiveFailures: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lakehouse_pulse_ingestion_consecutive_failures",
			Help: "Current consecutive failed-run count.",
		}),
		pm25Avg: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lakehouse_pulse_air_quality_pm25_avg",
			Help: "Latest average PM2.5 value from fetched OpenAQ records.",
		}),
		pm10Avg: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lakehouse_pulse_air_quality_pm10_avg",
			Help: "Latest average PM10 value from fetched OpenAQ records.",
		}),
		coAvg: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lakehouse_pulse_air_quality_co_avg",
			Help: "Latest average CO value from fetched OpenAQ records.",
		}),
		airQualityUpdatedAt: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lakehouse_pulse_air_quality_updated_at_timestamp_seconds",
			Help: "Last timestamp when air quality gauges were updated.",
		}),
	}
	prometheus.MustRegister(
		m.runTotal, m.runErrors, m.fetchErrors, m.publishTotal, m.publishErrors,
		m.runDuration, m.lastSuccess, m.consecutiveFailures,
		m.pm25Avg, m.pm10Avg, m.coAvg, m.airQualityUpdatedAt,
	)
	return m
}

func main() {
	cfg := loadConfig()
	logger := log.New(os.Stdout, "ingestion ", log.LstdFlags|log.LUTC)
	metrics := newMetricState()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler(metrics, cfg.PollInterval))
	mux.Handle("/metrics", promhttp.Handler())

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

	runIngestionLoop(ctx, cfg, metrics, logger)
	logger.Println("shutdown signal received")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("http server shutdown error: %v", err)
	}
}

func runIngestionLoop(ctx context.Context, cfg config, metrics *metricState, logger *log.Logger) {
	runID := 0
	run := func() {
		runID++
		start := time.Now()
		if err := runOnce(ctx, cfg, metrics, logger, runID); err != nil {
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

func runOnce(ctx context.Context, cfg config, metrics *metricState, logger *log.Logger, runID int) error {
	results, err := fetchOpenAQ(ctx, cfg, metrics)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		logger.Printf("run_id=%d status=ok records=0 detail=no results from OpenAQ", runID)
		return nil
	}
	metrics.observeAirQuality(results)

	publishFailures := 0
	for _, result := range results {
		record := map[string]any{
			"source":     "openaq",
			"country":    cfg.Country,
			"fetched_at": time.Now().UTC().Format(time.RFC3339),
			"result":     result,
		}
		if err := publishWithRetry(ctx, cfg, metrics, record); err != nil {
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

func fetchOpenAQ(ctx context.Context, cfg config, metrics *metricState) ([]map[string]any, error) {
	client := &http.Client{Timeout: cfg.HTTPTimeout}
	var lastErr error

	for attempt := 1; attempt <= cfg.MaxRetries; attempt++ {
		results, err := fetchOpenAQV3Results(ctx, client, cfg)
		if err == nil {
			return results, nil
		}
		lastErr = fmt.Errorf("attempt %d OpenAQ v3 fetch failed: %w", attempt, err)
		metrics.incFetchError()

		if attempt < cfg.MaxRetries {
			backoff := cfg.InitialBackoff * time.Duration(1<<(attempt-1))
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	return nil, lastErr
}

func fetchOpenAQV3Results(ctx context.Context, client *http.Client, cfg config) ([]map[string]any, error) {
	if strings.TrimSpace(cfg.OpenAQAPIKey) == "" {
		return nil, fmt.Errorf("OPENAQ_API_KEY is required for OpenAQ v3")
	}
	base, err := openAQBaseURL(cfg.OpenAQURL)
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	params.Set("countries_id", strconv.Itoa(cfg.OpenAQCountryID))
	params.Set("limit", strconv.Itoa(cfg.OpenAQLimit))
	if city := strings.TrimSpace(cfg.OpenAQCity); city != "" {
		params.Set("cities", city)
	}
	locationsURL := fmt.Sprintf("%s/locations?%s", base, params.Encode())
	locations, err := fetchOpenAQLocations(ctx, client, locationsURL, cfg.OpenAQAPIKey)
	if err != nil {
		return nil, err
	}
	locations = filterLocationsByCity(locations, cfg.OpenAQCity)
	if len(locations) == 0 {
		return nil, fmt.Errorf("no OpenAQ locations found for city=%q", cfg.OpenAQCity)
	}
	results := make([]map[string]any, 0, len(locations))
	for _, loc := range locations {
		result, ok := buildResultFromLocation(ctx, client, base, cfg.OpenAQAPIKey, loc, cfg.Country)
		if ok {
			results = append(results, result)
		}
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no usable OpenAQ sensor data found")
	}
	return results, nil
}

func fetchOpenAQLocations(ctx context.Context, client *http.Client, endpoint, apiKey string) ([]map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build locations request: %w", err)
	}
	req.Header.Set("X-API-Key", apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request locations: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read locations response: %w", err)
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("openaq authentication failed: verify OPENAQ_API_KEY")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("locations status=%d body=%s", resp.StatusCode, truncate(string(body), 300))
	}

	var payload struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode locations response: %w", err)
	}
	return payload.Results, nil
}

func filterLocationsByCity(locations []map[string]any, city string) []map[string]any {
	city = strings.ToLower(strings.TrimSpace(city))
	if city == "" {
		return locations
	}
	filtered := make([]map[string]any, 0, len(locations))
	for _, loc := range locations {
		name, ok := loc["name"].(string)
		if !ok {
			name = ""
		}
		locality, ok := loc["locality"].(string)
		if !ok {
			locality = ""
		}
		timezone, ok := loc["timezone"].(string)
		if !ok {
			timezone = ""
		}
		haystack := strings.ToLower(strings.TrimSpace(name + " " + locality + " " + timezone))
		if strings.Contains(haystack, city) {
			filtered = append(filtered, loc)
		}
	}
	return filtered
}

func buildResultFromLocation(ctx context.Context, client *http.Client, base, apiKey string, location map[string]any, countryCode string) (map[string]any, bool) {
	locationIDRaw, ok := readFloatAny(location["id"])
	if !ok {
		return nil, false
	}
	locationID := int(locationIDRaw)

	measurementsRaw, ok := fetchLocationLatest(ctx, client, base, apiKey, locationID)
	if !ok || len(measurementsRaw) == 0 {
		return nil, false
	}

	measurements := make([]map[string]any, 0, 3)
	seen := map[string]bool{}
	for _, mRaw := range measurementsRaw {
		parameterObj, ok := mRaw["parameter"].(map[string]any)
		if !ok {
			continue
		}
		parameterName, ok := parameterObj["name"].(string)
		if !ok {
			continue
		}
		parameterName = strings.ToLower(strings.TrimSpace(parameterName))
		if parameterName != "pm25" && parameterName != "pm10" && parameterName != "co" {
			continue
		}
		if seen[parameterName] {
			continue
		}
		units, ok := parameterObj["units"].(string)
		if !ok {
			units = ""
		}
		value, ok := readFloatAny(mRaw["value"])
		if !ok {
			continue
		}
		period, ok := mRaw["period"].(map[string]any)
		if !ok {
			continue
		}
		datetimeTo, ok := period["datetimeTo"].(map[string]any)
		if !ok {
			continue
		}
		updatedAt, ok := datetimeTo["utc"].(string)
		if !ok {
			continue
		}

		seen[parameterName] = true
		measurements = append(measurements, map[string]any{
			"parameter":   parameterName,
			"value":       value,
			"unit":        units,
			"lastUpdated": updatedAt,
		})
		if len(seen) == 3 {
			break
		}
	}

	if len(measurements) == 0 {
		return nil, false
	}

	locationName, ok := location["name"].(string)
	if !ok {
		locationName = ""
	}
	locality, ok := location["locality"].(string)
	if !ok {
		locality = ""
	}
	city := locality
	if strings.TrimSpace(city) == "" {
		city = locationName
	}
	country := countryCode
	if countryObj, ok := location["country"].(map[string]any); ok {
		if code, ok := countryObj["code"].(string); ok && strings.TrimSpace(code) != "" {
			country = code
		}
	}

	coordinates := map[string]any{}
	if coords, ok := location["coordinates"].(map[string]any); ok {
		if lat, ok := readFloatAny(coords["latitude"]); ok {
			coordinates["latitude"] = lat
		}
		if lon, ok := readFloatAny(coords["longitude"]); ok {
			coordinates["longitude"] = lon
		}
	}

	return map[string]any{
		"location":     locationName,
		"city":         city,
		"country":      country,
		"coordinates":  coordinates,
		"measurements": measurements,
	}, true
}

func fetchLocationLatest(ctx context.Context, client *http.Client, base, apiKey string, locationID int) ([]map[string]any, bool) {
	endpoint := fmt.Sprintf("%s/locations/%d/latest", base, locationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, false
	}
	req.Header.Set("X-API-Key", apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false
	}

	var payload struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, false
	}
	return payload.Results, true
}

func openAQBaseURL(value string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil {
		return "", fmt.Errorf("invalid OPENAQ_URL: %w", err)
	}
	base := strings.TrimSuffix(parsed.String(), "/")
	base = strings.TrimSuffix(base, "/latest")
	if !strings.Contains(base, "/v3") {
		base = base + "/v3"
	}
	return base, nil
}

func readFloatAny(value any) (float64, bool) {
	switch parsed := value.(type) {
	case float64:
		return parsed, true
	case int:
		return float64(parsed), true
	case int64:
		return float64(parsed), true
	default:
		return 0, false
	}
}

func publishWithRetry(ctx context.Context, cfg config, metrics *metricState, message map[string]any) error {
	var lastErr error
	for attempt := 1; attempt <= cfg.MaxRetries; attempt++ {
		if err := publishKafkaREST(ctx, cfg, message); err != nil {
			lastErr = fmt.Errorf("attempt %d publish failed: %w", attempt, err)
			metrics.incPublishError()
			if attempt < cfg.MaxRetries {
				backoff := cfg.InitialBackoff * time.Duration(1<<(attempt-1))
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
			}
			continue
		}
		metrics.incPublished()
		return nil
	}
	return lastErr
}

func publishKafkaREST(ctx context.Context, cfg config, message map[string]any) error {
	endpoint := fmt.Sprintf("%s/topics/%s", strings.TrimRight(cfg.KafkaRESTURL, "/"), cfg.KafkaTopic)
	payloadBytes, err := json.Marshal(struct {
		Records []struct {
			Value map[string]any `json:"value"`
		} `json:"records"`
	}{
		Records: []struct {
			Value map[string]any `json:"value"`
		}{{Value: message}},
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.kafka.json.v2+json")
	req.Header.Set("Accept", "application/vnd.kafka.v2+json")

	client := &http.Client{Timeout: cfg.HTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("kafka rest status=%d and failed reading body: %w", resp.StatusCode, readErr)
		}
		return fmt.Errorf("kafka rest status=%d body=%s", resp.StatusCode, truncate(string(body), 300))
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

		maxDelay := int64((2 * pollInterval).Seconds())
		if maxDelay < 120 {
			maxDelay = 120
		}

		if now-lastSuccess > maxDelay || failures > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("degraded"))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

func (m *metricState) observeRun(duration time.Duration, success bool) {
	m.runTotal.Inc()
	m.runDuration.Observe(duration.Seconds())

	m.mu.Lock()
	defer m.mu.Unlock()
	if !success {
		m.runErrors.Inc()
		m.currentConsecutive++
		m.consecutiveFailures.Set(float64(m.currentConsecutive))
	} else {
		m.currentConsecutive = 0
		m.consecutiveFailures.Set(0)
		m.lastSuccessUnix = time.Now().Unix()
		m.lastSuccess.Set(float64(m.lastSuccessUnix))
	}
}

func (m *metricState) incFetchError() {
	m.fetchErrors.Inc()
}

func (m *metricState) incPublishError() {
	m.publishErrors.Inc()
}

func (m *metricState) incPublished() {
	m.publishTotal.Inc()
}

func (m *metricState) getHealthSnapshot() (lastSuccess int64, failures uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSuccessUnix, m.currentConsecutive
}

func (m *metricState) observeAirQuality(results []map[string]any) {
	pm25Sum, pm25Count := sumParameter(results, "pm25")
	pm10Sum, pm10Count := sumParameter(results, "pm10")
	coSum, coCount := sumParameter(results, "co")

	if pm25Count > 0 {
		m.pm25Avg.Set(pm25Sum / float64(pm25Count))
	}
	if pm10Count > 0 {
		m.pm10Avg.Set(pm10Sum / float64(pm10Count))
	}
	if coCount > 0 {
		m.coAvg.Set(coSum / float64(coCount))
	}
	m.airQualityUpdatedAt.Set(float64(time.Now().Unix()))
}

func sumParameter(results []map[string]any, parameter string) (float64, int) {
	sum := 0.0
	count := 0
	for _, result := range results {
		rawMeasurements, ok := result["measurements"].([]any)
		if !ok {
			continue
		}
		for _, item := range rawMeasurements {
			measurement, ok := item.(map[string]any)
			if !ok {
				continue
			}
			value, include := extractMeasurementValue(measurement, parameter)
			if include {
				sum += value
				count++
			}
		}
	}
	return sum, count
}

func extractMeasurementValue(measurement map[string]any, parameter string) (float64, bool) {
	name, ok := measurement["parameter"].(string)
	if !ok {
		return 0, false
	}
	name = strings.ToLower(strings.TrimSpace(name))
	if name != parameter {
		return 0, false
	}
	value, ok := readFloatAny(measurement["value"])
	if !ok {
		return 0, false
	}
	return value, true
}

func loadConfig() config {
	return config{
		ListenAddr:      getEnv("LISTEN_ADDR", ":8082"),
		OpenAQURL:       getEnv("OPENAQ_URL", "https://api.openaq.org/v3"),
		OpenAQAPIKey:    getEnv("OPENAQ_API_KEY", ""),
		OpenAQCountryID: getIntEnv("OPENAQ_COUNTRY_ID", 111),
		OpenAQLimit:     getIntEnv("OPENAQ_LOCATION_LIMIT", 3),
		OpenAQCity:      getEnv("OPENAQ_CITY", ""),
		Country:         getEnv("OPENAQ_COUNTRY", "TH"),
		KafkaRESTURL:    getEnv("KAFKA_REST_URL", "http://kafka-rest:8082"),
		KafkaTopic:      getEnv("KAFKA_TOPIC", "openaq.th.latest"),
		PollInterval:    getDurationEnv("POLL_INTERVAL", time.Hour),
		HTTPTimeout:     getDurationEnv("HTTP_TIMEOUT", 15*time.Second),
		MaxRetries:      getIntEnv("MAX_RETRIES", 5),
		InitialBackoff:  getDurationEnv("INITIAL_BACKOFF", 2*time.Second),
		ShutdownTimeout: getDurationEnv("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getIntEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func truncate(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max] + "..."
}
