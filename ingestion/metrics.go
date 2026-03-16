package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

type metricState struct {
	mu                  sync.Mutex
	runTotal            uint64
	runErrors           uint64
	fetchErrors         uint64
	publishTotal        uint64
	publishErrors       uint64
	consecutiveFailures uint64
	lastSuccessUnix     int64
	durationBuckets     []uint64
	durationCount       uint64
	durationSum         float64
	pm25Avg             float64
	pm10Avg             float64
	coAvg               float64
	airQualityUpdatedAt int64
}

var durationBucketBounds = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

func (m *metricState) observeRun(duration time.Duration, success bool) {
	seconds := duration.Seconds()
	m.mu.Lock()
	defer m.mu.Unlock()

	m.runTotal++
	if !success {
		m.runErrors++
		m.consecutiveFailures++
	} else {
		m.consecutiveFailures = 0
		m.lastSuccessUnix = time.Now().Unix()
	}

	m.durationCount++
	m.durationSum += seconds
	for idx, bound := range durationBucketBounds {
		if seconds <= bound {
			m.durationBuckets[idx]++
			return
		}
	}
}

func (m *metricState) incFetchError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchErrors++
}

func (m *metricState) incPublishError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishErrors++
}

func (m *metricState) incPublished() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishTotal++
}

func (m *metricState) getHealthSnapshot() (lastSuccess int64, failures uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSuccessUnix, m.consecutiveFailures
}

func (m *metricState) observeAirQuality(results []map[string]any) {
	pm25Sum, pm25Count := sumParameter(results, "pm25")
	pm10Sum, pm10Count := sumParameter(results, "pm10")
	coSum, coCount := sumParameter(results, "co")

	m.mu.Lock()
	defer m.mu.Unlock()
	if pm25Count > 0 {
		m.pm25Avg = pm25Sum / float64(pm25Count)
	}
	if pm10Count > 0 {
		m.pm10Avg = pm10Sum / float64(pm10Count)
	}
	if coCount > 0 {
		m.coAvg = coSum / float64(coCount)
	}
	m.airQualityUpdatedAt = time.Now().Unix()
}

func (m *metricState) handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(m.render()))
}

func (m *metricState) render() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b strings.Builder

	b.WriteString("# HELP lakehouse_pulse_ingestion_runs_total Total ingestion polling runs.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_runs_total counter\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_ingestion_runs_total %d\n", m.runTotal))

	b.WriteString("# HELP lakehouse_pulse_ingestion_run_errors_total Total ingestion runs that ended in error.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_run_errors_total counter\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_ingestion_run_errors_total %d\n", m.runErrors))

	b.WriteString("# HELP lakehouse_pulse_ingestion_fetch_errors_total Total OpenAQ fetch errors across retries.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_fetch_errors_total counter\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_ingestion_fetch_errors_total %d\n", m.fetchErrors))

	b.WriteString("# HELP lakehouse_pulse_ingestion_publish_total Total successfully published Kafka records.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_publish_total counter\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_ingestion_publish_total %d\n", m.publishTotal))

	b.WriteString("# HELP lakehouse_pulse_ingestion_publish_errors_total Total Kafka publish errors across retries.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_publish_errors_total counter\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_ingestion_publish_errors_total %d\n", m.publishErrors))

	b.WriteString("# HELP lakehouse_pulse_ingestion_run_duration_seconds Duration of ingestion runs in seconds.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_run_duration_seconds histogram\n")
	cumulative := uint64(0)
	for idx, bound := range durationBucketBounds {
		cumulative += m.durationBuckets[idx]
		b.WriteString(fmt.Sprintf("lakehouse_pulse_ingestion_run_duration_seconds_bucket{le=\"%.2f\"} %d\n", bound, cumulative))
	}
	b.WriteString(fmt.Sprintf("lakehouse_pulse_ingestion_run_duration_seconds_bucket{le=\"+Inf\"} %d\n", m.durationCount))
	b.WriteString(fmt.Sprintf("lakehouse_pulse_ingestion_run_duration_seconds_sum %.6f\n", m.durationSum))
	b.WriteString(fmt.Sprintf("lakehouse_pulse_ingestion_run_duration_seconds_count %d\n", m.durationCount))

	b.WriteString("# HELP lakehouse_pulse_ingestion_last_success_timestamp_seconds Unix timestamp of the last successful run.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_last_success_timestamp_seconds gauge\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_ingestion_last_success_timestamp_seconds %d\n", m.lastSuccessUnix))

	b.WriteString("# HELP lakehouse_pulse_ingestion_consecutive_failures Current consecutive failed-run count.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_consecutive_failures gauge\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_ingestion_consecutive_failures %d\n", m.consecutiveFailures))

	b.WriteString("# HELP lakehouse_pulse_air_quality_pm25_avg Latest average PM2.5 value from fetched OpenAQ records.\n")
	b.WriteString("# TYPE lakehouse_pulse_air_quality_pm25_avg gauge\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_air_quality_pm25_avg %.6f\n", m.pm25Avg))

	b.WriteString("# HELP lakehouse_pulse_air_quality_pm10_avg Latest average PM10 value from fetched OpenAQ records.\n")
	b.WriteString("# TYPE lakehouse_pulse_air_quality_pm10_avg gauge\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_air_quality_pm10_avg %.6f\n", m.pm10Avg))

	b.WriteString("# HELP lakehouse_pulse_air_quality_co_avg Latest average CO value from fetched OpenAQ records.\n")
	b.WriteString("# TYPE lakehouse_pulse_air_quality_co_avg gauge\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_air_quality_co_avg %.6f\n", m.coAvg))

	b.WriteString("# HELP lakehouse_pulse_air_quality_updated_at_timestamp_seconds Last timestamp when air quality gauges were updated.\n")
	b.WriteString("# TYPE lakehouse_pulse_air_quality_updated_at_timestamp_seconds gauge\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_air_quality_updated_at_timestamp_seconds %d\n", m.airQualityUpdatedAt))

	return b.String()
}
