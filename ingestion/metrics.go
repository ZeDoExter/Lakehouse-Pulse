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

type metricSnapshot struct {
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
	pm25Sum, pm25Count, pm10Sum, pm10Count, coSum, coCount := aggregateAirQuality(results)

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
	snapshot := m.snapshot()

	var b strings.Builder

	b.WriteString("# HELP lakehouse_pulse_ingestion_runs_total Total ingestion polling runs.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_runs_total counter\n")
	fmt.Fprintf(&b, "lakehouse_pulse_ingestion_runs_total %d\n", snapshot.runTotal)

	b.WriteString("# HELP lakehouse_pulse_ingestion_run_errors_total Total ingestion runs that ended in error.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_run_errors_total counter\n")
	fmt.Fprintf(&b, "lakehouse_pulse_ingestion_run_errors_total %d\n", snapshot.runErrors)

	b.WriteString("# HELP lakehouse_pulse_ingestion_fetch_errors_total Total OpenAQ fetch errors across retries.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_fetch_errors_total counter\n")
	fmt.Fprintf(&b, "lakehouse_pulse_ingestion_fetch_errors_total %d\n", snapshot.fetchErrors)

	b.WriteString("# HELP lakehouse_pulse_ingestion_publish_total Total successfully published Kafka records.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_publish_total counter\n")
	fmt.Fprintf(&b, "lakehouse_pulse_ingestion_publish_total %d\n", snapshot.publishTotal)

	b.WriteString("# HELP lakehouse_pulse_ingestion_publish_errors_total Total Kafka publish errors across retries.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_publish_errors_total counter\n")
	fmt.Fprintf(&b, "lakehouse_pulse_ingestion_publish_errors_total %d\n", snapshot.publishErrors)

	b.WriteString("# HELP lakehouse_pulse_ingestion_run_duration_seconds Duration of ingestion runs in seconds.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_run_duration_seconds histogram\n")
	cumulative := uint64(0)
	for idx, bound := range durationBucketBounds {
		cumulative += snapshot.durationBuckets[idx]
		fmt.Fprintf(&b, "lakehouse_pulse_ingestion_run_duration_seconds_bucket{le=\"%.2f\"} %d\n", bound, cumulative)
	}
	fmt.Fprintf(&b, "lakehouse_pulse_ingestion_run_duration_seconds_bucket{le=\"+Inf\"} %d\n", snapshot.durationCount)
	fmt.Fprintf(&b, "lakehouse_pulse_ingestion_run_duration_seconds_sum %.6f\n", snapshot.durationSum)
	fmt.Fprintf(&b, "lakehouse_pulse_ingestion_run_duration_seconds_count %d\n", snapshot.durationCount)

	b.WriteString("# HELP lakehouse_pulse_ingestion_last_success_timestamp_seconds Unix timestamp of the last successful run.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_last_success_timestamp_seconds gauge\n")
	fmt.Fprintf(&b, "lakehouse_pulse_ingestion_last_success_timestamp_seconds %d\n", snapshot.lastSuccessUnix)

	b.WriteString("# HELP lakehouse_pulse_ingestion_consecutive_failures Current consecutive failed-run count.\n")
	b.WriteString("# TYPE lakehouse_pulse_ingestion_consecutive_failures gauge\n")
	fmt.Fprintf(&b, "lakehouse_pulse_ingestion_consecutive_failures %d\n", snapshot.consecutiveFailures)

	b.WriteString("# HELP lakehouse_pulse_air_quality_pm25_avg Latest average PM2.5 value from fetched OpenAQ records.\n")
	b.WriteString("# TYPE lakehouse_pulse_air_quality_pm25_avg gauge\n")
	fmt.Fprintf(&b, "lakehouse_pulse_air_quality_pm25_avg %.6f\n", snapshot.pm25Avg)

	b.WriteString("# HELP lakehouse_pulse_air_quality_pm10_avg Latest average PM10 value from fetched OpenAQ records.\n")
	b.WriteString("# TYPE lakehouse_pulse_air_quality_pm10_avg gauge\n")
	fmt.Fprintf(&b, "lakehouse_pulse_air_quality_pm10_avg %.6f\n", snapshot.pm10Avg)

	b.WriteString("# HELP lakehouse_pulse_air_quality_co_avg Latest average CO value from fetched OpenAQ records.\n")
	b.WriteString("# TYPE lakehouse_pulse_air_quality_co_avg gauge\n")
	fmt.Fprintf(&b, "lakehouse_pulse_air_quality_co_avg %.6f\n", snapshot.coAvg)

	b.WriteString("# HELP lakehouse_pulse_air_quality_updated_at_timestamp_seconds Last timestamp when air quality gauges were updated.\n")
	b.WriteString("# TYPE lakehouse_pulse_air_quality_updated_at_timestamp_seconds gauge\n")
	fmt.Fprintf(&b, "lakehouse_pulse_air_quality_updated_at_timestamp_seconds %d\n", snapshot.airQualityUpdatedAt)

	return b.String()
}

func (m *metricState) snapshot() metricSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	return metricSnapshot{
		runTotal:            m.runTotal,
		runErrors:           m.runErrors,
		fetchErrors:         m.fetchErrors,
		publishTotal:        m.publishTotal,
		publishErrors:       m.publishErrors,
		consecutiveFailures: m.consecutiveFailures,
		lastSuccessUnix:     m.lastSuccessUnix,
		durationBuckets:     append([]uint64(nil), m.durationBuckets...),
		durationCount:       m.durationCount,
		durationSum:         m.durationSum,
		pm25Avg:             m.pm25Avg,
		pm10Avg:             m.pm10Avg,
		coAvg:               m.coAvg,
		airQualityUpdatedAt: m.airQualityUpdatedAt,
	}
}

func aggregateAirQuality(results []map[string]any) (pm25Sum float64, pm25Count int, pm10Sum float64, pm10Count int, coSum float64, coCount int) {
	for _, result := range results {
		switch measurements := result["measurements"].(type) {
		case []map[string]any:
			for _, measurement := range measurements {
				if name, value, ok := parseMeasurement(measurement); ok {
					switch name {
					case "pm25":
						pm25Sum += value
						pm25Count++
					case "pm10":
						pm10Sum += value
						pm10Count++
					case "co":
						coSum += value
						coCount++
					}
				}
			}
		case []any:
			for _, item := range measurements {
				measurement, ok := item.(map[string]any)
				if !ok {
					continue
				}
				if name, value, ok := parseMeasurement(measurement); ok {
					switch name {
					case "pm25":
						pm25Sum += value
						pm25Count++
					case "pm10":
						pm10Sum += value
						pm10Count++
					case "co":
						coSum += value
						coCount++
					}
				}
			}
		}
	}
	return pm25Sum, pm25Count, pm10Sum, pm10Count, coSum, coCount
}

func parseMeasurement(measurement map[string]any) (string, float64, bool) {
	name, _ := measurement["parameter"].(string)
	name = strings.ToLower(strings.TrimSpace(name))
	if name != "pm25" && name != "pm10" && name != "co" {
		return "", 0, false
	}
	value, ok := readFloatAny(measurement["value"])
	if !ok {
		return "", 0, false
	}
	return name, value, true
}
