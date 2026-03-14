package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

type PipelineStatus struct {
	KafkaLag     int64  `json:"kafka_lag"`
	HDFSUsage    string `json:"hdfs_usage"`
	LastSparkJob string `json:"last_spark_job"`
	Status       string `json:"status"`
}

type StatusTool struct {
	Prometheus       *PrometheusTool
	KafkaLagQuery    string
	NamenodeJMXURL   string
	SparkMasterURL   string
	ComponentTimeout time.Duration
}

func NewStatusTool(prometheus *PrometheusTool, kafkaLagQuery, namenodeJMXURL, sparkMasterURL string, timeout time.Duration) *StatusTool {
	return &StatusTool{
		Prometheus:       prometheus,
		KafkaLagQuery:    kafkaLagQuery,
		NamenodeJMXURL:   namenodeJMXURL,
		SparkMasterURL:   sparkMasterURL,
		ComponentTimeout: timeout,
	}
}

func (s *StatusTool) GetPipelineStatus(ctx context.Context) (PipelineStatus, error) {
	lagCtx, lagCancel := context.WithTimeout(ctx, s.ComponentTimeout)
	defer lagCancel()
	lagValue, err := s.Prometheus.QueryScalar(lagCtx, s.KafkaLagQuery)
	if err != nil {
		return PipelineStatus{}, fmt.Errorf("query kafka lag: %w", err)
	}

	hdfsCtx, hdfsCancel := context.WithTimeout(ctx, s.ComponentTimeout)
	defer hdfsCancel()
	hdfsUsage, hdfsRatio, err := s.fetchHDFSUsage(hdfsCtx)
	if err != nil {
		return PipelineStatus{}, fmt.Errorf("fetch hdfs usage: %w", err)
	}

	sparkCtx, sparkCancel := context.WithTimeout(ctx, s.ComponentTimeout)
	defer sparkCancel()
	lastSpark, sparkMinutesAgo, err := s.fetchLastSparkJob(sparkCtx)
	if err != nil {
		return PipelineStatus{}, fmt.Errorf("fetch spark status: %w", err)
	}

	status := "healthy"
	if lagValue > 500 || hdfsRatio > 0.9 || sparkMinutesAgo > 180 {
		status = "degraded"
	}

	return PipelineStatus{
		KafkaLag:     int64(lagValue),
		HDFSUsage:    hdfsUsage,
		LastSparkJob: lastSpark,
		Status:       status,
	}, nil
}

func (s *StatusTool) fetchHDFSUsage(ctx context.Context) (string, float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.NamenodeJMXURL, nil)
	if err != nil {
		return "", 0, fmt.Errorf("build namenode jmx request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("request namenode jmx: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("read namenode jmx response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("namenode jmx status=%d body=%s", resp.StatusCode, string(body))
	}

	var payload struct {
		Beans []struct {
			Name          string  `json:"name"`
			CapacityTotal float64 `json:"CapacityTotal"`
			CapacityUsed  float64 `json:"CapacityUsed"`
		} `json:"beans"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", 0, fmt.Errorf("decode namenode jmx response: %w", err)
	}

	for _, bean := range payload.Beans {
		if bean.CapacityTotal <= 0 {
			continue
		}
		usedGB := bean.CapacityUsed / (1024 * 1024 * 1024)
		totalGB := bean.CapacityTotal / (1024 * 1024 * 1024)
		usage := fmt.Sprintf("%.1fGB/%.1fGB", usedGB, totalGB)
		return usage, bean.CapacityUsed / bean.CapacityTotal, nil
	}
	return "", 0, fmt.Errorf("namenode jmx did not contain capacity bean")
}

func (s *StatusTool) fetchLastSparkJob(ctx context.Context) (string, float64, error) {
	url := fmt.Sprintf("%s/json/", s.SparkMasterURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", 0, fmt.Errorf("build spark request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("request spark master: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("read spark response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("spark status=%d body=%s", resp.StatusCode, string(body))
	}

	var payload struct {
		ActiveApps []struct {
			Name      string `json:"name"`
			StartTime any    `json:"starttime"`
		} `json:"activeapps"`
		CompletedApps []struct {
			Name      string `json:"name"`
			StartTime any    `json:"starttime"`
		} `json:"completedapps"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", 0, fmt.Errorf("decode spark response: %w", err)
	}

	var latestName string
	var latestTime time.Time
	collect := func(name string, start any) {
		if name == "" || start == nil {
			return
		}
		parsed, err := parseSparkTime(start)
		if err != nil {
			return
		}
		if parsed.After(latestTime) {
			latestTime = parsed
			latestName = name
		}
	}
	for _, app := range payload.ActiveApps {
		collect(app.Name, app.StartTime)
	}
	for _, app := range payload.CompletedApps {
		collect(app.Name, app.StartTime)
	}
	if latestTime.IsZero() {
		return "unknown", 9999, nil
	}
	minutesAgo := time.Since(latestTime).Minutes()
	return fmt.Sprintf("%s (%.0fm ago)", latestName, minutesAgo), minutesAgo, nil
}

func parseSparkTime(value any) (time.Time, error) {
	switch raw := value.(type) {
	case float64:
		return time.UnixMilli(int64(raw)), nil
	case int64:
		return time.UnixMilli(raw), nil
	case json.Number:
		millis, err := raw.Int64()
		if err != nil {
			return time.Time{}, err
		}
		return time.UnixMilli(millis), nil
	case string:
		if millis, err := strconv.ParseInt(raw, 10, 64); err == nil && millis > 0 {
			return time.UnixMilli(millis), nil
		}
		return parseSparkTimeString(raw)
	default:
		return time.Time{}, fmt.Errorf("unsupported spark time type %T", value)
	}
}

func parseSparkTimeString(value string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05.000GMT",
		"2006-01-02T15:04:05GMT",
		"2006-01-02T15:04:05.000Z",
	}
	for _, format := range formats {
		parsed, err := time.Parse(format, value)
		if err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported spark time format %q", value)
}
