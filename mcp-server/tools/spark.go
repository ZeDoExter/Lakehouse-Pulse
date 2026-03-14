package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type SparkTool struct {
	SubmitURL    string
	SparkMaster  string
	PythonJobURI string
	Client       *http.Client
}

type TriggerSparkResponse struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

func NewSparkTool(submitURL, sparkMaster, pythonJobURI string, timeout time.Duration) *SparkTool {
	return &SparkTool{
		SubmitURL:    submitURL,
		SparkMaster:  sparkMaster,
		PythonJobURI: pythonJobURI,
		Client:       &http.Client{Timeout: timeout},
	}
}

func (s *SparkTool) Trigger(ctx context.Context, jobName, date string) (TriggerSparkResponse, error) {
	request := map[string]any{
		"action":             "CreateSubmissionRequest",
		"appResource":        s.PythonJobURI,
		"mainClass":          "org.apache.spark.deploy.PythonRunner",
		"appArgs":            []string{"--job", jobName, "--date", date},
		"clientSparkVersion": "3.5.1",
		"environmentVariables": map[string]string{
			"SPARK_ENV_LOADED": "1",
		},
		"sparkProperties": map[string]string{
			"spark.master":            s.SparkMaster,
			"spark.app.name":          "lakehouse-pulse-" + jobName + "-" + date,
			"spark.submit.deployMode": "client",
		},
	}

	body, err := json.Marshal(request)
	if err != nil {
		return TriggerSparkResponse{}, fmt.Errorf("marshal spark submit request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.SubmitURL, bytes.NewReader(body))
	if err != nil {
		return TriggerSparkResponse{}, fmt.Errorf("build spark submit request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.Client.Do(req)
	if err != nil {
		return TriggerSparkResponse{}, fmt.Errorf("spark submit request failed: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return TriggerSparkResponse{}, fmt.Errorf("read spark submit response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return TriggerSparkResponse{}, fmt.Errorf("spark submit status=%d body=%s", resp.StatusCode, string(respBody))
	}

	var payload struct {
		SubmissionID string `json:"submissionId"`
		Success      bool   `json:"success"`
		Message      string `json:"message"`
	}
	if err := json.Unmarshal(respBody, &payload); err != nil {
		return TriggerSparkResponse{}, fmt.Errorf("decode spark submit response: %w", err)
	}
	if !payload.Success {
		return TriggerSparkResponse{}, fmt.Errorf("spark submit unsuccessful: %s", payload.Message)
	}
	return TriggerSparkResponse{
		JobID:  payload.SubmissionID,
		Status: "submitted",
	}, nil
}
