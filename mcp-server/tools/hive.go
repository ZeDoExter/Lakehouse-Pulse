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

type HiveTool struct {
	QueryAPIURL string
	Client      *http.Client
}

type QueryHiveResponse struct {
	Rows          []map[string]any `json:"rows"`
	ExecutionTime string           `json:"execution_time"`
}

func NewHiveTool(queryAPIURL string, timeout time.Duration) *HiveTool {
	return &HiveTool{
		QueryAPIURL: queryAPIURL,
		Client:      &http.Client{Timeout: timeout},
	}
}

func (h *HiveTool) Query(ctx context.Context, sql string) (QueryHiveResponse, error) {
	if h.QueryAPIURL == "" {
		return QueryHiveResponse{}, fmt.Errorf("query_hive is not configured: set HIVE_QUERY_API_URL")
	}
	started := time.Now()
	body, err := json.Marshal(map[string]string{"sql": sql})
	if err != nil {
		return QueryHiveResponse{}, fmt.Errorf("marshal hive request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.QueryAPIURL, bytes.NewReader(body))
	if err != nil {
		return QueryHiveResponse{}, fmt.Errorf("build hive request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.Client.Do(req)
	if err != nil {
		return QueryHiveResponse{}, fmt.Errorf("hive query request failed: %w", err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return QueryHiveResponse{}, fmt.Errorf("read hive response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return QueryHiveResponse{}, fmt.Errorf("hive query status=%d body=%s", resp.StatusCode, string(responseBody))
	}

	var decoded QueryHiveResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return QueryHiveResponse{}, fmt.Errorf("decode hive response: %w", err)
	}
	if decoded.ExecutionTime == "" {
		decoded.ExecutionTime = time.Since(started).String()
	}
	return decoded, nil
}
