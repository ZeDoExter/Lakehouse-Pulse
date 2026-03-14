package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type PrometheusTool struct {
	BaseURL string
	Client  *http.Client
}

func NewPrometheusTool(baseURL string, timeout time.Duration) *PrometheusTool {
	return &PrometheusTool{
		BaseURL: baseURL,
		Client:  &http.Client{Timeout: timeout},
	}
}

func (p *PrometheusTool) QueryScalar(ctx context.Context, expr string) (float64, error) {
	u, err := url.Parse(p.BaseURL)
	if err != nil {
		return 0, fmt.Errorf("invalid prometheus url: %w", err)
	}
	u.Path = "/api/v1/query"
	q := u.Query()
	q.Set("query", expr)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, fmt.Errorf("build prometheus query request: %w", err)
	}
	resp, err := p.Client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("prometheus query failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read prometheus response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("prometheus status=%d body=%s", resp.StatusCode, string(body))
	}

	var payload struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, fmt.Errorf("decode prometheus response: %w", err)
	}
	if payload.Status != "success" {
		return 0, fmt.Errorf("prometheus query error: %s", payload.Error)
	}
	if len(payload.Data.Result) == 0 || len(payload.Data.Result[0].Value) < 2 {
		return 0, fmt.Errorf("prometheus returned no data for expr=%q", expr)
	}

	raw := fmt.Sprintf("%v", payload.Data.Result[0].Value[1])
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("parse prometheus scalar %q: %w", raw, err)
	}
	return value, nil
}
