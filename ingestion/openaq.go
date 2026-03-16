package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type openAQResponse struct {
	Results []map[string]any `json:"results"`
}

var errOpenAQRequestBudgetExceeded = errors.New("openaq request budget exceeded")

type openAQRequestBudget struct {
	max  int
	used int
}

func newOpenAQRequestBudget(maxReqs int) *openAQRequestBudget {
	if maxReqs <= 0 {
		maxReqs = 1
	}
	return &openAQRequestBudget{max: maxReqs}
}

func (b *openAQRequestBudget) consume() bool {
	if b.used >= b.max {
		return false
	}
	b.used++
	return true
}

func (b *openAQRequestBudget) remaining() int {
	return b.max - b.used
}

func fetchOpenAQ(ctx context.Context, cfg config, metrics *metricState, client *http.Client) ([]map[string]any, error) {
	var lastErr error
	budget := newOpenAQRequestBudget(cfg.OpenAQMaxReqs)

	for attempt := 1; attempt <= cfg.MaxRetries; attempt++ {
		results, err := fetchOpenAQV3Results(ctx, client, cfg, budget)
		if err == nil {
			return results, nil
		}
		lastErr = fmt.Errorf("attempt %d OpenAQ v3 fetch failed: %w", attempt, err)
		metrics.incFetchError()
		if errors.Is(err, errOpenAQRequestBudgetExceeded) {
			break
		}

		if attempt < cfg.MaxRetries {
			backoff := min(cfg.InitialBackoff*time.Duration(1<<(attempt-1)), 30*time.Second)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	return nil, lastErr
}

func fetchOpenAQV3Results(ctx context.Context, client *http.Client, cfg config, budget *openAQRequestBudget) ([]map[string]any, error) {
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
	locations, err := fetchOpenAQLocations(ctx, client, locationsURL, cfg.OpenAQAPIKey, budget)
	if err != nil {
		return nil, err
	}
	locations = filterLocationsByCity(locations, cfg.OpenAQCity)
	if len(locations) == 0 {
		return nil, fmt.Errorf("no OpenAQ locations found for city=%q", cfg.OpenAQCity)
	}
	results := make([]map[string]any, 0, len(locations))
	for _, loc := range locations {
		result, ok := buildResultFromLocation(ctx, client, base, cfg.OpenAQAPIKey, loc, cfg.Country, budget)
		if ok {
			results = append(results, result)
		}
		if budget.remaining() == 0 {
			break
		}
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no usable OpenAQ sensor data found")
	}
	return results, nil
}

func fetchOpenAQLocations(ctx context.Context, client *http.Client, endpoint, apiKey string, budget *openAQRequestBudget) ([]map[string]any, error) {
	if !budget.consume() {
		return nil, fmt.Errorf("%w: max=%d", errOpenAQRequestBudgetExceeded, budget.max)
	}
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

	var payload openAQResponse
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
		name, _ := loc["name"].(string)
		locality, _ := loc["locality"].(string)
		timezone, _ := loc["timezone"].(string)
		haystack := strings.ToLower(strings.TrimSpace(name + " " + locality + " " + timezone))
		if strings.Contains(haystack, city) {
			filtered = append(filtered, loc)
		}
	}
	return filtered
}

func buildResultFromLocation(ctx context.Context, client *http.Client, base, apiKey string, location map[string]any, countryCode string, budget *openAQRequestBudget) (map[string]any, bool) {
	sensors, ok := location["sensors"].([]any)
	if !ok || len(sensors) == 0 {
		return nil, false
	}

	measurements := make([]map[string]any, 0, 3)
	seen := map[string]bool{}
	for _, sensorItem := range sensors {
		sensor, ok := sensorItem.(map[string]any)
		if !ok {
			continue
		}
		sensorIDRaw, ok := readFloatAny(sensor["id"])
		if !ok {
			continue
		}
		parameterObj, ok := sensor["parameter"].(map[string]any)
		if !ok {
			continue
		}
		parameterName, _ := parameterObj["name"].(string)
		parameterName = strings.ToLower(strings.TrimSpace(parameterName))
		if parameterName != "pm25" && parameterName != "pm10" && parameterName != "co" {
			continue
		}
		if seen[parameterName] {
			continue
		}
		if budget.remaining() == 0 {
			break
		}
		units, _ := parameterObj["units"].(string)
		value, updatedAt, err := fetchSensorHour(ctx, client, base, apiKey, int(sensorIDRaw), budget)
		if err != nil {
			if errors.Is(err, errOpenAQRequestBudgetExceeded) {
				break
			}
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

	locationName, _ := location["name"].(string)
	locality, _ := location["locality"].(string)
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

func fetchSensorHour(ctx context.Context, client *http.Client, base, apiKey string, sensorID int, budget *openAQRequestBudget) (float64, string, error) {
	if !budget.consume() {
		return 0, "", fmt.Errorf("%w: max=%d", errOpenAQRequestBudgetExceeded, budget.max)
	}
	endpoint := fmt.Sprintf("%s/sensors/%d/hours?limit=1", base, sensorID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, "", fmt.Errorf("build sensor request: %w", err)
	}
	req.Header.Set("X-API-Key", apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("request sensor: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", fmt.Errorf("read sensor response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, "", fmt.Errorf("sensor status=%d body=%s", resp.StatusCode, truncate(string(body), 240))
	}

	var payload struct {
		Results []struct {
			Value  float64 `json:"value"`
			Period struct {
				DateTimeTo struct {
					UTC string `json:"utc"`
				} `json:"datetimeTo"`
			} `json:"period"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, "", fmt.Errorf("decode sensor response: %w", err)
	}
	if len(payload.Results) == 0 {
		return 0, "", fmt.Errorf("sensor has no data")
	}
	return payload.Results[0].Value, payload.Results[0].Period.DateTimeTo.UTC, nil
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

func truncate(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max] + "..."
}
