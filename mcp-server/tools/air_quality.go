package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"
)

type AirQualityTool struct {
	OpenAQURL string
	APIKey    string
	CountryID int
	Country   string
	Client    *http.Client
}

type LatestAirQualityResponse struct {
	PM25      *float64 `json:"pm25,omitempty"`
	PM10      *float64 `json:"pm10,omitempty"`
	CO        *float64 `json:"co,omitempty"`
	UpdatedAt string   `json:"updated_at"`
}

func NewAirQualityTool(openAQURL, apiKey string, countryID int, country string, timeout time.Duration) *AirQualityTool {
	return &AirQualityTool{
		OpenAQURL: openAQURL,
		APIKey:    apiKey,
		CountryID: countryID,
		Country:   country,
		Client:    &http.Client{Timeout: timeout},
	}
}

func (a *AirQualityTool) LatestByCity(ctx context.Context, city string) (LatestAirQualityResponse, error) {
	if a.APIKey == "" {
		return LatestAirQualityResponse{}, fmt.Errorf("OPENAQ_API_KEY is required for OpenAQ v3")
	}
	locations, err := a.fetchLocations(ctx)
	if err != nil {
		return LatestAirQualityResponse{}, err
	}
	if len(locations) == 0 {
		return LatestAirQualityResponse{}, fmt.Errorf("no OpenAQ results found for city=%q", city)
	}

	var preferred []map[string]any
	var fallback []map[string]any
	for _, loc := range locations {
		name, _ := loc["name"].(string)
		locality, _ := loc["locality"].(string)
		timezone, _ := loc["timezone"].(string)
		haystack := strings.ToLower(strings.TrimSpace(name + " " + locality + " " + timezone))
		if strings.Contains(haystack, strings.ToLower(strings.TrimSpace(city))) {
			preferred = append(preferred, loc)
		} else {
			fallback = append(fallback, loc)
		}
	}

	candidates := append(preferred, fallback...)
	for _, chosen := range candidates {
		sensors, ok := chosen["sensors"].([]any)
		if !ok || len(sensors) == 0 {
			continue
		}

		parameterToSensor := map[string]int{}
		for _, item := range sensors {
			sensor, ok := item.(map[string]any)
			if !ok {
				continue
			}
			idFloat, ok := readFloat(sensor["id"])
			if !ok {
				continue
			}
			parameterObj, ok := sensor["parameter"].(map[string]any)
			if !ok {
				continue
			}
			name, _ := parameterObj["name"].(string)
			name = strings.ToLower(strings.TrimSpace(name))
			if name == "pm25" || name == "pm10" || name == "co" {
				parameterToSensor[name] = int(idFloat)
			}
		}

		result := LatestAirQualityResponse{}
		var latest string
		for _, parameter := range []string{"pm25", "pm10", "co"} {
			sensorID, ok := parameterToSensor[parameter]
			if !ok {
				continue
			}
			value, updatedAt, err := a.fetchLatestSensorHour(ctx, sensorID)
			if err != nil {
				continue
			}
			switch parameter {
			case "pm25":
				result.PM25 = &value
			case "pm10":
				result.PM10 = &value
			case "co":
				result.CO = &value
			}
			if updatedAt > latest {
				latest = updatedAt
			}
		}
		if latest != "" {
			result.UpdatedAt = latest
			return result, nil
		}
	}
	return LatestAirQualityResponse{}, fmt.Errorf("city=%q found but no pollutant measurements available", city)
}

func (a *AirQualityTool) fetchLocations(ctx context.Context) ([]map[string]any, error) {
	base, err := a.baseURL()
	if err != nil {
		return nil, err
	}
	locationsURL := fmt.Sprintf("%s/locations?countries_id=%d&limit=200", base, a.CountryID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, locationsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build locations request: %w", err)
	}
	req.Header.Set("X-API-Key", a.APIKey)

	resp, err := a.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openaq locations request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read openaq locations response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("openaq locations status=%d body=%s", resp.StatusCode, string(body))
	}

	var payload struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode openaq locations response: %w", err)
	}
	return payload.Results, nil
}

func (a *AirQualityTool) fetchLatestSensorHour(ctx context.Context, sensorID int) (float64, string, error) {
	base, err := a.baseURL()
	if err != nil {
		return 0, "", err
	}
	sensorURL := fmt.Sprintf("%s/sensors/%d/hours?limit=1", base, sensorID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sensorURL, nil)
	if err != nil {
		return 0, "", fmt.Errorf("build sensor request: %w", err)
	}
	req.Header.Set("X-API-Key", a.APIKey)

	resp, err := a.Client.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("openaq sensor request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", fmt.Errorf("read sensor response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, "", fmt.Errorf("openaq sensor status=%d body=%s", resp.StatusCode, string(body))
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
		return 0, "", fmt.Errorf("sensor %d returned no hourly values", sensorID)
	}
	return payload.Results[0].Value, payload.Results[0].Period.DateTimeTo.UTC, nil
}

func (a *AirQualityTool) baseURL() (string, error) {
	base := strings.TrimSpace(a.OpenAQURL)
	if base == "" {
		return "", fmt.Errorf("OPENAQ_URL is required")
	}
	base = strings.TrimSuffix(base, "/")
	if strings.Contains(base, "/v3/latest") {
		base = strings.TrimSuffix(base, "/latest")
	}
	if strings.Contains(base, "/v3/") {
		base = base[:strings.Index(base, "/v3/")+3]
	}
	if !strings.HasSuffix(base, "/v3") {
		base = strings.TrimSuffix(base, path.Base(base))
		base = strings.TrimSuffix(base, "/")
		if !strings.HasSuffix(base, "/v3") {
			base = base + "/v3"
		}
	}
	return base, nil
}

func readFloat(value any) (float64, bool) {
	switch parsed := value.(type) {
	case float64:
		return parsed, true
	case string:
		number, err := strconv.ParseFloat(parsed, 64)
		if err != nil {
			return 0, false
		}
		return number, true
	case float32:
		return float64(parsed), true
	case int:
		return float64(parsed), true
	case int64:
		return float64(parsed), true
	default:
		return 0, false
	}
}
