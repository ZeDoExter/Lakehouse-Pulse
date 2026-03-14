package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

type AirQualityTool struct {
	OpenAQURL string
	APIKey    string
	CountryID int
	Limit     int
	Country   string
	Client    *http.Client
}

type LatestAirQualityResponse struct {
	PM25      *float64 `json:"pm25,omitempty"`
	PM10      *float64 `json:"pm10,omitempty"`
	CO        *float64 `json:"co,omitempty"`
	UpdatedAt string   `json:"updated_at"`
}

func NewAirQualityTool(openAQURL, apiKey string, countryID, limit int, country string, timeout time.Duration) *AirQualityTool {
	if limit <= 0 {
		limit = 3
	}
	return &AirQualityTool{
		OpenAQURL: openAQURL,
		APIKey:    apiKey,
		CountryID: countryID,
		Limit:     limit,
		Country:   country,
		Client:    &http.Client{Timeout: timeout},
	}
}

func (a *AirQualityTool) LatestByCity(ctx context.Context, city string) (LatestAirQualityResponse, error) {
	if a.APIKey == "" {
		return LatestAirQualityResponse{}, fmt.Errorf("OPENAQ_API_KEY is required for OpenAQ v3")
	}
	locations, err := a.fetchLocations(ctx, city)
	if err != nil {
		return LatestAirQualityResponse{}, err
	}
	if len(locations) == 0 {
		return LatestAirQualityResponse{}, fmt.Errorf("no OpenAQ results found for city=%q", city)
	}

	var preferred []map[string]any
	var fallback []map[string]any
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
		if strings.Contains(haystack, strings.ToLower(strings.TrimSpace(city))) {
			preferred = append(preferred, loc)
		} else {
			fallback = append(fallback, loc)
		}
	}

	candidates := append(preferred, fallback...)
	for _, chosen := range candidates {
		locationIDRaw, ok := readFloat(chosen["id"])
		if !ok {
			continue
		}
		locationID := int(locationIDRaw)

		measurementsRaw, ok := a.fetchLocationLatest(ctx, locationID)
		if !ok || len(measurementsRaw) == 0 {
			continue
		}

		result := LatestAirQualityResponse{}
		var latest string
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

			value, ok := readFloat(mRaw["value"])
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

			switch parameterName {
			case "pm25":
				result.PM25 = &value
			case "pm10":
				result.PM10 = &value
			case "co":
				result.CO = &value
			}
			seen[parameterName] = true
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

func (a *AirQualityTool) fetchLocations(ctx context.Context, city string) ([]map[string]any, error) {
	base, err := a.baseURL()
	if err != nil {
		return nil, err
	}
	params := url.Values{}
	params.Set("countries_id", strconv.Itoa(a.CountryID))
	params.Set("limit", strconv.Itoa(a.Limit))
	if city = strings.TrimSpace(city); city != "" {
		params.Set("cities", city)
	}
	locationsURL := fmt.Sprintf("%s/locations?%s", base, params.Encode())
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

func (a *AirQualityTool) fetchLocationLatest(ctx context.Context, locationID int) ([]map[string]any, bool) {
	base, err := a.baseURL()
	if err != nil {
		return nil, false
	}
	endpoint := fmt.Sprintf("%s/locations/%d/latest", base, locationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, false
	}
	req.Header.Set("X-API-Key", a.APIKey)

	resp, err := a.Client.Do(req)
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
