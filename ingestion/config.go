package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	ListenAddr      string
	OpenAQURL       string
	OpenAQAPIKey    string
	OpenAQCountryID int
	OpenAQLimit     int
	OpenAQMaxReqs   int
	OpenAQCity      string
	Country         string
	KafkaBrokers    []string
	KafkaTopic      string
	PollInterval    time.Duration
	HTTPTimeout     time.Duration
	MaxRetries      int
	InitialBackoff  time.Duration
	ShutdownTimeout time.Duration
}

func loadConfig() config {
	return config{
		ListenAddr:      getEnv("LISTEN_ADDR", ":8082"),
		OpenAQURL:       getEnv("OPENAQ_URL", "https://api.openaq.org/v3"),
		OpenAQAPIKey:    getEnv("OPENAQ_API_KEY", ""),
		OpenAQCountryID: getIntEnv("OPENAQ_COUNTRY_ID", 111),
		OpenAQLimit:     getIntEnv("OPENAQ_LOCATION_LIMIT", 3),
		OpenAQMaxReqs:   getIntEnv("OPENAQ_MAX_REQUESTS", 10),
		OpenAQCity:      getEnv("OPENAQ_CITY", ""),
		Country:         getEnv("OPENAQ_COUNTRY", "TH"),
		KafkaBrokers:    strings.Split(getEnv("KAFKA_BROKERS", "kafka:29092"), ","),
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
