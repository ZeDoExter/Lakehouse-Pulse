package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	ListenAddr       string
	ToolTimeout      time.Duration
	HiveHost         string
	HivePort         int
	HiveUsername     string
	HiveDatabase     string
	HiveMaxRows      int
	HiveReadOnly     bool
	OpenAQURL        string
	OpenAQAPIKey     string
	OpenAQCountryID  int
	OpenAQLimit      int
	OpenAQCountry    string
	PrometheusURL    string
	KafkaLagPromQL   string
	NamenodeJMXURL   string
	SparkMasterUIURL string
	SparkSubmitURL   string
	SparkMaster      string
	SparkPythonJob   string
}

func loadConfig() config {
	return config{
		ListenAddr:       getEnv("MCP_LISTEN_ADDR", ":8084"),
		ToolTimeout:      getDuration("MCP_TOOL_TIMEOUT", 15*time.Second),
		HiveHost:         getEnv("HIVE_HOST", "hive-server2"),
		HivePort:         getInt("HIVE_PORT", 10000),
		HiveUsername:     getEnv("HIVE_USERNAME", "root"),
		HiveDatabase:     getEnv("HIVE_DATABASE", "default"),
		HiveMaxRows:      getInt("HIVE_MAX_ROWS", 500),
		HiveReadOnly:     getBool("HIVE_READ_ONLY", true),
		OpenAQURL:        getEnv("OPENAQ_URL", "https://api.openaq.org/v3"),
		OpenAQAPIKey:     getEnv("OPENAQ_API_KEY", ""),
		OpenAQCountryID:  getInt("OPENAQ_COUNTRY_ID", 111),
		OpenAQLimit:      getInt("OPENAQ_LOCATION_LIMIT", 3),
		OpenAQCountry:    getEnv("OPENAQ_COUNTRY", "TH"),
		PrometheusURL:    getEnv("PROMETHEUS_URL", "http://prometheus:9090"),
		KafkaLagPromQL:   getEnv("KAFKA_LAG_PROMQL", "max(lakehouse_pulse_ingestion_consecutive_failures)"),
		NamenodeJMXURL:   getEnv("NAMENODE_JMX_URL", "http://namenode:9870/jmx"),
		SparkMasterUIURL: getEnv("SPARK_MASTER_UI_URL", "http://spark-master:8080"),
		SparkSubmitURL:   getEnv("SPARK_SUBMIT_URL", "http://spark-master:6066/v1/submissions/create"),
		SparkMaster:      getEnv("SPARK_MASTER", "spark://spark-master:7077"),
		SparkPythonJob:   getEnv("SPARK_PYTHON_JOB", "file:/opt/spark/jobs/transform.py"),
	}
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getDuration(key string, fallback time.Duration) time.Duration {
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

func getInt(key string, fallback int) int {
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

func getBool(key string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes"
}
