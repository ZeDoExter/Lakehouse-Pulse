package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"lakehousepulse/mcp-server/tools"
)

type config struct {
	ListenAddr       string
	ToolTimeout      time.Duration
	HiveQueryAPIURL  string
	OpenAQURL        string
	OpenAQAPIKey     string
	OpenAQCountryID  int
	OpenAQCountry    string
	PrometheusURL    string
	KafkaLagPromQL   string
	NamenodeJMXURL   string
	SparkMasterUIURL string
	SparkSubmitURL   string
	SparkMaster      string
	SparkPythonJob   string
}

type serverMetrics struct {
	mu              sync.Mutex
	totalCalls      uint64
	errorCalls      uint64
	toolCalls       map[string]uint64
	toolErrorCalls  map[string]uint64
	totalDurationMs float64
}

type server struct {
	cfg     config
	logger  *log.Logger
	metrics *serverMetrics

	hive       *tools.HiveTool
	status     *tools.StatusTool
	airQuality *tools.AirQualityTool
	spark      *tools.SparkTool
}

func main() {
	cfg := loadConfig()
	logger := log.New(os.Stdout, "mcp-server ", log.LstdFlags|log.LUTC)
	metrics := &serverMetrics{
		toolCalls:      map[string]uint64{},
		toolErrorCalls: map[string]uint64{},
	}

	promTool := tools.NewPrometheusTool(cfg.PrometheusURL, cfg.ToolTimeout)
	srv := &server{
		cfg:     cfg,
		logger:  logger,
		metrics: metrics,
		hive:    tools.NewHiveTool(cfg.HiveQueryAPIURL, cfg.ToolTimeout),
		status:  tools.NewStatusTool(promTool, cfg.KafkaLagPromQL, cfg.NamenodeJMXURL, cfg.SparkMasterUIURL, cfg.ToolTimeout),
		airQuality: tools.NewAirQualityTool(
			cfg.OpenAQURL,
			cfg.OpenAQAPIKey,
			cfg.OpenAQCountryID,
			cfg.OpenAQCountry,
			cfg.ToolTimeout,
		),
		spark: tools.NewSparkTool(cfg.SparkSubmitURL, cfg.SparkMaster, cfg.SparkPythonJob, cfg.ToolTimeout),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", srv.handleHealthz)
	mux.HandleFunc("/metrics", srv.handleMetrics)
	mux.HandleFunc("/tools/query_hive", srv.handleQueryHive)
	mux.HandleFunc("/tools/get_pipeline_status", srv.handlePipelineStatus)
	mux.HandleFunc("/tools/get_air_quality_latest", srv.handleAirQualityLatest)
	mux.HandleFunc("/tools/trigger_spark_job", srv.handleTriggerSparkJob)

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Printf("starting on %s", cfg.ListenAddr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatalf("server failed: %v", err)
	}
}

func (s *server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(s.metrics.render()))
}

func (s *server) handleQueryHive(w http.ResponseWriter, r *http.Request) {
	s.handleTool("query_hive", w, r, func() (any, error) {
		var req struct {
			SQL string `json:"sql"`
		}
		if err := decodeJSON(r, &req); err != nil {
			return nil, err
		}
		if strings.TrimSpace(req.SQL) == "" {
			return nil, fmt.Errorf("field sql is required")
		}
		return s.hive.Query(r.Context(), req.SQL)
	})
}

func (s *server) handlePipelineStatus(w http.ResponseWriter, r *http.Request) {
	s.handleTool("get_pipeline_status", w, r, func() (any, error) {
		return s.status.GetPipelineStatus(r.Context())
	})
}

func (s *server) handleAirQualityLatest(w http.ResponseWriter, r *http.Request) {
	s.handleTool("get_air_quality_latest", w, r, func() (any, error) {
		var req struct {
			City string `json:"city"`
		}
		if err := decodeJSON(r, &req); err != nil {
			return nil, err
		}
		if strings.TrimSpace(req.City) == "" {
			return nil, fmt.Errorf("field city is required")
		}
		return s.airQuality.LatestByCity(r.Context(), req.City)
	})
}

func (s *server) handleTriggerSparkJob(w http.ResponseWriter, r *http.Request) {
	s.handleTool("trigger_spark_job", w, r, func() (any, error) {
		var req struct {
			Job  string `json:"job"`
			Date string `json:"date"`
		}
		if err := decodeJSON(r, &req); err != nil {
			return nil, err
		}
		if strings.TrimSpace(req.Job) == "" {
			return nil, fmt.Errorf("field job is required")
		}
		if strings.TrimSpace(req.Date) == "" {
			return nil, fmt.Errorf("field date is required")
		}
		return s.spark.Trigger(r.Context(), req.Job, req.Date)
	})
}

func (s *server) handleTool(name string, w http.ResponseWriter, r *http.Request, execFn func() (any, error)) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	start := time.Now()
	response, err := execFn()
	durationMs := float64(time.Since(start).Milliseconds())
	s.metrics.observe(name, durationMs, err != nil)

	if err != nil {
		s.logger.Printf("tool=%s status=error err=%q", name, err)
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func decodeJSON(r *http.Request, target any) error {
	if r.Body == nil {
		return fmt.Errorf("request body is required")
	}
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return fmt.Errorf("invalid json body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (m *serverMetrics) observe(tool string, durationMs float64, isError bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalCalls++
	m.toolCalls[tool]++
	m.totalDurationMs += durationMs
	if isError {
		m.errorCalls++
		m.toolErrorCalls[tool]++
	}
}

func (m *serverMetrics) render() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b strings.Builder
	b.WriteString("# HELP lakehouse_pulse_mcp_tool_calls_total Total number of MCP tool calls.\n")
	b.WriteString("# TYPE lakehouse_pulse_mcp_tool_calls_total counter\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_mcp_tool_calls_total %d\n", m.totalCalls))

	b.WriteString("# HELP lakehouse_pulse_mcp_tool_errors_total Total number of MCP tool call errors.\n")
	b.WriteString("# TYPE lakehouse_pulse_mcp_tool_errors_total counter\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_mcp_tool_errors_total %d\n", m.errorCalls))

	b.WriteString("# HELP lakehouse_pulse_mcp_tool_duration_ms_sum Total duration of MCP tool calls in ms.\n")
	b.WriteString("# TYPE lakehouse_pulse_mcp_tool_duration_ms_sum counter\n")
	b.WriteString(fmt.Sprintf("lakehouse_pulse_mcp_tool_duration_ms_sum %.0f\n", m.totalDurationMs))

	for tool, count := range m.toolCalls {
		b.WriteString(fmt.Sprintf("lakehouse_pulse_mcp_tool_calls_by_tool_total{tool=%q} %d\n", tool, count))
	}
	for tool, count := range m.toolErrorCalls {
		b.WriteString(fmt.Sprintf("lakehouse_pulse_mcp_tool_errors_by_tool_total{tool=%q} %d\n", tool, count))
	}
	return b.String()
}

func loadConfig() config {
	return config{
		ListenAddr:       getEnv("MCP_LISTEN_ADDR", ":8084"),
		ToolTimeout:      getDuration("MCP_TOOL_TIMEOUT", 15*time.Second),
		HiveQueryAPIURL:  getEnv("HIVE_QUERY_API_URL", ""),
		OpenAQURL:        getEnv("OPENAQ_URL", "https://api.openaq.org/v3"),
		OpenAQAPIKey:     getEnv("OPENAQ_API_KEY", ""),
		OpenAQCountryID:  getInt("OPENAQ_COUNTRY_ID", 111),
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
