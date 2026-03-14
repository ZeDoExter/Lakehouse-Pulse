package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"lakehousepulse/mcp-server/tools"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type config struct {
	ListenAddr       string
	ToolTimeout      time.Duration
	HiveQueryAPIURL  string
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

type mcpApp struct {
	cfg     config
	logger  *log.Logger
	metrics *serverMetrics

	hive       *tools.HiveTool
	status     *tools.StatusTool
	airQuality *tools.AirQualityTool
	spark      *tools.SparkTool
}

type serverMetrics struct {
	mu              sync.Mutex
	totalCalls      uint64
	errorCalls      uint64
	toolCalls       map[string]uint64
	toolErrorCalls  map[string]uint64
	totalDurationMs float64
}

func main() {
	cfg := loadConfig()
	logger := log.New(os.Stdout, "mcp-server ", log.LstdFlags|log.LUTC)
	app := newMCPApp(cfg, logger)
	s := app.newMCPServer()
	sseServer := server.NewSSEServer(s)

	mux := http.NewServeMux()
	mux.Handle("/sse", sseServer.SSEHandler())
	mux.Handle("/message", sseServer.MessageHandler())
	mux.HandleFunc("/healthz", app.handleHealthz)
	mux.HandleFunc("/metrics", app.handleMetrics)

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Printf("starting MCP SSE server on %s (endpoints: /sse, /message; diagnostics: /healthz, /metrics)", cfg.ListenAddr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatalf("server failed: %v", err)
	}
}

func newMCPApp(cfg config, logger *log.Logger) *mcpApp {
	promTool := tools.NewPrometheusTool(cfg.PrometheusURL, cfg.ToolTimeout)
	return &mcpApp{
		cfg:    cfg,
		logger: logger,
		metrics: &serverMetrics{
			toolCalls:      map[string]uint64{},
			toolErrorCalls: map[string]uint64{},
		},
		hive:   tools.NewHiveTool(cfg.HiveQueryAPIURL, cfg.ToolTimeout),
		status: tools.NewStatusTool(promTool, cfg.KafkaLagPromQL, cfg.NamenodeJMXURL, cfg.SparkMasterUIURL, cfg.ToolTimeout),
		airQuality: tools.NewAirQualityTool(
			cfg.OpenAQURL,
			cfg.OpenAQAPIKey,
			cfg.OpenAQCountryID,
			cfg.OpenAQLimit,
			cfg.OpenAQCountry,
			cfg.ToolTimeout,
		),
		spark: tools.NewSparkTool(cfg.SparkSubmitURL, cfg.SparkMaster, cfg.SparkPythonJob, cfg.ToolTimeout),
	}
}

func (a *mcpApp) newMCPServer() *server.MCPServer {
	s := server.NewMCPServer("lakehouse-pulse", "1.0.0")

	s.AddTool(mcp.NewTool("query_hive",
		mcp.WithDescription("Query Hive with SQL"),
		mcp.WithString("sql",
			mcp.Description("Hive SQL statement"),
			mcp.Required(),
		),
	), a.handleQueryHive)

	s.AddTool(mcp.NewTool("get_pipeline_status",
		mcp.WithDescription("Get pipeline health from Prometheus, HDFS, and Spark"),
	), a.handlePipelineStatus)

	s.AddTool(mcp.NewTool("get_air_quality_latest",
		mcp.WithDescription("Get latest PM2.5/PM10/CO readings for a city"),
		mcp.WithString("city",
			mcp.Description("City name, for example Bangkok"),
			mcp.Required(),
		),
	), a.handleAirQualityLatest)

	s.AddTool(mcp.NewTool("trigger_spark_job",
		mcp.WithDescription("Trigger Spark transform job by job name and date"),
		mcp.WithString("job",
			mcp.Description("Job name"),
			mcp.Required(),
		),
		mcp.WithString("date",
			mcp.Description("Business date in YYYY-MM-DD"),
			mcp.Required(),
		),
	), a.handleTriggerSparkJob)

	return s
}

func (a *mcpApp) handleQueryHive(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	sql, err := request.RequireString("sql")
	if err != nil {
		a.metrics.observe("query_hive", float64(time.Since(start).Milliseconds()), true)
		return nil, err
	}
	if strings.TrimSpace(sql) == "" {
		a.metrics.observe("query_hive", float64(time.Since(start).Milliseconds()), true)
		return nil, fmt.Errorf("argument sql is required")
	}
	result, err := a.hive.Query(ctx, sql)
	a.metrics.observe("query_hive", float64(time.Since(start).Milliseconds()), err != nil)
	if err != nil {
		a.logger.Printf("tool=query_hive status=error err=%q", err)
		return nil, err
	}
	return mcp.NewToolResultStructuredOnly(result), nil
}

func (a *mcpApp) handlePipelineStatus(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	result, err := a.status.GetPipelineStatus(ctx)
	a.metrics.observe("get_pipeline_status", float64(time.Since(start).Milliseconds()), err != nil)
	if err != nil {
		a.logger.Printf("tool=get_pipeline_status status=error err=%q", err)
		return nil, err
	}
	return mcp.NewToolResultStructuredOnly(result), nil
}

func (a *mcpApp) handleAirQualityLatest(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	city, err := request.RequireString("city")
	if err != nil {
		a.metrics.observe("get_air_quality_latest", float64(time.Since(start).Milliseconds()), true)
		return nil, err
	}
	if strings.TrimSpace(city) == "" {
		a.metrics.observe("get_air_quality_latest", float64(time.Since(start).Milliseconds()), true)
		return nil, fmt.Errorf("argument city is required")
	}
	result, err := a.airQuality.LatestByCity(ctx, city)
	a.metrics.observe("get_air_quality_latest", float64(time.Since(start).Milliseconds()), err != nil)
	if err != nil {
		a.logger.Printf("tool=get_air_quality_latest status=error err=%q", err)
		return nil, err
	}
	return mcp.NewToolResultStructuredOnly(result), nil
}

func (a *mcpApp) handleTriggerSparkJob(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	job, err := request.RequireString("job")
	if err != nil {
		a.metrics.observe("trigger_spark_job", float64(time.Since(start).Milliseconds()), true)
		return nil, err
	}
	date, err := request.RequireString("date")
	if err != nil {
		a.metrics.observe("trigger_spark_job", float64(time.Since(start).Milliseconds()), true)
		return nil, err
	}
	if strings.TrimSpace(job) == "" {
		a.metrics.observe("trigger_spark_job", float64(time.Since(start).Milliseconds()), true)
		return nil, fmt.Errorf("argument job is required")
	}
	if strings.TrimSpace(date) == "" {
		a.metrics.observe("trigger_spark_job", float64(time.Since(start).Milliseconds()), true)
		return nil, fmt.Errorf("argument date is required")
	}
	result, err := a.spark.Trigger(ctx, job, date)
	a.metrics.observe("trigger_spark_job", float64(time.Since(start).Milliseconds()), err != nil)
	if err != nil {
		a.logger.Printf("tool=trigger_spark_job status=error err=%q", err)
		return nil, err
	}
	return mcp.NewToolResultStructuredOnly(result), nil
}

func (a *mcpApp) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (a *mcpApp) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(a.metrics.render()))
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
