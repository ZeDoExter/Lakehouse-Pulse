package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"lakehousepulse/mcp-server/tools"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type mcpApp struct {
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
		hive:   tools.NewHiveTool(cfg.HiveHost, cfg.HivePort, cfg.HiveUsername, cfg.HiveDatabase, cfg.ToolTimeout, cfg.HiveMaxRows, cfg.HiveReadOnly),
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
