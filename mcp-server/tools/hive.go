package tools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beltran/gohive"
)

type HiveTool struct {
	host     string
	port     int
	username string
	database string
	timeout  time.Duration
	maxRows  int
	readOnly bool
}

type QueryHiveResponse struct {
	Rows          []map[string]any `json:"rows"`
	ExecutionTime string           `json:"execution_time"`
}

var allowedPrefixes = []string{"select", "with", "show", "describe", "explain"}

func NewHiveTool(host string, port int, username, database string, timeout time.Duration, maxRows int, readOnly bool) *HiveTool {
	if maxRows <= 0 {
		maxRows = 500
	}
	return &HiveTool{
		host:     host,
		port:     port,
		username: username,
		database: database,
		timeout:  timeout,
		maxRows:  maxRows,
		readOnly: readOnly,
	}
}

func (h *HiveTool) Query(ctx context.Context, sql string) (QueryHiveResponse, error) {
	if h.host == "" {
		return QueryHiveResponse{}, fmt.Errorf("query_hive is not configured: set HIVE_HOST")
	}
	clean := strings.TrimSpace(sql)
	if h.readOnly {
		lower := strings.ToLower(clean)
		allowed := false
		for _, prefix := range allowedPrefixes {
			if strings.HasPrefix(lower, prefix) {
				allowed = true
				break
			}
		}
		if !allowed {
			return QueryHiveResponse{}, fmt.Errorf("only read-only SQL is allowed (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN)")
		}
	}

	queryCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	started := time.Now()
	cfg := gohive.NewConnectConfiguration()
	cfg.Database = h.database
	cfg.Username = h.username

	conn, err := gohive.Connect(h.host, h.port, "NOSASL", cfg)
	if err != nil {
		return QueryHiveResponse{}, fmt.Errorf("connect to hive-server2 %s:%d: %w", h.host, h.port, err)
	}
	defer conn.Close()

	cursor := conn.Cursor()
	defer cursor.Close()

	cursor.Exec(queryCtx, clean)
	if cursor.Err != nil {
		return QueryHiveResponse{}, fmt.Errorf("hive exec: %w", cursor.Err)
	}

	rows := make([]map[string]any, 0)
	for cursor.HasMore(queryCtx) && len(rows) < h.maxRows {
		row := cursor.RowMap(queryCtx)
		if cursor.Err != nil {
			break
		}
		rows = append(rows, row)
	}

	return QueryHiveResponse{
		Rows:          rows,
		ExecutionTime: time.Since(started).String(),
	}, nil
}
