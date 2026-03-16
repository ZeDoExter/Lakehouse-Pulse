package main

import (
	"fmt"
	"strings"
	"sync"
)

type serverMetrics struct {
	mu              sync.Mutex
	totalCalls      uint64
	errorCalls      uint64
	toolCalls       map[string]uint64
	toolErrorCalls  map[string]uint64
	totalDurationMs float64
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
