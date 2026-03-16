# Lakehouse Pulse

Production-style lakehouse platform for Thailand air-quality data.

OpenAQ v3 -> Go ingestion -> Kafka -> Spark -> HDFS/Hive -> Prometheus/Grafana, with MCP tools for AI-callable operations.

## Quick Start

```bash
cp .env.example .env
# set OPENAQ_API_KEY in .env
docker compose up -d --build
docker compose ps
```

`OPENAQ_MAX_REQUESTS` defaults to `10` and caps OpenAQ HTTP calls per ingestion poll cycle.

## URLs

- Grafana: `http://localhost:3000` (`admin/admin`)
- Prometheus: `http://localhost:9090`
- Spark: `http://localhost:8080`
- HDFS NameNode: `http://localhost:9870`
- MCP SSE endpoint: `http://localhost:8084/sse`
- MCP message endpoint: `http://localhost:8084/message`

## MCP Usage (No curl for tools)

```bash
docker compose ps
```

Connect your MCP client to `http://localhost:8084/sse` and call tools:

- `get_air_quality_latest` with `{"city":"Bangkok"}`
- `get_pipeline_status` with `{}`
- `query_hive` with `{"sql":"SHOW DATABASES"}`
- `trigger_spark_job` with `{"job":"transform","date":"2026-03-14"}`

Legacy diagnostics endpoints still exist for ops/monitoring:

- `GET /healthz`
- `GET /metrics`

## Demo Checklist

- [ ] Core services are `Up` in `docker compose ps`
- [ ] Ingestion has successful runs (`ingestion /metrics` non-zero)
- [ ] MCP client connects to `http://localhost:8084/sse`
- [ ] `get_air_quality_latest` returns Bangkok values
- [ ] `query_hive` returns rows
- [ ] Prometheus targets are `UP`
- [ ] Grafana `Air Quality Overview` shows values
