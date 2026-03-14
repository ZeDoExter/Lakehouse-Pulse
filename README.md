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

## URLs

- Grafana: `http://localhost:3000` (`admin/admin`)
- Prometheus: `http://localhost:9090`
- Spark: `http://localhost:8080`
- HDFS NameNode: `http://localhost:9870`

## Smoke Test

```bash
curl -s http://localhost:8082/healthz
curl -s http://localhost:8084/healthz

curl -s -X POST http://localhost:8084/tools/get_air_quality_latest \
  -H 'Content-Type: application/json' -d '{"city":"Bangkok"}'

curl -s -X POST http://localhost:8084/tools/get_pipeline_status \
  -H 'Content-Type: application/json' -d '{}'

curl -s -X POST http://localhost:8084/tools/query_hive \
  -H 'Content-Type: application/json' -d '{"sql":"SHOW DATABASES"}'
```

## Demo Checklist

- [ ] Core services are `Up` in `docker compose ps`
- [ ] Ingestion has successful runs (`/metrics` non-zero)
- [ ] `get_air_quality_latest` returns Bangkok values
- [ ] `query_hive` returns rows
- [ ] Prometheus targets are `UP`
- [ ] Grafana `Air Quality Overview` shows values
