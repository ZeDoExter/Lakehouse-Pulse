#!/usr/bin/env bash
set -uo pipefail

while true; do
  mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars
  if /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /opt/spark/jobs/transform.py; then
    sleep "${SPARK_JOB_INTERVAL_SECONDS:-3600}"
  else
    sleep 30
  fi
done
