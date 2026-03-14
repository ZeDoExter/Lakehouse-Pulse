import os
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from flask import Flask, jsonify, request
from pyhive import hive


def env(name: str, default: str) -> str:
    value = os.getenv(name, "").strip()
    return value if value else default


def env_int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


ALLOWED_PREFIXES = ("select", "with", "show", "describe", "explain")

APP = Flask(__name__)


def json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def validate_sql(sql: str, read_only: bool) -> str:
    clean = sql.strip()
    if not clean:
        raise ValueError("field sql is required")
    if read_only and not clean.lower().startswith(ALLOWED_PREFIXES):
        raise ValueError("only read-only SQL is allowed by default")
    return clean


def execute_query(sql: str) -> dict[str, Any]:
    host = env("HIVE_HOST", "hive-server2")
    port = env_int("HIVE_PORT", 10000)
    username = env("HIVE_USERNAME", "root")
    database = env("HIVE_DATABASE", "default")
    timeout = env_int("HIVE_QUERY_TIMEOUT_SECONDS", 30)
    max_rows = env_int("HIVE_MAX_ROWS", 500)

    started = time.time()
    connection = hive.Connection(host=host, port=port, username=username, database=database)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(f"SET hive.server2.idle.operation.timeout={timeout * 1000}")
            cursor.execute(sql)
            if cursor.description is None:
                rows: list[dict[str, Any]] = []
            else:
                columns = [column[0] for column in cursor.description]
                fetched = cursor.fetchmany(max_rows)
                rows = [{columns[i]: json_safe(value) for i, value in enumerate(row)} for row in fetched]
        finally:
            cursor.close()
    finally:
        connection.close()

    execution_time = f"{time.time() - started:.3f}s"
    return {"rows": rows, "execution_time": execution_time}


@APP.get("/healthz")
def healthz() -> Any:
    return jsonify({"status": "ok"})


@APP.post("/query")
def query() -> Any:
    body = request.get_json(silent=True) or {}
    sql = body.get("sql", "")
    try:
        clean_sql = validate_sql(sql, env_bool("HIVE_READ_ONLY", True))
        result = execute_query(clean_sql)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(result)


if __name__ == "__main__":
    APP.run(host="0.0.0.0", port=8085)
