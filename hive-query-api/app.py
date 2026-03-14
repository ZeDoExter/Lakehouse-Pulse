import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from flask import Flask, jsonify, request
from pyhive import hive
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    hive_host: str = "hive-server2"
    hive_port: int = 10000
    hive_username: str = "root"
    hive_database: str = "default"
    hive_query_timeout_seconds: int = 30
    hive_max_rows: int = 500
    hive_read_only: bool = True

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


SETTINGS = Settings()
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
    started = time.time()
    connection = hive.Connection(
        host=SETTINGS.hive_host,
        port=SETTINGS.hive_port,
        username=SETTINGS.hive_username,
        database=SETTINGS.hive_database,
    )
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(f"SET hive.server2.idle.operation.timeout={SETTINGS.hive_query_timeout_seconds * 1000}")
            cursor.execute(sql)
            if cursor.description is None:
                rows: list[dict[str, Any]] = []
            else:
                columns = [column[0] for column in cursor.description]
                fetched = cursor.fetchmany(SETTINGS.hive_max_rows)
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
        clean_sql = validate_sql(sql, SETTINGS.hive_read_only)
        result = execute_query(clean_sql)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(result)


if __name__ == "__main__":
    APP.run(host="0.0.0.0", port=8085)
