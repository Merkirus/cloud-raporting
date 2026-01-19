import sqlite3
from pathlib import Path
from typing import Any, Mapping

REQUIRED_KEYS = {
    "job_id", "worker_id", "timestamp",
    "method", "endpoint", "status_code",
    "latency_ms", "ttfb_ms",
    "response_size_bytes", "error_msg",
    "scenario_step", "is_success",
}

def init_db(db_path: str, schema_path: str = "schema.sql") -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            con.executescript(f.read())
        con.commit()
    finally:
        con.close()

def insert_raw_result(db_path: str, dto: Mapping[str, Any]) -> None:
    missing = REQUIRED_KEYS - set(dto.keys())
    if missing:
        raise ValueError(f"Missing keys in dto: {sorted(missing)}")

    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO request_results (
              job_id, worker_id, timestamp,
              method, endpoint, status_code,
              latency_ms, ttfb_ms,
              response_size_bytes, error_msg,
              scenario_step, is_success
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(dto["job_id"]),
                int(dto["worker_id"]),
                str(dto["timestamp"]),
                str(dto["method"]),
                str(dto["endpoint"]),
                int(dto["status_code"]),
                float(dto["latency_ms"]),
                (float(dto["ttfb_ms"]) if dto["ttfb_ms"] is not None else None),
                int(dto["response_size_bytes"]),
                (str(dto["error_msg"]) if dto["error_msg"] is not None else None),
                int(dto["scenario_step"]),
                1 if bool(dto["is_success"]) else 0,
            ),
        )
        con.commit()
    finally:
        con.close()
