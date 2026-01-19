import sqlite3
from typing import Any


REQUIRED_KEYS = [
    "job_id", "worker_id", "timestamp",
    "method", "endpoint", "status_code",
    "latency_ms", "ttfb_ms",
    "response_size_bytes", "error_msg",
    "scenario_step", "is_success",
]


class JobRepository:
    """
    Repo zapisujące 'job' w Waszym rozumieniu = pojedynczy event requestu (DTO).
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def insert_job(self, dto: dict[str, Any]) -> None:
        # lekka walidacja
        missing = [k for k in REQUIRED_KEYS if k not in dto]
        if missing:
            raise ValueError(f"Missing keys in DTO: {missing}")

        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                """
                INSERT INTO request_results(
                  job_id, worker_id, timestamp,
                  method, endpoint, status_code,
                  latency_ms, ttfb_ms,
                  response_size_bytes, error_msg,
                  scenario_step, is_success
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(dto["job_id"]),
                    int(dto["worker_id"]),
                    str(dto["timestamp"]),
                    str(dto["method"]),
                    str(dto["endpoint"]),
                    int(dto["status_code"]),
                    None if dto["latency_ms"] is None else float(dto["latency_ms"]),
                    None if dto["ttfb_ms"] is None else float(dto["ttfb_ms"]),
                    None if dto["response_size_bytes"] is None else int(dto["response_size_bytes"]),
                    dto["error_msg"],  # None albo string
                    None if dto["scenario_step"] is None else int(dto["scenario_step"]),
                    1 if bool(dto["is_success"]) else 0,
                ),
            )
            con.commit()
        finally:
            con.close()
