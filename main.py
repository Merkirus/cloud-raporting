import os
import sqlite3
import json
from datetime import datetime
from aggregates_sessions import compute_session_aggregates
from report_pdf import generate_pdf_for_sessions

from storage import init_db, insert_raw_result
from session_repo import SessionRepository


DB_PATH = os.getenv("REPORT_DB", "data/perf.db")
SCHEMA_PATH = os.getenv("REPORT_SCHEMA", "schema.sql")
INPUT_JSON = os.getenv("INPUT_JSON", "sample.json")

SESSION_DESC = os.getenv("SESSION_DESC", "Local ingest test")
SESSION_TOTAL_DEPTH = int(os.getenv("SESSION_TOTAL_DEPTH", "1"))

def load_jobs(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("INPUT_JSON must contain a JSON array (list) of DTO objects")
    return data


def main() -> None:
    init_db(DB_PATH, SCHEMA_PATH)

    jobs = load_jobs(INPUT_JSON)

    jobs_depth: dict[int, int] = {}

    for dto in jobs:
        insert_raw_result(DB_PATH, dto)

        jid = int(dto["job_id"])
        jobs_depth[jid] = jobs_depth.get(jid, 0) + 1

    repo = SessionRepository(DB_PATH)
    session_id = repo.create_session_with_jobs(
        description=SESSION_DESC,
        total_depth=SESSION_TOTAL_DEPTH,
        jobs_depth=jobs_depth,
        status="DONE",
        started_at=datetime.now().isoformat(timespec="seconds"),
    )

    print(f"[OK] Inserted RAW rows: {len(jobs)}")
    print(f"[OK] Created analysis session: session_id={session_id}")
    print(f"[OK] Session jobs: {len(jobs_depth)} (unique job_id)")

    compute_session_aggregates(DB_PATH, session_ids=None, bucket_seconds=10)
    generate_pdf_for_sessions(DB_PATH, session_ids=None, out_path=f'raport_{session_id}.pdf')

if __name__ == "__main__":
    main()
