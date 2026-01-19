import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable


@dataclass(frozen=True)
class Session:
    session_id: int
    started_at: str
    description: str | None
    total_depth: int
    status: str


class SessionRepository:
    """
    Repo do zarządzania sesją, ale w trybie:
    - w trakcie batcha: stan trzymasz w pamięci
    - na końcu batcha: persistujesz całość (session + joby) w 1 transakcji
    Zakłada tabele:
      - analysis_sessions(session_id, started_at, description, total_depth, status)
      - analysis_session_jobs(session_id, job_id, depth)
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        return con

    # --- persist whole session at end ---

    def create_session_with_jobs(
        self,
        description: str | None,
        total_depth: int,
        jobs_depth: dict[int, int],
        status: str = "DONE",
        started_at: str | None = None,
    ) -> int:
        """
        Tworzy sesję i przypisuje do niej joby z ich depth.
        Robi wszystko w JEDNEJ transakcji.
        Zwraca session_id.

        jobs_depth: {job_id: depth}
        status: DONE | FAILED | RUNNING (najczęściej DONE/FAILED na końcu)
        """
        if not jobs_depth:
            raise ValueError("jobs_depth is empty - cannot create empty session")

        con = self._connect()
        try:
            con.execute("BEGIN")

            started = started_at or datetime.now().isoformat(timespec="seconds")

            con.execute(
                """
                INSERT INTO analysis_sessions(started_at, description, total_depth, status)
                VALUES (?, ?, ?, ?)
                """,
                (started, description, int(total_depth), status),
            )
            session_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])

            rows = [(session_id, int(job_id), int(depth)) for job_id, depth in jobs_depth.items()]
            con.executemany(
                """
                INSERT INTO analysis_session_jobs(session_id, job_id, depth)
                VALUES (?, ?, ?)
                """,
                rows,
            )

            con.commit()
            return session_id

        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    # --- read helpers (pod PDF / debug) ---

    def get_session(self, session_id: int) -> Session | None:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT * FROM analysis_sessions WHERE session_id=?",
                (int(session_id),),
            ).fetchone()
            if not row:
                return None
            return Session(
                session_id=int(row["session_id"]),
                started_at=str(row["started_at"]),
                description=row["description"],
                total_depth=int(row["total_depth"]),
                status=str(row["status"]),
            )
        finally:
            con.close()

    def get_session_job_depths(self, session_id: int) -> dict[int, int]:
        con = self._connect()
        try:
            rows = con.execute(
                """
                SELECT job_id, depth
                FROM analysis_session_jobs
                WHERE session_id=?
                ORDER BY job_id ASC
                """,
                (int(session_id),),
            ).fetchall()
            return {int(r["job_id"]): int(r["depth"]) for r in rows}
        finally:
            con.close()

    def get_session_job_ids(self, session_id: int) -> list[int]:
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT job_id FROM analysis_session_jobs WHERE session_id=? ORDER BY job_id ASC",
                (int(session_id),),
            ).fetchall()
            return [int(r["job_id"]) for r in rows]
        finally:
            con.close()
