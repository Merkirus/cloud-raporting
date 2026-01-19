import sqlite3
from typing import Iterable


def _percentile(sorted_vals: list[float], p: float) -> float | None:
    """
    p w zakresie 0..100
    Metoda liniowej interpolacji (prosta i stabilna do raportów).
    """
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return float(sorted_vals[0])

    # rank w [0, n-1]
    r = (p / 100.0) * (n - 1)
    lo = int(r)
    hi = min(lo + 1, n - 1)
    frac = r - lo
    return float(sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac)


def _normalize_session_ids(con: sqlite3.Connection, session_ids) -> list[int]:
    if session_ids is None or session_ids == []:
        rows = con.execute("SELECT session_id FROM analysis_sessions ORDER BY session_id ASC").fetchall()
        return [int(r[0]) for r in rows]
    if isinstance(session_ids, int):
        return [int(session_ids)]
    return [int(x) for x in session_ids]


def _iter_raw_for_session(con: sqlite3.Connection, session_id: int) -> list[sqlite3.Row]:
    return con.execute(
        """
        SELECT rr.*
        FROM request_results rr
        JOIN analysis_session_jobs sj ON sj.job_id = rr.job_id
        WHERE sj.session_id = ?
        """,
        (session_id,),
    ).fetchall()


def _clear_session_aggregates(con: sqlite3.Connection, session_id: int, bucket_seconds: int) -> None:
    con.execute("DELETE FROM session_summary WHERE session_id=? AND bucket_seconds=?", (session_id, bucket_seconds))
    con.execute("DELETE FROM session_endpoint_summary WHERE session_id=?", (session_id,))
    con.execute(
        "DELETE FROM session_timeseries_summary WHERE session_id=? AND bucket_seconds=?",
        (session_id, bucket_seconds),
    )


def compute_session_aggregates(
    db_path: str,
    session_ids: int | list[int] | None = None,
    bucket_seconds: int = 10,
) -> list[int]:
    """
    Liczy agregaty DLA SESJI (batch) na bazie request_results + analysis_session_jobs.

    session_ids:
      - None lub [] -> wszystkie sesje
      - int -> jedna sesja
      - list[int] -> wiele sesji

    Zwraca listę session_id, które policzono.
    """
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        sids = _normalize_session_ids(con, session_ids)

        for sid in sids:
            rows = _iter_raw_for_session(con, sid)
            if not rows:
                # brak danych RAW dla tej sesji -> pomijamy
                continue

            _clear_session_aggregates(con, sid, bucket_seconds)

            total = len(rows)
            success = sum(1 for r in rows if int(r["is_success"]) == 1)

            status_2xx = sum(1 for r in rows if 200 <= int(r["status_code"]) < 300)
            status_4xx = sum(1 for r in rows if 400 <= int(r["status_code"]) < 500)
            status_5xx = sum(1 for r in rows if 500 <= int(r["status_code"]) < 600)

            lat = [float(r["latency_ms"]) for r in rows if r["latency_ms"] is not None]
            ttfb = [float(r["ttfb_ms"]) for r in rows if r["ttfb_ms"] is not None]

            lat.sort()
            ttfb.sort()

            latency_avg = (sum(lat) / len(lat)) if lat else None
            ttfb_avg = (sum(ttfb) / len(ttfb)) if ttfb else None

            latency_p50 = _percentile(lat, 50)
            latency_p90 = _percentile(lat, 90)
            latency_p95 = _percentile(lat, 95)
            latency_p99 = _percentile(lat, 99)
            ttfb_p95 = _percentile(ttfb, 95)

            success_rate = (success / total) if total else 0.0

            # ---- session_summary UPSERT
            con.execute(
                """
                INSERT INTO session_summary(
                  session_id, bucket_seconds,
                  total_requests, success_requests, success_rate,
                  status_2xx, status_4xx, status_5xx,
                  latency_avg, latency_p50, latency_p90, latency_p95, latency_p99,
                  ttfb_avg, ttfb_p95
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(session_id) DO UPDATE SET
                  bucket_seconds=excluded.bucket_seconds,
                  total_requests=excluded.total_requests,
                  success_requests=excluded.success_requests,
                  success_rate=excluded.success_rate,
                  status_2xx=excluded.status_2xx,
                  status_4xx=excluded.status_4xx,
                  status_5xx=excluded.status_5xx,
                  latency_avg=excluded.latency_avg,
                  latency_p50=excluded.latency_p50,
                  latency_p90=excluded.latency_p90,
                  latency_p95=excluded.latency_p95,
                  latency_p99=excluded.latency_p99,
                  ttfb_avg=excluded.ttfb_avg,
                  ttfb_p95=excluded.ttfb_p95
                """,
                (
                    sid, bucket_seconds,
                    total, success, success_rate,
                    status_2xx, status_4xx, status_5xx,
                    latency_avg, latency_p50, latency_p90, latency_p95, latency_p99,
                    ttfb_avg, ttfb_p95,
                ),
            )

            # ---- endpoint_summary (per endpoint+method)
            # grupowanie w Pythonie (szybkie na MVP)
            by_ep: dict[tuple[str, str], list[sqlite3.Row]] = {}
            for r in rows:
                key = (str(r["endpoint"]), str(r["method"]))
                by_ep.setdefault(key, []).append(r)

            for (endpoint, method), grp in by_ep.items():
                cnt = len(grp)
                succ = sum(1 for r in grp if int(r["is_success"]) == 1)
                sr = (succ / cnt) if cnt else 0.0
                s5 = sum(1 for r in grp if 500 <= int(r["status_code"]) < 600)

                lat2 = [float(r["latency_ms"]) for r in grp if r["latency_ms"] is not None]
                lat2.sort()
                lat_avg = (sum(lat2) / len(lat2)) if lat2 else None
                lat_p95 = _percentile(lat2, 95)
                lat_p99 = _percentile(lat2, 99)

                con.execute(
                    """
                    INSERT INTO session_endpoint_summary(
                      session_id, endpoint, method,
                      count, success_rate, status_5xx,
                      latency_avg, latency_p95, latency_p99
                    )
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(session_id, endpoint, method) DO UPDATE SET
                      count=excluded.count,
                      success_rate=excluded.success_rate,
                      status_5xx=excluded.status_5xx,
                      latency_avg=excluded.latency_avg,
                      latency_p95=excluded.latency_p95,
                      latency_p99=excluded.latency_p99
                    """,
                    (sid, endpoint, method, cnt, sr, s5, lat_avg, lat_p95, lat_p99),
                )

            # ---- timeseries (overall) — bucket po timestamp
            # Uwaga: zakładam ISO timestamp; bucket robimy po sekundach: floor(epoch/bucket)*bucket
            # Żeby nie robić parse datetime na każdym rekordzie (MVP), użyjemy SQL strftime('%s', ...)
            ts_rows = con.execute(
                """
                SELECT
                  datetime((strftime('%s', rr.timestamp) / ?) * ?, 'unixepoch') AS bucket_start,
                  COUNT(*) AS count,
                  AVG(rr.latency_ms) AS latency_avg,
                  SUM(CASE WHEN rr.is_success=1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS success_rate,
                  SUM(CASE WHEN rr.status_code BETWEEN 500 AND 599 THEN 1 ELSE 0 END) AS status_5xx
                FROM request_results rr
                JOIN analysis_session_jobs sj ON sj.job_id = rr.job_id
                WHERE sj.session_id = ?
                GROUP BY bucket_start
                ORDER BY bucket_start ASC
                """,
                (bucket_seconds, bucket_seconds, sid),
            ).fetchall()

            # p95 w bucketach liczymy w Pythonie (bo SQLite nie ma percentyla)
            for tr in ts_rows:
                bucket_start = tr["bucket_start"]

                bucket_lat = [
                    float(r["latency_ms"]) for r in con.execute(
                        """
                        SELECT rr.latency_ms
                        FROM request_results rr
                        JOIN analysis_session_jobs sj ON sj.job_id = rr.job_id
                        WHERE sj.session_id = ?
                          AND datetime((strftime('%s', rr.timestamp) / ?) * ?, 'unixepoch') = ?
                          AND rr.latency_ms IS NOT NULL
                        """,
                        (sid, bucket_seconds, bucket_seconds, bucket_start),
                    ).fetchall()
                ]
                bucket_lat.sort()
                bucket_p95 = _percentile(bucket_lat, 95)

                con.execute(
                    """
                    INSERT INTO session_timeseries_summary(
                      session_id, bucket_seconds, bucket_start,
                      count, success_rate, status_5xx,
                      latency_avg, latency_p95
                    )
                    VALUES (?,?,?,?,?,?,?,?)
                    ON CONFLICT(session_id, bucket_seconds, bucket_start) DO UPDATE SET
                      count=excluded.count,
                      success_rate=excluded.success_rate,
                      status_5xx=excluded.status_5xx,
                      latency_avg=excluded.latency_avg,
                      latency_p95=excluded.latency_p95
                    """,
                    (
                        sid, bucket_seconds, bucket_start,
                        int(tr["count"]), float(tr["success_rate"]), int(tr["status_5xx"]),
                        None if tr["latency_avg"] is None else float(tr["latency_avg"]),
                        bucket_p95,
                    ),
                )

        con.commit()
        return sids
    finally:
        con.close()
