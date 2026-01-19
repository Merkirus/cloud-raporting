-- RAW
CREATE TABLE IF NOT EXISTS request_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id INTEGER NOT NULL,
  worker_id INTEGER NOT NULL,
  timestamp TEXT NOT NULL,          -- ISO 8601
  method TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  status_code INTEGER NOT NULL,

  latency_ms REAL NOT NULL,
  ttfb_ms REAL,
  response_size_bytes INTEGER NOT NULL,
  error_msg TEXT,
  scenario_step INTEGER NOT NULL,
  is_success INTEGER NOT NULL        -- 0/1
);

CREATE INDEX IF NOT EXISTS idx_rr_job ON request_results(job_id);
CREATE INDEX IF NOT EXISTS idx_rr_job_ep ON request_results(job_id, endpoint, method);
CREATE INDEX IF NOT EXISTS idx_rr_time ON request_results(timestamp);

-- 1) Sesja analizy (start batcha)
CREATE TABLE IF NOT EXISTS analysis_sessions (
  session_id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at TEXT NOT NULL,
  description TEXT,
  total_depth INTEGER NOT NULL,
  status TEXT NOT NULL            -- 'RUNNING' | 'DONE' | 'FAILED'
);

CREATE INDEX IF NOT EXISTS idx_sessions_status ON analysis_sessions(status);


-- 2) Tabela łącząca sesja <-> job
CREATE TABLE IF NOT EXISTS analysis_session_jobs (
  session_id INTEGER NOT NULL,
  job_id INTEGER NOT NULL,
  depth INTEGER NOT NULL DEFAULT 0,  -- liczba odebranych WIADOMOŚCI dla tego joba w tej sesji

  PRIMARY KEY (session_id, job_id),
  FOREIGN KEY (session_id) REFERENCES analysis_sessions(session_id)
);

CREATE INDEX IF NOT EXISTS idx_sess_jobs_session ON analysis_session_jobs(session_id);
CREATE INDEX IF NOT EXISTS idx_sess_jobs_depth ON analysis_session_jobs(session_id, depth);

-- ===== SESSION AGGREGATES =====

CREATE TABLE IF NOT EXISTS session_summary (
  session_id INTEGER PRIMARY KEY,
  bucket_seconds INTEGER NOT NULL,

  total_requests INTEGER NOT NULL,
  success_requests INTEGER NOT NULL,
  success_rate REAL NOT NULL,

  status_2xx INTEGER NOT NULL,
  status_4xx INTEGER NOT NULL,
  status_5xx INTEGER NOT NULL,

  latency_avg REAL,
  latency_p50 REAL,
  latency_p90 REAL,
  latency_p95 REAL,
  latency_p99 REAL,

  ttfb_avg REAL,
  ttfb_p95 REAL
);

CREATE TABLE IF NOT EXISTS session_endpoint_summary (
  session_id INTEGER NOT NULL,
  endpoint TEXT NOT NULL,
  method TEXT NOT NULL,

  count INTEGER NOT NULL,
  success_rate REAL NOT NULL,
  status_5xx INTEGER NOT NULL,

  latency_avg REAL,
  latency_p95 REAL,
  latency_p99 REAL,

  PRIMARY KEY (session_id, endpoint, method)
);

CREATE TABLE IF NOT EXISTS session_timeseries_summary (
  session_id INTEGER NOT NULL,
  bucket_seconds INTEGER NOT NULL,
  bucket_start TEXT NOT NULL,

  count INTEGER NOT NULL,
  success_rate REAL NOT NULL,
  status_5xx INTEGER NOT NULL,

  latency_avg REAL,
  latency_p95 REAL,

  PRIMARY KEY (session_id, bucket_seconds, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_sess_ep ON session_endpoint_summary(session_id);
CREATE INDEX IF NOT EXISTS idx_sess_ts ON session_timeseries_summary(session_id, bucket_seconds, bucket_start);
