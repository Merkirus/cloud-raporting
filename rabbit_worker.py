import os
import json
import time
import base64
import pika

from storage import init_db, insert_raw_result
from session_repo import SessionRepository
from aggregates_sessions import compute_session_aggregates
from report_pdf import generate_pdf_for_sessions  # PDF dla listy sesji / None


# ====== Rabbit config ======
RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")

# START (summary)
SUMMARY_EXCHANGE = os.getenv("SUMMARY_EXCHANGE", "summary_exchange")
SUMMARY_QUEUE = os.getenv("SUMMARY_QUEUE", "summary_queue")
SUMMARY_KEY = os.getenv("SUMMARY_KEY", "analysis_start")

# RAW jobs queue (to jest kolejka z jobami!)
RAW_QUEUE = os.getenv("RAW_QUEUE", "perf.raw")

# Reply (done)
DONE_KEY = os.getenv("DONE_KEY", "analysis_done")  # backend powinien zbindować

# ====== DB / report config ======
DB_PATH = os.getenv("REPORT_DB", "data/perf.db")
SCHEMA_PATH = os.getenv("REPORT_SCHEMA", "schema.sql")
BUCKET_SECONDS = int(os.getenv("BUCKET_SECONDS", "10"))
TIMEOUT_SECONDS = float(os.getenv("ANALYSIS_TIMEOUT_SECONDS", "2.0"))

REPORTS_DIR = os.getenv("REPORTS_DIR", "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)


def publish_done(ch, payload: dict):
    ch.basic_publish(
        exchange=SUMMARY_EXCHANGE,
        routing_key=DONE_KEY,
        body=json.dumps(payload).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2),
    )


def decode_raw_to_list(body: bytes) -> list[dict]:
    msg = json.loads(body.decode("utf-8"))
    return msg if isinstance(msg, list) else [msg]


def wait_for_analysis_start(ch) -> tuple[str, int]:
    """
    Blokuje dopóki nie przyjdzie analysis_start.
    Oczekuje JSON: {"description":"...", "totalDepth":3}
    """
    while True:
        method, props, body = ch.basic_get(queue=SUMMARY_QUEUE, auto_ack=False)
        if method is None:
            time.sleep(0.05)
            continue

        try:
            msg = json.loads(body.decode("utf-8"))
            desc = str(msg.get("description", ""))
            total_depth = int(msg["totalDepth"])
            ch.basic_ack(method.delivery_tag)
            return desc, total_depth
        except Exception as e:
            print(f"[START] bad message: {e}")
            ch.basic_nack(method.delivery_tag, requeue=False)


def finalize_session(ch, description: str, total_depth: int, jobs_depth: dict[int, int]):
    """
    1) zapis sesji + mapping jobów
    2) agregaty sesji
    3) PDF sesji
    4) reply z PDF (base64)
    """
    if not jobs_depth:
        publish_done(ch, {
            "event": "analysis_done",
            "ok": False,
            "description": description,
            "error": "Timeout/finish but no jobs received.",
        })
        return

    repo = SessionRepository(DB_PATH)
    session_id = repo.create_session_with_jobs(
        description=description,
        total_depth=total_depth,
        jobs_depth=jobs_depth,
        status="DONE",
        started_at=None,
    )

    compute_session_aggregates(DB_PATH, session_ids=session_id, bucket_seconds=BUCKET_SECONDS)

    out_pdf = os.path.join(REPORTS_DIR, f"raport_session_{session_id}.pdf")
    generate_pdf_for_sessions(
        db_path=DB_PATH,
        session_ids=session_id,
        out_path=out_pdf,
        bucket_seconds=BUCKET_SECONDS,
    )

    with open(out_pdf, "rb") as f:
        pdf_bytes = f.read()

    publish_done(ch, {
        "event": "analysis_done",
        "ok": True,
        "description": description,
        "session_id": session_id,
        "jobs_count": len(jobs_depth),
        "totalDepth": total_depth,
        "pdf_filename": os.path.basename(out_pdf),
        "pdf_size_bytes": len(pdf_bytes),
        "pdf_b64": base64.b64encode(pdf_bytes).decode("ascii"),
    })


def consume_raw_for_one_session(ch, description: str, total_depth: int):
    """
    Zaczyna konsumować RAW dopiero po analysis_start.
    Kończy gdy:
      - 2 sek bez wiadomości RAW => timeout
      - wszyscy joby w mapie mają depth == totalDepth
    """
    jobs_depth: dict[int, int] = {}
    last_msg_ts = time.monotonic()

    def complete_by_depth() -> bool:
        return bool(jobs_depth) and all(d >= total_depth for d in jobs_depth.values())

    # consume z inactivity_timeout: iteruje wiadomości; jak brak przez TIMEOUT_SECONDS -> yield (None,None,None)
    for method, props, body in ch.consume(queue=RAW_QUEUE, auto_ack=False, inactivity_timeout=TIMEOUT_SECONDS):
        if method is None:
            # timeout: nie przyszło nic przez TIMEOUT_SECONDS
            finalize_session(ch, description, total_depth, jobs_depth)
            # ważne: kończymy konsumpcję RAW dla tej sesji
            break

        try:
            dtos = decode_raw_to_list(body)

            # zapis RAW do DB
            for dto in dtos:
                insert_raw_result(DB_PATH, dto)

            # depth liczymy jako "jedna wiadomość" per job_id obecny w tej wiadomości
            job_ids = {int(dto["job_id"]) for dto in dtos}
            for jid in job_ids:
                jobs_depth[jid] = jobs_depth.get(jid, 0) + 1

            last_msg_ts = time.monotonic()
            ch.basic_ack(method.delivery_tag)

            if complete_by_depth():
                finalize_session(ch, description, total_depth, jobs_depth)
                break

        except Exception as e:
            print(f"[RAW] bad message: {e}")
            ch.basic_nack(method.delivery_tag, requeue=False)

    # PRZERWIJ generator consume (czyści stan po stronie pika)
    try:
        ch.cancel()
    except Exception:
        pass


def main():
    init_db(DB_PATH, SCHEMA_PATH)

    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBIT_HOST, port=RABBIT_PORT, credentials=creds, heartbeat=60)
    )
    ch = conn.channel()
    ch.basic_qos(prefetch_count=1)

    # declare / bind START
    ch.exchange_declare(exchange=SUMMARY_EXCHANGE, exchange_type="direct", durable=True)
    ch.queue_declare(queue=SUMMARY_QUEUE, durable=True)
    ch.queue_bind(queue=SUMMARY_QUEUE, exchange=SUMMARY_EXCHANGE, routing_key=SUMMARY_KEY)

    # declare RAW (ale UWAGA: nie konsumujemy dopóki nie ma startu)
    ch.queue_declare(queue=RAW_QUEUE, durable=True)

    print(f"[*] Waiting for analysis_start on {SUMMARY_EXCHANGE}:{SUMMARY_KEY} (queue={SUMMARY_QUEUE})")
    print(f"[*] RAW queue (will be consumed ONLY after start): {RAW_QUEUE}")
    print(f"[*] Timeout: {TIMEOUT_SECONDS}s | Reply: {SUMMARY_EXCHANGE}:{DONE_KEY}")

    while True:
        # 1) czekaj na start
        desc, td = wait_for_analysis_start(ch)
        print(f"[START] description='{desc}' totalDepth={td}")

        # 2) dopiero teraz konsumuj RAW dla tej sesji
        consume_raw_for_one_session(ch, desc, td)

        # 3) wracamy do czekania na kolejny analysis_start
        print("[*] Session finished. Waiting for next analysis_start...")


if __name__ == "__main__":
    main()
