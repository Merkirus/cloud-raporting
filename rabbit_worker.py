import os
import json
import time
import base64
import pika

from storage import init_db, insert_raw_result
from session_repo import SessionRepository
from aggregates_sessions import compute_session_aggregates
from report_pdf import generate_pdf_for_sessions


# ====== Rabbit config ======
RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")

SUMMARY_EXCHANGE = os.getenv("SUMMARY_EXCHANGE", "summary_exchange")

SUMMARY_QUEUE = os.getenv("SUMMARY_QUEUE", "summary_queue")
SUMMARY_KEY = os.getenv("SUMMARY_KEY", "analysis_start")

DONE_QUEUE = os.getenv("DONE_QUEUE", "summary_done_queue")
DONE_KEY = os.getenv("DONE_KEY", "analysis_done")

RAW_QUEUE = os.getenv("RAW_QUEUE", "perf.raw")


# ====== DB / report config ======
DB_PATH = os.getenv("REPORT_DB", "data/perf.db")
SCHEMA_PATH = os.getenv("REPORT_SCHEMA", "schema.sql")
BUCKET_SECONDS = int(os.getenv("BUCKET_SECONDS", "10"))
TIMEOUT_SECONDS = float(os.getenv("ANALYSIS_TIMEOUT_SECONDS", "5.0"))

REPORTS_DIR = os.getenv("REPORTS_DIR", "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)


# ================= Rabbit helpers =================

def publish_done(ch, payload: dict):
    ch.basic_publish(
        exchange=SUMMARY_EXCHANGE,
        routing_key=DONE_KEY,
        body=json.dumps(payload).encode(),
        properties=pika.BasicProperties(
            delivery_mode=2,           # trwała wiadomość
            content_type="application/json",
            priority=0                 # opcjonalne, jeśli Twoja kolejka obsługuje priorytety
        )
    )
    print("[DONE] published")



def decode_raw_to_list(body: bytes) -> list[dict]:
    msg = json.loads(body.decode())
    return msg if isinstance(msg, list) else [msg]


# ================= START =================

def wait_for_analysis_start(ch) -> tuple[str, int]:
    print("[*] Waiting for analysis_start...")
    while True:
        method, props, body = ch.basic_get(queue=SUMMARY_QUEUE, auto_ack=False)
        if not method:
            time.sleep(0.2)
            continue

        try:
            msg = json.loads(body.decode())
            desc = str(msg.get("description", ""))
            total_depth = int(msg["totalDepth"])
            ch.basic_ack(method.delivery_tag)
            return desc, total_depth
        except Exception as e:
            print("[START] invalid:", e)
            ch.basic_nack(method.delivery_tag, requeue=False)


# ================= RAW drain =================

def drain_raw_queue(ch):
    while True:
        method, _, _ = ch.basic_get(queue=RAW_QUEUE, auto_ack=True)
        if not method:
            return


# ================= SESSION =================

def finalize_session(ch, description, total_depth, jobs_depth):
    if not jobs_depth:
        publish_done(ch, {
            "event": "analysis_done",
            "ok": False,
            "description": description,
            "error": "No RAW data received"
        })
        return

    repo = SessionRepository(DB_PATH)
    session_id = repo.create_session_with_jobs(
        description=description,
        total_depth=total_depth,
        jobs_depth=jobs_depth,
        status="DONE",
        started_at=None
    )

    compute_session_aggregates(DB_PATH, session_ids=session_id, bucket_seconds=BUCKET_SECONDS)

    out_pdf = os.path.join(REPORTS_DIR, f"raport_session_{session_id}.pdf")
    generate_pdf_for_sessions(DB_PATH, session_id, out_pdf, BUCKET_SECONDS)

    with open(out_pdf, "rb") as f:
        pdf = f.read()

    publish_done(ch, {
        "event": "analysis_done",
        "ok": True,
        "description": description,
        "session_id": session_id,
        "jobs_count": len(jobs_depth),
        "totalDepth": total_depth,
        "pdf_filename": os.path.basename(out_pdf),
        "pdf_size_bytes": len(pdf),
        "pdf_b64": base64.b64encode(pdf).decode()
    })


def consume_raw_for_one_session(ch, description, total_depth):
    jobs_depth = {}
    last_msg = time.monotonic()

    print("[RAW] collecting...")

    while True:
        method, props, body = ch.basic_get(queue=RAW_QUEUE, auto_ack=False)

        now = time.monotonic()

        # inactivity timeout
        if not method:
            if now - last_msg > TIMEOUT_SECONDS:
                print("[RAW] timeout")
                finalize_session(ch, description, total_depth, jobs_depth)
                drain_raw_queue(ch)
                return
            time.sleep(0.2)
            continue

        try:
            dtos = decode_raw_to_list(body)

            for dto in dtos:
                insert_raw_result(DB_PATH, dto)

            for jid in {int(x["job_id"]) for x in dtos}:
                jobs_depth[jid] = jobs_depth.get(jid, 0) + 1

            ch.basic_ack(method.delivery_tag)
            last_msg = now

            print(f"[RAW] jobs={jobs_depth}")

            if jobs_depth and all(v >= total_depth for v in jobs_depth.values()):
                print("[RAW] depth complete")
                finalize_session(ch, description, total_depth, jobs_depth)
                drain_raw_queue(ch)
                return

        except Exception as e:
            print("[RAW] bad msg:", e)
            ch.basic_nack(method.delivery_tag, requeue=False)


# ================= main =================

def main():
    init_db(DB_PATH, SCHEMA_PATH)

    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(RABBIT_HOST, RABBIT_PORT, credentials=creds, heartbeat=60)
    )
    ch = conn.channel()
    ch.basic_qos(prefetch_count=1)
    ch.confirm_delivery()   # publisher confirms

    # exchange
    ch.exchange_declare(exchange=SUMMARY_EXCHANGE, exchange_type="direct", durable=True)

    # START queue
    ch.queue_declare(queue=SUMMARY_QUEUE, durable=True)
    ch.queue_bind(queue=SUMMARY_QUEUE, exchange=SUMMARY_EXCHANGE, routing_key=SUMMARY_KEY)

    # DONE queue
    ch.queue_declare(queue=DONE_QUEUE, durable=True)
    ch.queue_bind(queue=DONE_QUEUE, exchange=SUMMARY_EXCHANGE, routing_key=DONE_KEY)
    print(f"[INFO] DONE queue '{DONE_QUEUE}' bound to {SUMMARY_EXCHANGE}:{DONE_KEY}")

    # RAW queue
    ch.queue_declare(queue=RAW_QUEUE, durable=True)

    print("=== ANALYSIS WORKER READY ===")

    while True:
        desc, td = wait_for_analysis_start(ch)
        print(f"[START] {desc} depth={td}")

        drain_raw_queue(ch)
        consume_raw_for_one_session(ch, desc, td)


if __name__ == "__main__":
    main()
