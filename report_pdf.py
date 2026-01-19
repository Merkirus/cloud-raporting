import sqlite3
from datetime import datetime

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
)
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

FONT = "fonts/DejaVuSans.ttf"
FONT_BOLD = "fonts/DejaVuSans-Bold.ttf"


def _fetch_one(con, sql, args=()):
    row = con.execute(sql, args).fetchone()
    return dict(row) if row else None


def _fetch_all(con, sql, args=()):
    return [dict(r) for r in con.execute(sql, args).fetchall()]


def _normalize_session_ids(con, session_ids):
    """
    session_ids:
      - None albo [] => wszystkie sesje
      - int => jedna sesja
      - list[int] => lista sesji
    """
    if session_ids is None or session_ids == []:
        rows = con.execute("SELECT session_id FROM analysis_sessions ORDER BY session_id ASC").fetchall()
        return [int(r["session_id"]) for r in rows]

    if isinstance(session_ids, int):
        return [int(session_ids)]

    return [int(x) for x in session_ids]


def _make_table(data, col_widths=None):
    tbl = Table(data, repeatRows=1, colWidths=col_widths)
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "DejaVu-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "DejaVu"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ]
        )
    )
    return tbl


def _register_fonts(styles):
    pdfmetrics.registerFont(TTFont("DejaVu", FONT))
    pdfmetrics.registerFont(TTFont("DejaVu-Bold", FONT_BOLD))

    # Paragraphy
    styles["Normal"].fontName = "DejaVu"
    styles["Title"].fontName = "DejaVu-Bold"
    styles["Heading1"].fontName = "DejaVu-Bold"
    styles["Heading2"].fontName = "DejaVu-Bold"


def _render_single_session(
    story,
    con,
    styles,
    session_id: int,
    out_bucket_seconds: int,
):
    """
    Renderuje jedną sesję do istniejącego story.
    Zakłada, że agregaty sesji są już policzone i siedzą w:
      - session_summary
      - session_endpoint_summary
      - session_timeseries_summary
    """
    sess = _fetch_one(con, "SELECT * FROM analysis_sessions WHERE session_id=?", (session_id,))
    if not sess:
        story.append(Paragraph(f"Brak sesji session_id={session_id}", styles["Heading1"]))
        story.append(PageBreak())
        return

    summ = _fetch_one(con, "SELECT * FROM session_summary WHERE session_id=?", (session_id,))
    eps = _fetch_all(
        con,
        """
        SELECT * FROM session_endpoint_summary
        WHERE session_id=?
        ORDER BY latency_p95 DESC
        """,
        (session_id,),
    )
    ts = _fetch_all(
        con,
        """
        SELECT * FROM session_timeseries_summary
        WHERE session_id=?
          AND bucket_seconds=?
        ORDER BY bucket_start ASC
        """,
        (session_id, out_bucket_seconds),
    )

    # ===== Header sesji =====
    story.append(Paragraph(f"Sesja #{session_id}", styles["Heading1"]))
    desc = sess.get("description") or ""
    story.append(Paragraph(f"Opis: {desc}" if desc else "Opis: —", styles["Normal"]))
    story.append(Paragraph(f"Start: {sess.get('started_at')} | Status: {sess.get('status')}", styles["Normal"]))
    story.append(Paragraph(f"TotalDepth: {sess.get('total_depth')}", styles["Normal"]))
    story.append(Spacer(1, 4 * mm))

    # ===== Summary =====
    story.append(Paragraph("1. Podsumowanie sesji", styles["Heading2"]))

    if not summ:
        story.append(Paragraph("Brak session_summary (agregaty sesji niepoliczone).", styles["Normal"]))
        story.append(PageBreak())
        return

    summary_data = [
        ["Metryka", "Wartość"],
        ["Liczba żądań", summ["total_requests"]],
        [
            "Success rate",
            f"{summ['success_rate']*100:.2f}% ({summ['success_requests']}/{summ['total_requests']})",
        ],
        [
            "Latency avg / p95 / p99 (ms)",
            f"{summ['latency_avg']:.2f} / {summ['latency_p95']:.2f} / {summ['latency_p99']:.2f}",
        ],
        [
            "TTFB avg / p95 (ms)",
            "-" if summ["ttfb_avg"] is None else f"{summ['ttfb_avg']:.2f} / {summ['ttfb_p95']:.2f}",
        ],
        [
            "Status 2xx / 4xx / 5xx",
            f"{summ['status_2xx']} / {summ['status_4xx']} / {summ['status_5xx']}",
        ],
    ]
    story.append(_make_table(summary_data, col_widths=[70 * mm, 90 * mm]))
    story.append(Spacer(1, 6 * mm))

    # ===== Endpoints =====
    story.append(Paragraph("2. Statystyki per endpoint (sesja)", styles["Heading2"]))

    ep_rows = [["Endpoint", "Method", "Count", "Success", "Latency p95 (ms)", "5xx"]]
    for e in eps:
        ep_rows.append(
            [
                e["endpoint"],
                e["method"],
                e["count"],
                f"{e['success_rate']*100:.1f}%",
                "-" if e["latency_p95"] is None else f"{e['latency_p95']:.2f}",
                e["status_5xx"],
            ]
        )
    story.append(_make_table(ep_rows))
    story.append(Spacer(1, 6 * mm))

    # ===== Timeseries =====
    story.append(Paragraph("3. Trend w czasie (overall, sesja)", styles["Heading2"]))

    ts_rows = [["Bucket start", "Count", "Success", "Latency avg", "Latency p95", "5xx"]]
    for t in ts:
        ts_rows.append(
            [
                t["bucket_start"],
                t["count"],
                f"{t['success_rate']*100:.1f}%",
                "-" if t["latency_avg"] is None else f"{t['latency_avg']:.2f}",
                "-" if t["latency_p95"] is None else f"{t['latency_p95']:.2f}",
                t["status_5xx"],
            ]
        )
    story.append(_make_table(ts_rows))
    story.append(PageBreak())


def generate_pdf_for_sessions(
    db_path: str,
    session_ids=None,  # int | list[int] | None
    out_path: str = "raport_sesje.pdf",
    bucket_seconds: int = 10,
):
    """
    Generuje JEDEN PDF, który zawiera kolejne sekcje dla wielu sesji.

    session_ids:
      - None albo [] => wszystkie sesje z analysis_sessions
      - int => jedna sesja
      - list[int] => wybrane sesje
    """
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    styles = getSampleStyleSheet()
    _register_fonts(styles)

    session_ids = _normalize_session_ids(con, session_ids)

    doc = SimpleDocTemplate(
        out_path,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=15 * mm,
        bottomMargin=15 * mm,
    )

    story = []

    # ===== Global title =====
    story.append(Paragraph("Raport wydajności — zestaw sesji", styles["Title"]))
    story.append(
        Paragraph(
            f"Wygenerowano: {datetime.now().isoformat(timespec='seconds')} | "
            f"Sesje: {('wszystkie' if session_ids else 'brak')}",
            styles["Normal"],
        )
    )
    story.append(Spacer(1, 6 * mm))
    story.append(PageBreak())

    # ===== Render each session =====
    for sid in session_ids:
        _render_single_session(story, con, styles, sid, bucket_seconds)

    con.close()
    doc.build(story)
