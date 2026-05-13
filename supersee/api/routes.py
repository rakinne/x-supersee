"""Case routes: queue, detail, decision.

Three handlers:

    GET  /cases             queue view (?all=1 to include low-band/closed)
    GET  /cases/{id}        detail view (reads graph state for narrative+audit)
    POST /cases/{id}/decision   HITL resume; stale-resume guarded; redirects

Templates inherit `base.html` (Gotham II aesthetic; chosen via /design-shotgun).
The data shaping happens here — by the time the template renders, fields are
display-ready (truncated addresses, formatted ages, etc.).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from langgraph.types import Command

from supersee import clock, db
from supersee.config import settings
from supersee.graph import app as graph_app

logger = logging.getLogger(__name__)
router = APIRouter()

# Statuses where the HITL form is shown / decisions are accepted.
_HITL_STATUSES = {"pending_hitl", "escalated"}

# Statuses included in the default queue view (everything else is "audit" view).
_QUEUE_STATUSES = ("pending", "in_progress", "pending_hitl", "escalated")


# ===========================================================================
# Helpers
# ===========================================================================

def _short(addr: str | None, *, prefix: int = 9, suffix: int = 5) -> str:
    if not addr:
        return "—"
    if len(addr) <= prefix + suffix + 1:
        return addr
    return f"{addr[:prefix]}…{addr[-suffix:]}"


def _age(created_at: datetime | None) -> str:
    if created_at is None:
        return "—"
    now = clock.now()
    if created_at.tzinfo is None:
        # Defensive: shouldn't happen with TIMESTAMPTZ, but be safe.
        from datetime import UTC

        created_at = created_at.replace(tzinfo=UTC)
    delta = now - created_at
    secs = int(delta.total_seconds())
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m"
    if secs < 86400:
        return f"{secs // 3600}h"
    return f"{secs // 86400}d"


def _band_class(band: str | None) -> str:
    if band == "high":
        return "high"
    if band == "medium":
        return "med"
    return "low"


def _amount_display(amount_xrp: float | None) -> str:
    if amount_xrp is None:
        return "—"
    if amount_xrp == int(amount_xrp):
        return f"{int(amount_xrp):,} XRP"
    return f"{amount_xrp:,.2f} XRP"


async def _build_nav(filters: dict[str, Any] | None = None) -> dict[str, Any]:
    """Top-bar + bottom-bar state. Pulled live each render — cheap aggregate queries."""
    pool = db.get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status IN ('pending','in_progress','pending_hitl')) AS pending,
                    COUNT(*) FILTER (WHERE status = 'escalated') AS escalated,
                    COUNT(*) FILTER (WHERE risk_band = 'high'   AND status IN ('pending','in_progress','pending_hitl','escalated')) AS high_open,
                    COUNT(*) AS total
                FROM cases
            """)
            row = await cur.fetchone()
            pending = int(row[0] or 0) if row else 0
            escalated = int(row[1] or 0) if row else 0
            high_open = int(row[2] or 0) if row else 0
            total = int(row[3] or 0) if row else 0

            await cur.execute("SELECT COUNT(*) FROM ofac_sdn_crypto")
            ofac_row = await cur.fetchone()
            ofac_count = int(ofac_row[0] or 0) if ofac_row else 0

            await cur.execute("SELECT last_validated_ledger FROM ingestor_cursor WHERE id = 1")
            led_row = await cur.fetchone()
            ledger_index = int(led_row[0]) if led_row else None

    return {
        "pending_count": pending + escalated,
        "escalated_count": escalated,
        "high_count": high_open,
        "total_cases": total,
        "ofac_count": ofac_count,
        "ledger_index": f"{ledger_index:,}" if ledger_index else None,
        "connection": "xrpl ws connected",
        "xrpl_host": settings.runtime.xrpl_ws_url.replace("wss://", "").rstrip("/"),
        "analyst": "r.foote",
        "mock_llm": settings.runtime.mock_llm or not settings.enrichment.anthropic_api_key,
        "version": "0.1.0",
    }


def _build_filters(
    *, show_all: bool, status_counts: dict[str, int], band_counts: dict[str, int],
    q: str | None,
) -> dict[str, Any]:
    """Build the sidebar filter rows. Each option is a link that toggles itself."""
    base = "/cases?all=1" if show_all else "/cases"

    status_options = []
    for status, label in [
        ("pending_hitl", "pending_hitl"),
        ("escalated", "escalated"),
        ("approved", "approved"),
        ("auto_closed", "auto_closed"),
        ("rejected", "rejected"),
        ("errored", "errored"),
    ]:
        in_default_queue = status in _QUEUE_STATUSES
        checked = (show_all or in_default_queue)
        status_options.append({
            "label": label,
            "count": status_counts.get(status, 0),
            "checked": checked,
            "href": base,
        })

    band_options = []
    for band, label in [("high", "HIGH"), ("medium", "MED"), ("low", "LOW")]:
        band_options.append({
            "label": label,
            "count": band_counts.get(band, 0),
            "checked": True,
            "href": base,
        })

    return {
        "q": q,
        "status_options": status_options,
        "status_total": sum(status_counts.values()),
        "band_options": band_options,
        "band_total": sum(band_counts.values()),
    }


# ===========================================================================
# GET /cases
# ===========================================================================

@router.get("/cases", response_class=HTMLResponse)
async def cases_queue(request: Request, all: int = 0, q: str | None = None) -> HTMLResponse:
    """Render the queue. `?all=1` includes closed/low-band cases."""
    from supersee.api import templates  # avoid circular at import time

    show_all = bool(all)
    pool = db.get_pool()

    async with pool.connection() as conn, conn.cursor() as cur:
        where: str
        params: tuple[Any, ...]
        if show_all:
            where, params = "", ()
        else:
            where, params = (
                "WHERE status = ANY(%s)",
                (list(_QUEUE_STATUSES),),
            )

        await cur.execute(f"""
                SELECT id, event_id, status, risk_score, risk_band,
                       rule_hits, created_at,
                       (SELECT account_src FROM events WHERE id = c.event_id) AS account_src,
                       (SELECT account_dst FROM events WHERE id = c.event_id) AS account_dst,
                       (SELECT amount_xrp  FROM events WHERE id = c.event_id) AS amount_xrp
                FROM cases c
                {where}
                ORDER BY
                    CASE risk_band WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                    created_at DESC
                LIMIT 200
            """, params)
        rows = await cur.fetchall()

        # Aggregate counts for sidebar
        await cur.execute("SELECT status, COUNT(*) FROM cases GROUP BY status")
        status_counts = {r[0]: int(r[1]) for r in await cur.fetchall()}
        await cur.execute("SELECT risk_band, COUNT(*) FROM cases GROUP BY risk_band")
        band_counts = {r[0]: int(r[1]) for r in await cur.fetchall()}

    cases = []
    for r in rows:
        case_id = str(r[0])
        cases.append({
            "id": case_id,
            "id_short": case_id[:8],
            "status": r[2],
            "risk_score": float(r[3]) if r[3] is not None else 0.0,
            "risk_band": r[4],
            "band_class": _band_class(r[4]),
            "rule_hits_count": len(r[5]) if r[5] else 0,
            "age": _age(r[6]),
            "route": f"{_short(r[7])} → {_short(r[8])}",
            "amount_display": _amount_display(float(r[9])) if r[9] is not None else "—",
        })

    ctx = {
        "cases": cases,
        "show_all": show_all,
        "active_page": "queue",
        "nav": await _build_nav(),
        "filters": _build_filters(
            show_all=show_all, status_counts=status_counts,
            band_counts=band_counts, q=q,
        ),
    }
    return templates.TemplateResponse(request, "case_queue.html", ctx)


# ===========================================================================
# GET /cases/{case_id}
# ===========================================================================

@router.get("/cases/{case_id}", response_class=HTMLResponse)
async def case_detail(request: Request, case_id: str, flash: str | None = None) -> HTMLResponse:
    """Render one case's full detail: narrative, evidence, rules, audit, HITL."""
    from supersee.api import templates  # avoid circular at import time

    pool = db.get_pool()
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("""
                SELECT id, event_id, status, risk_score, risk_band,
                       rule_hits, created_at, updated_at,
                       (SELECT account_src   FROM events WHERE id = c.event_id),
                       (SELECT account_dst   FROM events WHERE id = c.event_id),
                       (SELECT amount_xrp    FROM events WHERE id = c.event_id),
                       (SELECT memo_decoded  FROM events WHERE id = c.event_id)
                FROM cases c
                WHERE id = %s
            """, (case_id,))
        row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, f"case {case_id} not found")

        # Recent analyst_decision artifact (if any) for closed cases.
        await cur.execute("""
                SELECT payload FROM case_artifacts
                WHERE case_id = %s AND kind = 'analyst_decision'
                ORDER BY created_at DESC LIMIT 1
            """, (case_id,))
        art_row = await cur.fetchone()
        decision_artifact = art_row[0] if art_row else None

        # Curated counts for sidebar
        await cur.execute("SELECT status, COUNT(*) FROM cases GROUP BY status")
        status_counts = {r[0]: int(r[1]) for r in await cur.fetchall()}
        await cur.execute("SELECT risk_band, COUNT(*) FROM cases GROUP BY risk_band")
        band_counts = {r[0]: int(r[1]) for r in await cur.fetchall()}

    rule_hits = list(row[5] or [])
    rule_hits = sorted(rule_hits, key=lambda h: -float(h.get("score", 0)))

    # Graph state: narrative + audit_log
    narrative: dict[str, Any] | None = None
    audit_log: list[dict[str, Any]] = []
    triage_path: str | None = None
    try:
        graph = graph_app.get_graph()
        snapshot = await graph.aget_state({"configurable": {"thread_id": case_id}})
        values = snapshot.values or {}
        narrative = values.get("narrative")
        audit_log = list(values.get("audit_log") or [])
        triage_path = values.get("triage_path")
    except RuntimeError:
        # graph not initialized (e.g., in tests); fall through with empty state
        pass

    # Format audit_log timestamps + detail pretty-print
    for a in audit_log:
        at = a.get("at")
        if at and isinstance(at, str) and len(at) >= 19:
            a["at_short"] = at[11:19]  # HH:MM:SS from ISO
        else:
            a["at_short"] = at or ""
        detail = a.get("detail") or {}
        try:
            a["detail_pretty"] = json.dumps(detail, default=str)[:500]
        except Exception:
            a["detail_pretty"] = str(detail)[:500]

    status = row[2]
    case = {
        "id": str(row[0]),
        "id_short": str(row[0])[:8],
        "event_id": row[1],
        "status": status,
        "is_decidable": status in _HITL_STATUSES,
        "risk_score": float(row[3]) if row[3] is not None else 0.0,
        "risk_band": row[4],
        "created_at": row[6].isoformat(timespec="seconds") if row[6] else "",
        "age": _age(row[6]),
        "account_src": row[8],
        "account_src_short": _short(row[8]),
        "account_dst": row[9],
        "account_dst_short": _short(row[9]) if row[9] else None,
        "amount_xrp": float(row[10]) if row[10] is not None else None,
        "amount_display": _amount_display(float(row[10])) if row[10] is not None else "—",
        "memo_decoded": row[11],
        "triage_path": triage_path,
        "analyst_decision": decision_artifact.get("analyst_decision") if decision_artifact else None,
    }

    ctx = {
        "case": case,
        "narrative": narrative,
        "rule_hits": rule_hits,
        "audit_log": audit_log,
        "flash": flash,
        "active_page": "queue",
        "nav": await _build_nav(),
        "filters": _build_filters(
            show_all=False, status_counts=status_counts,
            band_counts=band_counts, q=None,
        ),
    }
    return templates.TemplateResponse(request, "case_detail.html", ctx)


# ===========================================================================
# POST /cases/{case_id}/decision
# ===========================================================================

@router.post("/cases/{case_id}/decision")
async def case_decision(
    case_id: str,
    decision: str = Form(...),
    note: str = Form(""),
    actor: str = Form("anonymous"),
) -> RedirectResponse:
    """Resume the suspended LangGraph investigation with the analyst's decision.

    Stale-resume guard (eng-review hardening 1A): verifies the case is still
    in a HITL-awaiting state before sending Command(resume=...). If the case
    has already been decided (or never reached HITL), returns 303 to /cases/{id}
    with an explanatory flash message rather than 409 — the analyst already
    has a visible signal in the UI of the current case state.
    """
    decision = decision.strip().lower()
    if decision not in {"approve", "reject", "escalate"}:
        return RedirectResponse(
            url=f"/cases/{case_id}?flash=invalid+decision+'{decision}'",
            status_code=303,
        )

    pool = db.get_pool()
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("SELECT status FROM cases WHERE id = %s", (case_id,))
        row = await cur.fetchone()
    if row is None:
        raise HTTPException(404, f"case {case_id} not found")

    current_status = row[0]
    if current_status not in _HITL_STATUSES:
        flash = f"case+already+{current_status}+(no+resume+possible)"
        return RedirectResponse(url=f"/cases/{case_id}?flash={flash}", status_code=303)

    # Run the graph resume inline. record_outcome is pure DB writes (<500ms budget).
    try:
        graph = graph_app.get_graph()
        await graph.ainvoke(
            Command(
                resume={
                    "decision": decision,
                    "note": note or "",
                    "actor": actor or "anonymous",
                }
            ),
            {"configurable": {"thread_id": case_id}},
        )
    except Exception as e:
        logger.exception("decision resume failed for case=%s: %s", case_id, e)
        flash = f"resume+failed:+{type(e).__name__}"
        return RedirectResponse(url=f"/cases/{case_id}?flash={flash}", status_code=303)

    return RedirectResponse(url=f"/cases/{case_id}", status_code=303)
