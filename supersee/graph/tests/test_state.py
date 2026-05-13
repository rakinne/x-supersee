"""Sanity tests for InvestigationState shape + audit_log reducer behavior."""

from __future__ import annotations

from operator import add

from supersee.graph.state import AuditLogEntry, InvestigationState


def test_can_instantiate_minimal_state() -> None:
    """All keys are Optional (total=False), so an empty dict is valid."""
    s: InvestigationState = {}
    assert s == {}


def test_can_set_subset_of_keys() -> None:
    s: InvestigationState = {
        "case_id": "abc",
        "account_src": "rSrc",
        "risk_band": "high",
    }
    assert s["case_id"] == "abc"
    assert s["risk_band"] == "high"
    assert s.get("account_dst") is None


def test_audit_log_reducer_is_concatenation() -> None:
    """The `add` reducer on audit_log must concatenate lists.

    LangGraph reads the Annotated metadata on the TypedDict and applies
    the reducer to merge per-node returns. We don't run that machinery
    here; we just verify the reducer function matches our expectation
    (list1 + list2).
    """
    a: list[AuditLogEntry] = [{"node": "fetch_context", "event": "ok", "detail": {}, "at": "T1", "attempt": 1}]
    b: list[AuditLogEntry] = [{"node": "enrich_external", "event": "ok", "detail": {}, "at": "T2", "attempt": 1}]
    merged = add(a, b)
    assert merged == a + b
    assert [e["node"] for e in merged] == ["fetch_context", "enrich_external"]


def test_audit_log_entry_shape() -> None:
    e: AuditLogEntry = {
        "node": "build_narrative",
        "at": "2026-05-13T12:00:00+00:00",
        "event": "ok",
        "detail": {"confidence": 0.9},
        "attempt": 2,
    }
    assert e["attempt"] == 2
    assert e["detail"]["confidence"] == 0.9
