"""LangGraph investigation app: stateful, checkpointed compliance reasoning.

Module map:

    state.py       InvestigationState TypedDict (the contract between nodes)
    narrative.py   Pydantic Narrative schema + LLM clients (Mock + Anthropic)
    triage.py      Deterministic dispatch from narrative+band to triage_path
    nodes.py       Six node implementations (fetch_context, enrich_external,
                   build_narrative, triage_branch, hitl_pause, record_outcome)
    app.py         build_graph(...) + module-level singletons + init/close
                   for the FastAPI lifespan to drive

The investigation is one persistent thread per case. thread_id = case_id.
AsyncPostgresSaver (shared with the app's connection pool from db.py)
holds the checkpoint state across nodes, process restarts, and HITL pauses.
"""
