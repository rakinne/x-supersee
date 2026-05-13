# Supersee

Supersee tails the public XRPL WebSocket, scores every Payment transaction
in real time with a deterministic rule set, and turns the suspicious ones
into checkpointed LangGraph investigations with human-in-the-loop approval.
The whole stack runs on a single VM with `docker compose up`.

Built as a portfolio piece showing how agentic AI can be embedded into
regulated decision-support workflows where reliability, traceability, and
controlled escalation matter as much as model output quality.

## Live numbers

Observed during a single `docker compose up` run against the public XRPL
WebSocket:

| metric | value |
| --- | --- |
| Payments ingested | ~9 per second (after filtering to XRP-only Payments) |
| Cases scored | every Payment, ~9 per second |
| Low-band auto-closed | ~50% of cases |
| Medium / high routed to LangGraph | ~50% |
| Graph throughput | bounded by `asyncio.Semaphore(4)` to keep LLM costs sane |
| OFAC SDN crypto addresses cached | 33 cross-chain (FBI Lazarus via OpenSanctions, BTC + ETH + POLYGON; refreshed daily) |
| Process wedges observed under 401 LLM-auth flood | 0 (graceful canned-narrative fallback closes every case) |

## What it does

```
                                                +-----------------+
  XRPL WebSocket                                | Analyst         |
  (xrplcluster.com)  --> ingestor.py    -->     | http://.../cases|
                          Payment filter        |  approve/reject |
                          tx_hash dedupe        |  /escalate      |
                          events table          +--------+--------+
                              |                          |
                              v                          | Command(resume=...)
                          asyncio.Queue                  |
                              |                          v
                          scorer/                     LangGraph
                          rules.py (7 rules)           AsyncPostgresSaver
                          score_event.py               (checkpointed)
                          history.py (Lookups)
                              |                          ^
                +-------------+--------------+           |
                |                            |           |
                v                            v           |
        risk_band='low'                risk_band in      |
        status='auto_closed'           {medium, high}    |
        no LLM call                    asyncio.Queue --> graph/nodes.py
                                                        fetch_context
                                                        enrich_external
                                                        build_narrative (LLM)
                                                        triage_branch
                                                        hitl_pause  (interrupt)
                                                        record_outcome
```

The split between **deterministic scoring** and **agentic reasoning** is the
load-bearing decision. The scorer is auditable, testable, and runs every
event for nearly free. The LangGraph investigation runs only on the ~5% of
events that warrant the cost, and produces a structured narrative with
evidence, risk factors, and a recommended action that an analyst can act on.

Every node appends to an `audit_log` that survives in the case record, so a
reviewer can replay the agent's reasoning step by step. That auditability is
the difference between "we used AI" and "we built a regulated-tooling-grade
AI system."

## Quickstart

```bash
docker compose up -d
open http://localhost:8000/cases
```

That is the whole demo. Within ~6 seconds of startup the analyst queue starts
filling with real flagged XRPL Payments. Click any row, read the narrative,
pick approve / reject / escalate. The decision posts to FastAPI, runs
`Command(resume=...)` against the suspended LangGraph thread, writes the final
status and an immutable audit artifact to Postgres, and redirects back to the
detail page with the new state visible.

If you do not have an Anthropic API key handy, set `SUPERSEE_MOCK_LLM=true`
in `compose.yaml` before `up`. The canned-narrative generator produces
plausible compliance narratives by dispatching on the rule hits. Strong
enough for an end-to-end demo without an LLM call.

## Three demo paths

Pick whichever is easiest to show.

### 1. Live XRPL feed

```bash
docker compose up
open http://localhost:8000/cases
```

The ingestor subscribes to `wss://xrplcluster.com/`, normalizes validated
Payment transactions, and pushes new event IDs onto the scorer queue. The
queue fills with real cases inside a few seconds. Watch the `escalated` and
`pending_hitl` rows accumulate, then click into one and approve it.

### 2. Scripted fixture replay (full pipeline)

```bash
docker compose exec app supersee investigate fixtures/sanctioned-payment.json
```

Drives a self-contained fixture through scorer + LangGraph + auto-approve
HITL, then prints the full result (case ID, narrative, audit log) as JSON.
Useful for showing the deterministic path end-to-end without needing live
XRPL traffic. The fixture stages an OFAC SDN destination address and a
tornado-cash memo, so the case lands in the high band and the agent
recommends escalation.

### 3. Scoring-only smoke test (no DB / no LLM needed)

```bash
docker compose exec app supersee score fixtures/sanctioned-payment.json --pretty
```

Runs only the deterministic scorer on a fixture event and prints
`{risk_score, risk_band, rule_hits}` in JSON. Fastest way to see the
scoring rationale for a specific input.

## Architecture, where to look

A rendered Mermaid system diagram lives in
[`ARCHITECTURE.md`](ARCHITECTURE.md): live pipeline, LangGraph nodes,
Postgres tables, and the HITL resume edge in one picture. Below are the
specific source files for the parts the diagram glosses over.

- [`supersee/graph/state.py`](supersee/graph/state.py): the
  `InvestigationState` TypedDict. The contract every graph node reads and
  writes against. `audit_log` uses the `add` reducer so concatenation across
  nodes is automatic.
- [`supersee/graph/triage.py`](supersee/graph/triage.py): the deterministic
  triage truth table. High band always reaches a human; medium auto-closes
  only on a high-confidence `no_action` narrative; low confidence always
  escalates to analyst regardless of band.
- [`supersee/graph/narrative.py`](supersee/graph/narrative.py): pydantic
  schema for the LLM output (`Narrative`), four backends (Mock, Static,
  Failing, Anthropic), and the factory that picks one based on env config.
  Tool-calling structured output enforces the schema at the model boundary.
- [`supersee/graph/nodes.py`](supersee/graph/nodes.py): the six nodes.
  `hitl_pause` calls `interrupt()` and persists state; `record_outcome` is
  wrapped in tenacity retries with a dead-letter path on terminal write
  failure.
- [`supersee/scorer/rules.py`](supersee/scorer/rules.py): seven rule
  functions, each pure, each unit-tested. OFAC weight is calibrated so a
  single SDN match always clears the high band on its own.
- [`supersee/pipeline.py`](supersee/pipeline.py): the live pipeline:
  ingestor → scorer queue → scorer loop → graph queue → graph loop. Crash
  recovery sweep at startup re-enqueues orphaned events and pending cases
  without re-driving suspended HITL cases.

The full design doc lives at
[`~/.gstack/projects/rakinne-x-supersee/`](https://github.com/rakinne/x-supersee)
(local user data, not in the repo): includes the problem statement, the
premise challenge, the alternatives considered, the eng-review findings,
the data model with full DDL, and the test plan.

## Production-shaped, not just functional

A few things this project does that AI demo apps usually skip:

- **AsyncPostgresSaver checkpointer.** Every node transition writes
  durable checkpoint state. A process restart picks up suspended HITL
  cases without losing work. Verified by an integration test that kills
  the graph mid-flight and resumes from a fresh compile.
  See [`supersee/graph/tests/test_integration.py`](supersee/graph/tests/test_integration.py).
- **Per-task restart supervisor.** `run_with_restart` wraps every
  background task. One blown coroutine cannot wedge the process.
  Exponential backoff caps at 60 seconds. `CancelledError` propagates
  for clean shutdown. See [`supersee/supervisor.py`](supersee/supervisor.py).
- **Bounded LangGraph concurrency.** `asyncio.Semaphore(4)` around
  `graph.ainvoke` prevents a startup burst from stampeding the LLM API.
  Configurable via `SUPERSEE_RUNTIME_LANGGRAPH_CONCURRENCY`.
- **Crash-recovery sweep.** At lifespan startup, before the consumers
  start, the sweep re-enqueues events without case rows (scorer
  dropped them) and pending / in-progress cases (graph dropped them).
  Does *not* re-enqueue `pending_hitl` cases; those are correctly
  suspended in the checkpointer waiting on an analyst.
- **Graceful LLM-failure fallback.** When the Anthropic call fails
  (transport error, schema validation, auth), `build_narrative` falls
  back to a canned low-confidence narrative that routes the case to
  HITL. Every event completes the pipeline. No partial state. No
  exception escapes the graph. Verified live against an invalid API key.
- **Stale-resume guard on the decision endpoint.** `POST
  /cases/{id}/decision` checks the case status before invoking
  `Command(resume=...)`. A double-clicked approve or a stale tab gets
  a flash message, not a crash. See `supersee/api/routes.py`.
- **Honest data sourcing.** The OFAC SDN cache pulls from
  OpenSanctions' pre-parsed dataset (FBI Lazarus, ~33 BTC/ETH addresses
  with real NK attribution) rather than parsing Treasury's raw
  `SDN_ADVANCED.XML`. The narrower coverage is documented; demo
  fixtures stage synthetic OFAC hits for cross-asset matches the
  Lazarus dataset does not cover.

## Tests and tooling

```bash
docker compose exec app pytest supersee          # full suite
docker compose exec app mypy supersee --strict   # type check
docker compose exec app supersee score <fixture> # quick rule check
```

- **196 tests** across unit + DB-integration + ASGI-integration layers.
- **mypy --strict** clean on all source files.
- **pyproject.toml** pins every dependency (LangGraph 0.4, langchain-anthropic
  0.3, FastAPI 0.115, psycopg 3.2 with binary+pool extras, tenacity, jinja2,
  python-multipart). `click<8.3` pinned to dodge a regression in single-decl
  bool flag semantics.
- **PostgreSQL 16** schema in `migrations/`. Migrations runner in
  `supersee/db.py` uses `pg_advisory_xact_lock` for safe concurrent boot.
- **CI:** [GitHub Actions workflow](.github/workflows/ci.yml) runs ruff,
  mypy --strict, and the full test suite on every push with
  `SUPERSEE_MOCK_LLM=true` (no Anthropic key needed in CI). Postgres is a
  `services:` container; migrations apply via the project's own runner.

## What is deferred

A handful of MVP-scope choices are documented in
[`TODOS.md`](TODOS.md) with debug paths for the active issues. Highlights:

- **Real Travel Rule integration.** The schema has the `travel_rule_payload`
  column; the live pipeline doesn't populate it because real Travel Rule
  data flows over off-chain VASP channels the demo doesn't tap.
- **Graph-based mixer detection.** Currently a curated-list lookup. Real
  1-hop / multi-hop neighborhood analysis is a multi-week project of its
  own.
- **Hosted public demo VM.** TODOS lists the Hetzner CX22 stretch goal.
- **Anthropic 401 flood debug path.** If the host's `ANTHROPIC_API_KEY` is
  invalid, every medium / high case hits the canned-fallback warning under
  live load. TODOS documents the rotate / `SUPERSEE_MOCK_LLM=true` / code-
  side-degrade options.
- **events table partitioning.** The current single-table design works for
  hours-to-days of continuous traffic; partitioning gets captured for any
  long-running hosted deployment.

## Repo map

```
supersee/
├── api/                   FastAPI app, routes, Gotham-themed templates
│   ├── routes.py          GET /cases, GET /cases/{id}, POST /cases/{id}/decision
│   ├── templates/         base.html + case_queue.html + case_detail.html
│   └── tests/             ASGI integration tests via httpx
├── clock.py               clock.now() indirection for deterministic fixture replays
├── config/                pydantic-settings (scorer / runtime / enrichment)
├── db.py                  AsyncConnectionPool + migrations runner
├── enrichment/
│   ├── ofac.py            OpenSanctions SDN crypto wallet ingestion
│   └── tests/             parser + retry tests
├── graph/                 LangGraph investigation app
│   ├── state.py           InvestigationState TypedDict
│   ├── narrative.py       Pydantic Narrative + Mock + Anthropic clients
│   ├── triage.py          deterministic triage truth table
│   ├── nodes.py           six nodes
│   ├── app.py             build_graph + init_graph + checkpointer
│   └── tests/             54 tests (unit + DB integration)
├── ingestor.py            XRPL WebSocket → events table → scorer queue
├── pipeline.py            scorer + graph loops + recovery_sweep
├── scheduler.py           hourly: OFAC refresh + case_artifacts retention
├── scorer/
│   ├── rules.py           7 pure rule functions
│   ├── score_event.py     aggregator
│   ├── history.py         TTL-cached Lookups + per-event ScoringContext
│   └── tests/             80+ tests
├── supervisor.py          run_with_restart
└── tests/                 supervisor / ingestor / pipeline tests

migrations/
├── 001_initial.sql        11 tables + triggers + indices
└── 002_ofac_sdn_uid_text.sql

fixtures/
└── sanctioned-payment.json  the canonical demo fixture

compose.yaml + Dockerfile + pyproject.toml
TODOS.md                  deferred items + debug paths
README.md (this file)
```

## How this was built

Each design decision is traceable in the git history. The branch contains
office-hours, plan-eng-review, design-shotgun, and per-feature commits, all
on `main`. The design doc + the review artifacts persist outside the repo
in `~/.gstack/projects/rakinne-x-supersee/`. The UI aesthetic was chosen
via a head-to-head between Claude-generated and Codex-generated mockups
(seven directions explored, "Gotham II" by Codex won).

The choice of XRPL as the substrate is deliberate: real public WebSocket,
no API keys, no cost, genuine event volume. The choice to frame the
project as crypto-AML rather than generic crypto monitoring is also
deliberate: it lets the system speak the language of the asset-management
audience the project is built for (digital-asset compliance for firms
entering spot-crypto ETF and tokenized fund markets).

---

*Built May 2026. MIT.*
