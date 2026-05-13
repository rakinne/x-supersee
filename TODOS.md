# TODOS

Deferred items from /office-hours and /plan-eng-review on 2026-05-13.
Not blocking MVP shipping; revisit after the core demo lands.

## Performance & Scale

### Partition the `events` table by month
- **What:** Add `PARTITION BY RANGE (inserted_at)` to `events`, monthly partitions, automated rollover.
- **Why:** XRPL sustains 30-50 tx/sec = ~3M rows/day. The single-table design works fine for hours-to-days; a long-running hosted demo will accumulate ~90M rows/month and queries will slow.
- **When to do:** Before any hosted demo is run continuously for more than 2 weeks, or before the `events` table exceeds 50M rows. Not needed for the laptop demo.
- **Depends on:** Nothing.
- **Start at:** A new migration `00X_partition_events.sql` plus a small `partition_manager.py` that creates next month's partition on the 25th of each month.

## Production Hardening (post-MVP)

### Real Travel Rule data ingestion
- **What:** Connect to a Travel Rule data exchange (TRP, OpenVASP, Sygna, or similar) and populate `cases.travel_rule_payload` from real source/destination VASP data.
- **Why:** The MVP populates Travel Rule fields only for synthetic fixtures. Real production would need this.
- **When to do:** If the project ever becomes a real product, not a portfolio piece.
- **Depends on:** A real VASP agreement / provider integration.
- **Start at:** Pick a provider, sign up for sandbox credentials, write an adapter in `enrichment/travel_rule.py`.

### Real mixer-detection graph analysis
- **What:** Replace the direct-match `mixer_direct` rule with a real heuristic: graph distance to known mixers (Tornado Cash, Wasabi, Samourai). Use NetworkX or a graph DB for the lookup.
- **Why:** Direct-match catches the obvious cases; real mixers hop through 2-5 intermediate addresses to obscure flows.
- **When to do:** After MVP, if mixer detection becomes a feature worth selling.
- **Depends on:** A graph-shaped account-history representation, not the current `TEXT[]` column.
- **Start at:** Read Chainalysis's public methodology papers; experiment with NetworkX on a small known-mixer dataset.

### Authentication on the analyst UI
- **What:** Session-cookie auth + an `analysts` table + role check on the decision endpoint.
- **Why:** UI is currently localhost-bound inside compose. Any hosted deployment needs real auth.
- **When to do:** Before the optional hosted demo goes public.
- **Depends on:** Nothing.
- **Start at:** Add `fastapi-users` or `authx`, an `analysts` table migration, and middleware on `/cases/*`.

### Multi-LLM provider support
- **What:** Abstract the LLM client behind a `LLMClient` protocol; ship adapters for `claude-haiku-4-5`, `gpt-4o-mini`, and a local Ollama option.
- **Why:** Different reviewers may want to swap providers; some companies prohibit specific vendors.
- **When to do:** If a reviewer specifically asks "can I run this with a different model?"
- **Depends on:** Nothing.
- **Start at:** Define the `LLMClient` protocol in `graph/llm.py`, refactor `build_narrative` to use it, add `langchain-openai` as an optional dep.

### Hosted public demo VM
- **What:** Deploy supersee to a small VM (Hetzner CX22, ~€5/mo) with a public read-only view of the case queue.
- **Why:** "Make it impossible to ignore." A live URL on a resume is a stronger signal than a recorded video.
- **When to do:** After the core repo is solid and the README is good.
- **Depends on:** Auth on the analyst UI (above) before any write paths are public.
- **Start at:** Provision the VM, set up Caddy for HTTPS, deploy with `docker compose up -d`, point a subdomain at it.

## LLM Auth Failure Spam (active issue)

### Anthropic 401 fallback flood under live traffic
- **What:** `ANTHROPIC_API_KEY` in the env is invalid (401 from Anthropic). The
  graceful canned-fallback path in `supersee.graph.nodes.build_narrative`
  triggers on every medium/high case. Under live XRPL traffic (~9 Payments/sec,
  ~6 cases/sec medium-band) this floods the logs with one WARNING per case.
- **Why it matters:** every demo case ends up with `confidence=0.0` low-confidence
  narratives, which routes everything to HITL (correct under low-confidence rule)
  but defeats the purpose of having real LLM narratives in the demo.
- **Debug path when ready:**
  1. Verify which key is leaking through. The Anthropic key in the running
     container's env comes from the host shell via compose.yaml's
     `${ANTHROPIC_API_KEY:-}` substitution. Check `echo $ANTHROPIC_API_KEY`
     locally; rotate via Anthropic console if expired.
  2. If you want to demo without rotating: set `SUPERSEE_MOCK_LLM=true` in
     compose.yaml (forces the canned-narrative path; richer than the
     auth-failure fallback because the canned generator dispatches on
     rule_hits → escalate/review/no_action with proper confidence).
  3. Code-side mitigation (optional): detect 401 specifically in
     `AnthropicNarrativeClient.generate` and flip the module-level
     narrative client to `MockNarrativeClient` after N consecutive failures.
     Saves the LLM call latency on every subsequent case.
- **When to do:** before any hosted demo. The current state works (graceful
  fallback completes the flow), but the WARNING spam is loud.

## Observability (deferred)

### OpenTelemetry tracing
- **What:** Wire OTel SDK into the ingestor, scorer, and LangGraph nodes; export to a public Jaeger or Honeycomb free tier.
- **Why:** Distributed tracing makes the case lifecycle visible end-to-end. Looks great in a demo.
- **When to do:** After the core demo is recorded. Could be a stretch goal for the hosted demo.
- **Depends on:** Hosted demo VM (so there's a real trace backend pointing at).
- **Start at:** Add `opentelemetry-sdk` + `opentelemetry-instrumentation-fastapi` + an OTLP exporter; configure via env vars.
