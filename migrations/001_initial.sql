-- 001_initial.sql
--
-- Initial schema for supersee. Mirrors the data model section of the design
-- doc at ~/.gstack/projects/rakinne-x-supersee/rakinnefoote-main-design-*.md.
--
-- Tables (11): events, account_history, cases, case_artifacts,
--              ofac_sdn_crypto, watchlist, mixer_addresses, exchange_labels,
--              ingestor_cursor, enrichment_cache, rate_limits.
--
-- The migrations runner owns `schema_migrations` (created outside this file).
-- LangGraph's checkpointer owns `checkpoints`, `checkpoint_writes`, and
-- `checkpoint_blobs` (created by AsyncPostgresSaver.setup() at app startup).

-- ===========================================================================
-- Shared trigger helpers
-- ===========================================================================

CREATE OR REPLACE FUNCTION supersee_touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Keeps account_history.known_counterparties bounded so high-volume accounts
-- (exchange wallets) don't grow the row indefinitely. Truncates oldest;
-- new_counterparty rule degrades gracefully past the cap (see perf notes).
CREATE OR REPLACE FUNCTION supersee_trim_known_counterparties() RETURNS TRIGGER AS $$
DECLARE
    n INTEGER;
    keep_from INTEGER;
BEGIN
    IF NEW.known_counterparties IS NULL THEN
        RETURN NEW;
    END IF;
    n := array_length(NEW.known_counterparties, 1);
    IF n IS NOT NULL AND n > 1000 THEN
        keep_from := n - 999;
        NEW.known_counterparties := NEW.known_counterparties[keep_from : n];
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ===========================================================================
-- events: inbound XRPL transactions, deduped and normalized
-- ===========================================================================

CREATE TABLE events (
    id            BIGSERIAL PRIMARY KEY,
    tx_hash       TEXT UNIQUE NOT NULL,
    ledger_index  BIGINT NOT NULL,
    validated_at  TIMESTAMPTZ NOT NULL,
    tx_type       TEXT NOT NULL,
    account_src   TEXT NOT NULL,
    account_dst   TEXT,
    amount_xrp    NUMERIC,
    memo_decoded  TEXT,
    raw_json      JSONB NOT NULL,
    inserted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Load-bearing for the velocity_burst rule's 5-minute window query.
CREATE INDEX events_account_src_validated_idx
    ON events (account_src, validated_at DESC);

-- Used by the analyst UI "recent events" view and replay tooling.
CREATE INDEX events_validated_at_idx ON events (validated_at DESC);

-- ===========================================================================
-- account_history: per-account rolling state for velocity, P99, counterparties
-- ===========================================================================

CREATE TABLE account_history (
    account              TEXT PRIMARY KEY,
    total_seen           BIGINT NOT NULL DEFAULT 0,
    last_seen_at         TIMESTAMPTZ,
    rolling_p99_7d       NUMERIC,
    rolling_p99_at       TIMESTAMPTZ,
    known_counterparties TEXT[] NOT NULL DEFAULT '{}',
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER account_history_touch_updated
    BEFORE UPDATE ON account_history
    FOR EACH ROW EXECUTE FUNCTION supersee_touch_updated_at();

CREATE TRIGGER account_history_trim_counterparties
    BEFORE INSERT OR UPDATE ON account_history
    FOR EACH ROW EXECUTE FUNCTION supersee_trim_known_counterparties();

-- ===========================================================================
-- cases: the unit of investigation
-- ===========================================================================

CREATE TABLE cases (
    id                  UUID PRIMARY KEY,
    event_id            BIGINT NOT NULL REFERENCES events(id),
    status              TEXT NOT NULL,
    risk_score          NUMERIC NOT NULL,
    risk_band           TEXT NOT NULL,
    rule_hits           JSONB NOT NULL,
    travel_rule_payload JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT cases_status_check CHECK (status IN (
        'pending', 'in_progress', 'pending_hitl',
        'auto_closed', 'approved', 'rejected', 'escalated', 'errored'
    )),
    CONSTRAINT cases_risk_band_check CHECK (risk_band IN ('low', 'medium', 'high'))
);

CREATE TRIGGER cases_touch_updated
    BEFORE UPDATE ON cases
    FOR EACH ROW EXECUTE FUNCTION supersee_touch_updated_at();

-- Analyst queue view (default filter: non-low bands, ordered by recency).
CREATE INDEX cases_risk_band_created_idx
    ON cases (risk_band, created_at DESC);

-- Crash-recovery sweep query: WHERE status IN ('pending','in_progress','pending_hitl').
CREATE INDEX cases_status_created_idx
    ON cases (status, created_at);

-- Backref for the scorer's "have we already created a case for this event?" check.
CREATE UNIQUE INDEX cases_event_id_idx ON cases (event_id);

-- ===========================================================================
-- case_artifacts: append-only audit trail (enrichment blobs, narratives,
--                 analyst decisions)
-- ===========================================================================

CREATE TABLE case_artifacts (
    id         BIGSERIAL PRIMARY KEY,
    case_id    UUID NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
    kind       TEXT NOT NULL,
    payload    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT case_artifacts_kind_check CHECK (kind IN (
        'enrichment', 'narrative', 'analyst_decision', 'system'
    ))
);

CREATE INDEX case_artifacts_case_id_created_idx
    ON case_artifacts (case_id, created_at);

-- 90-day retention task (scheduler.py) uses this index to find old rows.
CREATE INDEX case_artifacts_created_at_idx
    ON case_artifacts (created_at);

-- ===========================================================================
-- ofac_sdn_crypto: sanctioned crypto addresses, refreshed daily
-- ===========================================================================

CREATE TABLE ofac_sdn_crypto (
    address    TEXT PRIMARY KEY,
    asset      TEXT NOT NULL,
    sdn_uid    INTEGER NOT NULL,
    source_url TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX ofac_sdn_crypto_asset_idx ON ofac_sdn_crypto (asset);

-- ===========================================================================
-- Curated lists (small, hand-managed via seed files and UI)
-- ===========================================================================

CREATE TABLE watchlist (
    address   TEXT PRIMARY KEY,
    label     TEXT,
    risk_tier TEXT,
    tags      TEXT[]
);

CREATE TABLE mixer_addresses (
    address TEXT PRIMARY KEY,
    label   TEXT,
    source  TEXT
);

CREATE TABLE exchange_labels (
    address     TEXT PRIMARY KEY,
    exchange    TEXT,
    wallet_kind TEXT
);

-- ===========================================================================
-- ingestor_cursor: single-row table tracking last_validated_ledger so a
--                  restart resumes from the right place
-- ===========================================================================

CREATE TABLE ingestor_cursor (
    id                    INTEGER PRIMARY KEY CHECK (id = 1),
    last_validated_ledger BIGINT NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER ingestor_cursor_touch_updated
    BEFORE UPDATE ON ingestor_cursor
    FOR EACH ROW EXECUTE FUNCTION supersee_touch_updated_at();

-- ===========================================================================
-- enrichment_cache: replaces the Redis path. K/V with TTL.
-- ===========================================================================

CREATE TABLE enrichment_cache (
    key        TEXT PRIMARY KEY,
    value      JSONB NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX enrichment_cache_expires_idx ON enrichment_cache (expires_at);

-- ===========================================================================
-- rate_limits: outbound HTTP rate counters (OpenSanctions, future Anthropic).
-- ===========================================================================

CREATE TABLE rate_limits (
    bucket       TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    count        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket, window_start)
);

CREATE INDEX rate_limits_window_start_idx ON rate_limits (window_start);
