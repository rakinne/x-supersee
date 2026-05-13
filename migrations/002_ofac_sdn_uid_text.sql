-- 002_ofac_sdn_uid_text.sql
--
-- Widen ofac_sdn_crypto.sdn_uid from INTEGER to TEXT.
--
-- migration 001 sized this column for Treasury's raw SDN_ADVANCED.XML
-- (numeric UIDs). The eng review then chose OpenSanctions as the source
-- instead (saves 1-2 days of XML parsing). OpenSanctions exposes entity
-- IDs like "ofac-12345" or "Q4242" — strings, not integers. Storing
-- them faithfully requires TEXT.
--
-- The table is currently empty in all environments (no production users
-- yet, and CI/dev pull a fresh schema on each boot), so this ALTER is
-- effectively a no-op rewrite. Documented for the audit trail.

ALTER TABLE ofac_sdn_crypto
    ALTER COLUMN sdn_uid TYPE TEXT;
