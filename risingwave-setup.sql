-- =====================================================================
-- RisingWave - consume the Debezium topic straight into a live table
-- =====================================================================
-- Connect with any Postgres client (RisingWave speaks the Postgres wire
-- protocol on 4566):
--   psql -h localhost -p 4566 -d dev -U root
--   -- or, if you don't have psql installed locally, run it from a
--   -- throwaway container on the same docker network:
--   docker run --rm --network cdc-oracle_default postgres:16-alpine \
--     psql -h risingwave -p 4566 -d dev -U root -f /dev/stdin < risingwave-setup.sql
--
-- CREATE TABLE (not CREATE SOURCE) is used so RisingWave actually stores
-- and maintains the data as a queryable changelog table, applying every
-- insert/update/delete it reads from Kafka - a plain CREATE SOURCE would
-- only let you query the raw stream, not a materialized current state.
--
-- FORMAT DEBEZIUM ENCODE JSON tells RisingWave to unwrap Debezium's
-- {op, before, after, source, ...} envelope automatically and apply each
-- change (insert/update/delete) against PRIMARY KEY (recid) - this is
-- why the connector's key.converter must actually emit {"RECID": "..."}
-- as the Kafka message key (see oracle-connector.json), since Debezium
-- format in RisingWave uses the key to identify which row to update.
-- =====================================================================

CREATE TABLE IF NOT EXISTS t24_account (
    recid     VARCHAR,
    xmlrecord VARCHAR,
    PRIMARY KEY (recid)
) WITH (
    connector = 'kafka',
    topic = 't24.T24.ACCOUNT',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE JSON;

-- =====================================================================
-- Audit / event log - every change, not just current state
-- =====================================================================
-- t24_account above only shows "what does the row look like right now" -
-- FORMAT DEBEZIUM collapses history away by applying each change over the
-- primary key. To see every operation (before, after, when), read the
-- SAME topic a second time with FORMAT PLAIN instead: it does not apply
-- inserts/updates/deletes against a key, it just appends every message as
-- its own permanent row - nothing here is ever overwritten or removed.
--
-- before/after/source are kept as JSONB rather than pre-extracted columns,
-- so this table survives if T24's columns ever change - t24_account_audit
-- below is what does the field extraction, and can be adjusted without
-- re-reading the topic from scratch.
CREATE TABLE IF NOT EXISTS t24_account_events (
    op       VARCHAR,
    "before" JSONB,
    "after"  JSONB,
    source   JSONB,
    ts_ms    BIGINT
) WITH (
    connector = 'kafka',
    topic = 't24.T24.ACCOUNT',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Readable view: operation type, before/after content, and two different
-- "when" columns -
--   db_commit_time  - source->>'ts_ms', when the change actually committed
--                     in Oracle (from the redo log itself)
--   captured_time   - top-level ts_ms, when Debezium/Kafka processed it
-- These differ by however long mining + streaming lag adds - usually a
-- few seconds in this lab.
--
-- before_xmlrecord is RECONSTRUCTED, not taken from Debezium's own
-- "before" field. XMLRECORD is a LOB (XMLTYPE); Oracle's redo log never
-- carries LOB before-images, so Debezium's before.XMLRECORD is always the
-- literal string '__debezium_unavailable_value' on UPDATE/DELETE -
-- confirmed by testing, not a hypothetical, and there is no Oracle-side
-- setting that fixes it. Instead, since t24_account_events already keeps
-- every version of every row forever, the real prior XML is simply the
-- previous row's "after" value - LAG() reconstructs it with no change to
-- Oracle or T24's schema needed.
--
-- Tested: insert -> update -> update produced the correct chain
-- (ROUND-ONE -> ROUND-TWO), not the placeholder. Only limitation: a row's
-- very first version (its INSERT/SNAPSHOT) has no prior row to reconstruct
-- from, so before_xmlrecord is genuinely NULL there - which is correct,
-- since no earlier version exists.
--
-- MATERIALIZED (not a plain view, not CREATE TABLE ... AS SELECT):
--   - CREATE TABLE ... AS SELECT was tested directly and confirmed to take
--     a ONE-TIME snapshot that never updates again - a fresh insert in
--     Oracle never appeared in it. Wrong choice for something meant to
--     keep growing forever.
--   - A plain CREATE VIEW stays live (re-runs the query each time), but
--     has no storage of its own - it recomputes the whole LAG() over
--     t24_account_events on every single query, which gets more expensive
--     as the event log grows.
--   - MATERIALIZED VIEW gets both: RisingWave's streaming engine keeps it
--     incrementally, continuously up to date in the background (confirmed:
--     a fresh insert AND a fresh update both appeared automatically, with
--     before_xmlrecord correctly reconstructed, no manual refresh), while
--     queries against it read pre-computed, stored results - same speed
--     as a table.
CREATE MATERIALIZED VIEW IF NOT EXISTS t24_account_audit AS
WITH base AS (
    SELECT
        CASE op
            WHEN 'r' THEN 'SNAPSHOT'
            WHEN 'c' THEN 'INSERT'
            WHEN 'u' THEN 'UPDATE'
            WHEN 'd' THEN 'DELETE'
            ELSE op
        END AS operation,
        COALESCE("after"->>'RECID', "before"->>'RECID') AS recid,
        "after"->>'XMLRECORD' AS after_xmlrecord,
        to_timestamp((source->>'ts_ms')::bigint / 1000.0) AS db_commit_time,
        to_timestamp(ts_ms / 1000.0)                       AS captured_time
    FROM t24_account_events
)
SELECT
    recid,
    operation,
    LAG(after_xmlrecord) OVER (PARTITION BY recid ORDER BY captured_time) AS before_xmlrecord,
    after_xmlrecord,
    db_commit_time,
    captured_time
FROM base;
