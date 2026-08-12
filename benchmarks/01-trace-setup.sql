-- =====================================================================
-- CDC trace - one row per change, with the arrival time at every stage.
-- =====================================================================
-- Run FIRST (scan.startup.mode = 'latest', so this only sees changes
-- made after it exists):
--   Get-Content benchmarks\01-trace-setup.sql | docker exec -i \
--     cdc-superset-db psql -h cdc-risingwave -p 4566 -d dev -U root
--
-- The end product is the view t24_cdc_trace at the bottom. Everything
-- above it exists to feed that view.
--
-- Keyed on kafka_offset, not recid - keeps insert/update/delete of the
-- same recid as separate traceable rows instead of collapsing them.
-- =====================================================================

DROP VIEW               IF EXISTS t24_cdc_trace;
DROP MATERIALIZED VIEW  IF EXISTS t24_trace_parsed;
DROP TABLE              IF EXISTS t24_trace_arrivals;
DROP TABLE              IF EXISTS t24_trace_events;

-- Stage 1, raw landing. FORMAT PLAIN keeps this append-only - every
-- change is its own row, not collapsed over the primary key.
CREATE TABLE t24_trace_events (
    op       VARCHAR,
    "before" JSONB,
    "after"  JSONB,
    source   JSONB,
    ts_ms    BIGINT
)
-- t2: set by the PRODUCER (Debezium), not the broker on append - marks
-- "Debezium finished", making oracle_to_kafka_ms a clean redo-mining measure.
INCLUDE timestamp AS kafka_ts
INCLUDE offset    AS kafka_offset    -- unique id per change event
WITH (
    connector = 'kafka',
    topic = 't24.T24.ACCOUNT',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;

-- Stage 3, parsed layer - mirrors t24_account_columns but grouped by
-- kafka_offset (not recid) so every event stays individually addressable,
-- not overwritten in place on update.
CREATE MATERIALIZED VIEW t24_trace_parsed AS
WITH unpivoted AS (
    SELECT
        e.kafka_offset,
        COALESCE(e."after"->>'RECID', e."before"->>'RECID') AS recid,
        m[1] AS field, m[2] AS position, m[3] AS value
    FROM t24_trace_events e,
         LATERAL (
             SELECT regexp_matches(
                 COALESCE(e."after"->>'XMLRECORD', e."before"->>'XMLRECORD'),
                 '<(c\d+)(?:\s+m="(\d+)")?>([^<]*)</\1>', 'g') AS m
         ) AS t
),
resolved AS (
    SELECT
        u.kafka_offset,
        u.recid,
        COALESCE(
            exact.resolved_name_en,
            CASE WHEN base.is_multivalue
                 THEN base.resolved_name_en || '_' || LPAD(COALESCE(u.position, '1'), 2, '0')
                 ELSE base.resolved_name_en
            END
        ) AS column_name
    FROM unpivoted u
    LEFT JOIN lookup_metadata exact
           ON exact.field_index = u.field AND exact.m_index = u.position
    LEFT JOIN lookup_metadata base
           ON base.field_index = u.field AND base.m_index IS NULL
)
SELECT kafka_offset,
       MIN(recid)            AS recid,
       COUNT(column_name)    AS columns_parsed
FROM resolved
GROUP BY kafka_offset;

-- t3/t4 filled by 02-trace-poller.py. RisingWave can't stamp per-row
-- arrival itself (proctime()/now() are restricted, and proctime() on a
-- table is per-barrier, not per-row) - polling gives a real per-row time.
CREATE TABLE t24_trace_arrivals (
    kafka_offset BIGINT,
    t3_ms        BIGINT,   -- t3: first visible in t24_trace_events (raw)
    t4_ms        BIGINT    -- t4: first visible in t24_trace_parsed (parsed)
);

-- =====================================================================
-- THE VIEW - one row per change, arrival time at each stage, total.
-- =====================================================================
CREATE VIEW t24_cdc_trace AS
SELECT
    e.recid,
    e.op,

    -- Not a timing column - the reliable way to scope a query to one
    -- run, since offsets only increase. run-trace.ps1 relies on this.
    e.kafka_offset,

    -- Arrival times
    to_timestamp(e.t1_oracle_ms / 1000.0) AS t1_oracle,      -- Oracle executed the INSERT
    to_timestamp(e.t2_kafka_ms  / 1000.0) AS t2_kafka,       -- written to the Kafka topic
    to_timestamp(a.t3_ms        / 1000.0) AS t3_risingwave,  -- queryable in RisingWave
    to_timestamp(a.t4_ms        / 1000.0) AS t4_parsed,      -- queryable, flattened

    -- Time spent in each leg
    e.t2_kafka_ms - e.t1_oracle_ms AS oracle_to_kafka_ms,  -- Debezium mining redo
    a.t3_ms       - e.t2_kafka_ms  AS kafka_to_rw_ms,      -- consume + barrier
    a.t4_ms       - a.t3_ms        AS rw_to_parsed_ms,     -- flattening

    -- Total. Falls back to t3 (not t4) for deletes, which never parse.
    COALESCE(a.t4_ms, a.t3_ms) - e.t1_oracle_ms AS total_ms,

    -- 'exact' = Oracle stamped c250 via t24.stamp_c250() (ms precision).
    -- 'second' = fell back to source.ts_ms (whole seconds, up to 1000ms
    -- error) - true for any change not made by our generators.
    e.t1_precision
FROM (
    SELECT
        kafka_offset::BIGINT AS kafka_offset,
        COALESCE("after"->>'RECID', "before"->>'RECID') AS recid,
        CASE op WHEN 'c' THEN 'insert'
                WHEN 'u' THEN 'update'
                WHEN 'd' THEN 'delete'
                WHEN 'r' THEN 'snapshot'
                ELSE op END AS op,

        -- t1: c250's value, parsed from Oracle's YYMMDDHH24MISSFF3
        -- compact-datetime text into epoch ms. Falls back to
        -- source.ts_ms (whole seconds only) if c250 isn't in that shape
        -- (a 15-digit run of no other punctuation) - true for any
        -- change not made by our generators.
        CASE WHEN c250_raw IS NOT NULL
             THEN (EXTRACT(EPOCH FROM (
                     '20' || substring(c250_raw, 1, 2)  || '-' || substring(c250_raw, 3, 2) || '-' ||
                     substring(c250_raw, 5, 2) || ' ' || substring(c250_raw, 7, 2) || ':' ||
                     substring(c250_raw, 9, 2) || ':' || substring(c250_raw, 11, 2) || '.' ||
                     substring(c250_raw, 13, 3)
                   )::TIMESTAMP) * 1000)::BIGINT
             ELSE (source->>'ts_ms')::BIGINT
        END AS t1_oracle_ms,

        CASE WHEN c250_raw IS NOT NULL THEN 'exact' ELSE 'second' END AS t1_precision,

        (EXTRACT(EPOCH FROM kafka_ts) * 1000)::BIGINT AS t2_kafka_ms
    FROM (
        SELECT *,
               (regexp_match(COALESCE("after"->>'XMLRECORD', "before"->>'XMLRECORD', ''),
                              '<c250[^>]*>(\d{15})</c250>'))[1] AS c250_raw
        FROM t24_trace_events
        -- Skip tombstones - Debezium's null-payload delete marker, not
        -- a real data change.
        WHERE op IS NOT NULL
    ) t
) e
-- LEFT, not inner - an inner join would silently hide any change the
-- poller wasn't running for. LEFT keeps it visible with NULL timings.
LEFT JOIN t24_trace_arrivals a ON a.kafka_offset = e.kafka_offset;
