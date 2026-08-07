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

-- Stage 2, raw landing. FORMAT PLAIN keeps this append-only - every
-- change is its own row, not collapsed over the primary key.
CREATE TABLE t24_trace_events (
    op       VARCHAR,
    "before" JSONB,
    "after"  JSONB,
    source   JSONB,
    ts_ms    BIGINT
)
-- t3: set by the PRODUCER (Debezium), not the broker on append - marks
-- "Debezium finished", making oracle_to_kafka_ms a clean redo-mining measure.
INCLUDE timestamp AS kafka_ts
INCLUDE offset    AS kafka_offset    -- unique id per change event
WITH (
    connector = 'kafka',
    topic = 't24.T24.ACCOUNT',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;

-- Stage 4, parsed layer - mirrors t24_account_columns but grouped by
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

-- t4/t5 filled by 02-trace-poller.py. RisingWave can't stamp per-row
-- arrival itself (proctime()/now() are restricted, and proctime() on a
-- table is per-barrier, not per-row) - polling gives a real per-row time.
CREATE TABLE t24_trace_arrivals (
    kafka_offset BIGINT,
    t4_ms        BIGINT,   -- t4: first visible in t24_trace_events (raw)
    t5_ms        BIGINT    -- t5: first visible in t24_trace_parsed (parsed)
);

-- =====================================================================
-- THE VIEW - one row per change, arrival time at each stage, total.
-- =====================================================================
--   SELECT * FROM t24_cdc_trace ORDER BY t1_oracle;
-- =====================================================================
CREATE VIEW t24_cdc_trace AS
SELECT
    e.recid,
    e.op,

    -- Not a timing column - the reliable way to scope a query to one
    -- run, since offsets only increase. run-trace.ps1 relies on this.
    e.kafka_offset,

    -- Arrival times
    to_timestamp(e.t1_generated_ms / 1000.0) AS t1_generated,   -- client built the row
    to_timestamp(e.t2_oracle_ms    / 1000.0) AS t2_oracle,      -- Oracle executed the INSERT
    to_timestamp(e.t3_kafka_ms     / 1000.0) AS t3_kafka,       -- written to the Kafka topic
    to_timestamp(a.t4_ms           / 1000.0) AS t4_risingwave,  -- queryable in RisingWave
    to_timestamp(a.t5_ms           / 1000.0) AS t5_parsed,      -- queryable, flattened

    -- Time spent in each leg
    e.t2_oracle_ms - e.t1_generated_ms AS gen_to_oracle_ms,  -- network + parse + insert
    e.t3_kafka_ms  - e.t2_oracle_ms    AS oracle_to_kafka_ms,-- Debezium mining redo
    a.t4_ms        - e.t3_kafka_ms     AS kafka_to_rw_ms,    -- consume + barrier
    a.t5_ms        - a.t4_ms           AS rw_to_parsed_ms,   -- flattening

    -- Total, with two fallbacks: t4 (not t5) for deletes, which never
    -- parse; t2 (not t1) when there's no client-side <bts> marker.
    COALESCE(a.t5_ms, a.t4_ms) - COALESCE(e.t1_generated_ms, e.t2_oracle_ms) AS total_ms,

    -- 'exact' = Oracle stamped t2 via SYSTIMESTAMP (ms precision).
    -- 'second' = fell back to source.ts_ms (whole seconds, up to 1000ms
    -- error) - true for any change not made by our generators.
    e.t2_precision
FROM (
    SELECT
        kafka_offset::BIGINT AS kafka_offset,
        COALESCE("after"->>'RECID', "before"->>'RECID') AS recid,
        CASE op WHEN 'c' THEN 'insert'
                WHEN 'u' THEN 'update'
                WHEN 'd' THEN 'delete'
                WHEN 'r' THEN 'snapshot'
                ELSE op END AS op,
        -- t1: stamped by the CLIENT before sending - NULL for changes
        -- not made by our generators.
        CASE WHEN COALESCE("after"->>'XMLRECORD', "before"->>'XMLRECORD', '') LIKE '%<bts>%'
             THEN split_part(split_part(COALESCE("after"->>'XMLRECORD', "before"->>'XMLRECORD'),
                                        '<bts>', 2), '</bts>', 1)::BIGINT
        END AS t1_generated_ms,

        -- t2: stamped by ORACLE during the INSERT - falls back to
        -- source.ts_ms (whole seconds only) if no <ots> marker.
        CASE WHEN COALESCE("after"->>'XMLRECORD', "before"->>'XMLRECORD', '') LIKE '%<ots>%'
             THEN split_part(split_part(COALESCE("after"->>'XMLRECORD', "before"->>'XMLRECORD'),
                                        '<ots>', 2), '</ots>', 1)::BIGINT
             ELSE (source->>'ts_ms')::BIGINT
        END AS t2_oracle_ms,

        CASE WHEN COALESCE("after"->>'XMLRECORD', "before"->>'XMLRECORD', '') LIKE '%<ots>%'
             THEN 'exact' ELSE 'second' END AS t2_precision,

        (EXTRACT(EPOCH FROM kafka_ts) * 1000)::BIGINT AS t3_kafka_ms
    FROM t24_trace_events
    -- Skip tombstones - Debezium's null-payload delete marker, not a
    -- real data change.
    WHERE op IS NOT NULL
) e
-- LEFT, not inner - an inner join would silently hide any change the
-- poller wasn't running for. LEFT keeps it visible with NULL timings.
LEFT JOIN t24_trace_arrivals a ON a.kafka_offset = e.kafka_offset;
