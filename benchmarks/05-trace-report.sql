-- =====================================================================
-- Full trace report - every change in the table, every column.
-- =====================================================================
--   Get-Content benchmarks\05-trace-report.sql | docker exec -i \
--     cdc-superset-db psql -h cdc-risingwave -p 4566 -d dev -U root
--
-- run-trace.ps1 reports one run; this dumps everything the trace holds
-- across every run since 01-trace-setup.sql was last executed.
--
-- To narrow it, add a WHERE to the last query:
--   ... WHERE recid LIKE 'SEED-%'           -- one workload
--   ... WHERE kafka_offset >= 125           -- one run
--   ... WHERE total_ms IS NULL              -- changes the poller missed
-- =====================================================================

\echo '== Totals =='

SELECT count(*)                          AS changes,
       count(total_ms)                   AS timed,
       count(*) - count(total_ms)        AS untimed,
       min(t1_oracle)                    AS first_change,
       max(t1_oracle)                    AS last_change,
       min(kafka_offset)                 AS first_offset,
       max(kafka_offset)                 AS last_offset
FROM t24_cdc_trace;

\echo ''
\echo '== By operation =='

-- 'untimed' = poller wasn't running for that change, so stages t3/t4 are
-- missing. t1/t2 stay exact - they ride inside the message/envelope.
SELECT op,
       count(*)                          AS changes,
       count(total_ms)                   AS timed,
       count(*) - count(total_ms)        AS untimed,
       round(avg(total_ms)::numeric, 0)  AS total_avg,
       min(total_ms)                     AS total_min,
       max(total_ms)                     AS total_max
FROM t24_cdc_trace
GROUP BY op ORDER BY op;

\echo ''
\echo '== By workload (recid prefix) =='

-- BENCH- = benchmarks/03-oracle-load.py
-- SEED-  = seed/seed_xml.py / seed/seed_blob.py
-- Both call t24.stamp_c250() (oracle/oracle-setup.sql) for t1, so
-- they're directly comparable.
SELECT split_part(recid, '-', 1)                   AS workload,
       op,
       count(*)                                    AS changes,
       round(avg(oracle_to_kafka_ms)::numeric, 0)  AS ora2kafka,
       round(avg(kafka_to_rw_ms)::numeric, 0)      AS kafka2rw,
       round(avg(rw_to_parsed_ms)::numeric, 0)     AS rw2parsed,
       round(avg(total_ms)::numeric, 0)            AS total_avg
FROM t24_cdc_trace
WHERE total_ms IS NOT NULL
GROUP BY 1, 2 ORDER BY 1, 2;

\echo ''
\echo '== Where the time goes (share of end-to-end) =='

SELECT round((100.0 * avg(oracle_to_kafka_ms) / avg(total_ms))::numeric, 1) AS ora2kafka_pct,
       round((100.0 * avg(kafka_to_rw_ms)     / avg(total_ms))::numeric, 1) AS kafka2rw_pct,
       round((100.0 * avg(rw_to_parsed_ms)    / avg(total_ms))::numeric, 1) AS rw2parsed_pct
FROM t24_cdc_trace
WHERE total_ms IS NOT NULL;

\echo ''
\echo '== Every row =='

SELECT recid,
       op,
       kafka_offset       AS "offset",
       t1_oracle,
       t2_kafka,
       t3_risingwave,
       t4_parsed,
       oracle_to_kafka_ms AS ora2kafka,
       kafka_to_rw_ms     AS kafka2rw,
       rw_to_parsed_ms    AS rw2parsed,
       total_ms,
       t1_precision       AS prec
FROM t24_cdc_trace
ORDER BY kafka_offset;
