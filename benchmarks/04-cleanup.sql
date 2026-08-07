-- =====================================================================
-- Remove benchmark rows from Oracle. The deletes flow through CDC like
-- any other change, so t24_account drains itself.
-- =====================================================================
--   docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" \
--     "@/scripts/benchmarks/04-cleanup.sql"
--
-- One CDC event per deleted row - expect a burst if there are hundreds.
-- RisingWave trace objects need no cleanup - 01-trace-setup.sql recreates them.
-- =====================================================================

-- BENCH- from benchmarks/03-oracle-load.sql, LOAD- from
-- tests/test-insert-cdc-continuous.py, BULK- from
-- tests/test-insert-cdc-bulk.sql.
DELETE FROM t24.account
 WHERE recid LIKE 'BENCH-%' OR recid LIKE 'LOAD-%' OR recid LIKE 'BULK-%';
COMMIT;

SELECT COUNT(*) AS generated_rows_remaining FROM t24.account
 WHERE recid LIKE 'BENCH-%' OR recid LIKE 'LOAD-%' OR recid LIKE 'BULK-%';
SELECT COUNT(*) AS total_rows FROM t24.account;
