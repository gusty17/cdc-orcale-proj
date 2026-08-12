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

-- BENCH- from benchmarks/03-oracle-load.py, SEED- from seed/seed_xml.py
-- and seed/seed_blob.py (both tables). LOAD-/BULK- are from generators
-- that no longer exist (tests/test-insert-cdc-continuous.py,
-- tests/test-insert-cdc-bulk.sql) - kept here only to sweep up any rows
-- still left over from before they were removed.
DELETE FROM t24.account
 WHERE recid LIKE 'BENCH-%' OR recid LIKE 'SEED-%' OR recid LIKE 'LOAD-%' OR recid LIKE 'BULK-%';
DELETE FROM t24.account_blob
 WHERE recid LIKE 'BENCH-%' OR recid LIKE 'SEED-%' OR recid LIKE 'LOAD-%' OR recid LIKE 'BULK-%';
COMMIT;

SELECT COUNT(*) AS generated_rows_remaining FROM t24.account
 WHERE recid LIKE 'BENCH-%' OR recid LIKE 'SEED-%' OR recid LIKE 'LOAD-%' OR recid LIKE 'BULK-%';
SELECT COUNT(*) AS generated_blob_rows_remaining FROM t24.account_blob
 WHERE recid LIKE 'BENCH-%' OR recid LIKE 'SEED-%' OR recid LIKE 'LOAD-%' OR recid LIKE 'BULK-%';
SELECT COUNT(*) AS total_rows      FROM t24.account;
SELECT COUNT(*) AS total_blob_rows FROM t24.account_blob;
