-- =====================================================================
-- Manual CDC test - delete a test row and watch an op=d event hit Kafka
-- =====================================================================
-- Run against XEPDB1:
--   docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" "@/scripts/tests/test-delete-cdc.sql"
--
-- Deletes the MOST RECENTLY inserted row created by test-insert-cdc.sql
-- (recid pattern '9000000112345001-<timestamp>') - never the permanent
-- seed row itself, so this is safe to run without needing to reload
-- anything afterward. Run test-insert-cdc.sql at least once first, or
-- this has nothing to delete.
--
-- Debezium emits a delete as TWO Kafka messages: an op=d event carrying
-- the last known row content, followed by a tombstone (a message with the
-- same key and a null value) that tells downstream consumers to drop any
-- cached copy of that key.
-- =====================================================================

SET SERVEROUTPUT ON

DECLARE
    v_recid VARCHAR2(255);
BEGIN
    SELECT recid INTO v_recid
      FROM (
          SELECT recid FROM t24.account
           WHERE recid LIKE '9000000112345001-%'
           ORDER BY recid DESC
      )
     WHERE ROWNUM = 1;

    DELETE FROM t24.account WHERE recid = v_recid;
    COMMIT;

    DBMS_OUTPUT.PUT_LINE('Deleted row: ' || v_recid);
    DBMS_OUTPUT.PUT_LINE('Watch topic t24.T24.ACCOUNT for an op=d event, then a tombstone (null value) for the same key.');
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No test rows found to delete. Run test-insert-cdc.sql first.');
END;
/
