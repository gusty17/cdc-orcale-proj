-- =====================================================================
-- Manual CDC test - delete a test row and watch an op=d event hit Kafka
-- =====================================================================
-- Run against XEPDB1:
--   docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" "@/scripts/tests/test-delete-cdc.sql"
--
-- Deletes the most recent row from test-insert-cdc.sql, never the seed
-- row - safe to run any time. Needs test-insert-cdc.sql run first.

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
