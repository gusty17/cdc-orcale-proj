-- =====================================================================
-- Manual CDC test - insert a new T24.ACCOUNT row and watch it hit Kafka
-- =====================================================================
-- Run against XEPDB1 (see setup.ps1's Invoke-SqlFile for the same
-- connection pattern):
--   docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" "@/scripts/tests/test-insert-cdc.sql"
--
-- Loads the same real sample XML (sample-data/account_xml_data_sample.xml)
-- under a new RECID, so the inserted row is a faithful copy of production
-- data rather than a hand-typed stub - and produces an INSERT-flavoured
-- event (op=c) rather than the UPDATE-flavoured ones already exercised
-- while building this connector.
--
-- Not idempotent by design: run it again and it inserts ANOTHER new row
-- (RECID is timestamp-suffixed), so every run is a fresh, visible event.
-- =====================================================================

SET SERVEROUTPUT ON

DECLARE
    v_clob    CLOB;
    v_bfile   BFILE := BFILENAME('SAMPLE_DIR', 'account_xml_data_sample.xml');
    v_dst     INTEGER := 1;
    v_src     INTEGER := 1;
    v_lang    INTEGER := 0;
    v_warn    INTEGER;
    v_recid   VARCHAR2(255);
BEGIN
    -- New, unique key each run: base sample id + timestamp suffix.
    v_recid := '9000000112345001-' || TO_CHAR(SYSTIMESTAMP, 'HH24MISS-FF3');

    DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);
    DBMS_LOB.FILEOPEN(v_bfile, DBMS_LOB.FILE_READONLY);
    DBMS_LOB.LOADCLOBFROMFILE(v_clob, v_bfile, DBMS_LOB.LOBMAXSIZE,
                              v_dst, v_src, 873 /*AL32UTF8*/, v_lang, v_warn);
    DBMS_LOB.FILECLOSE(v_bfile);

    -- Give this copy its own row/@id too, so it doesn't look like a literal
    -- duplicate of the seed row inside the XML itself.
    v_clob := REPLACE(v_clob, 'id="9000000112345001"', 'id="' || v_recid || '"');

    INSERT INTO t24.account (recid, xmlrecord) VALUES (v_recid, XMLTYPE(v_clob));
    COMMIT;

    DBMS_LOB.FREETEMPORARY(v_clob);
    DBMS_OUTPUT.PUT_LINE('Inserted new row: ' || v_recid);
    DBMS_OUTPUT.PUT_LINE('Watch topic t24.T24.ACCOUNT for an op=c (create) event with this RECID.');
END;
/
