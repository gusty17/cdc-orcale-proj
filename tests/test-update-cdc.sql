-- =====================================================================
-- Manual CDC test - update the seed row and watch an op=u event hit Kafka
-- =====================================================================
-- Run against XEPDB1:
--   docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" "@/scripts/tests/test-update-cdc.sql"
--
-- Sets a new, random working balance (c27) on the seed row (RECID
-- 9000000112345001), using a FULL XMLTYPE replacement rather than
-- UPDATEXML. Testing showed Oracle's in-place UPDATEXML edits don't carry
-- enough redo detail for LogMiner to decode, so Debezium silently drops
-- them - a full-value SET is the only style proven to reliably produce a
-- captured op=u event (see README.md, "UPDATEXML changes are silently
-- dropped").
--
-- Re-runnable any time: each run picks a new balance, so consecutive runs
-- are each a distinct, visible event. This does change the seed row's
-- content - re-run oracle-setup.sql afterward if you want the original
-- sample values restored.
-- =====================================================================

SET SERVEROUTPUT ON

DECLARE
    v_clob    CLOB;
    v_bfile   BFILE := BFILENAME('SAMPLE_DIR', 'account_xml_data_sample.xml');
    v_dst     INTEGER := 1;
    v_src     INTEGER := 1;
    v_lang    INTEGER := 0;
    v_warn    INTEGER;
    v_recid   VARCHAR2(255) := '9000000112345001';
    v_balance VARCHAR2(20);
BEGIN
    -- new, visibly different balance every run
    v_balance := TO_CHAR(ROUND(DBMS_RANDOM.VALUE(1000, 99999), 2), 'FM99999.00');

    DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);
    DBMS_LOB.FILEOPEN(v_bfile, DBMS_LOB.FILE_READONLY);
    DBMS_LOB.LOADCLOBFROMFILE(v_clob, v_bfile, DBMS_LOB.LOBMAXSIZE,
                              v_dst, v_src, 873 /*AL32UTF8*/, v_lang, v_warn);
    DBMS_LOB.FILECLOSE(v_bfile);

    -- swap the working balance (c27) inside the loaded document
    v_clob := REGEXP_REPLACE(v_clob, '<c27>[^<]*</c27>', '<c27>' || v_balance || '</c27>');

    UPDATE t24.account
       SET xmlrecord = XMLTYPE(v_clob)
     WHERE recid = v_recid;

    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20002,
            'Seed row ' || v_recid || ' not found - run oracle-setup.sql first.');
    END IF;

    COMMIT;
    DBMS_LOB.FREETEMPORARY(v_clob);

    DBMS_OUTPUT.PUT_LINE('Updated ' || v_recid || ' - new working balance (c27): ' || v_balance);
    DBMS_OUTPUT.PUT_LINE('Watch topic t24.T24.ACCOUNT for an op=u (update) event.');
END;
/
