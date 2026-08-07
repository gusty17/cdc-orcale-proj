-- =====================================================================
-- Manual CDC test - update the seed row and watch an op=u event hit Kafka
-- =====================================================================
-- Run against XEPDB1:
--   docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" "@/scripts/tests/test-update-cdc.sql"
--
-- Sets new random values for 3 columns on the seed row

-- =====================================================================

SET SERVEROUTPUT ON

DECLARE
    v_clob    CLOB;
    v_bfile   BFILE := BFILENAME('SAMPLE_DIR', 'account_xml_data_sample.xml');
    v_dst     INTEGER := 1;
    v_src     INTEGER := 1;
    v_lang    INTEGER := 0;
    v_warn    INTEGER;
    v_recid            VARCHAR2(255) := '9000000112345001';
    v_open_actual_bal  VARCHAR2(20);   -- c23
    v_amnt_last_cr     VARCHAR2(20);   -- c29
    v_amnt_last_dr     VARCHAR2(20);   -- c38
BEGIN
    -- new, visibly different values every run
    v_open_actual_bal := TO_CHAR(ROUND(DBMS_RANDOM.VALUE(1000, 99999), 2), 'FM99999.00');
    v_amnt_last_cr    := TO_CHAR(ROUND(DBMS_RANDOM.VALUE(100, 9999), 2), 'FM9999.00');
    v_amnt_last_dr    := TO_CHAR(-ROUND(DBMS_RANDOM.VALUE(100, 9999), 2), 'FM9999.00');

    DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);
    DBMS_LOB.FILEOPEN(v_bfile, DBMS_LOB.FILE_READONLY);
    DBMS_LOB.LOADCLOBFROMFILE(v_clob, v_bfile, DBMS_LOB.LOBMAXSIZE,
                              v_dst, v_src, 873 /*AL32UTF8*/, v_lang, v_warn);
    DBMS_LOB.FILECLOSE(v_bfile);

    -- swap the 3 dashboard columns inside the loaded document
    v_clob := REGEXP_REPLACE(v_clob, '<c23>[^<]*</c23>', '<c23>' || v_open_actual_bal || '</c23>');
    v_clob := REGEXP_REPLACE(v_clob, '<c29>[^<]*</c29>', '<c29>' || v_amnt_last_cr    || '</c29>');
    v_clob := REGEXP_REPLACE(v_clob, '<c38>[^<]*</c38>', '<c38>' || v_amnt_last_dr    || '</c38>');

    UPDATE t24.account
       SET xmlrecord = XMLTYPE(v_clob)
     WHERE recid = v_recid;

    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20002,
            'Seed row ' || v_recid || ' not found - run oracle/oracle-setup.sql first.');
    END IF;

    COMMIT;
    DBMS_LOB.FREETEMPORARY(v_clob);

    DBMS_OUTPUT.PUT_LINE('Updated ' || v_recid || ':');
    DBMS_OUTPUT.PUT_LINE('  open_actual_bal  (c23) = ' || v_open_actual_bal);
    DBMS_OUTPUT.PUT_LINE('  amnt_last_cr_cust(c29) = ' || v_amnt_last_cr);
    DBMS_OUTPUT.PUT_LINE('  amnt_last_dr_cust(c38) = ' || v_amnt_last_dr);
    DBMS_OUTPUT.PUT_LINE('Watch topic t24.T24.ACCOUNT for an op=u (update) event.');
END;
/
