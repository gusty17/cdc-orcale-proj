-- =====================================================================
-- Bulk CDC test - insert many T24.ACCOUNT rows in ONE transaction, with
-- a single COMMIT at the end.
-- =====================================================================
-- Run against XEPDB1:
--   docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" "@/scripts/tests/test-insert-cdc-bulk.sql"
-- =====================================================================

SET SERVEROUTPUT ON

DECLARE
    -- Raise to make the burst harder - 100 already exceeds real-time drain.
    v_num_rows PLS_INTEGER := 100;

    v_template CLOB;
    v_clob     CLOB;
    v_bfile    BFILE;
    v_dst      INTEGER;
    v_src      INTEGER;
    v_lang     INTEGER := 0;
    v_warn     INTEGER;
    v_recid    VARCHAR2(255);
    v_batch    VARCHAR2(30);

    v_ts       NUMBER;
BEGIN
    -- Read the sample once, outside the loop - not once per row.
    v_dst := 1;
    v_src := 1;
    v_bfile := BFILENAME('SAMPLE_DIR', 'account_xml_data_sample.xml');
    DBMS_LOB.CREATETEMPORARY(v_template, TRUE);
    DBMS_LOB.FILEOPEN(v_bfile, DBMS_LOB.FILE_READONLY);
    DBMS_LOB.LOADCLOBFROMFILE(v_template, v_bfile, DBMS_LOB.LOBMAXSIZE,
                              v_dst, v_src, 873 /*AL32UTF8*/, v_lang, v_warn);
    DBMS_LOB.FILECLOSE(v_bfile);

    -- One batch id for the whole transaction, so every row of this run is
    -- identifiable as having arrived together.
    v_ts := (CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - DATE '1970-01-01') * 86400000
            + TO_NUMBER(TO_CHAR(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'FF3'));
    v_batch := TO_CHAR(v_ts, 'FM9999999999999');

    FOR i IN 1..v_num_rows LOOP
        v_recid := 'BULK-' || v_batch || '-' || i;

        v_clob := REPLACE(v_template, 'id="9000000112345001"', 'id="' || v_recid || '"');
        v_clob := REPLACE(v_clob, '</row>', '<bts>' || v_batch || '</bts></row>');

        -- <ots> stamped by Oracle here, <bts> by the client - splits
        -- "row built" from "INSERT executed".
        INSERT INTO t24.account (recid, xmlrecord)
        VALUES (v_recid, XMLTYPE(REPLACE(v_clob, '</row>',
                '<ots>' || TO_CHAR(
                    (SELECT EXTRACT(DAY    FROM d) * 86400000
                          + EXTRACT(HOUR   FROM d) * 3600000
                          + EXTRACT(MINUTE FROM d) * 60000
                          + ROUND(EXTRACT(SECOND FROM d) * 1000)
                       FROM (SELECT SYS_EXTRACT_UTC(SYSTIMESTAMP)
                                    - TIMESTAMP '1970-01-01 00:00:00' AS d FROM dual)),
                    'FM9999999999999') || '</ots></row>')));
        -- No COMMIT here. That is the whole design.
    END LOOP;

    COMMIT;

    DBMS_LOB.FREETEMPORARY(v_template);
    DBMS_OUTPUT.PUT_LINE('committed ' || v_num_rows || ' rows as batch ' || v_batch
                         || ' in a single transaction');
    DBMS_OUTPUT.PUT_LINE('watch them drain: SELECT count(*) FROM t24_account WHERE recid LIKE ''BULK-'
                         || v_batch || '%'';');
END;
/
