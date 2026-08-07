-- =====================================================================
-- CDC trace - Oracle side. Generates inserts, updates and deletes, each
-- stamping its own commit time into the row.
-- =====================================================================
-- Run AFTER 01-trace-setup.sql, with 02-trace-poller.py already running:
--   docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" \
--     "@/scripts/benchmarks/03-oracle-load.sql"
--
-- Commit time is written into the XML as <bts>epoch_ms</bts>, giving an
-- exact millisecond t1 - the alternative (source.ts_ms) is whole seconds only.
--
-- <bts> is deliberately not a <cNNN> tag, so the flattening regex ignores it.
--
-- Deletes fall back to source.ts_ms ('second' precision) - no XML to
-- stamp a time into.
--
-- Pacing must stay below the throughput knee or you measure queueing,
-- not arrival time - 200ms/5rps saturates and never recovers; 2000ms drains.
-- =====================================================================

SET SERVEROUTPUT ON

DECLARE
    v_inserts    PLS_INTEGER := 20;
    v_updates    PLS_INTEGER := 10;
    v_deletes    PLS_INTEGER := 10;
    v_delay_ms   NUMBER      := 2000;

    TYPE t_recids IS TABLE OF VARCHAR2(255) INDEX BY PLS_INTEGER;
    v_recids     t_recids;

    v_template   CLOB;
    -- build_xml()'s result must land in a variable before the DML -
    -- PLS-00231: a function declared in a block can't be called from SQL.
    v_clob       CLOB;
    v_bfile      BFILE;
    v_dst        INTEGER;
    v_src        INTEGER;
    v_lang       INTEGER := 0;
    v_warn       INTEGER;
    v_recid      VARCHAR2(255);

    -- NUMBER, not PLS_INTEGER - epoch ms (~1.78e12) overflows PLS_INTEGER (ORA-01426).
    v_ts         NUMBER;

    -- Epoch ms from SYSTIMESTAMP - whole seconds and the FF3 fraction are
    -- computed separately since CAST(...AS DATE) drops fractional seconds.
    -- SYS_EXTRACT_UTC to match Kafka/Debezium's UTC stamps.
    FUNCTION now_ms RETURN NUMBER IS
        v_now TIMESTAMP := SYS_EXTRACT_UTC(SYSTIMESTAMP);
    BEGIN
        RETURN (CAST(v_now AS DATE) - DATE '1970-01-01') * 86400000
               + TO_NUMBER(TO_CHAR(v_now, 'FF3'));
    END;

    -- Stamps the id and the commit-time marker into a copy of the sample.
    FUNCTION build_xml(p_recid VARCHAR2, p_ts NUMBER) RETURN CLOB IS
        v CLOB;
    BEGIN
        v := REPLACE(v_template, 'id="9000000112345001"', 'id="' || p_recid || '"');
        -- <bts> only - <ots> is added by Oracle in the DML below, so the
        -- two stamps come from the two sides being measured.
        RETURN REPLACE(v, '</row>',
                       '<bts>' || TO_CHAR(p_ts, 'FM9999999999999') || '</bts></row>');
    END;
BEGIN
    -- Loaded once, outside the loops - re-reading per change would
    -- inflate the very number being measured.
    v_dst := 1;
    v_src := 1;
    v_bfile := BFILENAME('SAMPLE_DIR', 'account_xml_data_sample.xml');
    DBMS_LOB.CREATETEMPORARY(v_template, TRUE);
    DBMS_LOB.FILEOPEN(v_bfile, DBMS_LOB.FILE_READONLY);
    DBMS_LOB.LOADCLOBFROMFILE(v_template, v_bfile, DBMS_LOB.LOBMAXSIZE,
                              v_dst, v_src, 873 /*AL32UTF8*/, v_lang, v_warn);
    DBMS_LOB.FILECLOSE(v_bfile);

    ---------------------------------------------------------------- INSERT
    FOR i IN 1..v_inserts LOOP
        v_ts    := now_ms;
        v_recid := 'BENCH-' || TO_CHAR(v_ts, 'FM9999999999999') || '-' || i;
        v_recids(i) := v_recid;
        v_clob  := build_xml(v_recid, v_ts);

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
        COMMIT;

        DBMS_SESSION.SLEEP(v_delay_ms / 1000);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('inserted ' || v_inserts);

    ---------------------------------------------------------------- UPDATE
    -- Re-stamps <bts> with the update's own commit time, so the update
    -- event carries an exact t1 of its own rather than the insert's.
    FOR i IN 1..v_updates LOOP
        v_ts   := now_ms;
        v_clob := build_xml(v_recids(i), v_ts);

        UPDATE t24.account
           SET xmlrecord = XMLTYPE(REPLACE(v_clob, '</row>',
                   '<ots>' || TO_CHAR(
                       (SELECT EXTRACT(DAY    FROM d) * 86400000
                             + EXTRACT(HOUR   FROM d) * 3600000
                             + EXTRACT(MINUTE FROM d) * 60000
                             + ROUND(EXTRACT(SECOND FROM d) * 1000)
                          FROM (SELECT SYS_EXTRACT_UTC(SYSTIMESTAMP)
                                       - TIMESTAMP '1970-01-01 00:00:00' AS d FROM dual)),
                       'FM9999999999999') || '</ots></row>'))
         WHERE recid = v_recids(i);
        COMMIT;

        DBMS_SESSION.SLEEP(v_delay_ms / 1000);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('updated ' || v_updates);

    ---------------------------------------------------------------- DELETE
    -- Deletes the tail of the inserted set, so the updated rows above
    -- keep their own trace entries intact.
    FOR i IN REVERSE (v_inserts - v_deletes + 1)..v_inserts LOOP
        DELETE FROM t24.account WHERE recid = v_recids(i);
        COMMIT;

        DBMS_SESSION.SLEEP(v_delay_ms / 1000);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('deleted ' || v_deletes);

    DBMS_LOB.FREETEMPORARY(v_template);
    DBMS_OUTPUT.PUT_LINE('done - ' || (v_inserts + v_updates + v_deletes) || ' change events');
END;
/
