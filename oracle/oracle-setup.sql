-- T24.ACCOUNT holds two columns, RECID and XMLRECORD - a 1:1 copy of
-- production data, which everything downstream is built against.
-- =====================================================================

SET SERVEROUTPUT ON
SET ECHO OFF
WHENEVER SQLERROR EXIT SQL.SQLCODE

PROMPT
PROMPT ============================================================
PROMPT Application schema T24
PROMPT ============================================================

DECLARE
    v_cnt PLS_INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_cnt FROM dba_users WHERE username = 'T24';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER t24 IDENTIFIED BY t24 ' ||
                          'DEFAULT TABLESPACE users QUOTA UNLIMITED ON users';
        DBMS_OUTPUT.PUT_LINE('Created user T24.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('User T24 already exists.');
    END IF;
END;
/

GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE TO t24;

PROMPT
PROMPT ============================================================
PROMPT T24.ACCOUNT  -  RECID + XMLRECORD, nothing else
PROMPT ============================================================

--   RECID     VARCHAR2(255)  - the record key
--   XMLRECORD XMLTYPE        - the whole record as XML

DECLARE
    v_cnt PLS_INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_cnt
      FROM dba_tables WHERE owner = 'T24' AND table_name = 'ACCOUNT';

    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[
            CREATE TABLE t24.account (
                recid     VARCHAR2(255) NOT NULL,
                xmlrecord XMLTYPE,
                CONSTRAINT pk_account PRIMARY KEY (recid)
            )
        ]';
        DBMS_OUTPUT.PUT_LINE('Created T24.ACCOUNT.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('T24.ACCOUNT already exists.');
    END IF;
END;
/

PROMPT
PROMPT ============================================================
PROMPT Table-level supplemental logging on T24.ACCOUNT
PROMPT ============================================================

-- ALL COLUMNS, not a log group naming XMLRECORD directly - Oracle rejects
-- LOB columns in a log group (ORA-30569). XMLRECORD still reaches
-- Debezium via LOB redo entries, consumed through lob.enabled=true.
DECLARE
    v_cnt PLS_INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_cnt
      FROM dba_log_groups
     WHERE owner = 'T24'
       AND table_name = 'ACCOUNT'
       AND log_group_type = 'ALL COLUMN LOGGING';

    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE t24.account ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
        DBMS_OUTPUT.PUT_LINE('Added ALL COLUMNS supplemental logging on T24.ACCOUNT.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('T24.ACCOUNT already has ALL COLUMNS supplemental logging.');
    END IF;
END;
/

PROMPT
PROMPT ============================================================
PROMPT Seed row - the complete sample record, tag for tag
PROMPT ============================================================

CREATE OR REPLACE DIRECTORY sample_dir AS '/scripts/sample-data';

-- Loaded from the file, not typed inline, so it's byte-for-byte the real
-- sample. RECID is read from the file's row/@id - a lab convenience only;
-- in production RECID is stored independently of the XML.
DECLARE
    v_clob  CLOB;
    v_bfile BFILE := BFILENAME('SAMPLE_DIR', 'account_xml_data_sample.xml');
    v_dst   INTEGER := 1;
    v_src   INTEGER := 1;
    v_lang  INTEGER := 0;
    v_warn  INTEGER;
    v_recid VARCHAR2(255);
BEGIN
    DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);
    DBMS_LOB.FILEOPEN(v_bfile, DBMS_LOB.FILE_READONLY);
    -- 873 = AL32UTF8
    DBMS_LOB.LOADCLOBFROMFILE(v_clob, v_bfile, DBMS_LOB.LOBMAXSIZE,
                              v_dst, v_src, 873, v_lang, v_warn);
    DBMS_LOB.FILECLOSE(v_bfile);

    SELECT XMLCAST(XMLQUERY('/row/@id' PASSING XMLTYPE(v_clob) RETURNING CONTENT) AS VARCHAR2(255))
      INTO v_recid FROM dual;

    MERGE INTO t24.account a
    USING (SELECT v_recid AS recid FROM dual) s
    ON (a.recid = s.recid)
    WHEN MATCHED THEN
        UPDATE SET a.xmlrecord = XMLTYPE(v_clob)
    WHEN NOT MATCHED THEN
        INSERT (recid, xmlrecord) VALUES (s.recid, XMLTYPE(v_clob));
    COMMIT;

    DBMS_LOB.FREETEMPORARY(v_clob);
    DBMS_OUTPUT.PUT_LINE('Loaded record ' || v_recid || ' from sample file.');
END;
/
