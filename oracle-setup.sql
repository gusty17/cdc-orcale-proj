-- =====================================================================
-- Step 1 (continued) - Application schema + table-level supplemental log
-- =====================================================================
-- Run as SYSDBA against the PDB:
--   sqlplus sys/oracle@//localhost:1521/XEPDB1 as sysdba @oracle-setup.sql
--
-- T24.ACCOUNT holds exactly two columns, RECID and XMLRECORD, and the
-- XMLRECORD carries the complete record - every tag from the source
-- sample, unmodified. That makes the lab row a 1:1 copy of production
-- data, which is what the connector and everything downstream is built
-- against.
--
-- Idempotent - safe to re-run.
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
--
-- Every business field (currency, balances, customer, dates, ...) lives
-- inside the XML as c1, c2, c8, ... - see sample-data/lookup_metadata.csv
-- for the c-number -> business-name mapping. Nothing is projected out into
-- its own column here: extraction is a downstream concern.
--
-- Note on the real source table: it additionally declares 9 VIRTUAL
-- columns (CURRENCY, CATEGORY, CUSTOMER, ...) that run EXTRACTVALUE over
-- the XML at read time. They store nothing, produce no redo, and Debezium
-- streams the text of their defining expression instead of a value - so
-- oracle-connector.json pins column.include.list to RECID|XMLRECORD. That
-- guard stays in place even though this table has no virtual columns to
-- exclude, because the production table does.
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

-- ALL COLUMNS rather than an explicit log group over (RECID, XMLRECORD):
-- Oracle rejects LOB-backed columns inside a supplemental log group, so
-- naming XMLRECORD there fails with "ORA-30569: data type of given column
-- is not supported in a log group". ALL COLUMNS covers RECID (the only
-- plain stored column), and the XML itself reaches Debezium through the
-- LOB redo entries that lob.enabled=true consumes.
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

-- Loaded straight from the file rather than typed inline, so the row is
-- byte-for-byte the source sample: all 60 distinct tags, the repeated
-- multi-value entries (c46-c50, c99, c100, c249, c250) and the Arabic
-- account titles all survive.
DECLARE
    v_clob  CLOB;
    v_bfile BFILE := BFILENAME('SAMPLE_DIR', 'account_xml_data_sample.xml');
    v_dst   INTEGER := 1;
    v_src   INTEGER := 1;
    v_lang  INTEGER := 0;
    v_warn  INTEGER;
BEGIN
    DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);
    DBMS_LOB.FILEOPEN(v_bfile, DBMS_LOB.FILE_READONLY);
    -- 873 = AL32UTF8
    DBMS_LOB.LOADCLOBFROMFILE(v_clob, v_bfile, DBMS_LOB.LOBMAXSIZE,
                              v_dst, v_src, 873, v_lang, v_warn);
    DBMS_LOB.FILECLOSE(v_bfile);

    MERGE INTO t24.account a
    USING (SELECT '9000000112345001' AS recid FROM dual) s
    ON (a.recid = s.recid)
    WHEN MATCHED THEN
        UPDATE SET a.xmlrecord = XMLTYPE(v_clob)
    WHEN NOT MATCHED THEN
        INSERT (recid, xmlrecord) VALUES (s.recid, XMLTYPE(v_clob));
    COMMIT;

    DBMS_LOB.FREETEMPORARY(v_clob);
    DBMS_OUTPUT.PUT_LINE('Loaded full sample record 9000000112345001.');
END;
/

PROMPT
PROMPT ============================================================
PROMPT Verification
PROMPT ============================================================

SET LINESIZE 200
COLUMN column_name FORMAT A16
COLUMN data_type   FORMAT A10

PROMPT -- Two columns. (SYS_NC00003$ is Oracle's own binary-XML storage for
PROMPT --  XMLRECORD, not a column anyone declared.)
SELECT internal_column_id AS col, column_name, data_type,
       hidden_column AS hidden, virtual_column AS virt, segment_column_id AS seg
  FROM dba_tab_cols
 WHERE owner = 'T24' AND table_name = 'ACCOUNT'
 ORDER BY internal_column_id;

SELECT log_group_name, log_group_type, always
  FROM dba_log_groups
 WHERE owner = 'T24' AND table_name = 'ACCOUNT';

PROMPT -- The stored XML is complete: node count and a few spot checks.
SELECT a.recid,
       (SELECT COUNT(*) FROM XMLTABLE('/row/*' PASSING a.xmlrecord)) AS xml_nodes,
       DBMS_LOB.GETLENGTH(a.xmlrecord.getClobVal())                  AS xml_chars
  FROM t24.account a;

COLUMN customer FORMAT A12
COLUMN currency FORMAT A10
COLUMN balance  FORMAT A14
COLUMN title    FORMAT A24
COLUMN opened   FORMAT A10
SELECT XMLCAST(XMLQUERY('/row/c1'  PASSING a.xmlrecord RETURNING CONTENT) AS VARCHAR2(50)) AS customer,
       XMLCAST(XMLQUERY('/row/c8'  PASSING a.xmlrecord RETURNING CONTENT) AS VARCHAR2(50)) AS currency,
       XMLCAST(XMLQUERY('/row/c27' PASSING a.xmlrecord RETURNING CONTENT) AS VARCHAR2(50)) AS balance,
       XMLCAST(XMLQUERY('/row/c3'  PASSING a.xmlrecord RETURNING CONTENT) AS VARCHAR2(50)) AS title,
       XMLCAST(XMLQUERY('/row/c78' PASSING a.xmlrecord RETURNING CONTENT) AS VARCHAR2(50)) AS opened
  FROM t24.account a;

PROMPT
PROMPT Step 1 complete. Oracle is ready for the Debezium connector.
PROMPT
