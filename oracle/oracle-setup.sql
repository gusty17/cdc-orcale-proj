-- Creates the T24 schema and its two account tables - T24.ACCOUNT
-- (XMLTYPE) and T24.ACCOUNT_BLOB (BLOB) - plus the config each needs to
-- reach Debezium. No data loading here: seeding/inserting is a separate,
-- deliberate step - see seed/seed_xml.py and seed/seed_blob.py, which
-- read sample files directly from the host and build every INSERT
-- client-side, so this file has nothing else to provision for them.
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

-- CREATE PROCEDURE is for stamp_c250() below - must be a stored,
-- schema-level function, not block-local, because PL/SQL blocks cannot
-- call a block-local function from a SQL statement (PLS-00231), and
-- calling it from inside an INSERT/UPDATE is what keeps the timestamp
-- atomic with the commit.
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE PROCEDURE TO t24;

PROMPT
PROMPT ============================================================
PROMPT Helper function - shared by seed/, tests/, and benchmarks/
PROMPT ============================================================

-- Stamps c250 (T24's real 'date_time' field) with Oracle's own current
-- moment, to the millisecond. One canonical place for this so every
-- caller - seed/_xml_ops.py's insert_one()/update_one(), used by
-- seed/seed_xml.py, tests/test-update-cdc.py, and
-- benchmarks/03-oracle-load.py - shares the exact same logic instead of
-- each re-implementing the REGEXP_REPLACE.
--
-- c250 appears twice in the sample data (the base tag and its m="2"
-- sibling); REGEXP_REPLACE with backreferences keeps whichever opening
-- tag was actually there while replacing only the value inside it.
--
-- SYS_EXTRACT_UTC, not bare SYSTIMESTAMP - matches Kafka/Debezium's UTC
-- convention explicitly rather than relying on the container also
-- happening to run in UTC.
--
-- seed/seed_blob.py does NOT call this - REGEXP_REPLACE only works on
-- text (CLOB/VARCHAR2), never BLOB, and that script deliberately builds
-- its BLOB client-side to avoid needing a text-to-BLOB bridge function.
CREATE OR REPLACE FUNCTION t24.stamp_c250(p_xml CLOB) RETURN CLOB IS
BEGIN
    RETURN REGEXP_REPLACE(p_xml, '(<c250[^>]*>)[^<]*(</c250>)',
        '\1' || TO_CHAR(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'YYMMDDHH24MISSFF3') || '\2');
END;
/

GRANT EXECUTE ON t24.stamp_c250 TO system;

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
PROMPT T24.ACCOUNT_BLOB  -  RECID + BLOBRECORD, the BLOB storage path
PROMPT ============================================================

-- A SEPARATE table, not a second column on T24.ACCOUNT. In T24 a given
-- table is either XML or BLOB, never both - a second column would put
-- both representations in every row, doubling redo per change and
-- corrupting the latency baseline the benchmarks measure.
--   RECID      VARCHAR2(255)  - the record key
--   BLOBRECORD BLOB           - the whole record as raw bytes (XML text,
--                                stored as a BLOB instead of XMLTYPE)

DECLARE
    v_cnt PLS_INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_cnt
      FROM dba_tables WHERE owner = 'T24' AND table_name = 'ACCOUNT_BLOB';

    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[
            CREATE TABLE t24.account_blob (
                recid      VARCHAR2(255) NOT NULL,
                blobrecord BLOB,
                CONSTRAINT pk_account_blob PRIMARY KEY (recid)
            )
        ]';
        DBMS_OUTPUT.PUT_LINE('Created T24.ACCOUNT_BLOB.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('T24.ACCOUNT_BLOB already exists.');
    END IF;
END;
/

PROMPT
PROMPT ============================================================
PROMPT Table-level supplemental logging on T24.ACCOUNT_BLOB
PROMPT ============================================================

-- Same requirement, same reason as T24.ACCOUNT above: ALL COLUMNS, not a
-- log group naming BLOBRECORD directly - ORA-30569 rejects a LOB column
-- in a named log group. Applies identically to a native BLOB.
DECLARE
    v_cnt PLS_INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_cnt
      FROM dba_log_groups
     WHERE owner = 'T24'
       AND table_name = 'ACCOUNT_BLOB'
       AND log_group_type = 'ALL COLUMN LOGGING';

    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE t24.account_blob ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
        DBMS_OUTPUT.PUT_LINE('Added ALL COLUMNS supplemental logging on T24.ACCOUNT_BLOB.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('T24.ACCOUNT_BLOB already has ALL COLUMNS supplemental logging.');
    END IF;
END;
/
