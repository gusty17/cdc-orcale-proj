-- =====================================================================
-- Step 1 - Oracle prerequisites for Debezium LogMiner CDC
-- =====================================================================
-- Run as SYSDBA, connected to the ROOT container (CDB$ROOT):
--   sqlplus sys/oracle@//localhost:1521/XE as sysdba @oracle-prereqs.sql
--
-- Target: Oracle XE 21c (CDB = XE, PDB = XEPDB1)
-- This script is idempotent - safe to re-run.
-- =====================================================================

SET SERVEROUTPUT ON
SET ECHO OFF
WHENEVER SQLERROR EXIT SQL.SQLCODE

ALTER SESSION SET CONTAINER = CDB$ROOT;

PROMPT
PROMPT ============================================================
PROMPT 1.1  ARCHIVELOG mode
PROMPT ============================================================

-- Debezium reads redo + archived redo through LogMiner, so the database
-- MUST be in ARCHIVELOG mode. Turning it on requires a restart in MOUNT
-- state, which cannot be done from an ordinary session - so we only
-- verify here and fail loudly with instructions.
DECLARE
    v_log_mode v$database.log_mode%TYPE;
BEGIN
    SELECT log_mode INTO v_log_mode FROM v$database;
    DBMS_OUTPUT.PUT_LINE('log_mode = ' || v_log_mode);

    IF v_log_mode <> 'ARCHIVELOG' THEN
        RAISE_APPLICATION_ERROR(-20001,
            'Database is in ' || v_log_mode || '. Enable ARCHIVELOG first.' || CHR(10) ||
            'With the gvenzl/oracle-xe container: set ENABLE_ARCHIVELOG=true and'  || CHR(10) ||
            'recreate the volume, or run manually as SYSDBA:'                      || CHR(10) ||
            '    SHUTDOWN IMMEDIATE;'                                              || CHR(10) ||
            '    STARTUP MOUNT;'                                                   || CHR(10) ||
            '    ALTER DATABASE ARCHIVELOG;'                                       || CHR(10) ||
            '    ALTER DATABASE OPEN;');
    END IF;
END;
/

PROMPT
PROMPT ============================================================
PROMPT 1.2  Database-level supplemental logging (minimal)
PROMPT ============================================================

-- Without this, LogMiner cannot reconstruct the row that a redo entry
-- belongs to and Debezium emits incomplete / unusable change events.
DECLARE
    v_min VARCHAR2(8);
BEGIN
    SELECT supplemental_log_data_min INTO v_min FROM v$database;

    IF v_min = 'NO' THEN
        EXECUTE IMMEDIATE 'ALTER DATABASE ADD SUPPLEMENTAL LOG DATA';
        DBMS_OUTPUT.PUT_LINE('Minimal supplemental logging ENABLED.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Minimal supplemental logging already enabled (' || v_min || ').');
    END IF;
END;
/

-- Force logging keeps NOLOGGING / direct-path operations out of the blind
-- spot; without it a bulk load can silently bypass CDC.
DECLARE
    v_force VARCHAR2(39);
BEGIN
    SELECT force_logging INTO v_force FROM v$database;

    IF v_force <> 'YES' THEN
        EXECUTE IMMEDIATE 'ALTER DATABASE FORCE LOGGING';
        DBMS_OUTPUT.PUT_LINE('Force logging ENABLED.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Force logging already enabled.');
    END IF;
END;
/


PROMPT
PROMPT ============================================================
PROMPT 1.3  Redo log sizing (dev convenience, optional)
PROMPT ============================================================

-- XE ships with small redo logs; frequent switches mean Debezium spends
-- its time re-mining archives. Not required, but recommended for a lab.
-- Left commented so this script stays non-destructive.
--
--   ALTER DATABASE ADD LOGFILE GROUP 4 ('/opt/oracle/oradata/XE/redo04.log') SIZE 400M;
--   ALTER DATABASE ADD LOGFILE GROUP 5 ('/opt/oracle/oradata/XE/redo05.log') SIZE 400M;
--   ALTER DATABASE ADD LOGFILE GROUP 6 ('/opt/oracle/oradata/XE/redo06.log') SIZE 400M;
--   -- then ALTER SYSTEM SWITCH LOGFILE / CHECKPOINT until groups 1-3 are
--   -- INACTIVE and drop them.

PROMPT
PROMPT ============================================================
PROMPT 1.4  LogMiner tablespaces (CDB + PDB)
PROMPT ============================================================

-- The Debezium user needs a default tablespace it can write to; keeping
-- it separate from SYSTEM/USERS makes it easy to cap and to drop later.
DECLARE
    v_cnt PLS_INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_cnt FROM dba_tablespaces WHERE tablespace_name = 'LOGMINER_TBS';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE 'CREATE TABLESPACE LOGMINER_TBS DATAFILE ' ||
                          '''/opt/oracle/oradata/XE/logminer_tbs.dbf'' ' ||
                          'SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED';
        DBMS_OUTPUT.PUT_LINE('Created LOGMINER_TBS in CDB$ROOT.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('LOGMINER_TBS already exists in CDB$ROOT.');
    END IF;
END;
/

ALTER SESSION SET CONTAINER = XEPDB1;

DECLARE
    v_cnt PLS_INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_cnt FROM dba_tablespaces WHERE tablespace_name = 'LOGMINER_TBS';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE 'CREATE TABLESPACE LOGMINER_TBS DATAFILE ' ||
                          '''/opt/oracle/oradata/XE/XEPDB1/logminer_tbs.dbf'' ' ||
                          'SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED';
        DBMS_OUTPUT.PUT_LINE('Created LOGMINER_TBS in XEPDB1.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('LOGMINER_TBS already exists in XEPDB1.');
    END IF;
END;
/

ALTER SESSION SET CONTAINER = CDB$ROOT;

PROMPT
PROMPT ============================================================
PROMPT 1.5  Debezium capture user (common user, CONTAINER=ALL)
PROMPT ============================================================

-- In a CDB the connector logs in to the root container and switches into
-- the PDB, so the account has to be a common user (C## prefix).
DECLARE
    v_cnt PLS_INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_cnt FROM dba_users WHERE username = 'C##DBZUSER';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER c##dbzuser IDENTIFIED BY dbz ' ||
                          'DEFAULT TABLESPACE logminer_tbs ' ||
                          'QUOTA UNLIMITED ON logminer_tbs ' ||
                          'CONTAINER=ALL';
        DBMS_OUTPUT.PUT_LINE('Created C##DBZUSER.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('C##DBZUSER already exists.');
    END IF;
END;
/

-- --- session / container -------------------------------------------------
GRANT CREATE SESSION                      TO c##dbzuser CONTAINER=ALL;
GRANT SET CONTAINER                       TO c##dbzuser CONTAINER=ALL;

-- --- LogMiner ------------------------------------------------------------
GRANT LOGMINING                           TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE                TO c##dbzuser CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE                 TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR              TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D            TO c##dbzuser CONTAINER=ALL;

-- --- snapshot / schema reads ---------------------------------------------
GRANT SELECT ANY TABLE                    TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION              TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY DICTIONARY               TO c##dbzuser CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE                 TO c##dbzuser CONTAINER=ALL;
GRANT LOCK ANY TABLE                      TO c##dbzuser CONTAINER=ALL;

-- --- the connector's own bookkeeping objects -----------------------------
GRANT CREATE TABLE                        TO c##dbzuser CONTAINER=ALL;
GRANT CREATE SEQUENCE                     TO c##dbzuser CONTAINER=ALL;

-- --- V$ views the connector polls ----------------------------------------
GRANT SELECT ON V_$DATABASE               TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG                    TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG_HISTORY            TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE                TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_LOGS            TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS        TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_PARAMETERS      TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG           TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS    TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$TRANSACTION            TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$MYSTAT                 TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$STATNAME               TO c##dbzuser CONTAINER=ALL;

PROMPT
PROMPT ============================================================
PROMPT 1.6  Verification
PROMPT ============================================================

SET LINESIZE 200
COLUMN name        FORMAT A32
COLUMN value       FORMAT A12
COLUMN dest_name   FORMAT A28
COLUMN destination FORMAT A32

SELECT log_mode,
       force_logging,
       supplemental_log_data_min AS supp_min,
       supplemental_log_data_pk  AS supp_pk,
       supplemental_log_data_all AS supp_all
  FROM v$database;

-- Informational only - this script no longer sets it. FALSE is expected
-- and correct; see the note above section 1.4.
SELECT name, value FROM v$parameter WHERE name = 'enable_goldengate_replication';

SELECT username, common, default_tablespace, account_status
  FROM dba_users WHERE username = 'C##DBZUSER';

-- The archive destination must be VALID, otherwise redo piles up and the
-- database eventually hangs with "archiver stuck".
SELECT dest_name, status, destination
  FROM v$archive_dest_status
 WHERE status <> 'INACTIVE';

PROMPT
PROMPT Step 1 (instance level) done. Next: oracle-setup.sql against XEPDB1.
PROMPT
