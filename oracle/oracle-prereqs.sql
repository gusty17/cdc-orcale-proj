-- =====================================================================
-- Step 1 - Oracle prerequisites for Debezium LogMiner CDC
-- =====================================================================


SET SERVEROUTPUT ON
SET ECHO OFF
WHENEVER SQLERROR EXIT SQL.SQLCODE

ALTER SESSION SET CONTAINER = CDB$ROOT;

PROMPT
PROMPT ============================================================
PROMPT 1.1  ARCHIVELOG mode
PROMPT ============================================================

-- LogMiner requires ARCHIVELOG (enabled by oracle-init/01_enable_archivelog.sql).
-- This just verifies it took effect and fails loudly if not.
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

--enables minimal supplemental logging if not already enabled.
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

-- Enable force logging if not already enabled.
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

-- Optional, commented out on purpose (kept non-destructive). Enlarge if
-- XE's small default redo logs cause frequent switches:
--
--   ALTER DATABASE ADD LOGFILE GROUP 4 ('/opt/oracle/oradata/XE/redo04.log') SIZE 400M;
--   ALTER DATABASE ADD LOGFILE GROUP 5 ('/opt/oracle/oradata/XE/redo05.log') SIZE 400M;
--   ALTER DATABASE ADD LOGFILE GROUP 6 ('/opt/oracle/oradata/XE/redo06.log') SIZE 400M;
--   -- then ALTER SYSTEM SWITCH LOGFILE / CHECKPOINT until groups 1-2 are
--   -- INACTIVE and drop them.

PROMPT
PROMPT ============================================================
PROMPT 1.4  LogMiner tablespaces (CDB + PDB)
PROMPT ============================================================

-- Dedicated tablespace for the Debezium user, kept separate from
-- SYSTEM/USERS so it's easy to cap or drop later.
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

-- Common user (C## prefix) - the connector logs into CDB root, then
-- switches into the PDB.
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

