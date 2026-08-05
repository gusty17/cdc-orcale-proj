-- Mounted into /container-entrypoint-startdb.d - runs on every container
-- start, as SYSDBA. gvenzl/oracle-xe:21 has no ENABLE_ARCHIVELOG env var,
-- and ARCHIVELOG can only be enabled in MOUNT state, which the entrypoint
-- has already passed by the time the DB is reachable - so this checks the
-- current mode and only runs the shutdown/mount/enable/open sequence when
-- it's actually needed. Once enabled it's permanent, so every later start
-- is a one-query no-op.

SET HEADING OFF
SET FEEDBACK OFF
SET PAGESIZE 0
SET LINESIZE 200
SET TRIMSPOOL ON
SET VERIFY OFF
SET TERMOUT OFF
WHENEVER SQLERROR EXIT SQL.SQLCODE

SPOOL /tmp/_enable_archivelog_step.sql

SELECT CASE
         WHEN log_mode = 'ARCHIVELOG' THEN
           'PROMPT CONTAINER: ARCHIVELOG already enabled, nothing to do.'
         ELSE
           'PROMPT CONTAINER: enabling ARCHIVELOG (restarting database)...' || CHR(10) ||
           'SHUTDOWN IMMEDIATE'                                            || CHR(10) ||
           'STARTUP MOUNT'                                                 || CHR(10) ||
           'ALTER DATABASE ARCHIVELOG;'                                    || CHR(10) ||
           'ALTER DATABASE OPEN;'                                          || CHR(10) ||
           'ALTER PLUGGABLE DATABASE ALL OPEN;'                            || CHR(10) ||
           'ALTER PLUGGABLE DATABASE ALL SAVE STATE;'                      || CHR(10) ||
           'PROMPT CONTAINER: ARCHIVELOG enabled.'
       END
  FROM v$database;

SPOOL OFF
SET TERMOUT ON

@/tmp/_enable_archivelog_step.sql

SET FEEDBACK ON
SET HEADING ON
SET PAGESIZE 14
