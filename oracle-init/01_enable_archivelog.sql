-- Runs on every container start. Enables ARCHIVELOG mode if it isn't already on

-- Settings below only shape the SELECT+SPOOL step further down: they make
-- its output plain text, so it can be saved as a valid, runnable script.
SET HEADING OFF     
SET FEEDBACK OFF    
SET PAGESIZE 0      
SET LINESIZE 200    
SET TRIMSPOOL ON    
SET VERIFY OFF      
SET TERMOUT OFF     
WHENEVER SQLERROR EXIT SQL.SQLCODE   
SPOOL /tmp/_enable_archivelog_step.sql

-- Generate the next script's content based on the current log_mode: a
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
SET TERMOUT ON   -- show the generated script's output as it runs next

@/tmp/_enable_archivelog_step.sql

-- Restore normal defaults for anything that runs after this script
SET FEEDBACK ON
SET HEADING ON
SET PAGESIZE 14
