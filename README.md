# Oracle → Kafka CDC (Debezium)

Change data capture from Oracle into Kafka topics, using the Debezium Oracle
connector in LogMiner mode.

```
Oracle XE 21c  ──redo/archived redo──▶  Debezium (Kafka Connect)  ──▶  Kafka topics  ──▶  RisingWave
   T24.ACCOUNT                              LogMiner adapter          t24.T24.ACCOUNT      t24_account
```

Current status: **steps 1-6 are done** — Oracle is configured for CDC, the
container stack runs (Kafka, Connect, RisingWave, Superset), the connector is
deployed, and change events flow end-to-end from Oracle into RisingWave —
both as a raw table and as 165 flattened business columns, the latter
computed entirely in RisingWave SQL with no external process — with a
reporting layer (Superset) on top.

---

## Quick start

```powershell
.\setup.ps1          # start containers + apply all Oracle prerequisites
.\setup.ps1 -SqlOnly # re-apply the SQL only (both scripts are idempotent)
.\setup.ps1 -Down    # tear down, volumes included
```

| Endpoint | Address |
| --- | --- |
| Oracle (CDB) | `localhost:1521/XE` — `sys/oracle as sysdba` |
| Oracle (PDB) | `localhost:1521/XEPDB1` — app schema `t24/t24` |
| Debezium capture user | `c##dbzuser/dbz` (common user, logs in at CDB root) |
| Kafka (from host) | `localhost:29092` |
| Kafka (from containers) | `kafka:9092` |
| Kafka Connect REST | `http://localhost:8083` |

---

## Step 1 — Oracle prerequisites

Split across three files by where they have to run:

| File | Runs where | What it does |
| --- | --- | --- |
| `oracle-init/01_enable_archivelog.sql` | inside the container, on every start | Enables ARCHIVELOG (needs a MOUNT-state restart) |
| `oracle-prereqs.sql` | CDB$ROOT, as SYSDBA | Supplemental logging, force logging, `LOGMINER_TBS`, `C##DBZUSER` + grants |
| `oracle-setup.sql` | XEPDB1, as SYSDBA | `T24.ACCOUNT` (RECID + XMLRECORD), table-level supplemental logging, loads the full sample record |

Two things worth knowing about, because they are not what the plan assumed:

**ARCHIVELOG can't be set by an env var on this image.** `gvenzl/oracle-xe:21`
has no `ENABLE_ARCHIVELOG` / `ENABLE_FORCE_LOGGING` support — those were added
on the newer `gvenzl/oracle-free` (23ai) images. And `ALTER DATABASE ARCHIVELOG`
only works in MOUNT state, which the entrypoint has already passed by the time
the database is reachable. So `oracle-init/01_enable_archivelog.sql` is mounted
into `/container-entrypoint-startdb.d`, checks `v$database.log_mode`, and only
generates + runs the `SHUTDOWN IMMEDIATE / STARTUP MOUNT / ALTER DATABASE
ARCHIVELOG / OPEN` sequence when it is actually needed. Every later start is a
single query. (`/container-entrypoint-initdb.d` is not usable here — the
faststart image ships with the database already built.)

**Table-level supplemental logging cannot name `XMLRECORD`.** The plan called
for a log group over `(RECID, XMLRECORD)`, but Oracle rejects LOB columns in a
log group:

```
ORA-30569: data type of given column is not supported in a log group
```

So `oracle-setup.sql` uses `ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` instead.
That covers `RECID` (the only non-LOB column), and the CLOB reaches Debezium
through LOB redo entries — which the connector reads when configured with
`lob.enabled=true`. That property will be needed in `oracle-connector.json`.

`XMLRECORD` is deliberately `CLOB` and not `XMLTYPE`: the Debezium Oracle
connector has no mapping for `XMLTYPE`.

---

## Step 2 — Container stack

`docker-compose.yml`, three services:

- **oracle** — `gvenzl/oracle-xe:21-slim-faststart`. CDB `XE`, PDB `XEPDB1`.
- **kafka** — `apache/kafka:3.8.0`, single node in KRaft mode (no ZooKeeper),
  with an internal (`kafka:9092`) and an external (`localhost:29092`) listener.
- **connect** — `quay.io/debezium/connect:3.6.0.Final`.

### Image verification

The open question was whether a Debezium Connect image carries the Oracle
connector, since the Oracle JDBC driver used to have to be added by hand.
Checked directly in the image — it does not need a custom build:

```
/kafka/connect/debezium-connector-oracle/
  debezium-connector-oracle-3.6.0.Final.jar
  ojdbc11-23.26.1.0.0.jar
  orai18n-23.26.1.0.0.jar
```

Confirmed live through the Connect REST API after startup:

```
class   : io.debezium.connector.oracle.OracleConnector
type    : source
version : 3.6.0.Final
```

Other things verified while bringing the stack up:

- Debezium 3.6.0.Final ships Kafka **client** 4.3.0 on Java 21 and talks to the
  3.8.0 broker without trouble. The `NotCoordinatorException` lines during the
  first ~20s of Connect startup are normal group-coordinator discovery, not an
  error.
- `apache/kafka:3.8.0` writes to a named volume at `/var/lib/kafka/data` with no
  permission workaround needed, so the Connect internal topics survive restarts
  and a redeploy doesn't force a fresh snapshot.
- `gvenzl/oracle-xe` prints a deprecation notice on boot ("PLEASE CONSIDER
  UPGRADING TO gvenzl/oracle-free"). Staying on XE 21c for now — it is what
  Debezium tests its Oracle connector against. Moving to `gvenzl/oracle-free`
  (23ai) later would remove the ARCHIVELOG hook, since that image supports
  `ENABLE_ARCHIVELOG` natively.

---

## Verified end state

```
LOG_MODE     FORCE_LOGGING  SUPP_MIN
------------ -------------- --------
ARCHIVELOG   YES            YES

enable_goldengate_replication = FALSE   <- left off, see below

USERNAME     COM  DEFAULT_TABLESPACE  ACCOUNT_STATUS
C##DBZUSER   YES  LOGMINER_TBS        OPEN

OWNER  TABLE_NAME  LOG_GROUP_TYPE       ALWAYS
T24    ACCOUNT     ALL COLUMN LOGGING   ALWAYS
```

`C##DBZUSER` was checked by logging in and reading `V$LOG` — grants are working.

### `enable_goldengate_replication` — dropped, not required

The original plan (and this file, until now) set this flag on the assumption
that Debezium's LogMiner connector needed it on 12c+. Our senior flagged that
leaving it on ties Oracle usage to an **Oracle GoldenGate license** in
production — the parameter's name is not cosmetic; Oracle's licensing
position treats it as a GoldenGate-replication feature regardless of whether
the GoldenGate product is actually installed.

Tested directly rather than taken on faith: turned the flag `FALSE`, wiped
the connector's offsets and topics for a clean run, and redeployed.

```
enable_goldengate_replication: TRUE -> FALSE

#1 op=r  recid=9000000112345001                       nodes=165  (initial snapshot)
#2 op=u  recid=9000000112345001                       nodes=3    c27=65000.00
#3 op=c  recid=9000000112345001-goldengate-off-test   nodes=165  (fresh insert)
```

Snapshot, streaming update, and streaming insert all captured correctly with
the flag off. `oracle-prereqs.sql` no longer sets it; section 1.3 is gone.

---

## Step 3 — The connector, and why it captures only 2 columns

`oracle-connector.json`. Deploy with:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors `
  -ContentType application/json -Body (Get-Content .\oracle-connector.json -Raw)
```

Events land on topic `t24.T24.ACCOUNT`.

### The source table stores exactly two things

From `sample-data/account_oracle_schema.md`, the real table lists 11 columns —
but only **one** of them occupies a segment:

```
COL COLUMN_NAME        DATA_TYPE  HIDDEN VIRTUAL SEGMENT
  1 RECID              VARCHAR2   NO     NO      1
  2 XMLRECORD          XMLTYPE    NO     YES
  3 SYS_NC00003$       BLOB       YES    NO      2      <- the XML, binary
  4 CURRENCY           VARCHAR2   NO     YES
  5 CO_CODE            VARCHAR2   NO     YES
  ... CATEGORY, CUSTOMER, MNEMONIC, CURR_NO,
      POSTING_RESTRICT, OPENING_DATE, INT_NO_BOOKING — all VIRTUAL
```

`CURRENCY`, `CATEGORY` and the rest are **virtual columns**: expressions that
run `EXTRACTVALUE(...)` over the XML at read time. They store nothing.

### Virtual columns silently corrupt streaming events

Tested here on a faithful copy of the real table. During the **initial
snapshot** they look perfect, because a snapshot is a plain `SELECT` and Oracle
evaluates the expression:

```
CURRENCY = EGP        CATEGORY = 6510        CUSTOMER = 90000001
```

But on a streaming **update**, LogMiner has no value to give — nothing was
stored — and what arrives is the *text of the defining expression*:

```
CURRENCY = CAST(EXTRACTVALUE(SYS_MAKEXML(0,"SYS_NC00003$"),'/row/c8[position()=1]') AS VARCHAR2(250))
CATEGORY = (empty)
CUSTOMER = (empty)
```

Correct at snapshot, junk forever after — the worst kind of failure, because it
passes a first smoke test. Hence:

```json
"column.include.list": "T24\\.ACCOUNT\\.(RECID|XMLRECORD)"
```

`lob.enabled=true` is also mandatory: `XMLRECORD` is XMLTYPE backed by a hidden
BLOB, and without it the XML never arrives at all.

The lab table itself is **RECID + XMLRECORD and nothing else** — a 1:1 copy of
what the source stores, holding the complete sample record (all 165 XML nodes,
multi-value tags and Arabic titles included). The `column.include.list` above
stays anyway, because the production table does have the virtual columns.

### Two more things that had to be found by running it

**1. Default mining strategy strands changes in the online redo log.**
With Debezium's default `log.mining.strategy=redo_log_catalog`, committed
updates never reached Kafka. The connector sat at the snapshot SCN with no
error while the database moved on:

```
db current_scn = 4008068     connector scn = 4006133   (stuck)
```

The changes were in the *current, unarchived* redo group, and only appeared
after a manual `ALTER SYSTEM SWITCH LOGFILE`. On XE's two 10 MB groups a
quiet table can sit unmined indefinitely. Setting

```json
"log.mining.strategy": "online_catalog"
```

made updates flow within seconds with no log switch. The cost is that
`online_catalog` cannot track DDL changes to the captured table — irrelevant
for a fixed `RECID`/`XMLRECORD` shape.

**2. `UPDATEXML` changes are silently dropped.**
How the XML is written decides whether CDC sees it at all:

| Statement | Captured? |
| --- | --- |
| `SET xmlrecord = XMLTYPE('<row>…')` — full replacement | yes |
| `SET xmlrecord = UPDATEXML(xmlrecord,'/row/c27/text()','…')` | **no** |

The `UPDATEXML` case is the dangerous one: Debezium advances its offset past
the transaction — so it *saw* it — and emits nothing. After that test the
database held `c27 = 55555.55` while the last Kafka event still said
`24178.54`, with the connector reporting `RUNNING` and no errors.

T24 rewrites the whole record, so the normal path is the safe one. But any
job that patches XML in place will silently diverge, and nothing will alert
you. Worth confirming with your senior how the source actually writes.

### Verified end to end

Four events, matching four writes (the `UPDATEXML` one excluded, as above):

```
#1 op=r  nodes=165  c27=24178.54     <- snapshot, full record
#2 op=u  nodes=5    c27=77777.77
#3 op=u  nodes=3    c27=22222.22
#4 op=u  nodes=165  c27=24178.54     <- full sample restored
```

Arabic survives the round trip. One caveat: on an update, `before.XMLRECORD` is
`__debezium_unavailable_value` — LOB before-images are not in the redo. Use
`after`, which is always complete.

### Picking your ~20 fields

Field selection happens **downstream**, not in the connector — the whole record
arrives as one XML document, so there is nothing to narrow at the Oracle end.
`sample-data/lookup_metadata.csv` maps the `cN` tags to business names
(289 rows). Repeated tags with `m="2"`, `m="3"` … are T24 multi-value fields —
an array, not a scalar.

---

## Step 4 — RisingWave, reading the topic directly

`docker-compose.yml` runs it in `single_node` mode (meta/compute/frontend/
compactor in one process — fine for a lab, not production). It speaks the
Postgres wire protocol on port 4566, so any Postgres client works, including
`psql` from a throwaway container if none is installed locally:

```powershell
docker run --rm --network cdc-oracle_default postgres:16-alpine `
  psql -h risingwave -p 4566 -d dev -U root -f /tmp/setup.sql
# (mount risingwave-setup.sql to /tmp/setup.sql, or pipe it via -f /dev/stdin)
```

`risingwave-setup.sql` creates a **table** (not a source) backed directly by
the Kafka topic:

```sql
CREATE TABLE t24_account (
    recid VARCHAR, xmlrecord VARCHAR, PRIMARY KEY (recid)
) WITH (
    connector = 'kafka', topic = 't24.T24.ACCOUNT',
    properties.bootstrap.server = 'kafka:9092', scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE JSON;
```

`FORMAT DEBEZIUM ENCODE JSON` unwraps Debezium's `{op, before, after, ...}`
envelope automatically and applies each insert/update/delete against
`PRIMARY KEY (recid)` — `CREATE TABLE` rather than `CREATE SOURCE` is what
makes RisingWave actually materialize and maintain current state, instead of
only exposing the raw event stream.

### Verified: insert, update, and delete all propagate correctly

```sql
SELECT recid, LENGTH(xmlrecord) FROM t24_account;
```

Confirmed a delete made directly in Oracle correctly removed the row here
too — RisingWave's table matched Oracle exactly before and after. Full XML
content (all tags, Arabic text) also checked byte-for-byte intact through
the whole pipeline: Oracle → Debezium → Kafka → RisingWave.

### Audit trail — every change, with before/after and both timestamps

`t24_account` only shows current state — `FORMAT DEBEZIUM` collapses history
by applying each change over the primary key. `risingwave-setup.sql` also
creates a second, append-only table reading the **same topic** a different
way:

```sql
CREATE TABLE t24_account_events (
    op VARCHAR, "before" JSONB, "after" JSONB, source JSONB, ts_ms BIGINT
) WITH (
    connector = 'kafka', topic = 't24.T24.ACCOUNT',
    properties.bootstrap.server = 'kafka:9092', scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
```

`FORMAT PLAIN` (vs `FORMAT DEBEZIUM` above) does **not** apply changes
against a key — every message becomes its own permanent row, nothing ever
overwritten or removed. `before`/`after`/`source` are kept as raw `JSONB`
rather than pre-extracted columns, so this table survives if T24's fields
ever change.

A materialized view on top, `t24_account_audit`, does the field extraction
and exposes two different "when" columns — `db_commit_time` (from
`source->>'ts_ms'`, when the change actually committed in Oracle) and
`captured_time` (top-level `ts_ms`, when Debezium/Kafka processed it). They
differ by mining + streaming lag — a few seconds in this lab.

### Why `MATERIALIZED VIEW`, not `TABLE` or a plain `VIEW`

Tested all three directly rather than assuming:

- **`CREATE TABLE ... AS SELECT`** — confirmed to take a **one-time
  snapshot** that never updates again. Inserted a fresh row in Oracle
  afterward; the source table and a plain view both picked it up, this did
  not — permanently frozen at creation time, no error or warning. Wrong for
  something meant to keep growing forever.
- **Plain `CREATE VIEW`** — stays live (re-runs the query fresh every time),
  but has no storage of its own — it recomputes the whole `LAG()` over
  `t24_account_events` on every single query, getting more expensive as the
  event log grows.
- **`CREATE MATERIALIZED VIEW`** — gets both: RisingWave's streaming engine
  keeps it incrementally, continuously up to date in the background (a
  fresh insert *and* a fresh update both appeared automatically, with
  `before_xmlrecord` correctly reconstructed, no manual refresh needed),
  while queries against it read pre-computed, stored results — same speed
  as a table.

### `before_xmlrecord` is reconstructed, not taken from Debezium directly

`XMLRECORD` is a LOB (`XMLTYPE`); Oracle's redo log never carries LOB
before-images, so Debezium's own `before.XMLRECORD` is always the literal
string `__debezium_unavailable_value` on `UPDATE`/`DELETE` — confirmed by
testing, not a hypothetical, and there's no Oracle-side setting that fixes
it (same limitation noted back in Step 3).

Instead, since `t24_account_events` already keeps **every** version of every
row forever, the real prior XML is simply the previous row's `after` value —
`t24_account_audit` reconstructs it with a window function, no change to
Oracle or T24's schema needed:

```sql
CREATE MATERIALIZED VIEW t24_account_audit AS
WITH base AS (
    SELECT
        CASE op WHEN 'r' THEN 'SNAPSHOT' WHEN 'c' THEN 'INSERT'
                WHEN 'u' THEN 'UPDATE'   WHEN 'd' THEN 'DELETE' ELSE op END AS operation,
        COALESCE("after"->>'RECID', "before"->>'RECID') AS recid,
        "after"->>'XMLRECORD' AS after_xmlrecord,
        to_timestamp((source->>'ts_ms')::bigint / 1000.0) AS db_commit_time,
        to_timestamp(ts_ms / 1000.0)                       AS captured_time
    FROM t24_account_events
)
SELECT recid, operation,
       LAG(after_xmlrecord) OVER (PARTITION BY recid ORDER BY captured_time) AS before_xmlrecord,
       after_xmlrecord, db_commit_time, captured_time
FROM base;
```

Verified with a real insert → update → update cycle (content markers
`ROUND-ONE` → `ROUND-TWO`):

```
operation | before    | after     | captured_time
INSERT    | (none)    | ROUND-ONE | 21:04:43
UPDATE    | ROUND-ONE | ROUND-TWO | 21:05:04
```

Correctly chained — actual real prior content, not the placeholder. Only
gap: a row's very first version (its own `INSERT`/`SNAPSHOT`) has no prior
row to reconstruct from, so `before_xmlrecord` is genuinely `NULL` there —
which is correct, since no earlier version exists to show.

Other options considered but not built: a `BEFORE UPDATE/DELETE` trigger on
`T24.ACCOUNT` copying the old XML to a shadow table (works, but requires
modifying the real T24 schema — needs actual DBA/vendor sign-off, not
something to add unilaterally); Flashback Query looked up per event
(technically possible, but means querying Oracle back per event, defeating
the point of CDC and adding load to production).

---

## Testing CDC manually

Three scripts, one per operation type, all living in `tests/` and all run
the same way — against XEPDB1, via `docker exec`:

```powershell
docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" "@/scripts/tests/<script>.sql"
```

| Script | Operation | Event you should see |
| --- | --- | --- |
| `tests/test-insert-cdc.sql` | Inserts a fresh copy of the sample record under a new, timestamped `RECID` | `op=c` |
| `tests/test-update-cdc.sql` | Sets a new random working balance (`c27`) on the seed row (`RECID 9000000112345001`) | `op=u` |
| `tests/test-delete-cdc.sql` | Deletes the most recently inserted test row (never the seed row) | `op=d`, then a tombstone |

Then read the topic to see events arrive:

```powershell
docker exec cdc-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 --topic t24.T24.ACCOUNT --from-beginning --timeout-ms 20000
```

Verified running all three in sequence (insert → update → delete):

```
op=u  recid=9000000112345001                 c27=16868.36   (test-update-cdc.sql)
op=c  recid=9000000112345001-144539-170      c27=24178.54   (test-insert-cdc.sql)
op=d  recid=9000000112345001-144539-170                     (test-delete-cdc.sql)
      recid=9000000112345001-144539-170      -> tombstone (null value)
```

Re-verified after moving all three scripts into their own `tests/` folder —
same sequence, same result, run via `@/scripts/tests/<script>.sql` instead
of `@/scripts/<script>.sql`.

A delete produces **two** Kafka messages: the `op=d` event itself (last known
row content), then a separate tombstone message — same key, `null` value —
which tells downstream consumers to drop any cached copy of that key.

`test-update-cdc.sql` overwrites the seed row's content (a new random
balance each run) — re-run `oracle-setup.sql` afterward if you want the
original sample values back. `test-delete-cdc.sql` only ever targets rows
matching the `test-insert-cdc.sql` naming pattern, so the seed row itself is
never at risk of being deleted by it.

### A stale-offset failure found while testing this

Redeploying the connector against a stack that had drifted (Oracle's data had
been recreated at some point, independent of Kafka) produced a crash loop:

```
ORA-01284: file /opt/oracle/homes/OraDBHome21cXE/dbs/arch1_29_....dbf cannot be opened
```

The connector's saved checkpoint pointed at an archived redo file that no
longer existed — Oracle's redo history and Kafka Connect's remembered offset
had gone out of sync. The connector doesn't detect this as fatal; it just
retries the same broken checkpoint forever, so nothing looks obviously wrong
from `/connectors/.../status` (state stays `RUNNING`) — only the logs show it.

Fix, if this ever recurs: stop the connector, clear its offsets, delete the
Kafka topics it owns, and let it start over with a fresh snapshot.

```powershell
Invoke-RestMethod -Method Put "http://localhost:8083/connectors/t24-account-cdc/stop"
Invoke-RestMethod -Method Delete "http://localhost:8083/connectors/t24-account-cdc/offsets"
Invoke-RestMethod -Method Delete "http://localhost:8083/connectors/t24-account-cdc"
docker exec cdc-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic t24.T24.ACCOUNT
docker exec cdc-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic schema-history.t24
# then re-POST oracle-connector.json
```

---

## Step 5 — Flattening the XML into real columns, natively in RisingWave

`t24_account` stores `xmlrecord` as one opaque string. `t24_account_columns`
(built in `risingwave-setup.sql`) splits it into 165 named business columns —
computed **entirely inside RisingWave SQL**. No external process reads Kafka
to produce it; RisingWave's own engine keeps it live, same as `t24_account`
and `t24_account_audit`.

This replaces an earlier Python-based version (`flatten/`, kept in the repo
for reference, no longer running) — rebuilt per a specific requirement: the
XML→columns transformation had to live in RisingWave itself, with the tag
lookup table also stored as a real RisingWave table, joined against every
row. See "Superseded: the Python version" below for what changed and why.

### RisingWave has no XML functions — so this is regex, not XML parsing

Confirmed directly: `rw_catalog.rw_functions` returns zero rows for anything
XML/XPath. The whole pipeline works around that in three stages, each
verified independently before combining:

**1. Unpivot** — explode each row's XML into one row per tag, using
`regexp_matches(..., 'g')` in a `LATERAL` subquery (confirmed: RisingWave's
parser needs the subquery wrapper — a bare `LATERAL regexp_matches(...)`
without one is rejected):

```sql
SELECT recid, m[1] AS field, m[2] AS position, m[3] AS value
  FROM t24_account,
       LATERAL (SELECT regexp_matches(xmlrecord, '<(c\d+)(?:\s+m="(\d+)")?>([^<]*)</\1>', 'g') AS m) AS t
```

**2. Resolve** — `LEFT JOIN` against `lookup_metadata`, a real RisingWave
table loaded from `sample-data/lookup_metadata.csv` (289 rows). `COPY`
doesn't work here — confirmed directly, RisingWave's parser rejects both
`COPY ... WITH (FORMAT csv, ...)` and plain `COPY ... CSV HEADER` — so it's
loaded via a generated bulk `INSERT` instead.

**3. Pivot** — fold back to wide columns via
`MAX(CASE WHEN column_name = '...' THEN value END)`, one branch per column.

### Three tag shapes, one join, two naming rules

| Shape | Example | Resolved via |
| --- | --- | --- |
| Scalar | `c1` occurs once | Base name straight from the lookup join |
| **`c20` = T24 LOCAL.REF** | each position is a *different* field | Exact `(field_index, m_index)` match wins — `m=4` → `arabic_title` |
| True array | `c47` repeats the same field | Base name + `LPAD(position, 2, '0')`, only for the 9 fields flagged `is_multivalue` |

The 9 array fields (`c46`–`c50`, `c99`, `c100`, `c249`, `c250`) only have a
single base row in the CSV — no per-position row like `c20` has — so the
numbering has to be computed in the `resolved` CTE, not looked up directly.
`is_multivalue` on `lookup_metadata` carries that distinction.

Parallel arrays stay aligned — `alt_acct_type_02` = `T24.IBAN` pairs with
`alt_acct_id_02` = `EG00ANON…`.

### Why `MATERIALIZED VIEW`, and two real bugs found building this

Same reasoning already proven for `t24_account_audit`: a plain
`CREATE TABLE ... AS SELECT` freezes at creation and never updates again
(confirmed earlier with `t24_account_audit_ctas`); a plain `VIEW` stays live
but re-runs the whole regex/join/pivot on every query. `MATERIALIZED VIEW`
gets both.

**Bug 1 — `DROP TABLE`/`DROP MATERIALIZED VIEW IF EXISTS` is not a graceful
no-op on a type mismatch.** Confirmed directly: dropping an object with
`DROP TABLE IF EXISTS` when it actually exists as a materialized view
*errors* (`Use DROP MATERIALIZED VIEW to drop a materialized view`) rather
than silently skipping — and the reverse is equally true. Since the
migration away from the old plain-`TABLE` Python-fed version only ever
matters once, that one-time step is documented as a manual note in
`risingwave-setup.sql` rather than automated — no single `DROP` statement
can stay silent across both possible prior states.

**Bug 2 — drop ordering.** RisingWave refuses to drop a table a materialized
view still depends on (`table used by 1 other objects` — confirmed
directly), so `t24_account_columns` must be dropped *before*
`lookup_metadata` is rebuilt, even though recreating it comes *after*.

### Verified end to end, including a full clean rebuild from zero

Confirmed matching every field already validated in the earlier Python
version — scalars, `c20` LOCAL.REF, true arrays, Arabic text:

```
customer=90000001  arabic_title=عميل تجريبي مجهول  cap_date_cr_int_23=20240930
alt_acct_id_02=EG00ANON000000000000000000000  co_code=EG0010039
```

Then re-verified with **zero Python running at any point**:
- A live `UPDATE` in Oracle appeared automatically — values matched exactly.
- A live `INSERT` then `DELETE` both propagated correctly.
- The whole `risingwave-setup.sql` re-run three times in a row against
  already-populated state — zero errors after the two bugs above were fixed.
- The RisingWave data volume was wiped completely and the file re-run from
  absolute zero — table, lookup data, and materialized view all rebuilt
  correctly, then a fresh insert/delete cycle confirmed still worked.

### Superseded: the Python version (`flatten/`)

Kept in the repo for reference. It worked (verified extensively at the
time), and the bugs found building it were real and are still relevant
lessons about RisingWave's write semantics — but it's no longer part of the
running pipeline. What it caught, in case they resurface elsewhere:

- **`INSERT` on an existing PK upserts** in RisingWave — Debezium `r`/`c`/`u`
  all map to plain `INSERT`; `DELETE` maps to `DELETE`.
- **Writes are invisible until `FLUSH`.** Batching INSERTs and DELETEs into
  one flush caused a real bug: `DELETE ... WHERE recid = X` ran *before* the
  matching INSERT was visible, matched nothing, row survived — table ended
  up with 7 rows against Oracle's 1. Fixed by collapsing each `recid` to its
  final operation per batch.
- Schema was derived from tags **present in the sample**, not all 256
  lookup fields, so `mnemonic`/`posting_restrict`/`int_no_booking` had no
  column — same limitation the native version inherits, since both read the
  same sample.

---

## Step 6 — Superset, for reports on top of RisingWave

`docker-compose.yml` adds two services: `superset-db` (Superset's own
metadata — dashboards, charts, users; separate from RisingWave, which holds
the actual T24 data) and `superset` itself, on port 8088.

### Two problems found and fixed before it would even boot

**No Postgres driver in the base image.** `import psycopg2` fails out of the
box — confirmed directly in the image before touching compose. Reaching
RisingWave (Postgres wire protocol) needs it. Fixed without a custom
Dockerfile: `entrypoint: /app/docker/docker-bootstrap.sh` (present in the
image but unused by its own default `CMD`) + `DATABASE_DIALECT: postgres`
makes it run `pip install -e .[postgres]` on every boot, before starting the
app. Confirmed identical app behavior either way — `/usr/bin/run-server.sh`
(the image's real default) and `/app/docker/entrypoints/run-server.sh`
(what `docker-bootstrap.sh`'s `app-gunicorn` case calls) diffed byte-identical.

**The driver install silently did nothing on the first attempt.** Container
booted, then crashed with the exact same `ModuleNotFoundError: No module
named 'psycopg2'` — the install step never ran. Cause: the image runs as a
non-root `superset` user by default, and `docker-bootstrap.sh`'s install
step is gated on `whoami = root`; it skips silently otherwise, no error, no
log line. Fixed with `user: root` on the service. Re-verified: log then
showed `Installing postgres requirements` → `psycopg2-binary==2.9.9`
installed → worker booted clean.

**No default `superset_config.py` exists in the image either** — confirmed
empty on inspection. `superset/superset_config.py` is mounted at
`/app/pythonpath/superset_config.py` (the image's actual `PYTHONPATH`) to
supply `SECRET_KEY` (from `SUPERSET_SECRET_KEY`, not hardcoded — signs
session cookies, so a value checked into the repo would defeat the point)
and point `SQLALCHEMY_DATABASE_URI` at `superset-db`.

### One-time setup (not automated — same pattern as the connector/RisingWave steps)

```bash
docker exec cdc-superset superset db upgrade
docker exec cdc-superset superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@example.com --password admin
docker exec cdc-superset superset init
```

### Connecting to RisingWave

Log in at `http://localhost:8088` (`admin` / `admin`), then **Settings →
Database Connections → + Database**, SQLAlchemy URI:

```
postgresql+psycopg2://root:@risingwave:4566/dev
```

(`risingwave`, not `localhost` — Superset reaches it over the compose
network, same hostname every other service uses.)

### Verified end to end

Confirmed the whole chain works, not just that the container stays up — API
login, connection test, and a real SQL Lab query against live T24 data, all
through Superset's own API:

```json
{"status": "success", "data": [
  {"recid": "9000000112345001", "customer": "90000001",
   "currency": "EGP", "working_balance": "24178.54"}
]}
```

From here, any chart/dashboard built in Superset's UI against the
`RisingWave` connection is querying real, continuously-updating T24 data —
the same pipeline verified throughout this whole project, now with a
reporting layer on top.

---

## Not done yet

- Typed columns — everything in `t24_account_columns` is `VARCHAR`. T24 dates
  are `YYYYMMDD` strings and amounts are decimal strings; casting them needs
  per-field type rules that aren't in `lookup_metadata.csv`.
- No topic browser in the stack; `obsidiandynamics/kafdrop` is already pulled
  locally if one is wanted.
- Superset's `superset-db` uses a fixed dev password (`superset`/`superset`)
  and `SUPERSET_SECRET_KEY` is committed in `docker-compose.yml` — fine for
  this lab, not for anything internet-facing.




logs data -->/opt/oracle/homes/OraDBHome21cXE/dbs/
t24 tables --> /opt/oracle/oradata/XE/XEPDB1/users01.dbf
 