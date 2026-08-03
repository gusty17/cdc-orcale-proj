# Oracle → Kafka CDC (Debezium)

Change data capture from Oracle into Kafka topics, using the Debezium Oracle
connector in LogMiner mode.

```
Oracle XE 21c  ──redo/archived redo──▶  Debezium (Kafka Connect)  ──▶  Kafka topics
   T24.ACCOUNT                              LogMiner adapter
```

Current status: **steps 1 and 2 are done** — Oracle is configured for CDC and the
container stack runs. The connector itself (`oracle-connector.json`) is not
written yet.

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

---

## Testing CDC manually — `test-insert-cdc.sql`

Inserts a fresh copy of the real sample record under a new, timestamped
`RECID`, so you can watch a brand-new row travel from Oracle to Kafka without
touching the seed row. Not idempotent on purpose — every run creates another
new row, so every run is a visible, distinct event.

```powershell
docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" "@/scripts/test-insert-cdc.sql"
```

Then read the topic to see it arrive:

```powershell
docker exec cdc-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 --topic t24.T24.ACCOUNT --from-beginning --timeout-ms 20000
```

Look for `"op":"c"` (create) with the `RECID` the script printed.

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

## Not done yet

- Downstream flattening of the XML into ~20 typed columns (RisingWave, or a
  Kafka Connect SMT).
- `risingwave-setup.sql` — empty, left over from the earlier plan.
- No topic browser in the stack; `obsidiandynamics/kafdrop` is already pulled
  locally if one is wanted.
