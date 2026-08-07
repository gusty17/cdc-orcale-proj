# Oracle → Kafka CDC (Debezium)

Change data capture from Oracle into Kafka topics, using the Debezium Oracle
connector in LogMiner mode, streamed into RisingWave and reported on via Superset.

```
Oracle XE 21c ──redo/archived redo──▶ Debezium (Kafka Connect) ──▶ Kafka topics ──▶ RisingWave ──▶ Superset
   T24.ACCOUNT          LogMiner adapter          t24.T24.ACCOUNT      t24_account
```

Status: end-to-end and working. Oracle → Debezium → Kafka → RisingWave (raw
table + 165-column flattened view + audit trail) → Superset reporting.

---

## Quick start

```powershell
.\setup.ps1          # start containers + apply all Oracle/RisingWave prerequisites
.\setup.ps1 -SqlOnly # containers already running - just (re)apply config
.\setup.ps1 -Down    # tear down, volumes included
```

| Endpoint | Address |
| --- | --- |
| Oracle (CDB) | `localhost:1521/XE` — `sys/oracle as sysdba` |
| Oracle (PDB) | `localhost:1521/XEPDB1` — app schema `t24/t24` |
| Debezium capture user | `c##dbzuser/dbz` (common user, logs in at CDB root) |
| Kafka (from host) | `localhost:29092` |
| Kafka Connect REST | `http://localhost:8083` |
| RisingWave (psql) | `localhost:4566/dev`, user `root` |
| Superset | `http://localhost:8088` (`admin`/`admin`) |

`setup.ps1` runs every step below in order and is idempotent — safe to re-run
or run with `-SqlOnly` any time. Only run a file manually when debugging one step.

---

## File order and what each does

| # | File | Runs where | What it does |
| --- | --- | --- | --- |
| 1 | `oracle-init/01_enable_archivelog.sql` | inside the Oracle container, every start | Auto-run startup hook. Enables ARCHIVELOG if not already on (needs a MOUNT-state restart); a no-op on every later start |
| 2 | `oracle/oracle-prereqs.sql` | CDB$ROOT, as SYSDBA | Supplemental logging, force logging, `LOGMINER_TBS` tablespace, `C##DBZUSER` + grants |
| 3 | `oracle/oracle-setup.sql` | XEPDB1, as SYSDBA | Creates `T24.ACCOUNT` (RECID + XMLRECORD), table-level supplemental logging, loads the seed XML record |
| 4 | `oracle/oracle-connector.json` | POSTed to the Kafka Connect REST API | Deploys the Debezium Oracle connector |
| 5 | `risingwave/risingwave-setup.sql` | via `psql` against RisingWave | Creates `t24_account` (current state), `t24_account_events`/`t24_account_audit` (append-only history), `lookup_metadata`, `t24_account_columns` (165-column flattened view) |
| — | `superset/superset_config.py` | mounted into the `superset` container | Sets `SECRET_KEY` and points Superset's metadata DB at `superset-db` |
| 6 | *(no file — inline in `setup.ps1`)* | `docker exec cdc-superset superset ...` | Superset's one-time init: `db upgrade`, admin user, `init` (roles/permissions) |

Still manual after `setup.ps1`: adding the RisingWave connection inside
Superset's UI — see **Connecting Superset to RisingWave** below.

Manual/ad-hoc scripts, not part of `setup.ps1`:
- `tests/` — one-off insert/update/delete/bulk CDC checks (see file headers for run commands)
- `benchmarks/` — end-to-end latency tracing; see `benchmarks/README.md`

---

## Key config, and why it matters

| Where | Setting | Why |
| --- | --- | --- |
| `oracle-init/01_enable_archivelog.sql` | Runs on every start, not just first init | `gvenzl/oracle-xe:21` has no `ENABLE_ARCHIVELOG` env var, and `ALTER DATABASE ARCHIVELOG` only works in MOUNT state — this checks first so only the very first start pays for a restart |
| `oracle/oracle-setup.sql` | `ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`, not a log group naming `XMLRECORD` | Oracle rejects LOB columns in a log group (`ORA-30569`) |
| `oracle/oracle-connector.json` | `column.include.list` = only `RECID`/`XMLRECORD` | Production's other 9 columns are VIRTUAL (computed from the XML, store nothing). On a streaming update Debezium would emit the literal defining expression instead of a value — correct at snapshot, silently wrong after |
| `oracle/oracle-connector.json` | `lob.enabled = true` | `XMLRECORD` is XMLTYPE backed by a hidden BLOB — without this the XML never arrives at all |
| `oracle/oracle-connector.json` | `log.mining.strategy = online_catalog`, not the default | Default can stall indefinitely on XE's small redo logs (changes sit unmined until a manual log switch). Trade-off: no DDL tracking on the captured table, irrelevant for a fixed schema |
| `oracle/oracle-connector.json` | `enable_goldengate_replication` intentionally not set | That parameter ties Oracle usage to a GoldenGate license in production, even without GoldenGate installed. Verified unnecessary — snapshot, update, and insert all captured correctly with it off |
| Any write to `T24.ACCOUNT` | Full `XMLTYPE` replacement only, never `UPDATEXML` | `UPDATEXML` edits don't carry enough redo detail for LogMiner — Debezium advances its offset but emits nothing, silently. Confirm with the source system how it actually writes |
| `risingwave/risingwave-setup.sql` | `before.XMLRECORD` reconstructed via `LAG()` in `t24_account_audit`, not read from Debezium | Oracle's redo never carries LOB before-images — Debezium's own `before.XMLRECORD` is always `__debezium_unavailable_value` on UPDATE/DELETE |
| `risingwave/risingwave-setup.sql` | `t24_account_audit` / `t24_account_columns` are `MATERIALIZED VIEW`, not `TABLE AS SELECT` or plain `VIEW` | `TABLE AS SELECT` freezes at creation and never updates again; a plain `VIEW` re-runs the whole query on every read. Only `MATERIALIZED VIEW` stays both live and pre-computed |
| `risingwave/risingwave-setup.sql` | `t24_account_columns` flattens XML via regex (unpivot/resolve/pivot) | RisingWave has no XML/XPath functions at all |
| `risingwave/risingwave-setup.sql` | Adding a `lookup_metadata` row needs a matching `MAX(CASE WHEN column_name = '...')` line in the final `SELECT` | The pivot's column list is fixed at creation time — a new mapping with no matching line silently produces no column and no error |
| `risingwave-setup.sql` (top) | `ALTER SYSTEM SET barrier_interval_ms = 1000` | Made explicit rather than left implicit — controls how often streaming results become visible; rows that arrive close together become visible together, at the next barrier, not instantly per row |
| `docker-compose.yml` (`superset`) | `user: root` + `entrypoint: docker-bootstrap.sh` | Base image ships with no Postgres driver, and only installs one on boot when running as root — the default non-root user silently skips that step |

---

## Connecting Superset to RisingWave

`setup.ps1` handles Superset's `db upgrade` / admin-user / `init` steps
automatically. The one thing left to do by hand, since it's a UI action:
log in at `http://localhost:8088` (`admin`/`admin`), then **Settings →
Database Connections → + Database**, SQLAlchemy URI:
```
postgresql+psycopg2://root:@risingwave:4566/dev
```

---

## Not done yet

- Typed columns — everything in `t24_account_columns` is `VARCHAR`; per-field
  type rules (dates, decimals) aren't in `lookup_metadata.csv` yet.
- No topic browser in the stack (`obsidiandynamics/kafdrop` is pulled locally
  if wanted).
- Superset's `superset-db` password and `SUPERSET_SECRET_KEY` are fixed/committed
  in `docker-compose.yml` — fine for this lab, not for anything internet-facing.
