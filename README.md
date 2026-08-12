# Oracle → Kafka CDC (Debezium)

Change data capture from Oracle into Kafka topics, using the Debezium Oracle
connector in LogMiner mode, streamed into RisingWave and reported on via Superset.

```
                                                                        ┌─▶ t24.T24.ACCOUNT      ──▶ t24_account      ──▶ t24_account_columns      ──┐
Oracle XE 21c ──redo/archived redo──▶ Debezium (Kafka Connect) ───┤                                                                                  ├──▶ Superset
   T24.ACCOUNT / T24.ACCOUNT_BLOB      LogMiner adapter, ONE      └─▶ t24.T24.ACCOUNT_BLOB ──▶ t24_account_blob ──▶ t24_account_blob_columns ──┘
                                       connector, both tables
```

Two storage paths for the same kind of record: `T24.ACCOUNT` stores the
record as `XMLTYPE`, `T24.ACCOUNT_BLOB` stores it as a raw `BLOB` (the same
XML text, as bytes). Both flow through the identical pipeline shape - one
connector captures both tables, and each gets its own topic, upsert table,
and flattened 166-column view.

Status: end-to-end and working. Oracle → Debezium → Kafka → RisingWave (raw
table + 166-column flattened view + audit trail, per storage path) → Superset reporting.

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
| Kafka UI | `http://localhost:8080` — browse topics/messages, connector status |
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
| 3 | `oracle/oracle-setup.sql` | XEPDB1, as SYSDBA | Creates `T24.ACCOUNT` (RECID + XMLRECORD) and `T24.ACCOUNT_BLOB` (RECID + BLOBRECORD), table-level supplemental logging on both, two helper functions (`t24.clob_to_blob`, `t24.now_ms_utc`), the `SAMPLE_DIR` directory object. No data loading - both tables are empty until `seed/` is run |
| 4 | `oracle/oracle-connector.json` | POSTed to the Kafka Connect REST API | Deploys the Debezium Oracle connector - **one** connector, `table.include.list` covers both `T24.ACCOUNT` and `T24.ACCOUNT_BLOB` |
| 5 | `risingwave/risingwave-setup.sql` | via `psql` against RisingWave | Creates `t24_account`/`t24_account_blob` (current state), `t24_account_events`/`t24_account_blob_events` (append-only history), `lookup_metadata`, `t24_account_blob_text` (base64 → UTF8 decode), `t24_account_columns`/`t24_account_blob_columns` (166-column flattened views, identical pivot logic on both paths) |
| — | `superset/superset_config.py` | mounted into the `superset` container | Sets `SECRET_KEY` and points Superset's metadata DB at `superset-db` |
| 6 | *(no file — inline in `setup.ps1`)* | `docker exec cdc-superset superset ...` | Superset's one-time init: `db upgrade`, admin user, `init` (roles/permissions) |

Still manual after `setup.ps1`: adding the RisingWave connection inside
Superset's UI — see **Connecting Superset to RisingWave** below. Both
tables are also empty at this point - run `seed/` (below) to put data in.

Manual/ad-hoc scripts, not part of `setup.ps1`:
- `seed/` — `python seed/seed_xml.py` / `python seed/seed_blob.py`
  (`pip install oracledb` once first). Inserts one fresh row every
  `DELAY_SECONDS` (default 0.5s) until Ctrl+C: a new RECID, randomized
  business fields (balances, customer id, the anonymized name), and
  `c250` (date_time) overwritten with Oracle's own `SYSTIMESTAMP`, to
  the millisecond, at insert time. Codes/flags/dates other than `c250`
  are left as the template has them.
- `tests/` — one-shot update/delete against whichever row was most
  recently inserted (`test-update-cdc.py` / `test-delete-cdc.py`, `-blob`
  variants for `T24.ACCOUNT_BLOB`). Needs a row from `seed/` first.
- `benchmarks/` — end-to-end latency tracing; see `benchmarks/README.md`

---

## Key config, and why it matters

| Where | Setting | Why |
| --- | --- | --- |
| `oracle-init/01_enable_archivelog.sql` | Runs on every start, not just first init | `gvenzl/oracle-xe:21` has no `ENABLE_ARCHIVELOG` env var, and `ALTER DATABASE ARCHIVELOG` only works in MOUNT state — this checks first so only the very first start pays for a restart |
| `oracle/oracle-setup.sql` | `ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`, not a log group naming `XMLRECORD`/`BLOBRECORD` | Oracle rejects LOB columns in a log group (`ORA-30569`) — applies identically to `XMLTYPE` and a native `BLOB` |
| `oracle/oracle-connector.json` | `table.include.list` covers `T24.ACCOUNT` **and** `T24.ACCOUNT_BLOB` on **one** connector, not two | Redo mining is 65-71% of measured end-to-end latency — a second connector would open a second LogMiner session mining the same redo stream twice. Include-lists are fully matched, so both tables must be listed explicitly |
| `oracle/oracle-connector.json` | `column.include.list` = only `RECID`/`XMLRECORD` (and `RECID`/`BLOBRECORD` for the BLOB table) | Production's other 9 columns on `ACCOUNT` are VIRTUAL (computed from the XML, store nothing). On a streaming update Debezium would emit the literal defining expression instead of a value — correct at snapshot, silently wrong after. The list is a whitelist across *all* captured tables — omitting a table here drops every one of its columns |
| `oracle/oracle-connector.json` | `lob.enabled = true` | `XMLRECORD` is XMLTYPE backed by a hidden BLOB, and `BLOBRECORD` is a native BLOB — both reach Debezium via LOB redo entries, consumed identically through this one setting |
| `oracle/oracle-connector.json` | `binary.handling.mode` intentionally left unset | The default (`bytes`) already serializes as base64 under `JsonConverter` — the same wire format an explicit `base64` would give. Left unset to avoid any chance of perturbing how `XMLTYPE` currently arrives (as a JSON string) |
| `oracle/oracle-connector.json` | `log.mining.strategy = online_catalog`, not the default | Default can stall indefinitely on XE's small redo logs (changes sit unmined until a manual log switch). Trade-off: no DDL tracking on the captured table, irrelevant for a fixed schema |
| `oracle/oracle-connector.json` | `enable_goldengate_replication` intentionally not set | That parameter ties Oracle usage to a GoldenGate license in production, even without GoldenGate installed. Verified unnecessary — snapshot, update, and insert all captured correctly with it off |
| Any write to `T24.ACCOUNT` / `T24.ACCOUNT_BLOB` | Full replacement only (`XMLTYPE(...)` / `t24.clob_to_blob(...)`), never `UPDATEXML` | `UPDATEXML` edits don't carry enough redo detail for LogMiner — Debezium advances its offset but emits nothing, silently. Confirm with the source system how it actually writes |
| `risingwave/risingwave-setup.sql` | `before.XMLRECORD` / `before.BLOBRECORD` are always `__debezium_unavailable_value` on UPDATE/DELETE | Oracle's redo never carries LOB before-images. `t24_account_blob_text`'s decode guard explicitly filters this literal (and its base64 form) before calling `decode(...,'base64')`, which would otherwise error on it — `_` is not valid base64 |
| `risingwave/risingwave-setup.sql` | `t24_account_columns` / `t24_account_blob_columns` are `MATERIALIZED VIEW`, not `TABLE AS SELECT` or plain `VIEW`; `t24_account_blob_text` (the decode step) *is* a plain `VIEW` | `TABLE AS SELECT` freezes at creation and never updates again; a plain `VIEW` re-runs the whole query on every read — fine for a cheap decode inlined into the consuming MV's plan, wrong for the expensive regex/pivot, which needs `MATERIALIZED VIEW` to stay both live and pre-computed |
| `risingwave/risingwave-setup.sql` | `t24_account_columns` / `t24_account_blob_columns` flatten XML via regex (unpivot/resolve/pivot) | RisingWave has no XML/XPath functions at all |
| `risingwave/risingwave-setup.sql` | `t24_account_blob_columns` is a **generated**, not hand-copied, duplicate of `t24_account_columns` (identical 165-line pivot, 2 substitutions: MV name, `FROM` source) | Hand-transcribing 165 `MAX(CASE WHEN...)` lines risks silent drift between the two paths on the next edit. Regenerate the same way whenever field mappings change |
| `risingwave/risingwave-setup.sql` (top) | Ordered `DROP MATERIALIZED VIEW ... IF EXISTS` block before `DROP TABLE lookup_metadata` | Without it, re-running this file isn't idempotent — the bare `DROP TABLE lookup_metadata` fails once either flattening MV already exists and still depends on it |
| `risingwave/risingwave-setup.sql` | Adding a `lookup_metadata` row needs a matching `MAX(CASE WHEN column_name = '...')` line in **both** flattened MVs' final `SELECT` | The pivot's column list is fixed at creation time — a new mapping with no matching line silently produces no column and no error |
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

- Typed columns — everything in `t24_account_columns` / `t24_account_blob_columns`
  is `VARCHAR`; per-field type rules (dates, decimals) aren't in
  `lookup_metadata.csv` yet.
- Superset's `superset-db` password and `SUPERSET_SECRET_KEY` are fixed/committed
  in `docker-compose.yml` — fine for this lab, not for anything internet-facing.
- `benchmarks/run-trace.ps1`'s trace window (Kafka offset scoping, the
  poller) is XML-topic-only — its `update`/`delete` modes
  (`tests/test-update-cdc.py` / `test-delete-cdc.py`) and `continuous`
  mode (`seed/seed_xml.py`) all exercise `T24.ACCOUNT` only.
  `tests/test-update-cdc-blob.py` / `test-delete-cdc-blob.py` and
  `seed/seed_blob.py` cover the BLOB path standalone, but there's no
  `-Storage blob` option here yet for a side-by-side XMLTYPE-vs-BLOB
  latency comparison through this same tool.
