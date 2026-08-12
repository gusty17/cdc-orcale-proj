# CDC trace — arrival time at every stage

One row per change, with the time it arrived at each stage of the
pipeline and the total it took. Covers **inserts, updates and deletes**.

The deliverable is the view **`t24_cdc_trace`**:

```sql
SELECT * FROM t24_cdc_trace ORDER BY t1_oracle;
```

| column | meaning |
|---|---|
| `recid` | the changed row |
| `op` | `insert` / `update` / `delete` |
| `t1_oracle` | **x** — Oracle executed the change |
| `t2_kafka` | **y** — written to the Kafka topic |
| `t3_risingwave` | **z** — queryable in RisingWave |
| `t4_parsed` | **m** — queryable in the parsed/flattened form |
| `oracle_to_kafka_ms` | x → y |
| `kafka_to_rw_ms` | y → z |
| `rw_to_parsed_ms` | z → m |
| `total_ms` | **x → m**, the whole journey |
| `t1_precision` | `exact` (ms) or `second` — see below |

There is no `t1`. An earlier version stamped a client-side "message
built" time into a synthetic `<bts>` tag, but that tag has no equivalent
in T24's real schema, so seed/test rows would carry a field production
rows never would. `t1` is the first stage instead, and it rides on a
real column: `c250` is genuinely T24's `date_time` field.

## Sample output

```
 recid                  | op     | t1_oracle    | t2_kafka     | t3_risingwave | t4_parsed    | ora2kafka | kafka2rw | rw2parsed | total_ms
 BENCH-260811143752193-1| insert | 14:37:52.193 | 14:37:55.803 | 14:37:56.477  | 14:37:56.477 |      3610 |      674 |         0 |     4284
 BENCH-260811143758211-4| insert | 14:37:58.211 | 14:38:00.814 | 14:38:01.847  | 14:38:01.847 |      2603 |     1033 |         0 |     3636
 BENCH-260811143752193-1| update | 14:38:27.847 | 14:38:30.402 | 14:38:30.979  | 14:38:30.979 |      2555 |      577 |         0 |     3132
 BENCH-260811143801842-8| delete | 14:38:51     | 14:38:54.462 | 14:38:54.844  |              |      3462 |      382 |           |     3844
```

Typical total is **3-5 seconds**. Two things stand out:

**`rw_to_parsed_ms` is 0 on every row.** The flattening into named columns
completes in the same streaming barrier as the raw arrival, so parsing
adds no measurable delay. The 166-column MV is effectively free relative
to the rest of the pipeline.

**Oracle → Kafka dominates** (roughly 2-4s of a 3-5s total). That is
Debezium mining the redo log plus its polling interval, and it is the
only hop worth tuning first.

## The precision column, and why it exists

`t1` is the one timestamp the pipeline does not hand over cleanly.

- **`exact`** — the change wrote its commit time into `c250` via the
  shared `t24.stamp_c250()` function (`oracle/oracle-setup.sql`), so `t1`
  is good to the millisecond. Applies to inserts and updates made by
  `seed/seed_xml.py` or `benchmarks/03-oracle-load.py` — the latter
  imports `insert_one()`/`update_one()` straight from `seed/_xml_ops.py`
  (the same functions `seed/seed_xml.py` and `tests/test-update-cdc.py`
  call) rather than re-implementing them, so every generator measures
  the pipeline identically.
- **`second`** — `c250` isn't in that shape, so `t1` falls back to
  Debezium's `source.ts_ms`. Oracle takes that from the redo log as a
  `DATE`: **whole seconds only**. `t1` and `total_ms` therefore carry up
  to 1000ms of error. You can see it in the sample above — the delete
  row's `t1_oracle` ends in `:51` with no fraction.

Deletes are always `second`: a delete sends no new XML, so there is
nowhere to stamp the time. Any change not made by the generators —
including real T24 activity — is also `second`.

## How it is built

```
Oracle ──> Kafka ──> t24_trace_events ──> t24_trace_parsed
                     (raw, append-only)   (flattened, per event)
                            │                     │
                            └──── poller ─────────┘
                                     │
                              t24_trace_arrivals
                                     │
                               t24_cdc_trace  ← the view
```

Everything is keyed on **`kafka_offset`**, not `recid`. The offset is
unique per change event, so an insert, an update and a delete of the same
`recid` are three separate traceable rows — keying on `recid` would
collapse them into one.

`t24_trace_parsed` mirrors `t24_account_columns`' real work (regex
explode, double `lookup_metadata` join, aggregate) but groups by
`kafka_offset` so it stays append-only and every event stays individually
addressable. The production MV groups by `recid`, which overwrites in
place on update and makes per-event timing impossible.

### Why a poller fills t3 and t4

RisingWave cannot stamp arrival per row in SQL. All three options fail:

| attempt | result |
|---|---|
| `proctime()` in a materialized view | rejected — "only allowed in CREATE TABLE/SOURCE" |
| `now()` in a materialized view | rejected — "only allowed in WHERE, HAVING, ON and FROM" |
| `proctime()` column on the table | per **barrier**, not per row — 200 rows shared 16 timestamps ~2s apart, some stamped *earlier* than the event that produced them, producing negative latencies |

Polling for first visibility is per-row, and measures the thing that
actually matters: when a `SELECT` can return the change — i.e. what a
Superset dashboard would see.

## Running it

Order matters. `t24_trace_events` is `scan.startup.mode = 'latest'`, so it
must exist before any change is made, and the poller must be running
before the first change lands.

```powershell
# 1. trace objects
Get-Content benchmarks\01-trace-setup.sql | docker exec -i cdc-superset-db psql -h cdc-risingwave -p 4566 -d dev -U root

# 2. poller - background; stops 20s after changes stop
Get-Content benchmarks\02-trace-poller.py | docker exec -i cdc-superset python -

# 3. make changes (20 inserts, 10 updates, 10 deletes @ 2s)
python benchmarks\03-oracle-load.py

# 4. read the trace
docker exec cdc-superset-db psql -h cdc-risingwave -p 4566 -d dev -U root -c "SELECT * FROM t24_cdc_trace ORDER BY t1_oracle;"

# 5. remove benchmark rows when done
docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" "@/scripts/benchmarks/04-cleanup.sql"
```

Or drive it end-to-end (poller + load + report, scoped to just that run)
with `.\benchmarks\run-trace.ps1 -Load benchmark`. `-Load continuous`,
`-Load update` and `-Load delete` run `seed/seed_xml.py` and
`tests/test-update-cdc.py` / `tests/test-delete-cdc.py` instead — see
`run-trace.ps1 -?` for the full mode list.

The trace works on **any** change, not just the generators' — make an
update by hand in SQL*Plus and it appears, with `t1_precision = 'second'`.

## Things that will bite you

**Pace changes at least ~2s apart.** At 200ms (5 changes/sec) this
pipeline saturates: end-to-end climbed to 22s and never recovered, and
recovery took ~45s afterwards. Past the knee you are measuring queueing,
not arrival time. Confirm the topic offset is stable before starting:

```powershell
docker exec cdc-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic t24.T24.ACCOUNT
```

**Tombstones are excluded.** Debezium follows every delete with a second,
null-payload message so Kafka log compaction can drop the key. The view
filters them (`WHERE op IS NOT NULL`) — they are a Kafka mechanic, not a
data change. A 40-change run produces 50 topic messages.

**Poll resolution is 50ms**, so `t3` and `t4` are accurate to about that.
Both stages are read in a single `UNION ALL` query so they share one
instant — reading them separately let a barrier land between the two and
produced a negative `rw_to_parsed_ms` of about one poll interval.

**Absolute numbers do not transfer to production.** Oracle XE is capped
at 2 CPU threads / 2 GB RAM, RisingWave runs `single_node`, all on one
Docker host. What transfers: which hop dominates, and the method.
