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
| `t1_oracle` | **x** — changed in Oracle |
| `t2_kafka` | **y** — written to the Kafka topic |
| `t3_risingwave` | **z** — queryable in RisingWave |
| `t4_parsed` | **m** — queryable in the parsed/flattened form |
| `oracle_to_kafka_ms` | x → y |
| `kafka_to_rw_ms` | y → z |
| `rw_to_parsed_ms` | z → m |
| `total_ms` | **x → m**, the whole journey |
| `t1_precision` | `exact` (ms) or `second` — see below |

## Sample output

```
 recid                  | op     | t1_oracle    | t2_kafka     | t3_risingwave | t4_parsed    | o2k  | k2r  | r2p | total_ms
 BENCH-1785962567759-1  | insert | 20:42:47.759 | 20:42:51.303 | 20:42:51.977  | 20:42:51.977 | 3544 |  674 |   0 |     4218
 BENCH-1785962573778-4  | insert | 20:42:53.778 | 20:42:55.814 | 20:42:56.847  | 20:42:56.847 | 2036 | 1033 |   0 |     3069
 BENCH-1785962567759-1  | update | 20:43:27.847 | 20:43:30.402 | 20:43:30.979  | 20:43:30.979 | 2555 |  577 |   0 |     3132
 BENCH-1785962601842-18 | delete | 20:43:51     | 20:43:54.462 | 20:43:54.844  |              | 3462 |  382 |     |     3844
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

- **`exact`** — the load generator wrote its commit time into the row as
  `<bts>epoch_ms</bts>`, so `t1` is good to the millisecond. Applies to
  inserts and updates.
- **`second`** — no marker, so `t1` falls back to Debezium's
  `source.ts_ms`. Oracle takes that from the redo log as a `DATE`:
  **whole seconds only**. `t1` and `total_ms` therefore carry up to
  1000ms of error. You can see it in the sample above — the delete rows'
  `t1_oracle` ends in `:51` with no fraction.

Deletes are always `second`: a delete sends no new XML, so there is
nowhere to stamp the time. Any change not made by the generator —
including real T24 activity — is also `second`.

`<bts>` is deliberately not a `<cNNN>` tag. The flattening regex matches
`'<(c\d+)...>'` only, so the marker is invisible to it and creates no
spurious column.

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
docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" "@/scripts/benchmarks/03-oracle-load.sql"

# 4. read the trace
docker exec cdc-superset-db psql -h cdc-risingwave -p 4566 -d dev -U root -c "SELECT * FROM t24_cdc_trace ORDER BY t1_oracle;"

# 5. remove benchmark rows when done
docker exec cdc-oracle sqlplus -S -L "sys/oracle@//localhost:1521/XEPDB1 as sysdba" "@/scripts/benchmarks/04-cleanup.sql"
```

The trace works on **any** change, not just the generator's — make an
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
