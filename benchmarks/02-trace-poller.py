"""
Fills t4 (raw arrival) and t5 (parsed arrival) for the CDC trace.

Start in the background BEFORE making changes in Oracle:

    Get-Content benchmarks\\02-trace-poller.py | docker exec -i cdc-superset python -

Polls both stages by kafka_offset - tracks insert/update/delete of the
same recid separately, recording each offset's first visible poll.

Why polling: RisingWave can't stamp per-row arrival in SQL (proctime()/
now() are restricted, and per-table proctime() is per-barrier, not
per-row). First-visibility is both per-row and what actually matters.
"""

import time
import psycopg2

POLL_MS     = 50    # resolution: an offset is credited to the poll that first sees it
IDLE_STOP_S = 20    # finish once changes have clearly stopped
MAX_RUN_S   = 600   # hard ceiling so a background run can never hang

conn = psycopg2.connect(host="risingwave", port=4566, user="root", dbname="dev")
conn.autocommit = True
cur = conn.cursor()

raw    = {}   # kafka_offset -> t4_ms  (None = present before we started)
parsed = {}   # kafka_offset -> t5_ms  (None = present before we started)

# Seed with everything ALREADY visible - a change that landed before
# this poller started can't be timed meaningfully, and without this the
# first poll would invent fake arrival times for historical events.
cur.execute(
    "SELECT 'raw' AS stage, kafka_offset FROM t24_trace_events "
    "UNION ALL "
    "SELECT 'parsed', kafka_offset FROM t24_trace_parsed"
)
for stage, off in cur.fetchall():
    (raw if stage == "raw" else parsed)[off] = None

# Belt and braces: never write an offset a previous run already recorded.
cur.execute("SELECT kafka_offset FROM t24_trace_arrivals")
already_recorded = {row[0] for row in cur.fetchall()}

print(
    f"poller: watching for new changes "
    f"({len(raw)} existing events skipped, {len(already_recorded)} already recorded)",
    flush=True,
)

timed    = 0            # offsets actually timed by THIS run
last_new = time.time()
started  = time.time()

while True:
    # Both stages in ONE query so they share a single instant - polling
    # separately let a barrier land between them, producing a negative
    # rw_to_parsed_ms.
    #
    # Timestamp taken BEFORE the query, so it's never later than the
    # poll that observed the row - a small, bounded under-estimate.
    now_ms = int(time.time() * 1000)
    cur.execute(
        "SELECT 'raw' AS stage, kafka_offset FROM t24_trace_events "
        "UNION ALL "
        "SELECT 'parsed', kafka_offset FROM t24_trace_parsed"
    )

    for stage, off in cur.fetchall():
        target = raw if stage == "raw" else parsed
        if off not in target:
            target[off] = now_ms
            timed += 1
            last_new = time.time()

    # Gate on `timed`, not `raw` - `raw` is non-empty from the seed, which
    # would exit early even if no load has started yet.
    if timed and (time.time() - last_new) > IDLE_STOP_S:
        break
    if (time.time() - started) > MAX_RUN_S:
        break
    time.sleep(POLL_MS / 1000)

# Deletes carry no XML to parse, so t5 stays NULL - total_ms falls back to t4.
new_rows = [
    (int(off), t4, parsed.get(off))
    for off, t4 in raw.items()
    if t4 is not None and off not in already_recorded   # skip seeded + previously recorded
]

if new_rows:
    values = ",".join(cur.mogrify("(%s,%s,%s)", r).decode() for r in new_rows)
    cur.execute(
        "INSERT INTO t24_trace_arrivals (kafka_offset, t4_ms, t5_ms) VALUES " + values
    )

n_parsed = sum(1 for _, _, t5 in new_rows if t5 is not None)
print(f"poller: timed {len(new_rows)} new changes, {n_parsed} of them parsed "
      f"(poll interval {POLL_MS}ms)")
