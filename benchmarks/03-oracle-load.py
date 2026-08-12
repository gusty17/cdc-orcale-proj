"""
CDC trace - Oracle side load generator. Generates a paced sequence of
inserts, updates and deletes against T24.ACCOUNT.

Run AFTER 01-trace-setup.sql, with 02-trace-poller.py already running:

    python benchmarks/03-oracle-load.py

Reuses seed/_xml_ops.py's insert_one()/update_one() rather than
re-implementing the XML-building/INSERT/UPDATE logic here - the same
functions seed/seed_xml.py and tests/test-update-cdc.py call, all of
which route through t24.stamp_c250() (oracle/oracle-setup.sql) to write
Oracle's own commit time into c250, so every generator measures the
pipeline identically. Deletes fall back to source.ts_ms ('second'
precision) - no XML to stamp a time into.

Runs directly on the host, not in a container - connects to Oracle via
localhost:1521, same as seed/*.py and tests/*.py.

Pacing must stay below the throughput knee or you measure queueing, not
arrival time - 200ms/5rps saturates and never recovers; 2000ms drains.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "seed"))

import oracledb
from _common import connect, load_template
from _xml_ops import insert_one, update_one

INSERTS       = 20
UPDATES       = 10
DELETES       = 10
DELAY_SECONDS = 2.0


def main() -> None:
    template = load_template("account_data_sample.xml")
    conn = connect()
    cur = conn.cursor()
    cur.setinputsizes(xml=oracledb.DB_TYPE_CLOB)   # VARCHAR2 binds cap at 32767 bytes

    # ---------------------------------------------------------------- INSERT
    recids = []
    for _ in range(INSERTS):
        recid = insert_one(cur, template, prefix="BENCH")
        conn.commit()
        recids.append(recid)
        time.sleep(DELAY_SECONDS)
    print(f"inserted {INSERTS}")

    # ---------------------------------------------------------------- UPDATE
    for recid in recids[:UPDATES]:
        update_one(cur, template, recid)
        conn.commit()
        time.sleep(DELAY_SECONDS)
    print(f"updated {UPDATES}")

    # ---------------------------------------------------------------- DELETE
    # Deletes the tail of the inserted set, so the updated rows above
    # keep their own trace entries intact.
    for recid in reversed(recids[INSERTS - DELETES:]):
        cur.execute("DELETE FROM t24.account WHERE recid = :target_recid", target_recid=recid)
        conn.commit()
        time.sleep(DELAY_SECONDS)
    print(f"deleted {DELETES}")

    cur.close()
    conn.close()
    print(f"done - {INSERTS + UPDATES + DELETES} change events")


if __name__ == "__main__":
    main()
