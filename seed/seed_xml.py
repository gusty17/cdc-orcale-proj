"""
    pip install oracledb   # once; thin mode, no Oracle client needed
    python seed/seed_xml.py
"""

import time

import oracledb

from _common import DELAY_SECONDS, connect, load_template
from _xml_ops import insert_one


def main() -> None:
    template = load_template("account_data_sample.xml")
    conn = connect()
    cur = conn.cursor()
    cur.setinputsizes(xml=oracledb.DB_TYPE_CLOB)   # VARCHAR2 binds cap at 32767 bytes

    print(f"inserting into T24.ACCOUNT every {DELAY_SECONDS}s - Ctrl+C to stop\n")

    i = 0
    started = time.time()
    try:
        while True:
            cycle_start = time.time()
            i += 1
            recid = insert_one(cur, template)
            conn.commit()   # per row, so Ctrl+C never leaves one half-done

            # flush=True or output sits buffered and the terminal looks frozen.
            print(f"[{time.strftime('%H:%M:%S')}] {i:>7}  {recid}", flush=True)

            # Sleep the remainder, not the full interval, to avoid drift.
            time.sleep(max(0.0, DELAY_SECONDS - (time.time() - cycle_start)))
    except KeyboardInterrupt:
        pass
    finally:
        cur.close()
        conn.close()
        elapsed = time.time() - started
        rate = i / elapsed if elapsed else 0
        print(f"\nstopped - {i} rows into T24.ACCOUNT in {elapsed:.0f}s ({rate:.2f} rows/sec)")


if __name__ == "__main__":
    main()
