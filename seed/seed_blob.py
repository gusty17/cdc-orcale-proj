"""
    pip install oracledb   # once; thin mode, no Oracle client needed
    python seed/seed_blob.py
"""

import time

import oracledb

from _common import DELAY_SECONDS, connect, load_template, new_recid, oracle_now, randomize, set_c250

INSERT = """
INSERT INTO t24.account_blob (recid, blobrecord)
VALUES (:recid, :blob)
"""


def main() -> None:
    template = load_template("account_data_sample.xml")
    conn = connect()
    cur = conn.cursor()

    print(f"inserting into T24.ACCOUNT_BLOB every {DELAY_SECONDS}s - Ctrl+C to stop\n")

    i = 0
    started = time.time()
    try:
        while True:
            cycle_start = time.time()
            i += 1
            recid = new_recid("SEED")
            xml = randomize(template).replace('id="9000000112345001"', f'id="{recid}"')

            xml = set_c250(xml, oracle_now(cur))   # one round trip, right before inserting
            blob = xml.encode("utf-8")

            # Re-set every time - unclear if it survives the SELECT above on the same cursor.
            cur.setinputsizes(blob=oracledb.DB_TYPE_BLOB)
            cur.execute(INSERT, recid=recid, blob=blob)
            conn.commit()   # per row, so Ctrl+C never leaves one half-done

            print(f"[{time.strftime('%H:%M:%S')}] {i:>7}  {recid}", flush=True)

            time.sleep(max(0.0, DELAY_SECONDS - (time.time() - cycle_start)))
    except KeyboardInterrupt:
        pass
    finally:
        cur.close()
        conn.close()
        elapsed = time.time() - started
        rate = i / elapsed if elapsed else 0
        print(f"\nstopped - {i} rows into T24.ACCOUNT_BLOB in {elapsed:.0f}s ({rate:.2f} rows/sec)")


if __name__ == "__main__":
    main()
