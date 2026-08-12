"""
Update one existing T24.ACCOUNT_BLOB row - see test-update-cdc.py for
the XMLTYPE-path equivalent. Builds the BLOB client-side, same
trade-off as seed_blob.py (see C250_NOW_SQL in _common.py).

    python tests/test-update-cdc-blob.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "seed"))

import oracledb
from _common import connect, load_template, oracle_now, randomize, set_c250

FIND_ONE = "SELECT recid FROM t24.account_blob ORDER BY recid DESC FETCH FIRST 1 ROW ONLY"

UPDATE = """
UPDATE t24.account_blob
   SET blobrecord = :blob
 WHERE recid = :target_recid
"""


def main() -> None:
    conn = connect()
    cur = conn.cursor()

    cur.execute(FIND_ONE)
    row = cur.fetchone()
    if row is None:
        print("no rows in T24.ACCOUNT_BLOB - run seed/seed_blob.py first")
        return
    target_recid = row[0]

    template = load_template("account_data_sample.xml")
    xml = randomize(template).replace('id="9000000112345001"', f'id="{target_recid}"')
    xml = set_c250(xml, oracle_now(cur))
    blob = xml.encode("utf-8")

    cur.setinputsizes(blob=oracledb.DB_TYPE_BLOB)
    cur.execute(UPDATE, blob=blob, target_recid=target_recid)
    updated = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    print(f"updated {target_recid} in T24.ACCOUNT_BLOB ({updated} row)")


if __name__ == "__main__":
    main()
