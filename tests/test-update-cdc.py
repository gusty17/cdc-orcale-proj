"""
Update one existing T24.ACCOUNT row - picks the most recently inserted
row and does a full XMLTYPE replacement with a freshly randomized body.
Never partial (no UPDATEXML) - see README "Key config": partial edits
don't carry enough redo detail for LogMiner to see the change.

    python tests/test-update-cdc.py

recid is the primary key, so WHERE recid = :target_recid always matches
exactly one row, however the target was chosen.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "seed"))

import oracledb
from _common import connect, load_template
from _xml_ops import update_one

FIND_ONE = "SELECT recid FROM t24.account ORDER BY recid DESC FETCH FIRST 1 ROW ONLY"


def main() -> None:
    conn = connect()
    cur = conn.cursor()

    cur.execute(FIND_ONE)
    row = cur.fetchone()
    if row is None:
        print("no rows in T24.ACCOUNT - run seed/seed_xml.py first")
        return
    target_recid = row[0]

    template = load_template("account_data_sample.xml")
    cur.setinputsizes(xml=oracledb.DB_TYPE_CLOB)
    update_one(cur, template, target_recid)
    updated = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    print(f"updated {target_recid} in T24.ACCOUNT ({updated} row)")


if __name__ == "__main__":
    main()
