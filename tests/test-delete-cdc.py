"""
Delete one existing T24.ACCOUNT row - picks the most recently inserted
row and deletes it.

    python tests/test-delete-cdc.py

recid is the primary key, so WHERE recid = :target_recid always matches
exactly one row, whichever one was picked.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "seed"))

from _common import connect

FIND_ONE = "SELECT recid FROM t24.account ORDER BY recid DESC FETCH FIRST 1 ROW ONLY"


def main() -> None:
    conn = connect()
    cur = conn.cursor()

    cur.execute(FIND_ONE)
    row = cur.fetchone()
    if row is None:
        print("no rows in T24.ACCOUNT - nothing to delete")
        return
    target_recid = row[0]

    cur.execute("DELETE FROM t24.account WHERE recid = :target_recid", target_recid=target_recid)
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    print(f"deleted {target_recid} from T24.ACCOUNT ({deleted} row)")


if __name__ == "__main__":
    main()
