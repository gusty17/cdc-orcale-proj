"""
Delete one existing T24.ACCOUNT_BLOB row - see test-delete-cdc.py for
the XMLTYPE-path equivalent; only the table name differs.

    python tests/test-delete-cdc-blob.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "seed"))

from _common import connect

FIND_ONE = "SELECT recid FROM t24.account_blob ORDER BY recid DESC FETCH FIRST 1 ROW ONLY"


def main() -> None:
    conn = connect()
    cur = conn.cursor()

    cur.execute(FIND_ONE)
    row = cur.fetchone()
    if row is None:
        print("no rows in T24.ACCOUNT_BLOB - nothing to delete")
        return
    target_recid = row[0]

    cur.execute("DELETE FROM t24.account_blob WHERE recid = :target_recid", target_recid=target_recid)
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    print(f"deleted {target_recid} from T24.ACCOUNT_BLOB ({deleted} row)")


if __name__ == "__main__":
    main()
