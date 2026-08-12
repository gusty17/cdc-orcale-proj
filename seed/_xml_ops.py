"""
Shared INSERT/UPDATE logic for the XML storage path (T24.ACCOUNT.XMLRECORD).

Used by seed_xml.py (inserts only), tests/test-update-cdc.py (updates
only), and benchmarks/03-oracle-load.py (both) - one place for the
actual DML instead of three copies drifting apart.
"""

from _common import C250_NOW_SQL, new_recid, randomize

INSERT = f"""
INSERT INTO t24.account (recid, xmlrecord)
VALUES (:recid, XMLTYPE({C250_NOW_SQL}))
"""

UPDATE = f"""
UPDATE t24.account
   SET xmlrecord = XMLTYPE({C250_NOW_SQL})
 WHERE recid = :target_recid
"""


def insert_one(cur, template: str, prefix: str = "SEED") -> str:
    """Inserts one randomized row into T24.ACCOUNT. Does not commit -
    the caller controls pacing. Returns the new recid."""
    recid = new_recid(prefix)
    xml = randomize(template).replace('id="9000000112345001"', f'id="{recid}"')
    cur.execute(INSERT, recid=recid, xml=xml)
    return recid


def update_one(cur, template: str, target_recid: str) -> None:
    """Full XMLTYPE replacement of an existing row - never partial
    (LogMiner needs full redo detail). Does not commit."""
    xml = randomize(template).replace('id="9000000112345001"', f'id="{target_recid}"')
    cur.execute(UPDATE, xml=xml, target_recid=target_recid)
