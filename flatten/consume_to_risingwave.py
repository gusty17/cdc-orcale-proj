"""
consume_to_risingwave.py - Kafka -> parse XML -> flat columns -> RisingWave.

    python flatten/consume_to_risingwave.py                # run continuously
    python flatten/consume_to_risingwave.py --from-beginning --idle-exit 15

Consumes the Debezium topic, splits each XMLRECORD into the business columns
resolved by generate_schema.py, and writes them into a plain (NOT
Kafka-connected) RisingWave table.

Behaviour that was verified against the live stack before this was written:

  * INSERT on an existing PRIMARY KEY upserts in RisingWave - no duplicate,
    no error - so Debezium r / c / u all map to a plain INSERT.
  * DELETE works normally, so op=d maps to DELETE.
  * Writes stay INVISIBLE to readers until an explicit FLUSH. Every batch
    ends with one; without it the table looks permanently empty.
  * A delete also produces a tombstone (same key, null value). Those carry no
    JSON at all and are skipped.

Unmapped data is never dropped silently: a tag missing from column_map.json,
or an array position past the generated cap, logs a warning naming the tag.
Re-run generate_schema.py against a richer sample to widen the schema.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import psycopg2
from confluent_kafka import Consumer, KafkaError

SCRIPT_DIR = Path(__file__).resolve().parent
COLUMN_MAP_PATH = SCRIPT_DIR / "column_map.json"

DEFAULT_BOOTSTRAP = "localhost:29092"
DEFAULT_TOPIC = "t24.T24.ACCOUNT"
DEFAULT_RW_DSN = "host=localhost port=4566 dbname=dev user=root"

# <c8>EGP</c8> / <c20 m="2">x</c20> / <c100/> / <c20 m="32"></c20>
TAG_RE = re.compile(r'<(c\d+)(?:\s+m="(\d+)")?\s*(?:/>|>(.*?)</\1>)', re.DOTALL)

XML_ENTITIES = {"&lt;": "<", "&gt;": ">", "&quot;": '"', "&apos;": "'", "&amp;": "&"}


def unescape(text: str) -> str:
    # &amp; must go last so "&amp;lt;" doesn't collapse into "<"
    for entity, char in XML_ENTITIES.items():
        if entity != "&amp;":
            text = text.replace(entity, char)
    return text.replace("&amp;", "&")


def parse_xmlrecord(xml_text: str) -> dict[str, str]:
    """Split one XMLRECORD into {"field:position": value}."""
    values: dict[str, str] = {}
    for field, m_attr, body in TAG_RE.findall(xml_text or ""):
        position = int(m_attr) if m_attr else 1
        values[f"{field}:{position}"] = unescape(body).strip()
    return values


class Flattener:
    def __init__(self, column_map_path: Path):
        spec = json.loads(column_map_path.read_text(encoding="utf-8"))
        self.table: str = spec["table"]
        self.columns: list[str] = spec["columns"]
        self.map: dict[str, str] = spec["map"]
        self.unmapped: Counter[str] = Counter()

    def to_row(self, recid: str, xml_text: str) -> dict[str, str | None]:
        parsed = parse_xmlrecord(xml_text)
        row: dict[str, str | None] = {"recid": recid}
        row.update({name: None for name in self.columns})

        for key, value in parsed.items():
            column = self.map.get(key)
            if column is None:
                self.unmapped[key] += 1
                continue
            row[column] = value or None

        return row

    def report_unmapped(self) -> None:
        if not self.unmapped:
            return
        print()
        print(f"  WARNING: {len(self.unmapped)} tag/position(s) had no column and were NOT stored:")
        for key, count in self.unmapped.most_common():
            print(f"    {key:<12} x{count}")
        print("  Re-run generate_schema.py against a sample containing these to widen the schema.")


class RisingWaveWriter:
    def __init__(self, dsn: str, table: str, columns: list[str]):
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True
        self.table = table
        self.all_columns = ["recid", *columns]
        placeholders = ", ".join(["%s"] * len(self.all_columns))
        quoted = ", ".join(f'"{c}"' for c in self.all_columns)
        self.insert_sql = f"INSERT INTO {table} ({quoted}) VALUES ({placeholders})"
        self.delete_sql = f"DELETE FROM {table} WHERE recid = %s"

    def apply(self, upserts: pd.DataFrame, deletes: list[str]) -> None:
        """Callers must ensure a recid appears in at most one of the two.

        Mixing them in a single flush silently loses the delete: RisingWave
        keeps writes invisible until FLUSH, so a DELETE issued after an
        INSERT in the same batch matches nothing and the row survives.
        _collapse_pending() upstream guarantees the split.
        """
        if upserts.empty and not deletes:
            return

        with self.conn.cursor() as cur:
            if not upserts.empty:
                ordered = upserts[self.all_columns]
                # object dtype keeps None as SQL NULL; NaN would insert "nan"
                records = [tuple(r) for r in ordered.astype(object).where(pd.notna(ordered), None).values]
                cur.executemany(self.insert_sql, records)
            for recid in deletes:
                cur.execute(self.delete_sql, (recid,))
            # Without FLUSH the rows stay invisible to every reader.
            cur.execute("FLUSH")

    def close(self) -> None:
        self.conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help=f"Kafka bootstrap (default {DEFAULT_BOOTSTRAP})")
    ap.add_argument("--topic", default=DEFAULT_TOPIC, help=f"topic (default {DEFAULT_TOPIC})")
    ap.add_argument("--dsn", default=DEFAULT_RW_DSN, help="RisingWave libpq DSN")
    ap.add_argument("--group", default="t24-flatten", help="consumer group id")
    ap.add_argument("--from-beginning", action="store_true", help="read the topic from the start")
    ap.add_argument("--batch-size", type=int, default=100, help="rows per write+FLUSH")
    ap.add_argument("--idle-exit", type=float, default=0.0,
                    help="exit after N seconds with no new messages (0 = run forever)")
    args = ap.parse_args()

    flattener = Flattener(COLUMN_MAP_PATH)
    writer = RisingWaveWriter(args.dsn, flattener.table, flattener.columns)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([args.topic])

    print(f"Consuming {args.topic} @ {args.bootstrap}")
    print(f"Writing   {flattener.table} ({len(flattener.columns) + 1} columns) @ {args.dsn}")
    print("Ctrl-C to stop.\n")

    stats = Counter()
    # recid -> ("upsert", row) | ("delete", None), in arrival order.
    # Keying by recid collapses each row to its FINAL state in the batch, so a
    # recid is never both upserted and deleted in one flush - see the note on
    # RisingWaveWriter.apply for why mixing them would drop the delete.
    pending: dict[str, tuple[str, dict[str, str | None] | None]] = {}
    idle = 0.0

    def flush() -> None:
        if not pending:
            return
        rows = [row for kind, row in pending.values() if kind == "upsert" and row is not None]
        deletes = [recid for recid, (kind, _) in pending.items() if kind == "delete"]

        frame = pd.DataFrame(rows, columns=writer.all_columns) if rows else pd.DataFrame(columns=writer.all_columns)
        writer.apply(frame, deletes)
        print(f"  wrote {len(rows)} upsert(s), {len(deletes)} delete(s)")
        pending.clear()

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                flush()
                idle += 1.0
                if args.idle_exit and idle >= args.idle_exit:
                    print(f"\nNo new messages for {args.idle_exit:.0f}s - exiting.")
                    break
                continue

            idle = 0.0

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"  kafka error: {msg.error()}", file=sys.stderr)
                continue

            raw = msg.value()
            if raw is None:
                stats["tombstone"] += 1          # delete's companion message
                continue

            event = json.loads(raw)
            op = event.get("op")
            after, before = event.get("after"), event.get("before")

            if op == "d":
                recid = (before or {}).get("RECID")
                if recid:
                    pending[recid] = ("delete", None)
                    stats["delete"] += 1
            elif op in ("r", "c", "u"):
                recid = (after or {}).get("RECID")
                if recid:
                    pending[recid] = ("upsert", flattener.to_row(recid, (after or {}).get("XMLRECORD") or ""))
                    stats[{"r": "snapshot", "c": "insert", "u": "update"}[op]] += 1
            else:
                stats[f"skipped_op_{op}"] += 1

            if len(pending) >= args.batch_size:
                flush()

    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        flush()
        consumer.close()
        writer.close()

    print("\nProcessed:")
    for name, count in sorted(stats.items()):
        print(f"  {name:<12} {count}")
    flattener.report_unmapped()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
