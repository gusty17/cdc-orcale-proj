"""
Shared helpers for seed_xml.py / seed_blob.py - the parts that aren't
specific to XMLTYPE vs BLOB storage.
"""

import os
import random
import re
import time

import oracledb

DSN      = os.environ.get("ORACLE_DSN", "localhost:1521/XEPDB1")
USER     = os.environ.get("ORACLE_USER", "system")
PASSWORD = os.environ.get("ORACLE_PASSWORD", "oracle")

SAMPLE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sample-data")

# Same tag shape as the flattening regex in risingwave/risingwave-setup.sql - keep in sync.
_TAG_RE    = re.compile(r'(<(c\d+)(?:\s+m="\d+")?>)([^<]*)(</\2>)')
_AMOUNT_RE = re.compile(r'^-?\d+\.\d{2}$')

# XML path only - t24.stamp_c250() (oracle/oracle-setup.sql) needs text,
# not BLOB, so seed_blob.py stamps client-side instead (oracle_now() below).
C250_NOW_SQL = "t24.stamp_c250(:xml)"

_C250_RE = re.compile(r'(<c250[^>]*>)[^<]*(</c250>)')


def oracle_now(cur) -> str:
    """Oracle's current time, formatted like t24.stamp_c250(). Call
    right before inserting."""
    cur.execute("SELECT TO_CHAR(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'YYMMDDHH24MISSFF3') FROM DUAL")
    return cur.fetchone()[0]


def set_c250(xml: str, value: str) -> str:
    """Client-side equivalent of C250_NOW_SQL - replaces both <c250> tags."""
    return _C250_RE.sub(lambda m: f"{m.group(1)}{value}{m.group(2)}", xml)


DELAY_SECONDS = float(os.environ.get("DELAY_SECONDS", "0.5"))


def connect():
    return oracledb.connect(user=USER, password=PASSWORD, dsn=DSN)


def load_template(filename: str) -> str:
    with open(os.path.join(SAMPLE_DIR, filename), encoding="utf-8") as fh:
        return fh.read()


def new_recid(prefix: str) -> str:
    return f"{prefix}-{int(time.time() * 1000)}"


def randomize(xml: str) -> str:
    """Fresh values for business fields (amounts, customer id, name).
    Codes/flags/dates left alone - c250 is set separately, by the caller."""
    def replace_amount(m):
        open_tag, value, close_tag = m.group(1), m.group(3), m.group(4)
        if _AMOUNT_RE.match(value):
            new_value = float(value) * random.uniform(0.7, 1.3)
            return f"{open_tag}{new_value:.2f}{close_tag}"
        return m.group(0)

    xml = _TAG_RE.sub(replace_amount, xml)
    xml = re.sub(r'(<c1>)\d+(</c1>)',
                 lambda m: f"{m.group(1)}{random.randint(10_000_000, 99_999_999)}{m.group(2)}", xml)
    xml = re.sub(r'(<c11>)\d+(</c11>)',
                 lambda m: f"{m.group(1)}{random.randint(100, 999)}{m.group(2)}", xml)

    suffix = random.randint(100, 999)
    for pattern in (r'(<c3>)([^<]*)(</c3>)',
                    r'(<c5>)([^<]*)(</c5>)',
                    r'(<c20 m="4">)([^<]*)(</c20>)'):
        xml = re.sub(pattern, lambda m: f"{m.group(1)}{m.group(2)} - {suffix}{m.group(3)}", xml)

    return xml
