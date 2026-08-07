"""
Continuous CDC load - inserts one row per second, forever, until you stop it.

    pip install oracledb          # once; thin mode, no Oracle client needed
    python tests/test-insert-cdc-continuous.py

Rows carry a <bts>epoch_ms</bts> marker for benchmarks/01-trace-setup.sql's
poller. Not a <cNNN> tag, so the flattening regex ignores it.

"""

import os
import time

import oracledb

# connect to the Oracle database using environment variables, with defaults for local testing
DSN      = os.environ.get("ORACLE_DSN", "localhost:1521/XEPDB1")
USER     = os.environ.get("ORACLE_USER", "system")
PASSWORD = os.environ.get("ORACLE_PASSWORD", "oracle")

# One row per second - going much below this saturates the pipeline
# (measured: 0.2s/5rps pushed latency to 22s and it never recovered).
DELAY_SECONDS = float(os.environ.get("DELAY_SECONDS", "1.0"))

SAMPLE_XML = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "sample-data", "account_xml_data_sample.xml",
)

# <ots> stamped by Oracle server-side during the INSERT; <bts> stamped by
# this client beforehand - together they split "client -> Oracle" from

INSERT = """
INSERT INTO t24.account (recid, xmlrecord)
VALUES (
    :recid,
    XMLTYPE(REPLACE(:xml, '</row>',
        '<ots>' || TO_CHAR(
            (SELECT EXTRACT(DAY    FROM d) * 86400000
                  + EXTRACT(HOUR   FROM d) * 3600000
                  + EXTRACT(MINUTE FROM d) * 60000
                  + ROUND(EXTRACT(SECOND FROM d) * 1000)
               FROM (SELECT SYS_EXTRACT_UTC(SYSTIMESTAMP)
                            - TIMESTAMP '1970-01-01 00:00:00' AS d FROM dual)),
            'FM9999999999999') || '</ots></row>'))
)
"""


def main() -> None:
    # utf-8: the sample carries Arabic account titles.
    with open(SAMPLE_XML, encoding="utf-8") as fh:
        template = fh.read()

    conn = oracledb.connect(user=USER, password=PASSWORD, dsn=DSN)
    cur = conn.cursor()
    # Bind as CLOB - a VARCHAR2 bind caps at 32767 bytes, too small to trust.
    cur.setinputsizes(xml=oracledb.DB_TYPE_CLOB)

    print(f"connected to {DSN} as {USER}")
    print(f"inserting 1 row every {DELAY_SECONDS}s - Ctrl+C or close the terminal to stop\n")

    i = 0
    started = time.time()
    try:
        while True:
            cycle_start = time.time()
            i += 1
            ts = int(time.time() * 1000)
            recid = f"LOAD-{ts}-{i}"

            xml = template.replace('id="9000000112345001"', f'id="{recid}"')
            xml = xml.replace("</row>", f"<bts>{ts}</bts></row>")

            cur.execute(INSERT, recid=recid, xml=xml)
            conn.commit()   # commit per row, so stopping never leaves work half-done

            # flush=True or the output sits in a buffer and the terminal
            # looks frozen - the whole point here is watching it tick.
            print(f"[{time.strftime('%H:%M:%S')}] {i:>7}  {recid}", flush=True)

            # Sleep the REMAINDER of the interval, not the full interval:
            # the insert itself takes time, so a flat sleep would drift
            # and the real rate would be slower than requested.
            time.sleep(max(0.0, DELAY_SECONDS - (time.time() - cycle_start)))
    except KeyboardInterrupt:
        pass
    finally:
        elapsed = time.time() - started
        cur.close()
        conn.close()
        rate = i / elapsed if elapsed else 0
        print(f"\nstopped - {i} rows in {elapsed:.0f}s ({rate:.2f} rows/sec)")
        print("clean up with: docker exec cdc-oracle sqlplus -S -L "
              '"sys/oracle@//localhost:1521/XEPDB1 as sysdba" '
              '"@/scripts/benchmarks/04-cleanup.sql"')


if __name__ == "__main__":
    main()
