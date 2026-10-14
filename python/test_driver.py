"""Smoke-test the native adbc-questdb driver through the Python ADBC driver manager.

Requires a running QuestDB (see ../docker-compose.yml) and a driver library built
with the `ffi` feature, e.g.:

    cargo build --release --features ffi
"""

import os
import sys
from pathlib import Path

from adbc_driver_manager import dbapi

DRIVER_PATH = (
    Path(__file__).resolve().parent.parent
    / "target"
    / "release"
    / ("adbc_questdb.dll" if os.name == "nt" else "libadbc_questdb.so")
)
QUESTDB_URI = os.environ.get("QUESTDB_ADBC_TEST_URI", "ws://localhost:9000")


def connect():
    if not DRIVER_PATH.exists():
        sys.exit(f"driver library not found: {DRIVER_PATH} (build with --features ffi)")
    return dbapi.connect(
        driver=str(DRIVER_PATH),
        db_kwargs={"uri": QUESTDB_URI},
    )


def test_streams_large_result():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT x FROM long_sequence(100000)")
            reader = cur.fetch_record_batch()
            assert reader.schema.names == ["x"], reader.schema.names

            rows = 0
            batches = 0
            for batch in reader:
                rows += batch.num_rows
                batches += 1

            assert rows == 100_000, rows
            assert batches > 1, "expected incremental delivery in multiple batches"
            print(f"  large result: {rows} rows in {batches} batches")


def test_empty_result_has_schema():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT x FROM long_sequence(10) WHERE x < 0")
            reader = cur.fetch_record_batch()
            assert reader.schema.names == ["x"], reader.schema.names
            rows = sum(batch.num_rows for batch in reader)
            assert rows == 0, rows
            print("  empty result: schema present, 0 rows")


def test_mixed_types():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rnd_int() AS i, rnd_double() AS d, rnd_str() AS s, "
                "now() AS ts, rnd_boolean() AS b FROM long_sequence(100)"
            )
            reader = cur.fetch_record_batch()
            names = reader.schema.names
            assert names == ["i", "d", "s", "ts", "b"], names
            rows = sum(batch.num_rows for batch in reader)
            assert rows == 100, rows
            print(f"  mixed types: {rows} rows, fields {names}")


def test_query_error_propagates():
    with connect() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT * FROM table_that_does_not_exist")
            except Exception as exc:
                print(f"  missing table raised {type(exc).__name__}: {exc}")
            else:
                raise AssertionError("expected an error for a missing table")


def main():
    print(f"driver: {DRIVER_PATH}")
    print(f"uri:    {QUESTDB_URI}")
    for test in (
        test_streams_large_result,
        test_empty_result_has_schema,
        test_mixed_types,
        test_query_error_propagates,
    ):
        print(f"{test.__name__}:")
        test()
    print("all python smoke tests passed")


if __name__ == "__main__":
    main()
