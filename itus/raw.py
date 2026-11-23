"""Raw layer: every CSV lands as-is (all TEXT) in raw_<file stem>.

Idempotency: a file is only (re)loaded when its sha256 changes, so rerunning
never duplicates rows and _loaded_at keeps the time of the original load.
"""
import csv
import hashlib
import json
import re
from datetime import datetime, timezone

from . import config


def table_name_for(path):
    return "raw_" + re.sub(r"[^a-z0-9]+", "_", path.stem.lower()).strip("_")


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _table_exists(conn, name):
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _ensure_log(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS raw_file_log (
            file_name      TEXT PRIMARY KEY,
            table_name     TEXT NOT NULL,
            sha256         TEXT NOT NULL,
            row_count      INTEGER NOT NULL,
            malformed_rows INTEGER NOT NULL,
            columns        TEXT NOT NULL,   -- header exactly as received (json list)
            loaded_at      TEXT NOT NULL
        )"""
    )


def load_raw(conn, raw_dir=config.RAW_DIR):
    _ensure_log(conn)
    loaded, skipped = [], []

    for path in sorted(raw_dir.glob("*.csv")):
        digest = file_sha256(path)
        table = table_name_for(path)
        prev = conn.execute(
            "SELECT sha256 FROM raw_file_log WHERE file_name=?", (path.name,)
        ).fetchone()
        if prev and prev[0] == digest and _table_exists(conn, table):
            skipped.append(path.name)
            continue
        if prev and prev[0] != digest:
            # same file name, different bytes. Vendors should never do this; we
            # reload but shout about it.
            print(f"  WARNING: {path.name} changed on disk since last load, reloading")

        with open(path, newline="", encoding="utf-8-sig") as fh:
            reader = csv.reader(fh)
            header = next(reader)
            rows = list(reader)

        width = len(header)
        malformed = sum(1 for r in rows if len(r) != width)
        rows = [(r + [None] * width)[:width] for r in rows]
        loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        cols_sql = ", ".join(f'"{c}" TEXT' for c in header)
        placeholders = ", ".join("?" for _ in range(width + 3))
        with conn:
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(
                f'CREATE TABLE "{table}" ({cols_sql}, '
                "_source_file TEXT NOT NULL, _loaded_at TEXT NOT NULL, "
                "_row_number INTEGER NOT NULL, PRIMARY KEY (_source_file, _row_number))"
            )
            conn.executemany(
                f'INSERT INTO "{table}" VALUES ({placeholders})',
                [r + [path.name, loaded_at, i] for i, r in enumerate(rows, start=1)],
            )
            conn.execute(
                "INSERT OR REPLACE INTO raw_file_log VALUES (?,?,?,?,?,?,?)",
                (path.name, table, digest, len(rows), malformed, json.dumps(header), loaded_at),
            )
        loaded.append(path.name)

    return loaded, skipped
