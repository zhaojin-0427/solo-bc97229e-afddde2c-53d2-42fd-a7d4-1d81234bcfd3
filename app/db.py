"""SQLite persistence for resumable carrier-log uploads and committed batches.

All durable state lives in one SQLite database (WAL mode):

* ``uploads``       - one row per resumable upload, total size and SHA-256
  fixed at creation time;
* ``chunks``        - every confirmed byte range and its payload, keyed by
  ``(upload_id, start_offset, end_offset)``;
* ``batches``       - one row per successfully completed upload (at most one
  batch per upload, enforced by a UNIQUE constraint);
* ``batch_records`` - validated NDJSON records in original line order, only
  ever inserted inside the same transaction that flips the upload to
  ``completed`` - readers can therefore never observe a partial batch.

Writers take ``BEGIN IMMEDIATE`` so concurrent chunk uploads and completion
retries are serialized at the database level while readers keep working.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DEFAULT_DB_PATH = "/data/ingest.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS uploads (
    upload_id     TEXT PRIMARY KEY,
    total_bytes   INTEGER NOT NULL CHECK (total_bytes > 0),
    sha256        TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('open', 'completed')),
    created_at    TEXT NOT NULL,
    completed_at  TEXT,
    batch_id      TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
    upload_id    TEXT NOT NULL,
    start_offset INTEGER NOT NULL CHECK (start_offset >= 0),
    end_offset   INTEGER NOT NULL CHECK (end_offset > start_offset),
    data         BLOB NOT NULL,
    PRIMARY KEY (upload_id, start_offset, end_offset),
    FOREIGN KEY (upload_id) REFERENCES uploads(upload_id)
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id     TEXT PRIMARY KEY,
    upload_id    TEXT NOT NULL UNIQUE,
    record_count INTEGER NOT NULL,
    total_bytes  INTEGER NOT NULL,
    sha256       TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_records (
    batch_id   TEXT NOT NULL,
    line_no    INTEGER NOT NULL,
    record_id  TEXT NOT NULL,
    local_time TEXT NOT NULL,
    timezone   TEXT NOT NULL,
    payload    TEXT NOT NULL,
    PRIMARY KEY (batch_id, line_no),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);
"""


def db_path() -> str:
    """Database location, overridable via ``INGEST_DB_PATH`` (tests/compose)."""
    return os.environ.get("INGEST_DB_PATH", DEFAULT_DB_PATH)


def connect() -> sqlite3.Connection:
    """Open a short-lived connection with WAL and sane concurrency pragmas."""
    path = db_path()
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection):
    """Serialize writers: ``BEGIN IMMEDIATE`` acquires the write lock up front."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def init_db() -> None:
    """Create the schema on startup (idempotent); safe after a restart."""
    conn = connect()
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
