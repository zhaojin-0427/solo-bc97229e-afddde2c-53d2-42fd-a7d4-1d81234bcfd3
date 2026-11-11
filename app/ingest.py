"""Resumable-upload service logic on top of :mod:`app.db`.

Concurrency model
-----------------
Every mutating operation runs inside ``BEGIN IMMEDIATE``, so SQLite hands the
write lock to one writer at a time. All overlap/conflict checks and the write
happen in the same transaction, which makes the "identical overlap is an
idempotent retransmit, differing overlap rejects the whole chunk while stored
bytes stay untouched" rule hold even under concurrent uploads. Completion takes
the same lock and flips the upload to ``completed`` in the transaction that
inserts the batch; a lost completion response can therefore be retried safely
and always returns the same batch.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone

from pydantic import ValidationError

from . import db
from .schemas import ScanRecord

# Operational guard rails (file content itself is still verified exactly).
MAX_BYTES = int(os.environ.get("INGEST_MAX_BYTES", 100 * 1024 * 1024))
MAX_RECORDS = int(os.environ.get("INGEST_MAX_RECORDS", 100_000))


class IngestError(Exception):
    """Domain error carrying the HTTP status code and a structured payload."""

    def __init__(self, status_code: int, error: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.error = error
        self.message = message
        self.details = details or {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_hex_sha256(value: str) -> bool:
    try:
        int(value, 16)
    except ValueError:
        return False
    return len(value) == 64


def _merged_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge confirmed ``[start, end)`` intervals into disjoint intervals."""
    merged: list[tuple[int, int]] = []
    for start, end in sorted(ranges):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _missing(total_bytes: int, ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Subtract confirmed intervals from ``[0, total_bytes)`` -> gap intervals."""
    gaps: list[tuple[int, int]] = []
    cursor = 0
    for start, end in _merged_ranges(ranges):
        if start > cursor:
            gaps.append((cursor, start))
        cursor = max(cursor, end)
    if cursor < total_bytes:
        gaps.append((cursor, total_bytes))
    return gaps


def _upload_row(conn: sqlite3.Connection, upload_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM uploads WHERE upload_id = ?", (upload_id,)
    ).fetchone()
    if row is None:
        raise IngestError(404, "upload_not_found", f"upload {upload_id!r} does not exist")
    return row


def _status_payload(row: sqlite3.Row, conn: sqlite3.Connection) -> dict:
    chunks = conn.execute(
        "SELECT start_offset, end_offset FROM chunks WHERE upload_id = ?",
        (row["upload_id"],),
    ).fetchall()
    ranges = [(c["start_offset"], c["end_offset"]) for c in chunks]
    merged = _merged_ranges(ranges)
    received = sum(end - start for start, end in merged)
    return {
        "upload_id": row["upload_id"],
        "total_bytes": row["total_bytes"],
        "sha256": row["sha256"],
        "status": row["status"],
        "received_bytes": received,
        "missing_ranges": [
            {"start": s, "end": e} for s, e in _missing(row["total_bytes"], ranges)
        ],
        "batch_id": row["batch_id"],
        "created_at": row["created_at"],
        "completed_at": row["completed_at"],
    }


# ---------------------------------------------------------------------------
# session lifecycle
# ---------------------------------------------------------------------------


def create_upload(upload_id: str, total_bytes: int, sha256: str) -> dict:
    sha256 = sha256.lower()
    if not _is_hex_sha256(sha256):
        raise IngestError(
            422, "invalid_sha256", "sha256 must be 64 hexadecimal characters"
        )
    if total_bytes > MAX_BYTES:
        raise IngestError(
            422,
            "file_too_large",
            f"total_bytes {total_bytes} exceeds limit {MAX_BYTES}",
            {"limit_bytes": MAX_BYTES},
        )

    conn = db.connect()
    try:
        with db.transaction(conn):
            existing = conn.execute(
                "SELECT * FROM uploads WHERE upload_id = ?", (upload_id,)
            ).fetchone()
            if existing is not None:
                # Idempotent re-create: same parameters return the live session;
                # different parameters would silently change the contract.
                if (
                    existing["total_bytes"] != total_bytes
                    or existing["sha256"] != sha256
                ):
                    raise IngestError(
                        409,
                        "upload_conflict",
                        f"upload {upload_id!r} already exists with different "
                        "total_bytes/sha256; the contract is immutable",
                        {
                            "existing_total_bytes": existing["total_bytes"],
                            "existing_sha256": existing["sha256"],
                        },
                    )
            else:
                conn.execute(
                    "INSERT INTO uploads (upload_id, total_bytes, sha256, status, created_at) "
                    "VALUES (?, ?, ?, 'open', ?)",
                    (upload_id, total_bytes, sha256, _now()),
                )
            row = _upload_row(conn, upload_id)
            return _status_payload(row, conn)
    finally:
        conn.close()


def get_upload(upload_id: str) -> dict:
    conn = db.connect()
    try:
        return _status_payload(_upload_row(conn, upload_id), conn)
    finally:
        conn.close()


def get_missing_ranges(upload_id: str) -> dict:
    status = get_upload(upload_id)
    return {
        "upload_id": upload_id,
        "total_bytes": status["total_bytes"],
        "received_bytes": status["received_bytes"],
        "missing_ranges": status["missing_ranges"],
    }


# ---------------------------------------------------------------------------
# chunks
# ---------------------------------------------------------------------------


def put_chunk(upload_id: str, start_offset: int, end_offset: int, body: bytes) -> dict:
    if not body:
        raise IngestError(422, "empty_chunk", "chunk body must not be empty")

    conn = db.connect()
    try:
        with db.transaction(conn):
            row = _upload_row(conn, upload_id)
            # Immutability takes precedence: once completed, no chunk write of
            # any shape is accepted, regardless of range/body validity.
            if row["status"] == "completed":
                raise IngestError(
                    409,
                    "completed_immutable",
                    "upload is already completed; chunks can no longer be modified",
                    {"batch_id": row["batch_id"]},
                )
            if end_offset - start_offset != len(body):
                raise IngestError(
                    422,
                    "length_mismatch",
                    "end_offset - start_offset must equal the body length in bytes",
                    {"declared": end_offset - start_offset, "actual": len(body)},
                )
            total = row["total_bytes"]
            if start_offset < 0 or start_offset >= end_offset or end_offset > total:
                raise IngestError(
                    416,
                    "chunk_range_exceeds_file",
                    f"range [{start_offset}, {end_offset}) is outside [0, {total})",
                    {"total_bytes": total},
                )

            overlaps = conn.execute(
                "SELECT start_offset, end_offset, data FROM chunks "
                "WHERE upload_id = ? AND start_offset < ? AND end_offset > ?",
                (upload_id, end_offset, start_offset),
            ).fetchall()

            for existing in overlaps:
                lo = max(start_offset, existing["start_offset"])
                hi = min(end_offset, existing["end_offset"])
                new_part = body[lo - start_offset:hi - start_offset]
                old_part = existing["data"][lo - existing["start_offset"]:hi - existing["start_offset"]]
                if new_part != old_part:
                    # Whole chunk rejected; nothing is written, stored bytes unchanged.
                    raise IngestError(
                        409,
                        "chunk_conflict",
                        "overlapping bytes differ from an already confirmed chunk; "
                        "the whole chunk was rejected and stored content is unchanged",
                        {
                            "conflict_range": {"start": lo, "end": hi},
                            "stored_range": {
                                "start": existing["start_offset"],
                                "end": existing["end_offset"],
                            },
                        },
                    )

            already = conn.execute(
                "SELECT 1 FROM chunks WHERE upload_id = ? AND start_offset = ? AND end_offset = ?",
                (upload_id, start_offset, end_offset),
            ).fetchone()
            stored = False
            if already is None:
                # Only store when the range is not already fully covered by
                # identical bytes; keeps retransmits idempotent and storage small.
                fully_covered = any(
                    existing["start_offset"] <= start_offset and existing["end_offset"] >= end_offset
                    for existing in overlaps
                )
                if not fully_covered:
                    conn.execute(
                        "INSERT INTO chunks (upload_id, start_offset, end_offset, data) "
                        "VALUES (?, ?, ?, ?)",
                        (upload_id, start_offset, end_offset, body),
                    )
                    stored = True
            return {
                "start_offset": start_offset,
                "end_offset": end_offset,
                "stored": stored,
            }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# completion and reads
# ---------------------------------------------------------------------------


def _parse_ndjson(blob: bytes) -> list[dict]:
    """Validate every line with the existing ScanRecord rules; all errors at once."""
    try:
        text = blob.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise IngestError(
            409,
            "invalid_ndjson",
            "file content is not valid UTF-8",
            {"decode_error": str(exc)},
        )

    lines = text.splitlines()
    if not lines:
        raise IngestError(
            409, "invalid_ndjson", "file contains no records",
            {"errors": [{"line_no": 0, "reason": "empty file"}]},
        )

    errors: list[dict] = []
    records: list[dict] = []
    first_seen: dict[str, int] = {}
    for line_no, raw in enumerate(lines, start=1):
        if not raw.strip():
            errors.append({"line_no": line_no, "reason": "empty line is not a JSON object"})
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as exc:
            errors.append({"line_no": line_no, "reason": f"invalid JSON: {exc.msg}"})
            continue
        if not isinstance(obj, dict):
            errors.append({"line_no": line_no, "reason": "line must be a JSON object"})
            continue
        try:
            model = ScanRecord.model_validate(obj)
        except ValidationError as exc:
            for err in exc.errors():
                loc = ".".join(str(part) for part in err["loc"]) or "<root>"
                errors.append({"line_no": line_no, "field": loc, "reason": err["msg"]})
            continue
        rid = model.id
        if rid in first_seen:
            errors.append({
                "line_no": line_no,
                "field": "id",
                "reason": f"duplicate id {rid!r}; first seen on line {first_seen[rid]}",
            })
            continue
        first_seen[rid] = line_no
        records.append({"line_no": line_no, "obj": obj, "model": model})

    if len(records) > MAX_RECORDS:
        errors.append({
            "line_no": 0,
            "reason": f"record count {len(records)} exceeds limit {MAX_RECORDS}",
        })
    if errors:
        raise IngestError(
            409,
            "invalid_ndjson",
            "one or more lines failed validation; no records were committed",
            {"error_count": len(errors), "errors": errors[:50]},
        )
    return records


def complete_upload(upload_id: str) -> dict:
    conn = db.connect()
    try:
        with db.transaction(conn):
            row = _upload_row(conn, upload_id)
            if row["status"] == "completed":
                # Idempotent completion: concurrent/lost-response retry returns
                # the very same batch, never inserts a second one.
                batch = conn.execute(
                    "SELECT * FROM batches WHERE batch_id = ?", (row["batch_id"],)
                ).fetchone()
                return {
                    "batch_id": batch["batch_id"],
                    "record_count": batch["record_count"],
                    "sha256": batch["sha256"],
                    "total_bytes": batch["total_bytes"],
                    "created_at": batch["created_at"],
                }

            total = row["total_bytes"]
            chunks = conn.execute(
                "SELECT start_offset, end_offset, data FROM chunks WHERE upload_id = ?",
                (upload_id,),
            ).fetchall()
            ranges = [(c["start_offset"], c["end_offset"]) for c in chunks]
            gaps = _missing(total, ranges)
            if gaps:
                raise IngestError(
                    409,
                    "missing_bytes",
                    "upload still has byte gaps; completion refused",
                    {"missing_ranges": [{"start": s, "end": e} for s, e in gaps]},
                )

            buffer = bytearray(total)
            for c in chunks:
                buffer[c["start_offset"]:c["end_offset"]] = c["data"]
            blob = bytes(buffer)

            actual_sha = hashlib.sha256(blob).hexdigest()
            if actual_sha != row["sha256"].lower():
                raise IngestError(
                    409,
                    "digest_mismatch",
                    "SHA-256 of the assembled file does not match the digest "
                    "fixed at upload creation; no records were committed",
                    {"expected": row["sha256"].lower(), "actual": actual_sha},
                )

            records = _parse_ndjson(blob)

            batch_id = uuid.uuid4().hex
            created_at = _now()
            conn.execute(
                "INSERT INTO batches (batch_id, upload_id, record_count, total_bytes, sha256, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (batch_id, upload_id, len(records), total, actual_sha, created_at),
            )
            conn.executemany(
                "INSERT INTO batch_records (batch_id, line_no, record_id, local_time, timezone, payload) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        batch_id,
                        rec["line_no"],
                        rec["model"].id,
                        rec["model"].local_time.isoformat(),
                        rec["model"].timezone,
                        json.dumps(rec["obj"], ensure_ascii=False, separators=(",", ":")),
                    )
                    for rec in records
                ],
            )
            conn.execute(
                "UPDATE uploads SET status = 'completed', completed_at = ?, batch_id = ? "
                "WHERE upload_id = ?",
                (created_at, batch_id, upload_id),
            )
            return {
                "batch_id": batch_id,
                "record_count": len(records),
                "sha256": actual_sha,
                "total_bytes": total,
                "created_at": created_at,
            }
    finally:
        conn.close()


def list_records(batch_id: str, page: int, page_size: int) -> dict:
    conn = db.connect()
    try:
        batch = conn.execute(
            "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if batch is None:
            raise IngestError(404, "batch_not_found", f"batch {batch_id!r} does not exist")
        total_records = batch["record_count"]
        rows = conn.execute(
            "SELECT line_no, record_id, local_time, timezone, payload "
            "FROM batch_records WHERE batch_id = ? ORDER BY line_no ASC LIMIT ? OFFSET ?",
            (batch_id, page_size, (page - 1) * page_size),
        ).fetchall()
        records = []
        for r in rows:
            payload = json.loads(r["payload"])
            records.append({
                "line_no": r["line_no"],
                "id": r["record_id"],
                "local_time": r["local_time"],
                "timezone": r["timezone"],
                "payload": payload,
            })
        offset = page * page_size
        next_page = page + 1 if offset < total_records else None
        return {
            "batch_id": batch_id,
            "page": page,
            "page_size": page_size,
            "total_records": total_records,
            "next_page": next_page,
            "records": records,
        }
    finally:
        conn.close()


def list_batches(page: int, page_size: int) -> dict:
    conn = db.connect()
    try:
        total = conn.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"]
        rows = conn.execute(
            "SELECT batch_id, upload_id, record_count, total_bytes, sha256, created_at "
            "FROM batches ORDER BY created_at ASC, batch_id ASC LIMIT ? OFFSET ?",
            (page_size, (page - 1) * page_size),
        ).fetchall()
        offset = page * page_size
        return {
            "page": page,
            "page_size": page_size,
            "next_page": page + 1 if offset < total else None,
            "batches": [dict(r) for r in rows],
        }
    finally:
        conn.close()
