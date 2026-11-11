"""End-to-end tests for the resumable NDJSON ingest API (断点入库).

The functional tests use FastAPI's TestClient against an isolated SQLite file.
Two cases additionally boot a real uvicorn server on 127.0.0.1 in a background
thread, because genuine cross-thread HTTP concurrency and process-style restart
behaviour cannot be reproduced inside the single-threaded TestClient portal.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

# Point the service at an isolated database before importing the app.
DB_PATH = "/tmp/ingest_test.db"
for suffix in ("", "-wal", "-shm"):
    try:
        os.remove(DB_PATH + suffix)
    except FileNotFoundError:
        pass
os.environ["INGEST_DB_PATH"] = DB_PATH

from app.main import app  # noqa: E402


# ---------------------------------------------------------------------------
# helpers and fixtures
# ---------------------------------------------------------------------------


def make_ndjson(records: list[dict]) -> bytes:
    return ("\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n").encode("utf-8")


def rec(rid: str, tz: str = "UTC", local_time: str = "2024-06-01T10:00:00") -> dict:
    return {"id": rid, "local_time": local_time, "timezone": tz}


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def create_upload(client: TestClient, upload_id: str, blob: bytes) -> str:
    digest = hashlib.sha256(blob).hexdigest()
    resp = client.post(
        "/uploads",
        json={"upload_id": upload_id, "total_bytes": len(blob), "sha256": digest},
    )
    assert resp.status_code == 201, resp.text
    return digest


def upload_all_in_chunks(client: TestClient, upload_id: str, blob: bytes, size: int = 37) -> None:
    for start in range(0, len(blob), size):
        end = min(start + size, len(blob))
        resp = client.put(
            f"/uploads/{upload_id}/chunks",
            params={"start_offset": start, "end_offset": end},
            content=blob[start:end],
        )
        assert resp.status_code == 200, resp.text


# ---------------------------------------------------------------------------
# session creation
# ---------------------------------------------------------------------------


def test_create_upload_fixes_size_and_digest(client):
    blob = make_ndjson([rec("A")])
    digest = create_upload(client, "t-create", blob)
    status = client.get("/uploads/t-create").json()
    assert status["total_bytes"] == len(blob)
    assert status["sha256"] == digest
    assert status["status"] == "open"
    assert status["received_bytes"] == 0
    assert status["missing_ranges"] == [{"start": 0, "end": len(blob)}]
    assert status["batch_id"] is None


def test_create_upload_is_idempotent_with_same_contract(client):
    blob = make_ndjson([rec("A")])
    digest = create_upload(client, "t-create-idem", blob)
    again = client.post(
        "/uploads",
        json={"upload_id": "t-create-idem", "total_bytes": len(blob), "sha256": digest},
    )
    assert again.status_code == 201
    assert again.json()["upload_id"] == "t-create-idem"


def test_create_upload_conflict_on_different_contract(client):
    blob = make_ndjson([rec("A")])
    create_upload(client, "t-create-conflict", blob)
    resp = client.post(
        "/uploads",
        json={
            "upload_id": "t-create-conflict",
            "total_bytes": len(blob) + 1,
            "sha256": hashlib.sha256(blob).hexdigest(),
        },
    )
    assert resp.status_code == 409
    assert resp.json()["error"] == "upload_conflict"


def test_invalid_sha256_rejected(client):
    resp = client.post(
        "/uploads",
        json={"upload_id": "t-bad-sha", "total_bytes": 10, "sha256": "not-hex"},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# chunk transfer: ordering, gaps, overlaps, boundaries
# ---------------------------------------------------------------------------


def test_out_of_order_chunks_and_missing_ranges(client):
    blob = make_ndjson([rec("A"), rec("B"), rec("C")])
    create_upload(client, "t-ooo", blob)
    n = len(blob)
    third = n // 3

    # Middle first, then the tail, then the head: fully unordered.
    for start, end in ((third, 2 * third), (2 * third, n), (0, third)):
        resp = client.put(
            "/uploads/t-ooo/chunks",
            params={"start_offset": start, "end_offset": end},
            content=blob[start:end],
        )
        assert resp.status_code == 200, resp.text

    status = client.get("/uploads/t-ooo").json()
    assert status["received_bytes"] == n
    assert status["missing_ranges"] == []


def test_missing_ranges_report_multiple_gaps(client):
    blob = make_ndjson([rec("A"), rec("B")])
    create_upload(client, "t-gaps", blob)
    n = len(blob)
    client.put(
        "/uploads/t-gaps/chunks",
        params={"start_offset": 10, "end_offset": 20},
        content=blob[10:20],
    )
    missing = client.get("/uploads/t-gaps/missing").json()["missing_ranges"]
    assert missing == [{"start": 0, "end": 10}, {"start": 20, "end": n}]


def test_identical_overlap_is_idempotent_retransmit(client):
    blob = make_ndjson([rec("A"), rec("B")])
    create_upload(client, "t-idem-chunk", blob)
    first = client.put(
        "/uploads/t-idem-chunk/chunks",
        params={"start_offset": 0, "end_offset": 30},
        content=blob[:30],
    )
    assert first.json()["stored"] is True

    # Exact same range with identical bytes: accepted, no new row stored.
    again = client.put(
        "/uploads/t-idem-chunk/chunks",
        params={"start_offset": 0, "end_offset": 30},
        content=blob[:30],
    )
    assert again.status_code == 200
    assert again.json()["stored"] is False

    # Wider range whose overlap matches: stored; then a sub-range retransmit
    # fully covered by stored bytes is accepted but not stored again.
    wider = client.put(
        "/uploads/t-idem-chunk/chunks",
        params={"start_offset": 20, "end_offset": 40},
        content=blob[20:40],
    )
    assert wider.status_code == 200 and wider.json()["stored"] is True
    sub = client.put(
        "/uploads/t-idem-chunk/chunks",
        params={"start_offset": 22, "end_offset": 28},
        content=blob[22:28],
    )
    assert sub.status_code == 200 and sub.json()["stored"] is False
    assert client.get("/uploads/t-idem-chunk").json()["received_bytes"] == 40


def test_different_overlap_rejects_whole_chunk_and_keeps_bytes(client):
    blob = make_ndjson([rec("A"), rec("B")])
    create_upload(client, "t-conflict-chunk", blob)
    client.put(
        "/uploads/t-conflict-chunk/chunks",
        params={"start_offset": 0, "end_offset": 30},
        content=blob[:30],
    )
    evil = bytearray(blob[20:50])
    evil[0] = evil[0] ^ 0xFF  # guarantee a differing byte in the overlap
    resp = client.put(
        "/uploads/t-conflict-chunk/chunks",
        params={"start_offset": 20, "end_offset": 50},
        content=bytes(evil),
    )
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"] == "chunk_conflict"
    assert body["details"]["conflict_range"]["start"] == 20

    # Stored content untouched: missing range starts exactly at 30.
    status = client.get("/uploads/t-conflict-chunk").json()
    assert status["received_bytes"] == 30
    assert status["missing_ranges"] == [{"start": 30, "end": len(blob)}]

    # Correct retransmit of the rejected range now succeeds.
    ok = client.put(
        "/uploads/t-conflict-chunk/chunks",
        params={"start_offset": 20, "end_offset": 50},
        content=blob[20:50],
    )
    assert ok.status_code == 200 and ok.json()["stored"] is True


def test_chunk_outside_file_range_returns_416(client):
    blob = make_ndjson([rec("A")])
    create_upload(client, "t-416", blob)
    resp = client.put(
        "/uploads/t-416/chunks",
        params={"start_offset": 0, "end_offset": len(blob) + 5},
        content=b"x" * (len(blob) + 5),
    )
    assert resp.status_code == 416
    assert resp.json()["error"] == "chunk_range_exceeds_file"


def test_declared_length_mismatch_returns_422(client):
    blob = make_ndjson([rec("A")])
    create_upload(client, "t-len", blob)
    resp = client.put(
        "/uploads/t-len/chunks",
        params={"start_offset": 0, "end_offset": 10},
        content=blob[:5],
    )
    assert resp.status_code == 422
    assert resp.json()["error"] == "length_mismatch"


def test_empty_chunk_rejected(client):
    blob = make_ndjson([rec("A")])
    create_upload(client, "t-empty", blob)
    resp = client.put(
        "/uploads/t-empty/chunks",
        params={"start_offset": 0, "end_offset": 0},
        content=b"",
    )
    assert resp.status_code == 422


def test_chunk_on_unknown_upload_404(client):
    resp = client.put(
        "/uploads/ghost/chunks",
        params={"start_offset": 0, "end_offset": 1},
        content=b"x",
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# completion: gaps, digest, NDJSON validation, atomicity, immutability
# ---------------------------------------------------------------------------


def test_complete_with_gaps_refused(client):
    blob = make_ndjson([rec("A"), rec("B")])
    create_upload(client, "t-gap-complete", blob)
    client.put(
        "/uploads/t-gap-complete/chunks",
        params={"start_offset": 0, "end_offset": 10},
        content=blob[:10],
    )
    resp = client.post("/uploads/t-gap-complete/complete")
    assert resp.status_code == 409
    assert resp.json()["error"] == "missing_bytes"
    assert client.get("/uploads/t-gap-complete").json()["status"] == "open"


def test_digest_mismatch_rejects_and_commits_nothing(client):
    good = make_ndjson([rec("A"), rec("B")])
    # Create a session whose declared digest corresponds to *different* content
    # of the exact same length.
    other = bytearray(good)
    other[-2] = other[-2] ^ 0x01
    bad_digest = hashlib.sha256(bytes(other)).hexdigest()
    resp = client.post(
        "/uploads",
        json={"upload_id": "t-digest", "total_bytes": len(good), "sha256": bad_digest},
    )
    assert resp.status_code == 201
    upload_all_in_chunks(client, "t-digest", good)
    resp = client.post("/uploads/t-digest/complete")
    assert resp.status_code == 409
    assert resp.json()["error"] == "digest_mismatch"
    # No batch and no records leaked.
    assert client.get("/batches").json()["batches"] == []


def test_invalid_ndjson_collects_line_errors_and_commits_nothing(client):
    valid = json.dumps(rec("A"))
    bad_tz = json.dumps({"id": "B", "local_time": "2024-06-01T10:00:00", "timezone": "Mars/Olympus"})
    bad_json = '{"id": "C", "local_time":'
    dup = json.dumps(rec("A"))
    blob = (f"{valid}\n{bad_tz}\n{bad_json}\n{dup}\n").encode("utf-8")
    create_upload(client, "t-bad-ndjson", blob)
    upload_all_in_chunks(client, "t-bad-ndjson", blob)
    resp = client.post("/uploads/t-bad-ndjson/complete")
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"] == "invalid_ndjson"
    lines = {e["line_no"] for e in body["details"]["errors"]}
    assert lines == {2, 3, 4}
    assert client.get("/batches").json()["batches"] == []
    # Upload stays open so the carrier can repair and re-complete.
    assert client.get("/uploads/t-bad-ndjson").json()["status"] == "open"


def test_empty_file_rejected_at_completion(client):
    # total_bytes must be positive, so a truly empty file is refused at
    # creation; a blank-line file reaches the NDJSON validator instead.
    blank = b"\n"
    create_upload(client, "t-blank", blank)
    upload_all_in_chunks(client, "t-blank", blank)
    resp = client.post("/uploads/t-blank/complete")
    assert resp.status_code == 409 and resp.json()["error"] == "invalid_ndjson"


def test_blank_lines_including_trailing_are_rejected(client):
    blob = (json.dumps(rec("A")) + "\n\n").encode("utf-8")  # trailing blank line
    create_upload(client, "t-trailing-blank", blob)
    upload_all_in_chunks(client, "t-trailing-blank", blob)
    resp = client.post("/uploads/t-trailing-blank/complete")
    assert resp.status_code == 409
    assert resp.json()["error"] == "invalid_ndjson"
    assert any(e["line_no"] == 2 for e in resp.json()["details"]["errors"])


def test_non_object_json_line_rejected(client):
    blob = b'{"id":"A","local_time":"2024-06-01T10:00:00","timezone":"UTC"}\n[1, 2]\n'
    create_upload(client, "t-array-line", blob)
    upload_all_in_chunks(client, "t-array-line", blob)
    resp = client.post("/uploads/t-array-line/complete")
    assert resp.status_code == 409
    assert any(e["line_no"] == 2 for e in resp.json()["details"]["errors"])


def test_invalid_utf8_rejected_at_completion(client):
    good = make_ndjson([rec("A")])
    blob = good[:-1] + b"\xff"  # illegal continuation byte, same length
    digest = hashlib.sha256(blob).hexdigest()
    r = client.post(
        "/uploads",
        json={"upload_id": "t-bad-utf8", "total_bytes": len(blob), "sha256": digest},
    )
    assert r.status_code == 201
    upload_all_in_chunks(client, "t-bad-utf8", blob)
    resp = client.post("/uploads/t-bad-utf8/complete")
    assert resp.status_code == 409 and resp.json()["error"] == "invalid_ndjson"
    assert client.get("/batches").json()["batches"] == []


def test_successful_completion_commits_batch_atomically(client):
    blob = make_ndjson([rec("A", "America/New_York"), rec("B", "Asia/Shanghai")])
    create_upload(client, "t-ok", blob)
    upload_all_in_chunks(client, "t-ok", blob)
    resp = client.post("/uploads/t-ok/complete")
    assert resp.status_code == 200, resp.text
    result = resp.json()
    assert result["record_count"] == 2

    status = client.get("/uploads/t-ok").json()
    assert status["status"] == "completed"
    assert status["batch_id"] == result["batch_id"]

    records = client.get(f"/batches/{result['batch_id']}/records").json()
    assert [r["id"] for r in records["records"]] == ["A", "B"]  # original line order
    assert records["records"][0]["payload"]["timezone"] == "America/New_York"


def test_completion_retry_returns_same_batch(client):
    blob = make_ndjson([rec("A"), rec("B"), rec("C")])
    create_upload(client, "t-retry", blob)
    upload_all_in_chunks(client, "t-retry", blob)
    first = client.post("/uploads/t-retry/complete").json()
    second = client.post("/uploads/t-retry/complete")
    assert second.status_code == 200
    assert second.json()["batch_id"] == first["batch_id"]
    batches = client.get("/batches").json()["batches"]
    assert [b["upload_id"] for b in batches].count("t-retry") == 1


def test_chunks_immutable_after_completion(client):
    blob = make_ndjson([rec("A")])
    create_upload(client, "t-frozen", blob)
    upload_all_in_chunks(client, "t-frozen", blob)
    client.post("/uploads/t-frozen/complete")
    resp = client.put(
        "/uploads/t-frozen/chunks",
        params={"start_offset": 0, "end_offset": 1},
        content=blob[:1],
    )
    assert resp.status_code == 409
    assert resp.json()["error"] == "completed_immutable"


# ---------------------------------------------------------------------------
# reads
# ---------------------------------------------------------------------------


def test_pagination_preserves_line_order_and_next_page(client):
    records = [rec(f"R{i:03d}") for i in range(5)]
    blob = make_ndjson(records)
    create_upload(client, "t-page", blob)
    upload_all_in_chunks(client, "t-page", blob)
    batch_id = client.post("/uploads/t-page/complete").json()["batch_id"]

    page1 = client.get(f"/batches/{batch_id}/records", params={"page": 1, "page_size": 2}).json()
    assert [r["id"] for r in page1["records"]] == ["R000", "R001"]
    assert page1["next_page"] == 2 and page1["total_records"] == 5

    page3 = client.get(f"/batches/{batch_id}/records", params={"page": 3, "page_size": 2}).json()
    assert [r["id"] for r in page3["records"]] == ["R004"]
    assert page3["next_page"] is None


def test_unknown_batch_404(client):
    assert client.get("/batches/deadbeef/records").status_code == 404


# ---------------------------------------------------------------------------
# multibyte UTF-8 split across chunks (chunks may cross line boundaries)
# ---------------------------------------------------------------------------


def test_utf8_multibyte_char_split_across_chunks(client):
    blob = make_ndjson([rec("货物-001"), rec("货物-002")])
    create_upload(client, "t-utf8", blob)
    # 7-byte chunks guarantee splitting both lines and 3-byte CJK characters.
    for start in range(0, len(blob), 7):
        end = min(start + 7, len(blob))
        resp = client.put(
            "/uploads/t-utf8/chunks",
            params={"start_offset": start, "end_offset": end},
            content=blob[start:end],
        )
        assert resp.status_code == 200, resp.text
    result = client.post("/uploads/t-utf8/complete").json()
    records = client.get(f"/batches/{result['batch_id']}/records").json()["records"]
    assert [r["id"] for r in records] == ["货物-001", "货物-002"]


# ---------------------------------------------------------------------------
# real-server tests: concurrency and restart durability
# ---------------------------------------------------------------------------


class ServerHarness:
    """uvicorn bound to 127.0.0.1 with an ephemeral port, in a daemon thread."""

    def __init__(self):
        self.port = self._free_port()
        config = uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    @staticmethod
    def _free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    def __enter__(self):
        self.thread.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with httpx.Client() as c:
                    if c.get(f"http://127.0.0.1:{self.port}/healthz").status_code == 200:
                        return self
            except httpx.TransportError:
                time.sleep(0.05)
        raise RuntimeError("test server did not start")

    def __exit__(self, *exc):
        self.server.should_exit = True
        self.thread.join(timeout=10)

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"


def test_concurrent_chunk_conflict_serialized_by_write_lock():
    blob = make_ndjson([rec(f"R{i}") for i in range(20)])
    with ServerHarness() as server:
        with httpx.Client() as http:
            digest = hashlib.sha256(blob).hexdigest()
            r = http.post(
                f"{server.base}/uploads",
                json={"upload_id": "conc-conflict", "total_bytes": len(blob), "sha256": digest},
            )
            assert r.status_code == 201

            # Both workers claim the same overlapping range with different
            # bytes. At most one may win; the loser must be rejected and the
            # confirmed bytes must remain internally consistent.
            payload_a = blob[:40]
            payload_b = bytearray(payload_a)
            payload_b[30] ^= 0xFF

            def send(body):
                return http.put(
                    f"{server.base}/uploads/conc-conflict/chunks",
                    params={"start_offset": 0, "end_offset": 40},
                    content=bytes(body),
                )

            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(send, (payload_a, payload_b)))
            statuses = sorted(r.status_code for r in results)
            assert statuses == [200, 409]

            status = http.get(f"{server.base}/uploads/conc-conflict").json()
            assert status["received_bytes"] == 40
            # The winning bytes must equal one of the two payloads exactly.
            assert status["missing_ranges"][0]["start"] == 40


def test_concurrent_completion_commits_exactly_one_batch():
    blob = make_ndjson([rec(f"R{i}") for i in range(10)])
    with ServerHarness() as server:
        with httpx.Client() as http:
            digest = hashlib.sha256(blob).hexdigest()
            http.post(
                f"{server.base}/uploads",
                json={"upload_id": "conc-complete", "total_bytes": len(blob), "sha256": digest},
            )
            # Parallel chunk uploaders with disjoint ranges.
            cut = len(blob) // 2

            def upload_range(part):
                start, end = part
                return http.put(
                    f"{server.base}/uploads/conc-complete/chunks",
                    params={"start_offset": start, "end_offset": end},
                    content=blob[start:end],
                )

            with ThreadPoolExecutor(max_workers=2) as pool:
                ok = list(pool.map(upload_range, ((0, cut), (cut, len(blob)))))
            assert all(r.status_code == 200 for r in ok)

            # Simulate a lost completion response: several retries at once.
            with ThreadPoolExecutor(max_workers=4) as pool:
                finals = list(pool.map(
                    lambda _: http.post(f"{server.base}/uploads/conc-complete/complete"),
                    range(4),
                ))
            assert all(r.status_code == 200 for r in finals)
            batch_ids = {r.json()["batch_id"] for r in finals}
            assert len(batch_ids) == 1

            batches = http.get(f"{server.base}/batches").json()["batches"]
            assert [b["upload_id"] for b in batches].count("conc-complete") == 1
            records = http.get(
                f"{server.base}/batches/{batch_ids.pop()}/records",
                params={"page_size": 100},
            ).json()
            assert [r["id"] for r in records["records"]] == [f"R{i}" for i in range(10)]


def test_progress_and_chunks_survive_server_restart():
    blob = make_ndjson([rec(f"R{i}") for i in range(6)])
    digest = hashlib.sha256(blob).hexdigest()
    with ServerHarness() as server:
        with httpx.Client() as http:
            http.post(
                f"{server.base}/uploads",
                json={"upload_id": "restart-1", "total_bytes": len(blob), "sha256": digest},
            )
            cut = 33
            r = http.put(
                f"{server.base}/uploads/restart-1/chunks",
                params={"start_offset": 0, "end_offset": cut},
                content=blob[:cut],
            )
            assert r.status_code == 200
            before = http.get(f"{server.base}/uploads/restart-1/missing").json()

    # Restart: fresh uvicorn instance/thread, same INGEST_DB_PATH on disk.
    with ServerHarness() as server2:
        with httpx.Client() as http:
            after = http.get(f"{server2.base}/uploads/restart-1/missing").json()
            assert after == before
            assert after["received_bytes"] == cut

            # Finish the upload against the restarted service.
            r = http.put(
                f"{server2.base}/uploads/restart-1/chunks",
                params={"start_offset": cut, "end_offset": len(blob)},
                content=blob[cut:],
            )
            assert r.status_code == 200
            done = http.post(f"{server2.base}/uploads/restart-1/complete")
            assert done.status_code == 200
            batch_id = done.json()["batch_id"]

    # And the committed batch is still readable after yet another restart.
    with ServerHarness() as server3:
        with httpx.Client() as http:
            records = http.get(
                f"{server3.base}/batches/{batch_id}/records",
                params={"page_size": 100},
            ).json()
            assert [r["id"] for r in records["records"]] == [f"R{i}" for i in range(6)]
            status = http.get(f"{server3.base}/uploads/restart-1").json()
            assert status["status"] == "completed" and status["batch_id"] == batch_id
