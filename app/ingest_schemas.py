"""Request/response models for the resumable NDJSON ingest API."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class CreateUploadRequest(BaseModel):
    """Parameters fixed for the whole lifetime of an upload session."""

    upload_id: str = Field(
        min_length=1,
        max_length=128,
        examples=["carrier-cx-20261111-0001"],
        description="Client-chosen unique id for this upload session.",
    )
    total_bytes: int = Field(
        gt=0,
        examples=[4821],
        description="Total size of the UTF-8 NDJSON file in bytes, fixed at creation.",
    )
    sha256: str = Field(
        min_length=64,
        max_length=64,
        examples=["9f2c7b...e1"],
        description="Lowercase or uppercase hex SHA-256 of the complete file.",
    )


class ByteRange(BaseModel):
    start: int = Field(ge=0, description="Inclusive byte offset of the gap.")
    end: int = Field(gt=0, description="Exclusive byte offset of the gap.")


class ChunkAck(BaseModel):
    start_offset: int
    end_offset: int
    stored: bool = Field(
        description="true when a new row was written; false when the identical "
                    "range was already fully covered (idempotent retransmit).",
    )


class UploadStatus(BaseModel):
    upload_id: str
    total_bytes: int
    sha256: str
    status: Literal["open", "completed"] = Field(
        description="open = still accepting chunks; completed = immutable.",
    )
    received_bytes: int
    missing_ranges: list[ByteRange] = Field(
        description="Contiguous byte ranges not yet covered by confirmed chunks.",
    )
    batch_id: str | None = Field(
        default=None,
        description="Set once completion has atomically committed the batch.",
    )
    created_at: datetime
    completed_at: datetime | None = None


class MissingRanges(BaseModel):
    upload_id: str
    total_bytes: int
    received_bytes: int
    missing_ranges: list[ByteRange]


class CompletionResult(BaseModel):
    batch_id: str
    record_count: int
    sha256: str
    total_bytes: int
    created_at: datetime


class IngestedRecord(BaseModel):
    line_no: int = Field(description="1-based line number in the original NDJSON file.")
    id: str
    local_time: datetime
    timezone: str
    payload: dict = Field(description="The original JSON object of the line, unchanged.")


class PagedRecords(BaseModel):
    batch_id: str
    page: int
    page_size: int
    total_records: int
    next_page: int | None = Field(default=None, description="Null on the last page.")
    records: list[IngestedRecord]


class BatchSummary(BaseModel):
    batch_id: str
    upload_id: str
    record_count: int
    total_bytes: int
    sha256: str
    created_at: datetime


class PagedBatches(BaseModel):
    page: int
    page_size: int
    next_page: int | None = None
    batches: list[BatchSummary]


class IngestError(BaseModel):
    error: str
    message: str
    details: dict = Field(default_factory=dict)
