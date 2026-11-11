"""HTTP routes for resumable NDJSON upload (断点入库).

Typical carrier retransmit flow:

1. ``POST   /uploads``                       fix total bytes + SHA-256;
2. ``PUT    /uploads/{id}/chunks``           byte-range chunks, any order,
   safe to retransmit (identical overlap) or reject (different overlap);
3. ``GET    /uploads/{id}`` / ``/missing``   observe remaining byte gaps;
4. ``POST   /uploads/{id}/complete``         gap + digest + record checks,
   then one atomic SQLite commit; retried calls return the same batch;
5. ``GET    /batches`` / ``/batches/{id}/records``  paged downstream reads.
"""

from __future__ import annotations

from fastapi import APIRouter, Body, Query, Request

from . import ingest
from .ingest_schemas import (
    ChunkAck,
    CompletionResult,
    CreateUploadRequest,
    IngestError,
    MissingRanges,
    PagedBatches,
    PagedRecords,
    UploadStatus,
)

router = APIRouter(tags=["resumable-ingest"])

CREATE_EXAMPLE = {
    "upload_id": "carrier-cx-20261111-0001",
    "total_bytes": 220,
    "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
}


@router.post(
    "/uploads",
    response_model=UploadStatus,
    status_code=201,
    responses={
        409: {"model": IngestError, "description": "Upload id reused with different parameters"},
        422: {"model": IngestError, "description": "Invalid digest or total_bytes"},
    },
    summary="创建断点上传会话（固定文件总字节数与 SHA-256）",
)
def create_upload(
    req: CreateUploadRequest = Body(openapi_examples={"carrier_retransmit": {"value": CREATE_EXAMPLE}}),
) -> UploadStatus:
    return UploadStatus(**ingest.create_upload(req.upload_id, req.total_bytes, req.sha256))


@router.put(
    "/uploads/{upload_id}/chunks",
    response_model=ChunkAck,
    responses={
        404: {"model": IngestError},
        409: {"model": IngestError, "description": "Overlapping bytes differ, or upload completed"},
        416: {"model": IngestError, "description": "Byte range outside the fixed file size"},
        422: {"model": IngestError, "description": "Empty body or declared length mismatch"},
    },
    summary="按字节偏移上传分片（可乱序、可跨行；相同重叠可重传，不同则整片拒绝）",
)
async def put_chunk(
    upload_id: str,
    request: Request,
    start_offset: int = Query(ge=0, description="Inclusive byte offset of this chunk."),
    end_offset: int = Query(gt=0, description="Exclusive byte offset of this chunk."),
) -> ChunkAck:
    body = await request.body()
    ack = ingest.put_chunk(upload_id, start_offset, end_offset, body)
    return ChunkAck(**ack)


@router.get(
    "/uploads/{upload_id}",
    response_model=UploadStatus,
    responses={404: {"model": IngestError}},
    summary="查询上传进度（已确认覆盖的字节数与状态）",
)
def get_upload(upload_id: str) -> UploadStatus:
    return UploadStatus(**ingest.get_upload(upload_id))


@router.get(
    "/uploads/{upload_id}/missing",
    response_model=MissingRanges,
    responses={404: {"model": IngestError}},
    summary="查询缺失字节区间（补传调度依据）",
)
def get_missing(upload_id: str) -> MissingRanges:
    return MissingRanges(**ingest.get_missing_ranges(upload_id))


@router.post(
    "/uploads/{upload_id}/complete",
    response_model=CompletionResult,
    responses={
        404: {"model": IngestError},
        409: {"model": IngestError, "description": "Byte gaps, digest mismatch, or invalid NDJSON records"},
    },
    summary="完成上传：无空洞 + 摘要 + 全部记录校验通过后原子入库（重试幂等）",
)
def complete_upload(upload_id: str) -> CompletionResult:
    return CompletionResult(**ingest.complete_upload(upload_id))


@router.get(
    "/batches",
    response_model=PagedBatches,
    summary="分页列出已入库批次",
)
def list_batches(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
) -> PagedBatches:
    return PagedBatches(**ingest.list_batches(page, page_size))


@router.get(
    "/batches/{batch_id}/records",
    response_model=PagedRecords,
    responses={404: {"model": IngestError}},
    summary="按原行序分页读取批次内的扫描记录",
)
def list_records(
    batch_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
) -> PagedRecords:
    return PagedRecords(**ingest.list_records(batch_id, page, page_size))
