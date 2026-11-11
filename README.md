# Scan Time Normalizer — 航空货运扫描时间归一化 + 断点入库 API

服务提供两部分能力：

1. **`/normalize` 扫描时间归一化**：将承运商导出的**本地扫描时间**按 IANA 时区规则归一化为 **UTC**，
   并在夏令时回拨（本地时间重复）/ 拨快（本地时间不存在）场景下保持转运顺序语义正确；
2. **断点入库（resumable ingest）**：接收承运商断网后补传的 UTF-8 NDJSON 扫描日志，按字节偏移
   分片上传、乱序可重传，完成时核对无空洞、SHA-256 与全部记录后原子写入 SQLite，供后续归一化作业取数。

## 归一化语义

- 每条扫描记录的本地时间对应 **0、1 或 2 个合法 UTC 候选**；
- 航段/转运衔接约束两条记录之间的 UTC 耗时落在闭区间 `[min_seconds, max_seconds]`，
  衔接可共用记录，且**全部衔接必须同时满足**；
- 服务对每条记录返回**能出现在至少一套全局可行解释中的全部候选**：
  - 仅剩一个时标 → `determined`（确定），直接给出 `utc`；
  - 仍有多个 → `needs_review`（待核对），返回全部候选，**不擅自选取**；
- 任一记录无合法候选，或衔接整体无解 → **HTTP 409** 并明确原因，**不返回部分转换**。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

服务启动后：

- 交互式 API 文档（Swagger UI）：<http://localhost:8000/docs>
  —— 可直接演示 `/normalize` 示例与断点补传、分页读取全流程；
- 健康检查：`GET http://localhost:8000/healthz`；
- 归一化接口：`POST http://localhost:8000/normalize`；
- 断点入库：`POST /uploads` → `PUT /uploads/{id}/chunks` → `POST /uploads/{id}/complete`。

SQLite 数据库位于容器内 `/data/ingest.db`，由命名卷 `ingest_data` 持久化，
**容器重启后已确认分片、上传进度与已入库批次保持一致**。

## 断点入库接口

| 方法 & 路径 | 作用 |
| --- | --- |
| `POST /uploads` | 创建上传会话，**固定文件总字节数与 SHA-256**（不可变更） |
| `PUT /uploads/{id}/chunks?start_offset=&end_offset=` | 按字节偏移上传分片，body 为原始字节；可乱序、可跨行/跨字符 |
| `GET /uploads/{id}` | 查询状态：已覆盖字节数、缺失区间、批次号 |
| `GET /uploads/{id}/missing` | 仅返回缺失字节区间，供补传调度 |
| `POST /uploads/{id}/complete` | 完成上传：核对后原子入库；**重试幂等，返回同一批次** |
| `GET /batches` | 分页列出已入库批次 |
| `GET /batches/{batch_id}/records` | **按原行序**分页读取记录 |

### 分片与并发规则

- 分片以 `[start_offset, end_offset)` 字节区间定位，**可乱序到达、可跨 JSON 行与多字节 UTF-8 字符边界**；
- 与已确认分片的重叠**字节完全相同** → 接受且幂等（`stored=false`，不重复落库）；
- 重叠中**任一字节不同** → 整片 **409 拒绝**，已收内容保持不变，错误体给出首个冲突区间；
- 所有写操作在 SQLite `BEGIN IMMEDIATE` 事务内完成"校验 + 写入"，并发上传同样遵守上述规则；
- 完成上传后分片**不可修改**（任何分片写操作返回 409 `completed_immutable`）。

### 完成时的三重核对（任一失败均不暴露部分记录）

1. **无空洞**：全部 `[0, total_bytes)` 均被确认分片覆盖，否则 409 `missing_bytes`；
2. **摘要一致**：拼装后整文件 SHA-256 等于创建时固定值，否则 409 `digest_mismatch`；
3. **记录合法**：UTF-8 可解码、每行是 JSON 对象并通过现有 `ScanRecord` 字段校验
   （朴素整秒本地时间、合法 IANA 时区等），且 **ID 在文件内唯一**；
   错误逐行收集后返回 409 `invalid_ndjson`。

全部通过后，批次与全部记录在**同一个 SQLite 事务**内提交，上传状态同时翻转为 `completed`；
失败时不存在任何可被读取的部分记录，上传仍保持 `open` 可修复后重试。并发完成或完成响应丢失后
重试，只会入库一次并返回同一 `batch_id`（`batches.upload_id` 上有 UNIQUE 约束兜底）。

### 一条命令演示完整补传流程

服务启动后，在仓库根目录执行：

```bash
./demo/resumable_upload_demo.sh
```

脚本会：构造含中文 ID 的 NDJSON → 乱序分片（含跨行/跨字符）→ 相同重叠重传 → 冲突分片被拒且
字节不变 → 查询缺失区间 → 完成并拿到批次 → 重试完成返回同一批次 → 验证分片冻结 → 分页按原行序读回。


## 接口说明

### `POST /normalize`

**请求体**

| 字段 | 说明 |
| --- | --- |
| `records` | 1–200 条扫描记录：`id`（唯一）、`local_time`（精确到秒的朴素本地时间，不得带偏移）、`timezone`（IANA 时区名） |
| `links` | 0–400 条衔接：`from_id` / `to_id`（须为已声明记录）、`min_seconds` / `max_seconds`（非负整数秒，且 min ≤ max） |

**200 响应**：`records[*].status ∈ {determined, needs_review}`；`determined` 时 `utc` 为唯一可行时标，
`needs_review` 时 `utc = null` 且 `candidates` 列出全部仍可行的 UTC 候选（升序）。

**409 响应**（不返回任何部分转换）：

- `no_legal_candidate` — 本地时间在时区规则下不存在（落入拨快跳空），`invalid_records` 列出涉事记录；
- `no_global_solution` — 衔接互相矛盾、整体无解，`conflicting_links` 给出**不可约冲突子集**
  （再删掉任意一条衔接即可恢复有解），`involved_records` 列出涉事记录。

**422 响应**：请求体不合法（未知时区、重复 ID、引用未声明记录、min > max、超过 200/400 上限等）。

### 演示：夏令时回拨消歧

2024-11-03 美国冬令时回拨，纽约 `01:30` 出现两次（05:30Z / 06:30Z）：

```bash
curl -s -X POST http://localhost:8000/normalize \
  -H 'Content-Type: application/json' \
  -d '{
    "records": [
      {"id": "ARR_JFK", "local_time": "2024-11-03T01:30:00", "timezone": "America/New_York"},
      {"id": "DEP_JFK", "local_time": "2024-11-03T02:45:00", "timezone": "America/New_York"},
      {"id": "ARR_ORD", "local_time": "2024-11-03T04:10:00", "timezone": "America/Chicago"},
      {"id": "DEP_LAX", "local_time": "2024-11-03T03:20:00", "timezone": "America/Los_Angeles"},
      {"id": "XFR_ORD", "local_time": "2024-11-03T01:30:00", "timezone": "America/Chicago"}
    ],
    "links": [
      {"from_id": "ARR_JFK", "to_id": "DEP_JFK", "min_seconds": 3600, "max_seconds": 7200},
      {"from_id": "DEP_JFK", "to_id": "ARR_ORD", "min_seconds": 7200, "max_seconds": 10800},
      {"from_id": "ARR_ORD", "to_id": "DEP_LAX", "min_seconds": 1800, "max_seconds": 7200},
      {"from_id": "ARR_JFK", "to_id": "XFR_ORD", "min_seconds": 0, "max_seconds": 86400}
    ]
  }'
```

实际返回（节选）：

```json
{
  "status": "ok",
  "summary": {"total": 5, "determined": 4, "needs_review": 1},
  "records": [
    {"id": "ARR_JFK", "status": "determined", "utc": "2024-11-03T06:30:00Z", "candidates": ["2024-11-03T06:30:00Z"], "...": "..."},
    {"id": "XFR_ORD", "status": "needs_review", "utc": null, "candidates": ["2024-11-03T06:30:00Z", "2024-11-03T07:30:00Z"], "...": "..."}
  ]
}
```

`ARR_JFK` 的两个回拨候选中，`05:30Z` 会使到 `DEP_JFK`（07:45Z）的地面耗时达 2h15m、
超出 1–2h 窗口，被全局约束排除，故唯一确定为 `06:30Z`；`XFR_ORD` 的两个候选都满足全部
衔接，保持 `needs_review`，交由人工核对。

## 实现原理

### 时间归一化（`/normalize`）

1. **候选生成**（`app/solver.py: utc_candidates`）：对本地时间分别以 `fold=0/1` 附着时区后转 UTC，
   再回读校验——回拨重复得到 2 个候选，拨快跳空得到 0 个，常规时间 1 个。
2. **全局可行性 = 2-SAT**：每条含 2 个候选的记录对应一个布尔变量；每条衔接把"越界的候选组合"
   翻译成子句（禁止该组合）。2-SAT 可用蕴含图 + 强连通分量在线性时间内判定。
3. **逐候选过滤**：对每个候选施加单元子句假设后重判可满足性，保留下来的即为
   "能出现在至少一套全局可行解释中"的候选——精确求解，非启发式。
4. **冲突定位**：整体无解时，对衔接做贪心删除得到不可约冲突子集，作为 409 的明确原因。

规模上限（200 记录 / 400 衔接）下，最多约 400 次小规模 2-SAT 判定，响应为毫秒级。

### 断点入库

1. **一张 SQLite 库、四类表**（`app/db.py`，WAL 模式）：`uploads`（会话，固定总字节数与摘要）、
   `chunks`（已确认分片的区间与字节）、`batches`（已入库批次，`upload_id` UNIQUE）、
   `batch_records`（按 `line_no` 保存原行序的记录与原始 JSON）。
2. **写事务串行化**：每个分片写操作走 `BEGIN IMMEDIATE`，在同一事务内完成重叠比对与写入；
   重叠相同则幂等接受，重叠不同则回滚拒绝，已收字节不变——并发下由 SQLite 写锁保证。
3. **进度即区间运算**：对已确认区间做合并后与 `[0, total_bytes)` 求差，得到缺失字节区间；
   状态全部由库内分片行推导，重启后天然一致。
4. **完成 = 单事务原子提交**（`app/ingest.py: complete_upload`）：无空洞检查 → 按偏移拼装字节 →
   SHA-256 复核 → UTF-8 解码并逐行复用 `ScanRecord` 校验 + 文件内 ID 唯一性检查 →
   插入批次与全部记录并把会话翻转为 `completed`，全部在同一事务内；任一步失败整体回滚。
5. **完成幂等**：已 `completed` 的会话再次调用完成接口，直接返回原 `batch_id`，绝不二次入库；
   分片写操作在完成后一律 409 拒绝。

## 本地开发与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest httpx
export INGEST_DB_PATH=/tmp/ingest.db     # 默认 /data/ingest.db（容器内）
uvicorn app.main:app --reload            # 本地起服务，Swagger: http://127.0.0.1:8000/docs
python -m pytest tests/ -q               # 44 个用例
```

测试覆盖：归一化 19 例（消歧/传播/冲突/校验/上限）+ 断点入库 25 例，其中包含对真实 uvicorn
服务的并发分片冲突、并发完成只入库一次、以及重启后分片进度与已入库批次保持一致的验证。

## 目录结构

```
app/
  main.py            # FastAPI 入口、lifespan 建表、路由挂载、异常映射、Swagger 示例
  schemas.py         # /normalize 请求/响应模型与输入校验
  solver.py          # UTC 候选生成 + 2-SAT 全局可行性 + 不可约冲突子集
  db.py              # SQLite 连接、WAL 配置、建表 DDL、事务封装
  ingest_schemas.py  # 断点入库请求/响应模型
  ingest.py          # 会话/分片/完成/分页读取服务逻辑（重叠规则、三重核对、原子提交）
  ingest_api.py      # 断点入库 HTTP 路由
demo/
  resumable_upload_demo.sh   # 一键演示乱序补传、冲突拒绝、完成重试、分页读取
tests/test_api.py            # /normalize 端到端测试
tests/test_ingest.py         # 断点入库端到端 + 真实服务并发/重启测试
Dockerfile / docker-compose.yml / requirements.txt
```
