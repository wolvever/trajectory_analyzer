# Coding-Agent Trajectory Analytics System Design Doc

**Version:** v1.0  
**Scope:** A system to ingest, normalize, store, and analyze coding-agent trajectories with a unified programming model that works on **laptop (DuckDB)** and **distributed compute (Ray)**.

---

## 1. Background and Goals

### 1.1 Business Context

- One **App** has many **Sessions**.
- One **Session** is a continuous thread of work (e.g., a chat/workspace) and contains the full ordered **event log**.
- A **Session** contains multiple **Turns**. Each **Turn** corresponds to one user query cycle: `turn_start → ... ReAct loop ... → turn_end`.
- Within a Turn, the agent performs a ReAct-like loop: *think → action → observation → ...*, with possible interleaved events such as `condense`, `todo_update`, `error`, delegation to sub-agents, retries, etc.

### 1.2 Analytics Requirements

1. **Stats**
   - Total sessions, sessions per app/user
   - Turn distribution per session
   - Mean turns before user encounters errors
   - Error taxonomy and rate by segment/model/toolset/agent version

2. **Performance**
   - Per-model TTFT, latency, OTPS, tokens
   - Average tool execution time, p95/p99, timeout/error rates
   - Breakdown of latency: model time vs tool time vs orchestration time

3. **Effects / Experimentation**
   - SWE score (or any evaluation score) across model variants
   - Same model with improved tools (e.g., `str_replace_editor`, `bash` read accuracy)
   - Impact of context condensation on ReAct iterations / tool loops / time-to-success

### 1.3 Non-Functional Requirements

- **Easy to use**: 80% of analytics with SQL + simple Python API
- **Easy to extend**: new analysis is a small plugin with minimal boilerplate
- **High performance by default**: columnar storage, partition pruning, compaction
- **Easy setup**: laptop mode requires minimal infra
- **Distributed compute**: supports 1M+ trajectories and heavy ETL/eval jobs

---

## 2. Key Concepts and Data Units

### 2.1 Session

A session is the unit of business continuity. It contains all events from session start to end/abandon.

### 2.2 Turn

A turn is the unit of user query fulfillment. It begins at `turn_start` and ends at `turn_end` (or inferred end if missing). A session has multiple turns.

### 2.3 Model Span

A model span is one LLM invocation interval: `llm_request → llm_response_complete`. It is the unit for TTFT/OTPS/tokens/latency analysis.

### 2.4 Tool Call

A tool call is one action execution interval: `tool_call → tool_result`. It is the unit for tool latency, tool failures, patch-apply success, and effect studies about tooling.

### 2.5 Event Log

An append-only ordered event stream is the source of truth. All derived tables come from events.

---

## 3. Architecture Overview

### 3.1 Two-Tier Lake Model

**Tier A: Raw (Authoritative)**

- Stores canonical events losslessly for replay/debug/provenance.
- Partitioning as specified: `dt/app_id/session_id`

**Tier B: Derived (Analytics-Optimized)**

- Materialized tables designed for fast scans/aggregations and common joins.
- Partitioned by `dt/app_id` plus a workload key (e.g., `model`, `tool_name`) to optimize pruning and avoid small-file explosion.

### 3.2 Compute Engines

- **DuckDBEngine**: laptop/single-node OLAP query engine over Parquet
- **RayEngine**: distributed ETL & batch analytics engine for large-scale processing

### 3.3 Programming Model

A single **Analysis Plugin** API runs on either engine:

- SQL-first path for most metrics (DuckDB)
- Ray DataFrame path for heavy transforms or huge datasets

---

## 4. Storage Layout and Partitioning

### 4.1 Raw Events (Tier A)

**Partition:** `dt/app_id/session_id` (required)

```text
lake/raw/events/
  dt=YYYY-MM-DD/
    app_id=APP/
      session_id=SESSION/
        part-0000.parquet
        part-0001.parquet
```

**Notes**

- Raw may contain many small files; compaction can be applied per session/day.
- Raw is not optimized for aggregations; it is optimized for completeness.

### 4.2 Derived Tables (Tier B)

**Goal:** minimize scanned data for common questions, avoid partitioning by session_id.

Recommended partitions:

- `sessions`: `dt/app_id`
- `turns`: `dt/app_id`
- `model_spans`: `dt/app_id/model`
- `tool_calls`: `dt/app_id/tool_name`
- `errors`: `dt/app_id/error_type` (optional)
- `eval_runs`: `dt/benchmark/model` (or `benchmark/model`)

Example:

```text
lake/derived/model_spans/
  dt=YYYY-MM-DD/
    app_id=APP/
      model=MODEL/
        part-0000.parquet
```

### 4.3 Compaction Policy (Critical)

To prevent small-files problems and ensure consistent scan throughput:

- Target file size: **256MB–1GB** per Parquet file (derived tables)
- Compaction cadence: hourly/daily depending on ingestion volume
- Compaction key: partition folder (e.g., `dt/app_id/model`)

---

## 5. Canonical Schema

### 5.1 Tier A: `raw_events` (Authoritative Event Stream)

#### 5.1.1 `raw_events` Columns (Logical)

```sql
raw_events(
  -- partition keys
  dt DATE,
  app_id STRING,
  session_id STRING,

  -- ordering
  event_id BIGINT,
  ts TIMESTAMP,

  -- structure
  event_type STRING,
  turn_index INT,
  agent_id STRING,
  parent_event_id BIGINT,

  -- extracted dims
  user_id STRING,
  agent_impl STRING,
  agent_version STRING,

  -- LLM metrics (nullable)
  model STRING,
  provider STRING,
  request_id STRING,
  input_tokens INT,
  output_tokens INT,
  cache_tokens INT,
  ttft_ms INT,
  latency_ms INT,

  -- Tool metrics (nullable)
  tool_name STRING,
  tool_latency_ms INT,
  exit_code INT,

  -- Errors (nullable)
  error_type STRING,
  error_code STRING,

  -- payload
  payload JSON
)
```

#### 5.1.2 Event Types and Key Payload Expectations

- `session_start`: user/app metadata, runtime config
- `turn_start`: turn boundary marker; may include inferred query features (query_level, mmu)
- `user_msg`: user content, attachments
- `llm_request`: prompt/messages, tool schema reference, routing info
- `llm_response`: output, tool call intents (if any), streaming markers
- `tool_call`: tool name, args
- `tool_result`: stdout/stderr pointers, diff pointers, result metadata
- `condense`: summary text or pointer; if LLM-based also emits llm_request/response
- `todo_update`: updates to internal plan/todo list
- `error`: categorized error with message, links to related span/tool
- `turn_end`: turn completion marker, status
- `session_end`: session completion marker, status

---

### 5.2 Tier B: Derived Tables (Analytics-First)

#### 5.2.1 `sessions` (1 row per session)

```sql
sessions(
  dt DATE,
  app_id STRING,
  session_id STRING,
  user_id STRING,
  agent_impl STRING,
  agent_version STRING,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  duration_ms BIGINT,
  status STRING,
  success_reason STRING,
  turns_count INT,
  model_spans_count INT,
  tool_calls_count INT,
  total_input_tokens BIGINT,
  total_output_tokens BIGINT,
  total_cache_tokens BIGINT,
  total_cost_usd DOUBLE,
  first_error_turn INT,
  first_error_type STRING
)
```

#### 5.2.2 `turns` (1 row per user query)

```sql
turns(
  dt DATE,
  app_id STRING,
  session_id STRING,
  turn_index INT,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  duration_ms BIGINT,
  user_msg_event_id BIGINT,
  query_level STRING,
  mmu BOOLEAN,
  status STRING,
  finish_event_type STRING,
  react_iters INT,
  model_spans_count INT,
  tool_calls_count INT,
  condense_count INT,
  todo_update_count INT,
  error_count INT,
  input_tokens BIGINT,
  output_tokens BIGINT,
  cache_tokens BIGINT,
  avg_ttft_ms DOUBLE,
  avg_otps DOUBLE
)
```

#### 5.2.3 `model_spans` (1 row per LLM call interval)

```sql
model_spans(
  dt DATE,
  app_id STRING,
  session_id STRING,
  turn_index INT,
  span_id STRING,
  parent_span_id STRING,
  agent_id STRING,
  model STRING,
  provider STRING,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  ttft_ms INT,
  latency_ms INT,
  input_tokens INT,
  output_tokens INT,
  cache_tokens INT,
  otps DOUBLE,
  tool_intents_count INT,
  malformed_tool_call BOOLEAN
)
```

#### 5.2.4 `tool_calls` (1 row per tool execution interval)

```sql
tool_calls(
  dt DATE,
  app_id STRING,
  session_id STRING,
  turn_index INT,
  tool_call_id STRING,
  parent_span_id STRING,
  agent_id STRING,
  tool_name STRING,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  tool_latency_ms INT,
  status STRING,
  exit_code INT,
  error_type STRING,
  patch_applied BOOLEAN,
  patch_reject_reason STRING,
  read_accuracy_score DOUBLE,
  artifacts JSON
)
```

#### 5.2.5 `errors` (optional but recommended)

```sql
errors(
  dt DATE,
  app_id STRING,
  session_id STRING,
  turn_index INT,
  error_event_id BIGINT,
  ts TIMESTAMP,
  error_type STRING,
  error_code STRING,
  message STRING,
  related_tool_call_id STRING,
  related_span_id STRING
)
```

#### 5.2.6 `session_treatments` (experiments/effects)

```sql
session_treatments(
  dt DATE,
  app_id STRING,
  session_id STRING,
  experiment_id STRING,
  variant STRING,
  tags ARRAY<STRING>
)
```

#### 5.2.7 `eval_runs` (benchmark outcomes)

```sql
eval_runs(
  dt DATE,
  benchmark STRING,
  instance_id STRING,
  app_id STRING,
  session_id STRING,
  model STRING,
  agent_version STRING,
  variant STRING,
  pass BOOLEAN,
  score DOUBLE,
  details JSON
)
```

---

## 6. Derivation Rules (Events → Derived Tables)

### 6.1 Turn Boundaries

Primary: explicit `turn_start` and `turn_end`.

If missing `turn_end`, infer closure at the earliest of:

1. next `turn_start`
2. `session_end`
3. last event timestamp + grace window

`turn_index` assignment:

- increment on each `turn_start` per session
- fill `turn_index` on subsequent events until next turn boundary

### 6.2 Model Span Extraction

Pair `llm_request` and `llm_response` via `request_id`:

- `span_id = request_id`
- `start_ts = llm_request.ts`
- `end_ts = llm_response.ts` (or response completion ts)
- `ttft_ms`, `latency_ms`, tokens from extracted fields
- `otps = output_tokens / (latency_ms/1000)`

If missing response:

- emit an `errors` row (`span_incomplete`)
- optionally emit partial span with `status = partial`

### 6.3 Tool Call Extraction

Pair `tool_call` and `tool_result` via `request_id`:

- `tool_call_id = request_id`
- `start_ts = tool_call.ts`
- `end_ts = tool_result.ts`
- `tool_latency_ms` from extracted or derived

Link to parent model span:

- if tool call originates from a model response, store `parent_span_id`
- if runtime policy triggers tool call, `parent_span_id` nullable

### 6.4 ReAct Iteration Counting (`react_iters`)

Recommended operational definition:

- Count of “decision cycles” in a turn. One practical rule:
  - `react_iters = number of model_spans in the turn`, or
  - refined: count of model spans that occur after a tool_result or user_msg within the turn

Pick one definition and keep it consistent; store both if needed:

- `react_iters_model_span_based`
- `react_iters_action_based`

### 6.5 Error Taxonomy

Normalize `error_type` into a fixed set:

- `tool_error` (nonzero exit, patch reject, timeout)
- `model_error` (malformed tool call, invalid args)
- `runtime_error` (OOM, infra, sandbox down)
- `user_error` (missing deps, ambiguous constraints)
- `unknown`

---

## 7. Compute Engine Design

### 7.1 Engine Responsibility

An **Engine** provides execution primitives over derived Parquet tables:

- register/access tables
- filter/join/aggregate
- materialize results

Engines do **not** define storage schema, and do **not** own ingestion. They run analysis and ETL.

### 7.2 Engine Types

#### 7.2.1 DuckDBEngine (Laptop / Single Node)

Best for:

- Ad-hoc exploration
- Most group-by metrics over derived tables
- Low operational cost

Core capabilities:

- Register derived datasets as DuckDB views over Parquet globs
- Execute SQL and return pandas dataframes

#### 7.2.2 RayEngine (Distributed)

Best for:

- Building derived tables from raw events
- Large-scale batch analyses over huge partitions
- CPU-heavy transforms (classification, replay, embeddings, evaluation)

Core capabilities:

- Load Parquet into `ray.data.Dataset`
- map/join/groupby/aggregate at scale
- write Parquet outputs and run compaction pipelines

### 7.3 Engine Selection (Planner)

Default to DuckDB unless:

- scan size exceeds threshold
- plugin declares `requires_distributed = True`
- analysis requires raw event parsing or expensive transforms

A minimal planner can estimate data size using partition manifests.

---

## 8. Modules

### 8.1 Ingestion

**Module:** `adapters/*`

- One adapter per agent implementation (Codex/OpenHands/SWE-agent/your runtime)
- Output canonical `raw_events` rows

**Interface:**

- `parse_source(input_path|stream) -> Iterator[RawEvent]`
- `validate(event) -> bool`

### 8.2 Normalization / Derivation

**Module:** `derivation/*`

- Turn assignment (`turn_index`)
- Span pairing (`model_spans`)
- Tool pairing (`tool_calls`)
- Aggregations (`turns`, `sessions`, `errors`)

**Interface:**

- `build_derived(dt, app_id, input_raw_path, output_derived_paths, options)`

### 8.3 Compaction

**Module:** `compaction/*`

- Merge small Parquet files per partition to target file sizes
- Maintain stats/manifest

### 8.4 Catalog

**Module:** `catalog/*`

- Maps logical table names → physical Parquet globs, schema versions, partition keys
- Enables consistent access from both engines

### 8.5 Analysis Framework

**Module:** `analysis/*`

- Plugin system
- Runner
- Result materialization (tables + artifacts)
- Optional: report templates

---

## 9. Key Interfaces (Reference)

### 9.1 Catalog and Registry

```python
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Literal

EngineType = Literal["duckdb", "ray"]

@dataclass
class TableSpec:
    name: str
    path_glob: str
    schema_version: str
    partition_keys: List[str]

class Catalog:
    def get_table(self, name: str) -> TableSpec: ...

class DatasetRegistry:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def duckdb_scan_sql(self, table_name: str) -> str:
        spec = self.catalog.get_table(table_name)
        return f"SELECT * FROM parquet_scan('{spec.path_glob}')"

    def ray_read(self, table_name: str, filters: Optional[Dict[str, Any]] = None):
        ...
```

### 9.2 Engines

```python
class Engine:
    kind: EngineType

class DuckDBEngine(Engine):
    kind: EngineType = "duckdb"
    def __init__(self, conn):
        self.con = conn

    def register_table(self, table_name: str, registry: DatasetRegistry):
        scan_sql = registry.duckdb_scan_sql(table_name)
        self.con.execute(f"CREATE OR REPLACE VIEW {table_name} AS {scan_sql}")

    def sql(self, query: str, params: Optional[list] = None):
        return self.con.execute(query, params or []).df()

class RayEngine(Engine):
    kind: EngineType = "ray"
    def dataset(self, registry: DatasetRegistry, table_name: str, filters=None):
        return registry.ray_read(table_name, filters=filters)

    def write_parquet(self, ds, out_path: str, partition_cols: Optional[List[str]] = None):
        ...
```

### 9.3 Analysis Plugins

```python
from dataclasses import dataclass
from typing import Dict, Any, List

@dataclass
class AnalysisResult:
    tables: Dict[str, Any]
    artifacts: Dict[str, str]

class AnalysisPlugin:
    name: str
    required_tables: List[str]
    requires_distributed: bool = False

    def run(self, engine: Engine, registry: DatasetRegistry, params: Dict[str, Any]) -> AnalysisResult:
        raise NotImplementedError
```

---

## 10. Usage Examples

### 10.1 Laptop (DuckDB): TTFT/OTPS by Model

```python
import duckdb

catalog = ...
registry = DatasetRegistry(catalog)

con = duckdb.connect()
eng = DuckDBEngine(con)

for t in ["model_spans"]:
    eng.register_table(t, registry)

df = eng.sql("""
SELECT
  model,
  COUNT(*) AS n_calls,
  AVG(ttft_ms) AS avg_ttft_ms,
  AVG(otps) AS avg_otps,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttft_ms) AS p95_ttft_ms
FROM model_spans
WHERE app_id = ?
  AND dt BETWEEN ? AND ?
GROUP BY model
ORDER BY n_calls DESC
""", ["app123", "2026-02-01", "2026-02-08"])

print(df)
```

### 10.2 Laptop (DuckDB): Mean Turns Before First Error by Variant

```python
for t in ["errors", "session_treatments"]:
    eng.register_table(t, registry)

df = eng.sql("""
WITH first_err AS (
  SELECT session_id, MIN(turn_index) AS first_error_turn
  FROM errors
  WHERE app_id = ?
    AND dt BETWEEN ? AND ?
  GROUP BY session_id
)
SELECT
  st.variant,
  AVG(fe.first_error_turn) AS mean_turns_before_error,
  COUNT(*) AS sessions_with_error
FROM first_err fe
JOIN session_treatments st
  ON st.session_id = fe.session_id
WHERE st.app_id = ?
  AND st.dt BETWEEN ? AND ?
GROUP BY st.variant
ORDER BY sessions_with_error DESC
""", ["app123", "2026-02-01", "2026-02-08", "app123", "2026-02-01", "2026-02-08"])

print(df)
```

### 10.3 Distributed (Ray): Build `model_spans` from `raw_events`

```python
import ray
import ray.data as rd

ray.init(address="auto")

raw = rd.read_parquet("lake/raw/events/dt=2026-02-08/app_id=app123/**/part-*.parquet")

req = raw.filter(lambda r: r["event_type"] == "llm_request") \
         .select_columns(["dt","app_id","session_id","turn_index","agent_id","request_id","ts","model","provider","input_tokens","cache_tokens"])

res = raw.filter(lambda r: r["event_type"] == "llm_response") \
         .select_columns(["request_id","ts","ttft_ms","latency_ms","output_tokens"])

spans = req.join(res, on="request_id", how="inner") \
           .map(lambda r: {
               "dt": r["dt"],
               "app_id": r["app_id"],
               "session_id": r["session_id"],
               "turn_index": r["turn_index"],
               "span_id": r["request_id"],
               "agent_id": r["agent_id"],
               "model": r["model"],
               "provider": r["provider"],
               "start_ts": r["ts"],
               "end_ts": r["ts_1"],
               "ttft_ms": r["ttft_ms"],
               "latency_ms": r["latency_ms"],
               "input_tokens": r["input_tokens"],
               "output_tokens": r["output_tokens"],
               "cache_tokens": r["cache_tokens"],
               "otps": (r["output_tokens"] / max(1, r["latency_ms"])) * 1000.0
           })

spans = spans.repartition(128)
spans.write_parquet("lake/derived/model_spans/dt=2026-02-08/app_id=app123/model=gpt-5.2/")
```

### 10.4 Plugin Example: Condense Impact

```python
class CondenseImpact(AnalysisPlugin):
    name = "condense_impact"
    required_tables = ["turns", "session_treatments"]

    def run(self, engine, registry, params):
        if engine.kind == "duckdb":
            df = engine.sql("""
            SELECT
              st.variant,
              AVG(t.react_iters) AS avg_react_iters,
              AVG(t.duration_ms) AS avg_turn_ms,
              AVG(t.input_tokens) AS avg_in_tokens
            FROM turns t
            JOIN session_treatments st
              ON st.session_id = t.session_id
            WHERE t.app_id = ?
              AND t.dt BETWEEN ? AND ?
            GROUP BY st.variant
            ORDER BY avg_react_iters ASC
            """, [params["app_id"], params["dt_from"], params["dt_to"]])
            return AnalysisResult(tables={"condense_impact": df}, artifacts={})

        turns_ds = engine.dataset(registry, "turns", filters={"app_id": params["app_id"]})
        treat_ds = engine.dataset(registry, "session_treatments", filters={"app_id": params["app_id"]})
        joined = turns_ds.join(treat_ds, on="session_id", how="inner")
        return AnalysisResult(tables={"condense_impact": joined}, artifacts={})
```

---

## 11. Operational Considerations

### 11.1 Data Quality Checks

- monotonic ordering per session (`event_id` or `(ts, seq)`)
- request_id pairing completeness (span and tool pairing)
- latency sanity (`latency_ms >= 0`, `ttft_ms <= latency_ms`)
- token sanity (non-negative, within reasonable bounds)
- missing turn_end handling

### 11.2 Schema Evolution

- `schema_version` per dataset
- additive changes preferred
- for breaking changes: write new dataset version and update catalog pointer

### 11.3 Privacy / Security

- raw payload may contain user content or secrets; store blobs separately with access control
- consider redaction pipeline for logs/artifacts before analyst access

---

## 12. Implementation Plan (Incremental)

### Phase 1 (MVP: high leverage)

1. Canonical `raw_events` + 1 adapter (runtime)
2. Derived builders: `model_spans`, `tool_calls`, `turns`, `sessions`, `session_treatments`
3. DuckDBEngine + a small analysis runner
4. 5–10 starter analyses (TTFT/OTPS/tool latency/error distributions)

### Phase 2 (Scale + effects)

1. Ray derivation jobs + compaction
2. `errors` + robust taxonomy classifier
3. `eval_runs` integration (SWE)
4. effect analysis templates (A/B, diff-in-means, bootstrap CI)

### Phase 3 (Advanced)

1. Similarity search (optional vector store)
2. UI: timeline trajectory viewer + dashboards
3. online incremental derived updates (near-real-time partitions)

---

## 13. Summary of the Design Choices

- Use **raw event stream** as the single source of truth.
- Generate **derived tables** as the analysis primitives (`sessions`, `turns`, `model_spans`, `tool_calls`).
- Maintain **two engines**:
  - DuckDB for easy, local SQL analytics
  - Ray for distributed ETL and heavy jobs
- Standardize analysis via **plugins** so new metrics are easy to add.
- Preserve raw partitioning `dt/app_id/session_id`, but optimize derived tables for analytics and avoid `session_id` partitions.

## Appendix Candidate Additions

- Explicit per-`event_type` payload schema (tool args/results, LLM messages, condensed context structures)
- Minimal manifest system to estimate scan size and auto-select DuckDB vs Ray
- Recommended “golden queries” to validate derived-table correctness (pairing integrity, boundary integrity, counts reconciliation)
