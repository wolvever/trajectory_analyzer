# Ray-Backbone Trajectory Analytics System Design

**Version:** v2.0

This design uses **Ray Data as the execution backbone**, **Parquet + Arrow** as the storage/interchange layer, and an operator model where each operator receives a generic `Batch` and can use Arrow / Polars / DuckDB internally per batch.

## 1) Architecture

### 1.1 Components

- **Storage**
  - Tier A (Raw): authoritative append-only events in Parquet.
  - Required partition: `dt/app_id/session_id`.
  - Tier B (Derived): optional analysis tables with per-table partitions for performance.
  - Artifacts (stdout/stderr/diff/screenshots) are stored out-of-band and referenced by path.
- **Compute**
  - Ray Data for distributed read/transform/join/aggregate/write.
  - In-batch accelerators: Arrow compute, Polars, DuckDB.
- **Orchestration**
  - Operator DAG runner via `Context.apply`.
  - Catalog maps logical tables to path/schema/partitions.
- **UX**
  - Minimal API: `ctx.read()`, `ctx.apply()`, `ctx.write()`.

### 1.2 Dataflow

1. Read partition-pruned Parquet from lake.
2. Apply one or more operators.
3. Route operator outputs (single or multi-output).
4. Write derived tables.
5. Run downstream analyses from raw or derived tables.

## 2) Core data model

### 2.1 Tier A raw events

Each row is one event with partition keys `dt, app_id, session_id` plus ordering keys (`event_id` and/or `ts, seq`) and semantic fields:

- `event_type`, `turn_index`, `agent_id`, `request_id`
- optional metrics: `ttft_ms`, `latency_ms`, `input_tokens`, `output_tokens`, `cache_tokens`, `tool_latency_ms`
- error fields: `error_type`, `error_code`
- `payload` and artifact pointers

### 2.2 Tier B derived tables

Typical tables:

- `sessions` (`dt/app_id`)
- `turns` (`dt/app_id`)
- `turn_features` (`dt/app_id`)
- `errors` (`dt/app_id/error_type`)
- `eval_runs` (`dt/benchmark/model`)

## 3) Compute abstraction

### 3.1 Operator interface

```python
class Operator:
    outputs: tuple[str, ...] = ("out",)

    def __init__(self, batch_size: int | None = None):
        self.batch_size = batch_size

    def transform(self, ctx: "Context", batch: "Batch"):
        raise NotImplementedError
```

### 3.2 Generic Batch wrapper

```python
@dataclass(frozen=True)
class DuckBatch:
    con: "duckdb.DuckDBPyConnection"
    arrow: pa.Table

class Batch:
    def arrow(self) -> pa.Table: ...
    def polars(self) -> "polars.DataFrame": ...
    def duckdb(self) -> DuckBatch: ...
```

### 3.3 Output normalization

- single output table allowed when `len(outputs)==1`
- dict output must match declared outputs exactly
- all outputs normalized to Arrow tables for Ray transport

## 4) Modules

### A) Catalog and table registry

`TableSpec`: name/path/format/schema version/partition keys.

`Catalog` operations: `get`, `register`, `list`.

Partition pruning filters:

- `dt` or `dt_from/dt_to`
- `app_id`
- `session_id` (raw only)

### B) Context (IO + execution runtime)

```python
class Context:
    def read(self, table: str, *, filters=None, columns=None) -> ray.data.Dataset: ...
    def write(self, ds: ray.data.Dataset, table: str, *, path: str, partition_by=None, mode="append") -> None: ...
    def apply(self, op: Operator, ds: ray.data.Dataset) -> dict[str, ray.data.Dataset]: ...
```

Worker cache for local accelerators:

```python
class WorkerRuntime:
    def duckdb_con(self): ...
```

### C) Apply execution semantics

- **Single-output**: `map_batches` and return one dataset.
- **Multi-output (scalable)**: staging fan-out to parquet paths like `.../_staging/{run_id}/{op}/{output}/`, then read each output path back as dataset.

## 5) Why this design

- scales to large trajectory corpora with Ray
- keeps operators small and composable
- allows local vectorized SQL/dataframe logic inside distributed execution
- supports wide/multi-table derivations with robust output routing
- reproducible via Parquet outputs + catalog metadata
