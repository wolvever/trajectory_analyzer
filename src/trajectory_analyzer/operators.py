from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict

import pyarrow as pa


@dataclass(frozen=True)
class DuckBatch:
    conn: object
    arrow: pa.Table


class Batch:
    def __init__(self, arrow_table: pa.Table, runtime: "WorkerRuntime"):
        self._arrow = arrow_table
        self._runtime = runtime

    def arrow(self) -> pa.Table:
        return self._arrow

    def polars(self):
        import polars as pl

        return pl.from_arrow(self._arrow)

    def duckdb(self) -> DuckBatch:
        conn = self._runtime.duckdb_conn()
        conn.register("batch", self._arrow)
        return DuckBatch(conn=conn, arrow=self._arrow)


class Operator:
    outputs: tuple[str, ...] = ("out",)

    def __init__(self, batch_size: int | None = None):
        self.batch_size = batch_size

    def transform(self, ctx: "Context", batch: Batch):
        raise NotImplementedError


def normalize_op_output(out, outputs: tuple[str, ...]) -> Dict[str, pa.Table]:
    if isinstance(out, dict):
        keys = set(out)
        if keys != set(outputs):
            raise ValueError(f"Operator dict outputs {sorted(keys)} do not match expected {sorted(outputs)}")
        return {k: _to_arrow(v) for k, v in out.items()}

    if len(outputs) != 1:
        raise ValueError("Single-table operator output requires exactly one declared output.")
    return {outputs[0]: _to_arrow(out)}


def _to_arrow(value) -> pa.Table:
    if isinstance(value, pa.Table):
        return value
    if hasattr(value, "to_arrow"):
        return value.to_arrow()
    raise TypeError(f"Unsupported operator output type: {type(value)!r}")

if TYPE_CHECKING:
    from .context import Context
    from .runtime import WorkerRuntime
