from __future__ import annotations

import uuid
from pathlib import Path
from typing import Dict, Optional

import pyarrow as pa

from .catalog import Catalog, ReadFilters, resolve_partition_paths
from .operators import Batch, Operator, normalize_op_output
from .runtime import WorkerRuntime


class Context:
    def __init__(self, catalog: Catalog, staging_root: str = "./.ray_staging"):
        self.catalog = catalog
        self.staging_root = Path(staging_root)

    def read(self, table: str, *, filters: Optional[ReadFilters] = None, columns=None):
        import ray.data as rd

        spec = self.catalog.get(table)
        paths = resolve_partition_paths(spec, filters)
        return rd.read_parquet(paths, columns=columns)

    def write(self, ds, table: str, *, path: Optional[str] = None, partition_by=None, mode: str = "append") -> None:
        out = Path(path) if path else Path(self.catalog.get(table).path)
        if mode == "overwrite" and out.exists():
            for p in sorted(out.rglob("*"), reverse=True):
                if p.is_file():
                    p.unlink()
                elif p.is_dir():
                    p.rmdir()
            out.rmdir()
        out.mkdir(parents=True, exist_ok=True)
        kwargs = {}
        if partition_by:
            kwargs["partition_cols"] = list(partition_by)
        ds.write_parquet(str(out), **kwargs)

    def apply(self, op: Operator, ds):
        run_id = uuid.uuid4().hex
        runtime = WorkerRuntime()

        def mapper(batch_tbl: pa.Table) -> Dict[str, pa.Table]:
            wrapped = Batch(batch_tbl, runtime=runtime)
            out = op.transform(self, wrapped)
            return normalize_op_output(out, op.outputs)

        if len(op.outputs) == 1:
            out_name = op.outputs[0]

            def single_mapper(tbl: pa.Table) -> pa.Table:
                return mapper(tbl)[out_name]

            out_ds = ds.map_batches(single_mapper, batch_format="pyarrow", batch_size=op.batch_size)
            return {out_name: out_ds}

        # multi-output routing via tagged row fan-out
        def fanout(tbl: pa.Table) -> pa.Table:
            out_map = mapper(tbl)
            rows = []
            for name, out_tbl in out_map.items():
                for row in out_tbl.to_pylist():
                    row["__output__"] = name
                    rows.append(row)
            if not rows:
                return pa.table({"__output__": pa.array([], type=pa.string())})
            return pa.Table.from_pylist(rows)

        mixed = ds.map_batches(fanout, batch_format="pyarrow", batch_size=op.batch_size)
        out = {}
        for name in op.outputs:
            part = mixed.filter(lambda r, n=name: r.get("__output__") == n)
            out[name] = part.drop_columns(["__output__"])
        return out
