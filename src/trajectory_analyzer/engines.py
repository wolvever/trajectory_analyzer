from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Protocol

from .catalog import DatasetRegistry

EngineType = Literal["duckdb", "ray"]


class Engine(Protocol):
    kind: EngineType


@dataclass
class DuckDBEngine:
    conn: Any
    kind: EngineType = "duckdb"

    def register_table(self, table_name: str, registry: DatasetRegistry) -> None:
        scan_sql = registry.duckdb_scan_sql(table_name)
        self.conn.execute(f"CREATE OR REPLACE VIEW {table_name} AS {scan_sql}")

    def sql(self, query: str, params: Optional[List[Any]] = None):
        return self.conn.execute(query, params or []).df()


@dataclass
class RayEngine:
    kind: EngineType = "ray"

    def dataset(self, registry: DatasetRegistry, table_name: str, filters: Optional[Dict[str, Any]] = None):
        import ray.data as rd

        kwargs = registry.ray_read_kwargs(table_name, filters=filters)
        partition_filter = kwargs.pop("partition_filter", None)
        ds = rd.read_parquet(**kwargs)
        if partition_filter:
            ds = ds.filter(lambda r: all(r.get(k) == v for k, v in partition_filter.items()))
        return ds

    def write_parquet(self, ds: Any, out_path: str, partition_cols: Optional[List[str]] = None) -> None:
        kwargs: Dict[str, Any] = {}
        if partition_cols:
            kwargs["partition_cols"] = partition_cols
        ds.write_parquet(out_path, **kwargs)
