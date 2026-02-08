from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional


@dataclass(frozen=True)
class TableSpec:
    """Logical-to-physical mapping for a dataset table."""

    name: str
    path_glob: str
    schema_version: str
    partition_keys: List[str]


class Catalog:
    """In-memory catalog for dataset table specs."""

    def __init__(self, tables: Iterable[TableSpec]):
        self._tables = {t.name: t for t in tables}

    def get_table(self, name: str) -> TableSpec:
        try:
            return self._tables[name]
        except KeyError as exc:
            raise KeyError(f"Unknown table '{name}'. Known tables: {sorted(self._tables)}") from exc

    @classmethod
    def from_lake_root(cls, lake_root: str | Path, schema_version: str = "v1") -> "Catalog":
        root = Path(lake_root)
        specs = [
            TableSpec("raw_events", str(root / "raw/events/**/*.parquet"), schema_version, ["dt", "app_id", "session_id"]),
            TableSpec("sessions", str(root / "derived/sessions/**/*.parquet"), schema_version, ["dt", "app_id"]),
            TableSpec("turns", str(root / "derived/turns/**/*.parquet"), schema_version, ["dt", "app_id"]),
            TableSpec("model_spans", str(root / "derived/model_spans/**/*.parquet"), schema_version, ["dt", "app_id", "model"]),
            TableSpec("tool_calls", str(root / "derived/tool_calls/**/*.parquet"), schema_version, ["dt", "app_id", "tool_name"]),
            TableSpec("errors", str(root / "derived/errors/**/*.parquet"), schema_version, ["dt", "app_id", "error_type"]),
            TableSpec("session_treatments", str(root / "derived/session_treatments/**/*.parquet"), schema_version, ["dt", "app_id"]),
            TableSpec("eval_runs", str(root / "derived/eval_runs/**/*.parquet"), schema_version, ["dt", "benchmark", "model"]),
        ]
        return cls(specs)


class DatasetRegistry:
    """Helper to produce engine-specific scans with optional partition filters."""

    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def duckdb_scan_sql(self, table_name: str) -> str:
        spec = self.catalog.get_table(table_name)
        escaped = spec.path_glob.replace("'", "''")
        return f"SELECT * FROM parquet_scan('{escaped}', hive_partitioning=true)"

    def ray_read_kwargs(self, table_name: str, filters: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        spec = self.catalog.get_table(table_name)
        kwargs: Dict[str, Any] = {"paths": spec.path_glob}
        if filters:
            kwargs["partition_filter"] = self._as_partition_filter(spec, filters)
        return kwargs

    @staticmethod
    def _as_partition_filter(spec: TableSpec, filters: Mapping[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in filters.items() if k in set(spec.partition_keys)}
