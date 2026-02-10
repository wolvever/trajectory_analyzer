from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Protocol


@dataclass(frozen=True)
class TableSpec:
    name: str
    path: str
    format: str
    schema_version: str
    partition_keys: tuple[str, ...]
    description: str = ""


class Catalog(Protocol):
    def get(self, table: str) -> TableSpec: ...
    def register(self, table: str, spec: TableSpec) -> None: ...
    def list(self) -> list[str]: ...


class DatasetRegistry(Protocol):
    """Structural type expected by engines and analysis plugins."""

    def duckdb_scan_sql(self, table_name: str) -> str: ...
    def ray_read_kwargs(self, table_name: str, filters: Optional[Dict] = None) -> Dict: ...


class InMemoryCatalog:
    def __init__(self, specs: Optional[Iterable[TableSpec]] = None):
        self._tables: Dict[str, TableSpec] = {}
        for spec in specs or []:
            self.register(spec.name, spec)

    def get(self, table: str) -> TableSpec:
        if table not in self._tables:
            raise KeyError(f"Unknown table '{table}'. Registered: {sorted(self._tables)}")
        return self._tables[table]

    def register(self, table: str, spec: TableSpec) -> None:
        self._tables[table] = spec

    def list(self) -> list[str]:
        return sorted(self._tables)


@dataclass(frozen=True)
class ReadFilters:
    dt: Optional[str] = None
    dt_from: Optional[str] = None
    dt_to: Optional[str] = None
    app_id: Optional[str] = None
    session_id: Optional[str] = None


def _iso_dates(start: date, end: date) -> List[str]:
    vals: List[str] = []
    cur = start
    while cur <= end:
        vals.append(cur.isoformat())
        cur += timedelta(days=1)
    return vals


def _parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def resolve_partition_paths(spec: TableSpec, filters: Optional[ReadFilters]) -> List[str]:
    root = Path(spec.path)
    if not filters:
        return [str(root / "**/*.parquet")]

    days: List[str]
    if filters.dt:
        days = [filters.dt]
    elif filters.dt_from and filters.dt_to:
        days = _iso_dates(_parse_day(filters.dt_from), _parse_day(filters.dt_to))
    elif filters.dt_from:
        days = [filters.dt_from]
    elif filters.dt_to:
        days = [filters.dt_to]
    else:
        days = ["*"]

    app = filters.app_id or "*"
    session = filters.session_id if "session_id" in spec.partition_keys else "*"

    out: List[str] = []
    for d in days:
        out.append(str(root / f"dt={d}" / f"app_id={app}" / f"session_id={session}" / "*.parquet"))
    return out


def build_default_catalog(lake_root: str | Path, schema_version: str = "v2") -> InMemoryCatalog:
    root = Path(lake_root)
    specs = [
        TableSpec("raw_events", str(root / "raw/events"), "parquet", schema_version, ("dt", "app_id", "session_id"), "Authoritative event stream."),
        TableSpec("sessions", str(root / "derived/sessions"), "parquet", schema_version, ("dt", "app_id"), "Per-session stats."),
        TableSpec("turns", str(root / "derived/turns"), "parquet", schema_version, ("dt", "app_id"), "Per-turn stats."),
        TableSpec("turn_features", str(root / "derived/turn_features"), "parquet", schema_version, ("dt", "app_id"), "Wide turn feature table."),
        TableSpec("errors", str(root / "derived/errors"), "parquet", schema_version, ("dt", "app_id", "error_type"), "Normalized errors."),
        TableSpec("eval_runs", str(root / "derived/eval_runs"), "parquet", schema_version, ("dt", "benchmark", "model"), "Benchmark outcomes."),
    ]
    return InMemoryCatalog(specs)
