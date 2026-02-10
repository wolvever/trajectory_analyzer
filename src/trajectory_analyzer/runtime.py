from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class WorkerRuntime:
    _duckdb_conn: Optional[object] = field(default=None, init=False, repr=False)

    def duckdb_conn(self):
        if self._duckdb_conn is None:
            import duckdb

            self._duckdb_conn = duckdb.connect()
        return self._duckdb_conn
