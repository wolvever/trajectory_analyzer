from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class WorkerRuntime:
    _duckdb_con: Optional[object] = field(default=None, init=False, repr=False)

    def duckdb_con(self):
        if self._duckdb_con is None:
            import duckdb

            self._duckdb_con = duckdb.connect()
        return self._duckdb_con
