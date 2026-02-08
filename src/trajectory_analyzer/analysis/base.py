from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Protocol

from ..catalog import DatasetRegistry
from ..engines import Engine


@dataclass
class AnalysisResult:
    tables: Dict[str, Any]
    artifacts: Dict[str, str]


class AnalysisPlugin(Protocol):
    name: str
    required_tables: List[str]
    requires_distributed: bool

    def run(self, engine: Engine, registry: DatasetRegistry, params: Dict[str, Any]) -> AnalysisResult:
        ...
