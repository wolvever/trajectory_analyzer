from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from .base import AnalysisPlugin, AnalysisResult


@dataclass
class PlannerConfig:
    distributed_scan_threshold_bytes: int = 20 * 1024 * 1024 * 1024


class AnalysisRunner:
    def __init__(self, duckdb_engine: Any, ray_engine: Any, registry: Any, config: PlannerConfig | None = None):
        self.duckdb_engine = duckdb_engine
        self.ray_engine = ray_engine
        self.registry = registry
        self.config = config or PlannerConfig()

    def choose_engine(self, plugin: AnalysisPlugin, params: Dict[str, Any]) -> Any:
        if plugin.requires_distributed:
            return self.ray_engine
        estimated_scan = int(params.get("estimated_scan_bytes", 0))
        if estimated_scan >= self.config.distributed_scan_threshold_bytes:
            return self.ray_engine
        return self.duckdb_engine

    def run(self, plugin: AnalysisPlugin, params: Dict[str, Any]) -> AnalysisResult:
        engine = self.choose_engine(plugin, params)
        return plugin.run(engine=engine, registry=self.registry, params=params)
