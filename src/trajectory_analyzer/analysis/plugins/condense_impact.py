from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from ..base import AnalysisResult


@dataclass
class CondenseImpact:
    name: str = "condense_impact"
    required_tables: List[str] = None  # type: ignore[assignment]
    requires_distributed: bool = False

    def __post_init__(self) -> None:
        if self.required_tables is None:
            self.required_tables = ["turns", "session_treatments"]

    def run(self, engine: Any, registry: Any, params: Dict[str, Any]) -> AnalysisResult:
        if engine.kind == "duckdb":
            for t in self.required_tables:
                engine.register_table(t, registry)
            df = engine.sql(
                """
                SELECT
                  st.variant,
                  AVG(t.react_iters) AS avg_react_iters,
                  AVG(t.duration_ms) AS avg_turn_ms,
                  AVG(t.input_tokens) AS avg_in_tokens
                FROM turns t
                JOIN session_treatments st
                  ON st.session_id = t.session_id
                WHERE t.app_id = ?
                  AND t.dt BETWEEN ? AND ?
                GROUP BY st.variant
                ORDER BY avg_react_iters ASC
                """,
                [params["app_id"], params["dt_from"], params["dt_to"]],
            )
            return AnalysisResult(tables={"condense_impact": df}, artifacts={})

        turns_ds = engine.dataset(registry, "turns", filters={"app_id": params["app_id"]})
        treat_ds = engine.dataset(registry, "session_treatments", filters={"app_id": params["app_id"]})
        joined = turns_ds.join(treat_ds, on="session_id", how="inner")
        return AnalysisResult(tables={"condense_impact": joined}, artifacts={})
