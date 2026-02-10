from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from ..base import AnalysisResult


@dataclass
class EvalMetrics:
    """Compute evaluation-level metrics from AgentOS data.

    Required tables (passed via *params*):
      - ``raw_events`` – canonical event Arrow table
      - ``generation_status`` – per-app generation metadata Arrow table
      - ``conversations`` – per-conversation metadata Arrow table
    """

    name: str = "eval_metrics"
    required_tables: List[str] = None  # type: ignore[assignment]
    requires_distributed: bool = False

    def __post_init__(self) -> None:
        if self.required_tables is None:
            self.required_tables = ["raw_events", "generation_status", "conversations"]

    def run(self, engine: Any, registry: Any, params: Dict[str, Any]) -> AnalysisResult:
        import duckdb

        conn = duckdb.connect()
        conn.register("raw_events", params["raw_events"])
        conn.register("generation_status", params["generation_status"])
        conn.register("conversations", params["conversations"])

        # Success rate
        success_summary = conn.execute(
            """
            SELECT
                app_status,
                COUNT(*) AS app_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
            FROM generation_status
            GROUP BY app_status
            ORDER BY app_count DESC
            """
        ).df()

        # ReAct analysis
        react_analysis = conn.execute(
            """
            SELECT
                app_id,
                app_status,
                react_rounds,
                duration_s,
                CASE WHEN react_rounds > 0
                     THEN ROUND(duration_s / react_rounds, 2)
                     ELSE NULL
                END AS seconds_per_round,
                -- stats across all apps
                MIN(react_rounds) OVER () AS min_rounds,
                AVG(react_rounds) OVER () AS avg_rounds,
                MAX(react_rounds) OVER () AS max_rounds
            FROM generation_status
            ORDER BY react_rounds DESC
            """
        ).df()

        # Tool usage from raw events
        tool_usage = conn.execute(
            """
            SELECT
                tool_name,
                COUNT(*) AS call_count,
                COUNT(DISTINCT session_id) AS sessions_used
            FROM raw_events
            WHERE tool_name IS NOT NULL
            GROUP BY tool_name
            ORDER BY call_count DESC
            """
        ).df()

        # Cost efficiency: join conversations with generation_status
        cost_efficiency = conn.execute(
            """
            SELECT
                c.app_id,
                c.session_id,
                c.accumulated_cost,
                c.total_tokens,
                gs.react_rounds,
                gs.duration_s,
                CASE WHEN gs.react_rounds > 0
                     THEN ROUND(c.accumulated_cost / gs.react_rounds, 2)
                     ELSE NULL
                END AS cost_per_round,
                CASE WHEN c.accumulated_cost > 0
                     THEN ROUND(c.total_tokens / c.accumulated_cost, 2)
                     ELSE NULL
                END AS tokens_per_cost_unit
            FROM conversations c
            LEFT JOIN generation_status gs ON gs.app_id = c.app_id
            ORDER BY c.accumulated_cost DESC
            """
        ).df()

        # Agent delegation: extract delegated agent names from payload JSON
        agent_delegation = conn.execute(
            """
            SELECT
                app_id,
                session_id,
                json_extract_string(payload, '$.content.args.agent') AS delegated_agent,
                COUNT(*) AS delegation_count
            FROM raw_events
            WHERE event_type = 'delegate'
              AND json_extract_string(payload, '$.content.args.agent') IS NOT NULL
            GROUP BY app_id, session_id, delegated_agent
            ORDER BY delegation_count DESC
            """
        ).df()

        conn.close()

        return AnalysisResult(
            tables={
                "success_summary": success_summary,
                "react_analysis": react_analysis,
                "tool_usage": tool_usage,
                "cost_efficiency": cost_efficiency,
                "agent_delegation": agent_delegation,
            },
            artifacts={},
        )
