from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from ..base import AnalysisResult


@dataclass
class PerfMetrics:
    """Compute performance metrics from AgentOS evaluation data.

    Required tables (passed via *params*):
      - ``raw_events`` – canonical event Arrow table
      - ``generation_status`` – per-app generation metadata Arrow table
    """

    name: str = "perf_metrics"
    required_tables: List[str] = None  # type: ignore[assignment]
    requires_distributed: bool = False

    def __post_init__(self) -> None:
        if self.required_tables is None:
            self.required_tables = ["raw_events", "generation_status"]

    def run(self, engine: Any, registry: Any, params: Dict[str, Any]) -> AnalysisResult:
        import duckdb

        conn = duckdb.connect()
        conn.register("raw_events", params["raw_events"])
        conn.register("generation_status", params["generation_status"])

        # Duration per app from generation_status
        duration_summary = conn.execute(
            """
            SELECT
                app_id,
                app_status,
                app_type,
                duration_s,
                react_rounds,
                CASE WHEN duration_s > 0
                     THEN ROUND(duration_s / react_rounds, 2)
                     ELSE NULL
                END AS seconds_per_round
            FROM generation_status
            ORDER BY duration_s DESC
            """
        ).df()

        # Action type distribution per app
        action_distribution = conn.execute(
            """
            SELECT
                app_id,
                event_type,
                COUNT(*) AS event_count
            FROM raw_events
            WHERE event_type IS NOT NULL
            GROUP BY app_id, event_type
            ORDER BY app_id, event_count DESC
            """
        ).df()

        # Throughput: tokens per second using generation_status duration
        # and conversation-level token info from the last event per session
        throughput = conn.execute(
            """
            WITH last_event AS (
                SELECT
                    app_id,
                    session_id,
                    MAX(output_tokens) AS max_output_tokens,
                    MAX(input_tokens)  AS max_input_tokens
                FROM raw_events
                WHERE output_tokens IS NOT NULL
                GROUP BY app_id, session_id
            )
            SELECT
                gs.app_id,
                gs.duration_s,
                le.max_output_tokens AS completion_tokens,
                le.max_input_tokens  AS prompt_tokens,
                CASE WHEN gs.duration_s > 0
                     THEN ROUND(le.max_output_tokens / gs.duration_s, 2)
                     ELSE NULL
                END AS output_tokens_per_sec,
                CASE WHEN gs.duration_s > 0
                     THEN ROUND((COALESCE(le.max_output_tokens, 0) + COALESCE(le.max_input_tokens, 0)) / gs.duration_s, 2)
                     ELSE NULL
                END AS total_tokens_per_sec
            FROM generation_status gs
            LEFT JOIN last_event le ON le.app_id = gs.app_id
            ORDER BY gs.app_id
            """
        ).df()

        conn.close()

        return AnalysisResult(
            tables={
                "duration_summary": duration_summary,
                "action_distribution": action_distribution,
                "throughput": throughput,
            },
            artifacts={},
        )
