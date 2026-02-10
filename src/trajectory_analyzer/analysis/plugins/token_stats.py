from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from ..base import AnalysisResult


@dataclass
class TokenStats:
    """Compute token consumption statistics from AgentOS evaluation data.

    Required tables (passed via *params*):
      - ``raw_events`` – canonical event Arrow table
      - ``conversations`` – per-conversation metadata Arrow table
    """

    name: str = "token_stats"
    required_tables: List[str] = None  # type: ignore[assignment]
    requires_distributed: bool = False

    def __post_init__(self) -> None:
        if self.required_tables is None:
            self.required_tables = ["raw_events", "conversations"]

    def run(self, engine: Any, registry: Any, params: Dict[str, Any]) -> AnalysisResult:
        import duckdb

        conn = duckdb.connect()
        conn.register("raw_events", params["raw_events"])
        conn.register("conversations", params["conversations"])

        # Per-conversation token summary (from conversation metadata)
        token_summary = conn.execute(
            """
            SELECT
                app_id,
                session_id,
                llm_model,
                total_tokens,
                prompt_tokens,
                completion_tokens,
                accumulated_cost
            FROM conversations
            ORDER BY app_id
            """
        ).df()

        # Distribution stats across conversations
        token_distribution = conn.execute(
            """
            SELECT
                MIN(total_tokens)    AS min_total_tokens,
                QUANTILE_CONT(total_tokens, 0.25) AS q1_total_tokens,
                MEDIAN(total_tokens) AS median_total_tokens,
                AVG(total_tokens)    AS mean_total_tokens,
                QUANTILE_CONT(total_tokens, 0.75) AS q3_total_tokens,
                MAX(total_tokens)    AS max_total_tokens,
                MIN(prompt_tokens)    AS min_prompt_tokens,
                MEDIAN(prompt_tokens) AS median_prompt_tokens,
                AVG(prompt_tokens)    AS mean_prompt_tokens,
                MAX(prompt_tokens)    AS max_prompt_tokens,
                MIN(completion_tokens)    AS min_completion_tokens,
                MEDIAN(completion_tokens) AS median_completion_tokens,
                AVG(completion_tokens)    AS mean_completion_tokens,
                MAX(completion_tokens)    AS max_completion_tokens,
                MIN(accumulated_cost)    AS min_cost,
                MEDIAN(accumulated_cost) AS median_cost,
                AVG(accumulated_cost)    AS mean_cost,
                MAX(accumulated_cost)    AS max_cost
            FROM conversations
            """
        ).df()

        # Per-model breakdown
        model_breakdown = conn.execute(
            """
            SELECT
                llm_model,
                COUNT(*)                AS conversation_count,
                SUM(total_tokens)       AS total_tokens,
                SUM(prompt_tokens)      AS total_prompt_tokens,
                SUM(completion_tokens)  AS total_completion_tokens,
                SUM(accumulated_cost)   AS total_cost,
                AVG(total_tokens)       AS avg_tokens_per_conv,
                AVG(accumulated_cost)   AS avg_cost_per_conv
            FROM conversations
            GROUP BY llm_model
            ORDER BY total_tokens DESC
            """
        ).df()

        conn.close()

        return AnalysisResult(
            tables={
                "token_summary": token_summary,
                "token_distribution": token_distribution,
                "model_breakdown": model_breakdown,
            },
            artifacts={},
        )
