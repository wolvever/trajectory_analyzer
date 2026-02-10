#!/usr/bin/env python3
"""Run AgentOS evaluation analysis plugins and print results.

Usage::

    cd trajectory_analyzer
    python scripts/run_eval_analysis.py --data-dir ../data/evaluation
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from trajectory_analyzer.adapters import (
    load_conversations,
    load_events_as_arrow,
    load_generation_status,
)
from trajectory_analyzer.analysis.plugins import EvalMetrics, PerfMetrics, TokenStats


def _df_to_serialisable(df):
    """Convert a pandas DataFrame to a JSON-serialisable list of dicts."""
    return json.loads(df.to_json(orient="records", default_handler=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run AgentOS evaluation analysis")
    parser.add_argument(
        "--data-dir",
        type=str,
        default=str(Path(__file__).resolve().parents[2] / "data" / "evaluation"),
        help="Path to the evaluation data directory",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for JSON results (default: <data-dir>/analysis_results)",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir) if args.output_dir else data_dir / "analysis_results"

    # ---- Load data ----------------------------------------------------------
    print(f"Loading data from {data_dir} ...")
    raw_events = load_events_as_arrow(data_dir)
    generation_status = load_generation_status(data_dir)
    conversations = load_conversations(data_dir)

    print(f"  raw_events:        {raw_events.num_rows} rows")
    print(f"  generation_status: {generation_status.num_rows} rows")
    print(f"  conversations:     {conversations.num_rows} rows")
    print()

    params = {
        "raw_events": raw_events,
        "generation_status": generation_status,
        "conversations": conversations,
    }

    all_results = {}

    # ---- Token Stats --------------------------------------------------------
    print("=" * 60)
    print("TOKEN STATS")
    print("=" * 60)
    ts = TokenStats()
    ts_result = ts.run(engine=None, registry=None, params=params)
    all_results["token_stats"] = {}
    for name, df in ts_result.tables.items():
        print(f"\n--- {name} ---")
        print(df.to_string(index=False))
        all_results["token_stats"][name] = _df_to_serialisable(df)
    print()

    # ---- Perf Metrics -------------------------------------------------------
    print("=" * 60)
    print("PERFORMANCE METRICS")
    print("=" * 60)
    pm = PerfMetrics()
    pm_result = pm.run(engine=None, registry=None, params=params)
    all_results["perf_metrics"] = {}
    for name, df in pm_result.tables.items():
        print(f"\n--- {name} ---")
        print(df.to_string(index=False))
        all_results["perf_metrics"][name] = _df_to_serialisable(df)
    print()

    # ---- Eval Metrics -------------------------------------------------------
    print("=" * 60)
    print("EVALUATION METRICS")
    print("=" * 60)
    em = EvalMetrics()
    em_result = em.run(engine=None, registry=None, params=params)
    all_results["eval_metrics"] = {}
    for name, df in em_result.tables.items():
        print(f"\n--- {name} ---")
        print(df.to_string(index=False))
        all_results["eval_metrics"][name] = _df_to_serialisable(df)
    print()

    # ---- Save JSON results --------------------------------------------------
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "analysis_results.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, default=str, ensure_ascii=False)
    print(f"Results saved to {out_path}")


if __name__ == "__main__":
    main()
