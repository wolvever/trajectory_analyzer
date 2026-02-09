from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import pandas as pd


@dataclass
class DerivationOptions:
    grace_window_seconds: int = 5


def assign_turn_index(events: pd.DataFrame) -> pd.DataFrame:
    """Assign turn_index by incrementing on each turn_start per session."""

    ordered = events.sort_values(["session_id", "ts", "event_id"], kind="stable").copy()
    starts = (ordered["event_type"] == "turn_start").astype(int)
    ordered["turn_index"] = starts.groupby(ordered["session_id"]).cumsum()
    ordered.loc[ordered["turn_index"] == 0, "turn_index"] = pd.NA
    return ordered


def build_model_spans(raw_events: pd.DataFrame) -> pd.DataFrame:
    req_cols = [
        "dt",
        "app_id",
        "session_id",
        "turn_index",
        "agent_id",
        "request_id",
        "ts",
        "model",
        "provider",
        "input_tokens",
        "cache_tokens",
    ]
    res_cols = ["request_id", "ts", "ttft_ms", "latency_ms", "output_tokens"]

    req = raw_events.loc[raw_events["event_type"] == "llm_request", req_cols]
    res = raw_events.loc[raw_events["event_type"] == "llm_response", res_cols]

    spans = req.merge(res, on="request_id", how="inner", suffixes=("_req", "_res"))
    spans["otps"] = (spans["output_tokens"] * 1000.0) / spans["latency_ms"].clip(lower=1)

    return pd.DataFrame(
        {
            "dt": spans["dt"],
            "app_id": spans["app_id"],
            "session_id": spans["session_id"],
            "turn_index": spans["turn_index"],
            "span_id": spans["request_id"],
            "parent_span_id": pd.NA,
            "agent_id": spans["agent_id"],
            "model": spans["model"],
            "provider": spans["provider"],
            "start_ts": spans["ts_req"],
            "end_ts": spans["ts_res"],
            "ttft_ms": spans["ttft_ms"],
            "latency_ms": spans["latency_ms"],
            "input_tokens": spans["input_tokens"],
            "output_tokens": spans["output_tokens"],
            "cache_tokens": spans["cache_tokens"],
            "otps": spans["otps"],
            "tool_intents_count": pd.NA,
            "malformed_tool_call": False,
        }
    )


def build_tool_calls(raw_events: pd.DataFrame) -> pd.DataFrame:
    call_cols = ["dt", "app_id", "session_id", "turn_index", "agent_id", "request_id", "ts", "tool_name"]
    res_cols = ["request_id", "ts", "tool_latency_ms", "exit_code", "error_type"]

    calls = raw_events.loc[raw_events["event_type"] == "tool_call", call_cols]
    results = raw_events.loc[raw_events["event_type"] == "tool_result", res_cols]

    joined = calls.merge(results, on="request_id", how="inner", suffixes=("_call", "_result"))
    status = joined["exit_code"].fillna(0).apply(lambda c: "ok" if c == 0 else "error")

    return pd.DataFrame(
        {
            "dt": joined["dt"],
            "app_id": joined["app_id"],
            "session_id": joined["session_id"],
            "turn_index": joined["turn_index"],
            "tool_call_id": joined["request_id"],
            "parent_span_id": pd.NA,
            "agent_id": joined["agent_id"],
            "tool_name": joined["tool_name"],
            "start_ts": joined["ts_call"],
            "end_ts": joined["ts_result"],
            "tool_latency_ms": joined["tool_latency_ms"],
            "status": status,
            "exit_code": joined["exit_code"],
            "error_type": joined["error_type"],
            "patch_applied": pd.NA,
            "patch_reject_reason": pd.NA,
            "read_accuracy_score": pd.NA,
            "artifacts": pd.NA,
        }
    )


def build_turns(raw_events: pd.DataFrame) -> pd.DataFrame:
    scoped = raw_events.dropna(subset=["turn_index"]).copy()
    grp = scoped.groupby(["dt", "app_id", "session_id", "turn_index"], dropna=False)

    turns = grp.agg(
        start_ts=("ts", "min"),
        end_ts=("ts", "max"),
        model_spans_count=("event_type", lambda s: int((s == "llm_request").sum())),
        tool_calls_count=("event_type", lambda s: int((s == "tool_call").sum())),
        condense_count=("event_type", lambda s: int((s == "condense").sum())),
        todo_update_count=("event_type", lambda s: int((s == "todo_update").sum())),
        error_count=("event_type", lambda s: int((s == "error").sum())),
        input_tokens=("input_tokens", "sum"),
        output_tokens=("output_tokens", "sum"),
        cache_tokens=("cache_tokens", "sum"),
        avg_ttft_ms=("ttft_ms", "mean"),
    ).reset_index()

    turns["duration_ms"] = (turns["end_ts"] - turns["start_ts"]).dt.total_seconds() * 1000.0
    turns["react_iters"] = turns["model_spans_count"]
    turns["status"] = turns["error_count"].apply(lambda n: "fail" if n > 0 else "success")
    turns["finish_event_type"] = pd.NA
    turns["user_msg_event_id"] = pd.NA
    turns["query_level"] = pd.NA
    turns["mmu"] = pd.NA
    turns["avg_otps"] = pd.NA
    return turns[
        [
            "dt",
            "app_id",
            "session_id",
            "turn_index",
            "start_ts",
            "end_ts",
            "duration_ms",
            "user_msg_event_id",
            "query_level",
            "mmu",
            "status",
            "finish_event_type",
            "react_iters",
            "model_spans_count",
            "tool_calls_count",
            "condense_count",
            "todo_update_count",
            "error_count",
            "input_tokens",
            "output_tokens",
            "cache_tokens",
            "avg_ttft_ms",
            "avg_otps",
        ]
    ]


def build_sessions(turns: pd.DataFrame, raw_events: pd.DataFrame) -> pd.DataFrame:
    grp = turns.groupby(["dt", "app_id", "session_id"], dropna=False)
    sessions = grp.agg(
        start_ts=("start_ts", "min"),
        end_ts=("end_ts", "max"),
        turns_count=("turn_index", "count"),
        model_spans_count=("model_spans_count", "sum"),
        tool_calls_count=("tool_calls_count", "sum"),
        total_input_tokens=("input_tokens", "sum"),
        total_output_tokens=("output_tokens", "sum"),
        total_cache_tokens=("cache_tokens", "sum"),
    ).reset_index()
    sessions["duration_ms"] = (sessions["end_ts"] - sessions["start_ts"]).dt.total_seconds() * 1000.0
    sessions["status"] = "success"
    sessions["success_reason"] = pd.NA
    sessions["total_cost_usd"] = pd.NA

    first_errors = (
        raw_events.loc[raw_events["event_type"] == "error", ["session_id", "turn_index", "error_type"]]
        .sort_values(["session_id", "turn_index"], kind="stable")
        .drop_duplicates(subset=["session_id"], keep="first")
        .rename(columns={"turn_index": "first_error_turn", "error_type": "first_error_type"})
    )
    sessions = sessions.merge(first_errors, on="session_id", how="left")

    meta = (
        raw_events.sort_values(["session_id", "ts", "event_id"], kind="stable")
        .drop_duplicates(subset=["session_id"], keep="first")[["session_id", "user_id", "agent_impl", "agent_version"]]
    )
    sessions = sessions.merge(meta, on="session_id", how="left")

    return sessions[
        [
            "dt",
            "app_id",
            "session_id",
            "user_id",
            "agent_impl",
            "agent_version",
            "start_ts",
            "end_ts",
            "duration_ms",
            "status",
            "success_reason",
            "turns_count",
            "model_spans_count",
            "tool_calls_count",
            "total_input_tokens",
            "total_output_tokens",
            "total_cache_tokens",
            "total_cost_usd",
            "first_error_turn",
            "first_error_type",
        ]
    ]
