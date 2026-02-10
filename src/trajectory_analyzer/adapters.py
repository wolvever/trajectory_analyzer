"""Adapter that normalises OpenHands event data into a canonical flat schema.

Reads the native OpenHands event format (nested JSON with ``content.action``,
``content.llm_metrics``, ``ext.*`` metadata) as produced by AgentOS and the
OpenHands runtime.  Every event is flattened into the same superset of columns
so downstream operators and plugins see a uniform Arrow schema.

Canonical columns
-----------------
Partitioning & identity:
    dt              – date string (YYYY-MM-DD) extracted from the timestamp
    app_id          – application / project identifier
    session_id      – conversation or session identifier
    event_id        – sequential event number within the session
    ts              – full ISO-8601 timestamp

Classification:
    event_type      – canonical event kind (tool_call, message, delegate, …)
    source          – emitter: "user", "agent", or "environment"
    turn_index      – turn counter within a session

LLM / model info:
    agent_id        – agent or sub-agent name
    request_id      – LLM request / response correlation ID
    model           – LLM model identifier (e.g. "anthropic/claude-sonnet-4-20250514")
    provider        – LLM provider name (e.g. "openai")

Token counts:
    input_tokens    – prompt / input token count
    output_tokens   – completion / output token count
    cache_tokens    – cache-read token count

Latency:
    ttft_ms         – time-to-first-token in milliseconds
    latency_ms      – total request latency in milliseconds

Tool execution:
    tool_name       – name of the invoked tool / action
    tool_latency_ms – tool execution wall-clock time in milliseconds
    exit_code       – process exit code (for command-execution tools)

Errors:
    error_type      – error classification string
    error_code      – error code string or number

Cost:
    accumulated_cost – running accumulated cost from LLM metrics

Raw data:
    payload         – the complete original event serialised as JSON
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List

import pyarrow as pa

# ── Canonical column list (defines the order & full set of keys) ─────────

CANONICAL_COLUMNS: tuple[str, ...] = (
    "dt",
    "app_id",
    "session_id",
    "event_id",
    "ts",
    "event_type",
    "source",
    "turn_index",
    "agent_id",
    "request_id",
    "model",
    "provider",
    "input_tokens",
    "output_tokens",
    "cache_tokens",
    "ttft_ms",
    "latency_ms",
    "tool_name",
    "tool_latency_ms",
    "exit_code",
    "error_type",
    "error_code",
    "accumulated_cost",
    "payload",
)


def _empty_row() -> Dict[str, Any]:
    """Return a dict with every canonical column set to ``None``."""
    return {c: None for c in CANONICAL_COLUMNS}


# ── OpenHands action → canonical event_type mapping ──────────────────────

_ACTION_MAP: Dict[str, str] = {
    "run": "tool_call",
    "read": "tool_call",
    "write": "tool_call",
    "edit": "tool_call",
    "call_tool_mcp": "tool_call",
    "mcp": "tool_call",
    "delegate": "delegate",
    "message": "message",
    "think": "think",
    "finish": "finish",
    "agent_state_changed": "agent_state_changed",
}

# Actions that populate the tool_name field
_TOOL_ACTIONS = {"run", "read", "write", "edit", "call_tool_mcp", "mcp"}


# ── Adapter ──────────────────────────────────────────────────────────────


@dataclass
class OpenHandsAdapter:
    """Parse OpenHands event directories (``app-*/conv-*/events.json``).

    Walks ``data_dir/app-*/conv-*/`` and reads each ``events.json``,
    normalising every event into the canonical flat column schema.
    Also reads ``conversations.json`` per app to attach ``llm_model``
    metadata to every event in that conversation.
    """

    model_override: str | None = None
    _conv_meta: Dict[str, Dict[str, Any]] = field(default_factory=dict, repr=False)

    # ── helpers ───────────────────────────────────────────────────────

    def _load_conversation_meta(self, app_dir: Path) -> None:
        conv_file = app_dir / "conversations.json"
        if not conv_file.exists():
            return
        with conv_file.open("r", encoding="utf-8") as f:
            for c in json.load(f):
                cid = c.get("conversation_id") or c.get("id", "")
                self._conv_meta[cid] = c

    @staticmethod
    def _map_event_type(content: Dict[str, Any]) -> str:
        action = content.get("action")
        if action and action in _ACTION_MAP:
            return _ACTION_MAP[action]
        observation = content.get("observation")
        if observation and observation in _ACTION_MAP:
            return _ACTION_MAP[observation]
        if observation:
            return observation
        if action:
            return action
        return "unknown"

    # ── public API ────────────────────────────────────────────────────

    def load(self, data_dir: str | Path) -> Iterator[Dict[str, Any]]:
        """Walk ``data_dir/app-*/conv-*/events.json`` and yield canonical rows."""
        data_dir = Path(data_dir)

        for app_dir in sorted(data_dir.iterdir()):
            if not app_dir.is_dir() or not app_dir.name.startswith("app-"):
                continue

            app_id = app_dir.name
            self._load_conversation_meta(app_dir)

            for conv_dir in sorted(app_dir.iterdir()):
                if not conv_dir.is_dir() or not conv_dir.name.startswith("conv-"):
                    continue

                events_file = conv_dir / "events.json"
                if not events_file.exists():
                    continue

                session_id = conv_dir.name
                conv = self._conv_meta.get(session_id, {})
                model = self.model_override or conv.get("llm_model")

                with events_file.open("r", encoding="utf-8") as f:
                    events = json.load(f)

                for e in events:
                    yield self._parse_event(e, app_id, session_id, model)

    def _parse_event(
        self,
        e: Dict[str, Any],
        app_id: str,
        session_id: str,
        model: str | None,
    ) -> Dict[str, Any]:
        content = e.get("content") or {}
        ext = e.get("ext") or {}
        llm = content.get("llm_metrics") or {}
        tok = llm.get("accumulated_token_usage") or {}

        ts = content.get("timestamp")
        dt = ts[:10] if ts else "1970-01-01"

        action = content.get("action") or ""
        tool_name = action if action in _TOOL_ACTIONS else None

        row = _empty_row()
        row["dt"] = dt
        row["app_id"] = ext.get("miaoda_app_id") or app_id
        row["session_id"] = e.get("session_id") or session_id
        row["event_id"] = int(e.get("event_id", 0))
        row["ts"] = ts
        row["event_type"] = self._map_event_type(content)
        row["source"] = content.get("source") or ext.get("source")
        row["agent_id"] = ext.get("agent_name")
        row["model"] = model
        row["input_tokens"] = tok.get("prompt_tokens")
        row["output_tokens"] = tok.get("completion_tokens")
        row["cache_tokens"] = tok.get("cache_read_tokens")
        row["tool_name"] = tool_name
        row["accumulated_cost"] = llm.get("accumulated_cost")
        row["payload"] = json.dumps(e, ensure_ascii=False)
        return row


# ── Convenience loaders ──────────────────────────────────────────────────


def load_events_as_arrow(data_dir: str | Path) -> pa.Table:
    """Load all OpenHands events from ``data_dir`` into a single Arrow table."""
    adapter = OpenHandsAdapter()
    rows = list(adapter.load(data_dir))
    return pa.Table.from_pylist(rows) if rows else pa.table({})


def load_generation_status(data_dir: str | Path) -> pa.Table:
    """Read ``generation_status.json`` per app → Arrow table.

    Columns: app_id, app_status, app_type, duration_s, react_rounds, first_query
    """
    data_dir = Path(data_dir)
    rows: List[Dict[str, Any]] = []
    for app_dir in sorted(data_dir.iterdir()):
        if not app_dir.is_dir() or not app_dir.name.startswith("app-"):
            continue
        gs_file = app_dir / "generation_status.json"
        if not gs_file.exists():
            continue
        with gs_file.open("r", encoding="utf-8") as f:
            entries = json.load(f)
        for entry in entries:
            total_duration = 0.0
            total_rounds = 0
            for rd in entry.get("react_detail_list") or []:
                total_duration += rd.get("duration") or 0.0
                total_rounds += rd.get("react_rounds") or 0
            rows.append(
                {
                    "app_id": entry.get("app_id") or app_dir.name,
                    "app_status": entry.get("app_status"),
                    "app_type": entry.get("app_type"),
                    "duration_s": total_duration,
                    "react_rounds": total_rounds,
                    "first_query": entry.get("first_query"),
                }
            )
    return pa.Table.from_pylist(rows) if rows else pa.table({})


def load_conversations(data_dir: str | Path) -> pa.Table:
    """Read ``conversations.json`` per app → Arrow table.

    Columns: app_id, session_id, llm_model, total_tokens, prompt_tokens,
             completion_tokens, accumulated_cost, created_at
    """
    data_dir = Path(data_dir)
    rows: List[Dict[str, Any]] = []
    for app_dir in sorted(data_dir.iterdir()):
        if not app_dir.is_dir() or not app_dir.name.startswith("app-"):
            continue
        conv_file = app_dir / "conversations.json"
        if not conv_file.exists():
            continue
        with conv_file.open("r", encoding="utf-8") as f:
            convs = json.load(f)
        for c in convs:
            rows.append(
                {
                    "app_id": (c.get("ext") or {}).get("miaoda_app_id") or app_dir.name,
                    "session_id": c.get("conversation_id") or c.get("id"),
                    "llm_model": c.get("llm_model"),
                    "total_tokens": c.get("total_tokens"),
                    "prompt_tokens": c.get("prompt_tokens"),
                    "completion_tokens": c.get("completion_tokens"),
                    "accumulated_cost": c.get("accumulated_cost"),
                    "created_at": c.get("created_at"),
                }
            )
    return pa.Table.from_pylist(rows) if rows else pa.table({})
