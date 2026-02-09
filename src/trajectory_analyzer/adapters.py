from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List

import pyarrow as pa


@dataclass
class OpenHandsRawAdapter:
    app_id: str = "openhands"

    def parse_source(self, input_path: str | Path) -> Iterator[Dict[str, Any]]:
        path = Path(input_path)
        with path.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(e, dict):
                    continue
                yield {
                    "dt": (e.get("timestamp") or e.get("ts") or "1970-01-01")[:10],
                    "app_id": self.app_id,
                    "session_id": str(e.get("session_id") or e.get("conversation_id") or "unknown"),
                    "event_id": int(e.get("event_id") or i),
                    "ts": e.get("timestamp") or e.get("ts"),
                    "event_type": e.get("event_type") or e.get("type") or "unknown",
                    "turn_index": e.get("turn_index"),
                    "agent_id": e.get("agent_id"),
                    "request_id": e.get("request_id"),
                    "model": e.get("model"),
                    "provider": e.get("provider"),
                    "ttft_ms": e.get("ttft_ms"),
                    "latency_ms": e.get("latency_ms"),
                    "input_tokens": e.get("input_tokens"),
                    "output_tokens": e.get("output_tokens"),
                    "cache_tokens": e.get("cache_tokens"),
                    "tool_name": e.get("tool_name"),
                    "tool_latency_ms": e.get("tool_latency_ms"),
                    "exit_code": e.get("exit_code"),
                    "error_type": e.get("error_type"),
                    "error_code": e.get("error_code"),
                    "payload": json.dumps(e),
                }


def load_openhands_as_arrow(paths: Iterable[str | Path], app_id: str = "openhands") -> pa.Table:
    adapter = OpenHandsRawAdapter(app_id=app_id)
    rows: List[Dict[str, Any]] = []
    for p in paths:
        rows.extend(adapter.parse_source(p))
    return pa.Table.from_pylist(rows) if rows else pa.table({})
