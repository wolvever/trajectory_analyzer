from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from trajectory_analyzer.adapters import OpenHandsAdapter, load_events_as_arrow


class AdapterTests(unittest.TestCase):
    def _make_app_dir(self, base: Path, app_id: str, conv_id: str, events: list):
        """Create a minimal app-*/conv-*/events.json directory structure."""
        app_dir = base / app_id
        conv_dir = app_dir / conv_id
        conv_dir.mkdir(parents=True)

        with (conv_dir / "events.json").open("w", encoding="utf-8") as f:
            json.dump(events, f)

    def test_load_events_arrow(self):
        events = [
            {
                "event_id": 1,
                "session_id": "conv-001",
                "content": {
                    "timestamp": "2026-02-08T10:00:00.000000",
                    "source": "user",
                    "action": "message",
                },
                "ext": {"action": "message"},
            },
            {
                "event_id": 2,
                "session_id": "conv-001",
                "content": {
                    "timestamp": "2026-02-08T10:00:01.000000",
                    "source": "agent",
                    "action": "run",
                    "llm_metrics": {
                        "accumulated_cost": 0.5,
                        "accumulated_token_usage": {
                            "prompt_tokens": 100,
                            "completion_tokens": 20,
                        },
                    },
                },
                "ext": {"action": "run"},
            },
        ]

        with tempfile.TemporaryDirectory() as d:
            self._make_app_dir(Path(d), "app-test", "conv-001", events)

            tbl = load_events_as_arrow(d)
            self.assertEqual(tbl.num_rows, 2)
            self.assertIn("event_type", tbl.column_names)
            self.assertIn("input_tokens", tbl.column_names)

            # Check event_type mapping
            types = tbl.column("event_type").to_pylist()
            self.assertEqual(types, ["message", "tool_call"])

            # Check token extraction
            input_toks = tbl.column("input_tokens").to_pylist()
            self.assertEqual(input_toks, [None, 100])

    def test_adapter_yields_all_canonical_columns(self):
        events = [
            {
                "event_id": 1,
                "session_id": "conv-001",
                "content": {
                    "timestamp": "2026-02-08T10:00:00.000000",
                    "source": "agent",
                    "action": "think",
                },
                "ext": {},
            },
        ]

        with tempfile.TemporaryDirectory() as d:
            self._make_app_dir(Path(d), "app-test", "conv-001", events)

            adapter = OpenHandsAdapter()
            rows = list(adapter.load(d))
            self.assertEqual(len(rows), 1)
            # Every canonical column must be present
            self.assertEqual(len(rows[0]), 24)


if __name__ == "__main__":
    unittest.main()
