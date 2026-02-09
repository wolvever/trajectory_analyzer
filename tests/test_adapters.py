from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from trajectory_analyzer.adapters import load_openhands_as_arrow


class AdapterTests(unittest.TestCase):
    def test_load_openhands_arrow(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "traj.jsonl"
            rows = [
                {"timestamp": "2026-02-08T10:00:00Z", "conversation_id": "s1", "event_type": "turn_start"},
                {"timestamp": "2026-02-08T10:00:01Z", "conversation_id": "s1", "event_type": "llm_request", "request_id": "r1"},
            ]
            with p.open("w", encoding="utf-8") as f:
                f.write("not-json\n")
                for r in rows:
                    f.write(json.dumps(r) + "\n")

            tbl = load_openhands_as_arrow([p])
            self.assertEqual(tbl.num_rows, 2)
            self.assertIn("event_type", tbl.column_names)


if __name__ == "__main__":
    unittest.main()
