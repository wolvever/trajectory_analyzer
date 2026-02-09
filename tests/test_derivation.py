from __future__ import annotations

import unittest

import pandas as pd

from trajectory_analyzer.derivation import assign_turn_index, build_model_spans, build_tool_calls, build_turns


class DerivationTests(unittest.TestCase):
    def setUp(self):
        self.events = pd.DataFrame(
            [
                {
                    "dt": "2026-02-08",
                    "app_id": "app123",
                    "session_id": "s1",
                    "event_id": 1,
                    "ts": pd.Timestamp("2026-02-08T10:00:00Z"),
                    "event_type": "turn_start",
                    "agent_id": "root",
                    "request_id": None,
                    "model": None,
                    "provider": None,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cache_tokens": 0,
                    "ttft_ms": None,
                    "latency_ms": None,
                    "tool_name": None,
                    "tool_latency_ms": None,
                    "exit_code": None,
                    "error_type": None,
                    "user_id": "u1",
                    "agent_impl": "codex",
                    "agent_version": "v1",
                },
                {
                    "dt": "2026-02-08",
                    "app_id": "app123",
                    "session_id": "s1",
                    "event_id": 2,
                    "ts": pd.Timestamp("2026-02-08T10:00:01Z"),
                    "event_type": "llm_request",
                    "agent_id": "root",
                    "request_id": "r1",
                    "model": "gpt-5.2",
                    "provider": "openai",
                    "input_tokens": 100,
                    "output_tokens": 0,
                    "cache_tokens": 5,
                    "ttft_ms": None,
                    "latency_ms": None,
                    "tool_name": None,
                    "tool_latency_ms": None,
                    "exit_code": None,
                    "error_type": None,
                    "user_id": "u1",
                    "agent_impl": "codex",
                    "agent_version": "v1",
                },
                {
                    "dt": "2026-02-08",
                    "app_id": "app123",
                    "session_id": "s1",
                    "event_id": 3,
                    "ts": pd.Timestamp("2026-02-08T10:00:03Z"),
                    "event_type": "llm_response",
                    "agent_id": "root",
                    "request_id": "r1",
                    "model": "gpt-5.2",
                    "provider": "openai",
                    "input_tokens": 0,
                    "output_tokens": 50,
                    "cache_tokens": 0,
                    "ttft_ms": 500,
                    "latency_ms": 2000,
                    "tool_name": None,
                    "tool_latency_ms": None,
                    "exit_code": None,
                    "error_type": None,
                    "user_id": "u1",
                    "agent_impl": "codex",
                    "agent_version": "v1",
                },
                {
                    "dt": "2026-02-08",
                    "app_id": "app123",
                    "session_id": "s1",
                    "event_id": 4,
                    "ts": pd.Timestamp("2026-02-08T10:00:04Z"),
                    "event_type": "tool_call",
                    "agent_id": "root",
                    "request_id": "t1",
                    "model": None,
                    "provider": None,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cache_tokens": 0,
                    "ttft_ms": None,
                    "latency_ms": None,
                    "tool_name": "bash",
                    "tool_latency_ms": None,
                    "exit_code": None,
                    "error_type": None,
                    "user_id": "u1",
                    "agent_impl": "codex",
                    "agent_version": "v1",
                },
                {
                    "dt": "2026-02-08",
                    "app_id": "app123",
                    "session_id": "s1",
                    "event_id": 5,
                    "ts": pd.Timestamp("2026-02-08T10:00:05Z"),
                    "event_type": "tool_result",
                    "agent_id": "root",
                    "request_id": "t1",
                    "model": None,
                    "provider": None,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cache_tokens": 0,
                    "ttft_ms": None,
                    "latency_ms": None,
                    "tool_name": "bash",
                    "tool_latency_ms": 900,
                    "exit_code": 0,
                    "error_type": None,
                    "user_id": "u1",
                    "agent_impl": "codex",
                    "agent_version": "v1",
                },
            ]
        )

    def test_model_span_and_tool_call_derivation(self):
        with_turns = assign_turn_index(self.events)
        spans = build_model_spans(with_turns)
        tools = build_tool_calls(with_turns)
        turns = build_turns(with_turns)

        self.assertEqual(len(spans), 1)
        self.assertEqual(spans.iloc[0]["span_id"], "r1")
        self.assertAlmostEqual(float(spans.iloc[0]["otps"]), 25.0)

        self.assertEqual(len(tools), 1)
        self.assertEqual(tools.iloc[0]["status"], "ok")
        self.assertEqual(int(tools.iloc[0]["tool_latency_ms"]), 900)

        self.assertEqual(len(turns), 1)
        self.assertEqual(int(turns.iloc[0]["model_spans_count"]), 1)
        self.assertEqual(int(turns.iloc[0]["tool_calls_count"]), 1)


if __name__ == "__main__":
    unittest.main()
