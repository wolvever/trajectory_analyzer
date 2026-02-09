from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from .operators import Batch, Operator


class BuildTurnsAndErrors(Operator):
    outputs = ("turns", "errors")

    def transform(self, ctx, batch: Batch):
        tbl = batch.arrow()
        if tbl.num_rows == 0:
            return {"turns": pa.table({}), "errors": pa.table({})}

        d = tbl.to_pydict()
        turns_rows = {}
        error_rows = []
        turn_counters = {}

        for i in range(tbl.num_rows):
            sid = d.get("session_id", [None])[i]
            if sid is None:
                continue
            ev = d.get("event_type", [None])[i]
            explicit = d.get("turn_index", [None])[i]
            if explicit is not None:
                tidx = explicit
                turn_counters[sid] = int(tidx)
            else:
                cur = turn_counters.get(sid, 0)
                if ev == "turn_start":
                    cur += 1
                    turn_counters[sid] = cur
                tidx = turn_counters.get(sid)
            if tidx is None:
                continue
            key = (d["dt"][i], d["app_id"][i], sid, tidx)
            cur = turns_rows.setdefault(
                key,
                {
                    "dt": d["dt"][i],
                    "app_id": d["app_id"][i],
                    "session_id": sid,
                    "turn_index": tidx,
                    "react_iters": 0,
                    "error_count": 0,
                },
            )
            if d.get("event_type", [None])[i] == "llm_request":
                cur["react_iters"] += 1
            if d.get("event_type", [None])[i] == "error":
                cur["error_count"] += 1
                error_rows.append(
                    {
                        "dt": d["dt"][i],
                        "app_id": d["app_id"][i],
                        "session_id": sid,
                        "turn_index": tidx,
                        "error_type": d.get("error_type", [None])[i],
                        "error_code": d.get("error_code", [None])[i],
                    }
                )

        turns_tbl = pa.Table.from_pylist(list(turns_rows.values())) if turns_rows else pa.table({})
        errors_tbl = pa.Table.from_pylist(error_rows) if error_rows else pa.table({})

        if turns_tbl.num_rows > 0:
            turns_tbl = turns_tbl.append_column(
                "status",
                pc.if_else(pc.greater(turns_tbl["error_count"], 0), pa.scalar("fail"), pa.scalar("success")),
            )
        return {"turns": turns_tbl, "errors": errors_tbl}


class BuildSessions(Operator):
    outputs = ("sessions",)

    def transform(self, ctx, batch: Batch):
        d = batch.arrow().to_pydict()
        rows = {}
        for i in range(len(d.get("session_id", []))):
            key = (d["dt"][i], d["app_id"][i], d["session_id"][i])
            cur = rows.setdefault(
                key,
                {
                    "dt": d["dt"][i],
                    "app_id": d["app_id"][i],
                    "session_id": d["session_id"][i],
                    "turns_count": 0,
                    "error_turns": 0,
                },
            )
            cur["turns_count"] += 1
            if d.get("status", [None])[i] == "fail":
                cur["error_turns"] += 1
        return pa.Table.from_pylist(list(rows.values())) if rows else pa.table({})
