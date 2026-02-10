from __future__ import annotations

import json
from pathlib import Path

import ray.data as rd

from trajectory_analyzer import BuildSessions, BuildTurnsAndErrors, Context, build_default_catalog
from trajectory_analyzer.adapters import load_events_as_arrow

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT.parent / "data" / "evaluation"


def main() -> None:
    catalog = build_default_catalog(ROOT / "lake")
    ctx = Context(catalog)

    raw_tbl = load_events_as_arrow(DATA_DIR)
    ds = rd.from_arrow(raw_tbl)

    out1 = ctx.apply(BuildTurnsAndErrors(batch_size=1024), ds)
    turns = out1["turns"]
    errors = out1["errors"]
    sessions = ctx.apply(BuildSessions(batch_size=1024), turns)["sessions"]

    summary = {
        "turns_rows": turns.count(),
        "errors_rows": errors.count(),
        "sessions_rows": sessions.count(),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
