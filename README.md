# trajectory-analyzer (Ray backbone)

This repository implements a **Ray Data backbone** trajectory analytics system with Parquet/Arrow storage and extensible operator-based compute.

## Core architecture

- `catalog.py`: `TableSpec`, catalog registry, and partition-path pruning helpers
- `context.py`: `Context.read / apply / write` over Ray Data
- `operators.py`: minimal operator contract + generic `Batch` wrapper (Arrow/Polars/DuckDB views)
- `derivation_ops.py`: sample derivation operators (`BuildTurnsAndErrors`, `BuildSessions`)
- `adapters.py`: OpenHands JSONL to canonical raw-events Arrow table

## Quickstart

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

For a demo pipeline over fixture trajectories:

```bash
PYTHONPATH=src python scripts/run_ray_backbone_demo.py
```
