# trajectory-analyzer

MVP implementation of the coding-agent trajectory analytics system described in `docs/trajectory_analytics_system_design.md`.

## Implemented modules

- `trajectory_analyzer.catalog`: table specs, catalog, dataset registry
- `trajectory_analyzer.engines`: DuckDB and Ray engine wrappers
- `trajectory_analyzer.derivation`: event-to-derived builders (`model_spans`, `tool_calls`, `turns`, `sessions`)
- `trajectory_analyzer.analysis`: plugin abstractions, planner runner, and `CondenseImpact` example plugin

## Run tests

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```
