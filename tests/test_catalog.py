from __future__ import annotations

import unittest

from trajectory_analyzer.catalog import ReadFilters, TableSpec, build_default_catalog, resolve_partition_paths


class CatalogTests(unittest.TestCase):
    def test_partition_resolution_for_raw(self):
        spec = TableSpec(
            name="raw_events",
            path="lake/raw/events",
            format="parquet",
            schema_version="v2",
            partition_keys=("dt", "app_id", "session_id"),
        )
        paths = resolve_partition_paths(
            spec,
            ReadFilters(dt_from="2026-02-01", dt_to="2026-02-03", app_id="app123", session_id="s1"),
        )
        self.assertEqual(len(paths), 3)
        self.assertIn("dt=2026-02-01", paths[0])
        self.assertIn("session_id=s1", paths[0])

    def test_default_catalog_has_turn_features(self):
        cat = build_default_catalog("lake")
        self.assertIn("turn_features", cat.list())


if __name__ == "__main__":
    unittest.main()
