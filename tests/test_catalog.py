from __future__ import annotations

import unittest

from trajectory_analyzer.catalog import Catalog, DatasetRegistry


class CatalogTests(unittest.TestCase):
    def test_default_catalog_and_registry(self):
        catalog = Catalog.from_lake_root("lake")
        spec = catalog.get_table("model_spans")
        self.assertIn("derived/model_spans", spec.path_glob)
        registry = DatasetRegistry(catalog)
        sql = registry.duckdb_scan_sql("model_spans")
        self.assertIn("parquet_scan", sql)
        self.assertIn("hive_partitioning=true", sql)


if __name__ == "__main__":
    unittest.main()
