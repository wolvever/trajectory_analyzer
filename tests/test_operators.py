from __future__ import annotations

import unittest

import pyarrow as pa

from trajectory_analyzer.operators import normalize_op_output


class OperatorOutputTests(unittest.TestCase):
    def test_single_output_normalization(self):
        tbl = pa.table({"a": [1, 2]})
        out = normalize_op_output(tbl, ("out",))
        self.assertEqual(list(out), ["out"])
        self.assertEqual(out["out"].num_rows, 2)

    def test_multi_output_key_mismatch_raises(self):
        with self.assertRaises(ValueError):
            normalize_op_output({"wrong": pa.table({"x": [1]})}, ("turns", "errors"))


if __name__ == "__main__":
    unittest.main()
