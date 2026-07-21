#!/usr/bin/env python3

import csv
import tempfile
import unittest
from pathlib import Path

import summarize


class TestSummarize(unittest.TestCase):
    def test_pairsAndExcludesCallerInput(self):
        rows = [
            row("RE2", total=1_500, input_bytes=1_000, off_heap=100),
            row("Joni", total=1_400, input_bytes=1_000),
        ]
        comparisons = summarize.pair_rows(rows)
        self.assertEqual(len(comparisons), 1)
        self.assertEqual(comparisons[0]["re2_bytes"], 600)
        self.assertEqual(comparisons[0]["joni_bytes"], 400)
        self.assertEqual(comparisons[0]["re2_to_joni_ratio"], 1.5)

    def testReadsNumericFields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "census.csv"
            with path.open("w", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=row("RE2").keys())
                writer.writeheader()
                writer.writerow(row("RE2"))
            rows = summarize.read_rows(path)
            self.assertIsInstance(rows[0]["total_heap_bytes"], int)
            self.assertIsInstance(rows[0]["average_heap_bytes"], float)

    def testRejectsIncompletePair(self):
        with self.assertRaisesRegex(ValueError, "incomplete engine pair"):
            summarize.pair_rows([row("RE2")])


def row(engine, total=500, input_bytes=0, off_heap=0):
    return {
        "engine": engine,
        "workload": "captureSparse",
        "source_length": 1024,
        "lifecycle": "active-matcher",
        "operation": "find-all",
        "root_count": 1,
        "total_heap_bytes": total,
        "average_heap_bytes": float(total),
        "input_heap_bytes": input_bytes,
        "owned_matcher_heap_bytes": 100,
        "off_heap_bytes": off_heap,
        "accounted_dfa_bytes": 0,
        "dfa_resets": 0,
    }


if __name__ == "__main__":
    unittest.main()
