#!/usr/bin/env python3

import unittest

import summarize_jmh


class TestSummarizeJmh(unittest.TestCase):
    def testCombineSliceBracket(self):
        key = ("count", "literalSparse", 1024)
        rows = summarize_jmh.combine_slice_bracket(
            {key: {"time_ns": 80, "allocation_bytes": 100}},
            {key: {"time_ns": 125, "allocation_bytes": 120}})

        self.assertEqual(rows[key]["time_ns"], 100)
        self.assertEqual(rows[key]["allocation_bytes"], 110)
        self.assertEqual(rows[key]["before_time_ns"], 80)
        self.assertEqual(rows[key]["after_time_ns"], 125)

    def testRejectsMismatchedSliceBracket(self):
        with self.assertRaisesRegex(ValueError, "Slice bracket key mismatch"):
            summarize_jmh.combine_slice_bracket(
                {("count", "literalSparse", 1024): {"time_ns": 50, "allocation_bytes": 100}},
                {("count", "literalSparse", 32768): {"time_ns": 100, "allocation_bytes": 80}})

    def testComparison(self):
        key = ("count", "literalSparse", 1024)
        rows = summarize_jmh.compare(
            {key: {"time_ns": 50, "allocation_bytes": 100}},
            {key: {"time_ns": 100, "allocation_bytes": 80}})
        self.assertEqual(rows[0]["slice_to_joni_time_ratio"], 0.5)
        self.assertEqual(rows[0]["allocation_difference_bytes"], 20)

    def testRejectsMismatchedKeys(self):
        with self.assertRaisesRegex(ValueError, "benchmark key mismatch"):
            summarize_jmh.compare(
                {("count", "literalSparse", 1024): {"time_ns": 50, "allocation_bytes": 100}},
                {("count", "literalSparse", 32768): {"time_ns": 100, "allocation_bytes": 80}})


if __name__ == "__main__":
    unittest.main()
