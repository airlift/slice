import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("summarize.py")
SPEC = importlib.util.spec_from_file_location("traditional_summarize", MODULE_PATH)
SUMMARIZE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUMMARIZE)


class TestSummarize(unittest.TestCase):
    def testPairManifestIsStableAndUnique(self):
        pairs = SUMMARIZE.build_pairs()

        self.assertEqual(298, len(pairs))
        self.assertEqual(298, len({pair.identifier for pair in pairs}))
        self.assertEqual(298, len({(pair.java_benchmark, pair.java_parameters) for pair in pairs}))
        self.assertEqual(298, len({pair.native_benchmark for pair in pairs}))

        self.assertNotIn("(?:", SUMMARIZE.exact_filter({pair.native_benchmark for pair in pairs}))

    def testSelectsDisjointCompleteClassShards(self):
        class_names = (
            "BenchmarkRe2Search",
            "BenchmarkRe2SearchNfa",
            "BenchmarkRe2SearchExtra",
            "BenchmarkRe2Parse",
            "BenchmarkRe2FullMatch",
            "BenchmarkRe2Practical",
            "BenchmarkRe2Misc",
        )

        selected_identifiers = []
        for class_name in class_names:
            selected_identifiers.extend(pair.identifier for pair in SUMMARIZE.select_pairs(class_name))

        self.assertEqual(298, len(selected_identifiers))
        self.assertEqual(298, len(set(selected_identifiers)))
        self.assertEqual(
            SUMMARIZE.expected_unpaired_java_keys(),
            SUMMARIZE.select_unpaired_java_keys("BenchmarkRe2SearchExtra"))
        self.assertEqual(set(), SUMMARIZE.select_unpaired_java_keys("BenchmarkRe2Search"))

    def testRejectsUnknownClassShard(self):
        with self.assertRaisesRegex(ValueError, "No paired benchmarks for class"):
            SUMMARIZE.select_pairs("BenchmarkDoesNotExist")

    def testSummarizesCompleteResults(self):
        pairs = SUMMARIZE.build_pairs()
        java_rows = []
        native_before_rows = []
        native_after_rows = []
        for index, pair in enumerate(pairs):
            java_rows.append({
                "benchmark": pair.java_benchmark,
                "params": dict(pair.java_parameters),
                "primaryMetric": {
                    "scoreUnit": "ns/op",
                    "rawData": [[20 + index, 22 + index]],
                },
            })
            for rows, value in ((native_before_rows, 10 + index), (native_after_rows, 10 + index)):
                rows.append({
                    "run_type": "aggregate",
                    "aggregate_name": "median",
                    "run_name": pair.native_benchmark,
                    "time_unit": "ns",
                    "real_time": value,
                })
        for benchmark, parameters in SUMMARIZE.expected_unpaired_java_keys():
            java_rows.append({
                "benchmark": benchmark,
                "params": dict(parameters),
                "primaryMetric": {
                    "scoreUnit": "ns/op",
                    "rawData": [[20, 22]],
                },
            })

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            java_path = directory / "java.json"
            native_before_path = directory / "native-before.json"
            native_after_path = directory / "native-after.json"
            java_path.write_text(json.dumps(java_rows))
            native_before_path.write_text(json.dumps({"benchmarks": native_before_rows}))
            native_after_path.write_text(json.dumps({"benchmarks": native_after_rows}))

            rows = SUMMARIZE.summarize(java_path, native_before_path, native_after_path)
            SUMMARIZE.write_outputs(rows, directory / "output")
            SUMMARIZE.write_unpaired_java(java_path, directory / "output")

            self.assertEqual(298, len(rows))
            self.assertEqual(2.1, rows[0]["slice_native_ratio"])
            self.assertEqual(298, json.loads((directory / "output/summary.json").read_text())["pair_count"])
            self.assertEqual(3, len((directory / "output/unpaired-java.csv").read_text().splitlines()))

    def testRejectsMissingResult(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            java_path = directory / "java.json"
            native_path = directory / "native.json"
            java_path.write_text("[]")
            native_path.write_text('{"benchmarks": []}')

            with self.assertRaisesRegex(ValueError, "Missing traditional benchmark results"):
                SUMMARIZE.summarize(java_path, native_path, native_path)

    def testRejectsUnexpectedResult(self):
        pairs = SUMMARIZE.build_pairs()
        java_rows = []
        native_rows = []
        for pair in pairs:
            java_rows.append({
                "benchmark": pair.java_benchmark,
                "params": dict(pair.java_parameters),
                "primaryMetric": {
                    "scoreUnit": "ns/op",
                    "rawData": [[20]],
                },
            })
            native_rows.append({
                "run_type": "aggregate",
                "aggregate_name": "median",
                "run_name": pair.native_benchmark,
                "time_unit": "ns",
                "real_time": 10,
            })
        for benchmark, parameters in SUMMARIZE.expected_unpaired_java_keys():
            java_rows.append({
                "benchmark": benchmark,
                "params": dict(parameters),
                "primaryMetric": {
                    "scoreUnit": "ns/op",
                    "rawData": [[20]],
                },
            })
        java_rows.append({
            "benchmark": "io.airlift.slice.re2.Unexpected.benchmark",
            "primaryMetric": {
                "scoreUnit": "ns/op",
                "rawData": [[20]],
            },
        })

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            java_path = directory / "java.json"
            native_path = directory / "native.json"
            java_path.write_text(json.dumps(java_rows))
            native_path.write_text(json.dumps({"benchmarks": native_rows}))

            with self.assertRaisesRegex(ValueError, "Unexpected traditional benchmark results"):
                SUMMARIZE.summarize(java_path, native_path, native_path)

    def testMergesCompleteClassShards(self):
        pairs_by_class = {}
        for pair in SUMMARIZE.build_pairs():
            class_name = pair.java_benchmark.rsplit(".", 1)[0].rsplit(".", 1)[1]
            pairs_by_class.setdefault(class_name, []).append(pair)

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            shard_directories = []
            for class_name, pairs in pairs_by_class.items():
                shard_directory = directory / class_name
                shard_directory.mkdir()
                shard_directories.append(shard_directory)
                java_rows = []
                native_rows = []
                for pair in pairs:
                    java_rows.append({
                        "benchmark": pair.java_benchmark,
                        "params": dict(pair.java_parameters),
                        "primaryMetric": {
                            "scoreUnit": "ns/op",
                            "rawData": [[20]],
                        },
                    })
                    native_rows.append({
                        "run_type": "aggregate",
                        "aggregate_name": "median",
                        "run_name": pair.native_benchmark,
                        "time_unit": "ns",
                        "real_time": 10,
                    })
                for benchmark, parameters in SUMMARIZE.select_unpaired_java_keys(class_name):
                    java_rows.append({
                        "benchmark": benchmark,
                        "params": dict(parameters),
                        "primaryMetric": {
                            "scoreUnit": "ns/op",
                            "rawData": [[20]],
                        },
                    })
                (shard_directory / "traditional-java-all.json").write_text(json.dumps(java_rows))
                native_document = json.dumps({"benchmarks": native_rows})
                (shard_directory / "traditional-native-before.json").write_text(native_document)
                (shard_directory / "traditional-native-after.json").write_text(native_document)
                shard_rows = SUMMARIZE.summarize(
                    shard_directory / "traditional-java-all.json",
                    shard_directory / "traditional-native-before.json",
                    shard_directory / "traditional-native-after.json",
                    pairs,
                    SUMMARIZE.select_unpaired_java_keys(class_name))
                self.assertEqual(len(pairs), len(shard_rows))

            rows = SUMMARIZE.merge_shards(shard_directories, directory / "merged")

            self.assertEqual(298, len(rows))
            self.assertEqual(298, json.loads(
                (directory / "merged/traditional-normalized/summary.json").read_text())["pair_count"])
            self.assertEqual(3, len(
                (directory / "merged/traditional-normalized/unpaired-java.csv").read_text().splitlines()))


if __name__ == "__main__":
    unittest.main()
