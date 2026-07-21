import csv
import importlib.util
import math
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("summarize.py")
SPEC = importlib.util.spec_from_file_location("rebar_summarize", MODULE_PATH)
summarize = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(summarize)

REBAR_FIELDS = (
    "name",
    "model",
    "rebar_version",
    "engine",
    "engine_version",
    "err",
    "haystack_len",
    "iters",
    "total",
    "median",
    "mad",
    "mean",
    "stddev",
    "min",
    "max",
)


class TestSummarize(unittest.TestCase):
    def testReducesCompleteMeasurements(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            input_path = directory / "raw.csv"
            output_directory = directory / "normalized"
            rows = []
            rows.extend(self.measurements("curated/count-a", "count", "1000", {
                summarize.BUNDLED_ENGINE: "1us",
                summarize.PINNED_PORTABLE_ENGINE: "800ns",
                summarize.PINNED_HOST_BEFORE_ENGINE: "495ns",
                summarize.PINNED_HOST_AFTER_ENGINE: "500ns",
                summarize.SLICE_ENGINE: "1.00us",
                summarize.SLICE_OBJECT_ENGINE: "1.25us",
            }))
            rows.extend(self.measurements("curated/count-b", "count", "1000", {
                summarize.BUNDLED_ENGINE: "2us",
                summarize.PINNED_PORTABLE_ENGINE: "2us",
                summarize.PINNED_HOST_BEFORE_ENGINE: "2ms",
                summarize.PINNED_HOST_AFTER_ENGINE: "2ms",
                summarize.SLICE_ENGINE: "1us",
                summarize.SLICE_OBJECT_ENGINE: "1us",
            }))
            rows.extend(self.measurements("curated/compile", "compile", "", {
                summarize.BUNDLED_ENGINE: "0.001s",
                summarize.PINNED_PORTABLE_ENGINE: "1ms",
                summarize.PINNED_HOST_BEFORE_ENGINE: "1ms",
                summarize.PINNED_HOST_AFTER_ENGINE: "1ms",
                summarize.SLICE_ENGINE: "2.01ms",
                summarize.SLICE_OBJECT_ENGINE: "2ms",
            }))
            self.write_measurements(input_path, rows)

            normalized, models, buckets = summarize.reduce_results(input_path, output_directory)

            self.assertEqual([row["name"] for row in normalized], [
                "curated/compile", "curated/count-a", "curated/count-b"])
            count_a = normalized[1]
            self.assertAlmostEqual(count_a["re2_pinned_host_tuned_bracketed_median_ns"], math.sqrt(495 * 500))
            self.assertAlmostEqual(count_a["slice_to_re2_bundled_ratio"], 1.0)
            self.assertAlmostEqual(count_a["slice_to_slice_object_ratio"], 0.8)
            self.assertAlmostEqual(count_a["native_before_after_drift"], 500 / 495 - 1)
            self.assertEqual(count_a["slice_coefficient_of_variation"], 0)
            self.assertTrue(count_a["material"])

            compile_row = normalized[0]
            self.assertTrue(compile_row["material"])
            self.assertAlmostEqual(compile_row["absolute_deficit_ns"], 1_010_000)

            count_bundled = next(summary for summary in models if
                summary["model"] == "count" and summary["comparator"] == summarize.BUNDLED_ENGINE)
            self.assertEqual(count_bundled["row_count"], 2)
            self.assertAlmostEqual(count_bundled["geometric_mean_ratio"], math.sqrt(0.5))
            self.assertEqual(
                (count_bundled["wins"], count_bundled["parity"], count_bundled["losses"]),
                (1, 1, 0))

            compile_host_bucket = next(bucket for bucket in buckets if
                bucket["model"] == "compile" and
                bucket["comparator"] == summarize.PINNED_HOST_ENGINE and
                bucket["bucket"] == "slower-than-2.00x")
            self.assertEqual(compile_host_bucket["row_count"], 1)
            all_bundled_parity = next(bucket for bucket in buckets if
                bucket["model"] == "all" and
                bucket["comparator"] == summarize.BUNDLED_ENGINE and
                bucket["bucket"] == "within-0.90x-1.10x")
            self.assertEqual(all_bundled_parity["row_count"], 1)
            self.assertEqual(len(buckets), 3 * 3 * 6)
            self.assertEqual(self.read_csv(output_directory / "rows.csv")[0]["name"], "curated/compile")
            self.assertEqual(len(self.read_csv(output_directory / "models.csv")), 6)
            self.assertEqual(len(self.read_csv(output_directory / "ratio-buckets.csv")), 54)
            self.assertEqual(self.read_csv(output_directory / "native-drift.csv"), [])

    def testRequiresEveryNamedComparator(self):
        rows = self.measurements("curated/incomplete", "count", "100", {
            summarize.BUNDLED_ENGINE: "1us",
            summarize.PINNED_PORTABLE_ENGINE: "1us",
            summarize.PINNED_HOST_BEFORE_ENGINE: "1us",
            summarize.PINNED_HOST_AFTER_ENGINE: "1us",
            summarize.SLICE_ENGINE: "1us",
        })
        self.assertReductionFails(rows, "slice/re2-object")

    def testRejectsNativeBracketDriftAboveTwoPercent(self):
        rows = self.complete_measurements(before="100ns", after="102.01ns")
        self.assertReductionFails(rows, "Native before/after drift exceeded 2%")

    def testAcceptsNativeBracketDriftAtTwoPercent(self):
        rows = self.complete_measurements(before="100ns", after="102ns")
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            input_path = directory / "raw.csv"
            self.write_measurements(input_path, rows)
            normalized, _, _ = summarize.reduce_results(input_path, directory / "output")
            self.assertAlmostEqual(normalized[0]["native_before_after_drift"], 0.02)

    def testCanRetainExcessiveNativeDriftForConfirmation(self):
        rows = self.complete_measurements(before="100ns", after="103ns")
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            input_path = directory / "raw.csv"
            self.write_measurements(input_path, rows)
            normalized, _, _ = summarize.reduce_results(
                input_path, directory / "output", allow_native_drift=True)
            self.assertAlmostEqual(normalized[0]["native_before_after_drift"], 0.03)
            drift_rows = self.read_csv(directory / "output" / "native-drift.csv")
            self.assertEqual(drift_rows[0]["name"], "curated/complete")

    def testCanExcludeExcessiveNativeDrift(self):
        rows = self.complete_measurements()
        rows.extend(self.measurements("curated/unstable", "count", "100", {
            summarize.BUNDLED_ENGINE: "100ns",
            summarize.PINNED_PORTABLE_ENGINE: "100ns",
            summarize.PINNED_HOST_BEFORE_ENGINE: "100ns",
            summarize.PINNED_HOST_AFTER_ENGINE: "103ns",
            summarize.SLICE_ENGINE: "100ns",
            summarize.SLICE_OBJECT_ENGINE: "100ns",
        }))
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            input_path = directory / "raw.csv"
            output_directory = directory / "output"
            self.write_measurements(input_path, rows)

            normalized, models, _ = summarize.reduce_results(
                input_path,
                output_directory,
                exclude_native_drift=True)

            self.assertEqual([row["name"] for row in normalized], ["curated/complete"])
            self.assertTrue(all(summary["row_count"] == 1 for summary in models))
            drift_rows = self.read_csv(output_directory / "native-drift.csv")
            self.assertEqual(len(drift_rows), 1)
            self.assertEqual(drift_rows[0]["name"], "curated/unstable")
            self.assertAlmostEqual(float(drift_rows[0]["native_before_after_drift"]), 0.03)

    def testRejectsConflictingNativeDriftPolicies(self):
        rows = self.complete_measurements()
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            input_path = directory / "raw.csv"
            self.write_measurements(input_path, rows)
            with self.assertRaisesRegex(ValueError, "both allowed and excluded"):
                summarize.reduce_results(
                    input_path,
                    directory / "output",
                    allow_native_drift=True,
                    exclude_native_drift=True)

    def testRejectsMalformedMeasurements(self):
        cases = []
        invalid_duration = self.complete_measurements()
        invalid_duration[0]["median"] = "1minute"
        cases.append((invalid_duration, "Invalid median"))
        zero_duration = self.complete_measurements()
        zero_duration[0]["median"] = "0"
        cases.append((zero_duration, "Median must be positive"))
        failed = self.complete_measurements()
        failed[0]["err"] = "wrong result"
        cases.append((failed, "wrong result"))
        duplicate = self.complete_measurements()
        duplicate.append(dict(duplicate[0]))
        cases.append((duplicate, "Duplicate measurement"))
        inconsistent_model = self.complete_measurements()
        inconsistent_model[0]["model"] = "compile"
        cases.append((inconsistent_model, "inconsistent models"))

        for rows, message in cases:
            with self.subTest(message=message):
                self.assertReductionFails(rows, message)

    def testClassifiesRatioBucketBoundaries(self):
        expected = {
            0.899: "faster-than-0.90x",
            0.90: "within-0.90x-1.10x",
            1.10: "within-0.90x-1.10x",
            1.1001: "1.10x-1.25x",
            1.25: "1.10x-1.25x",
            1.50: "1.25x-1.50x",
            2.00: "1.50x-2.00x",
            2.001: "slower-than-2.00x",
        }
        for ratio, bucket in expected.items():
            with self.subTest(ratio=ratio):
                self.assertEqual(summarize.ratio_bucket(ratio), bucket)

    def testValidatesCampaignManifestAndProvenance(self):
        rows, manifest_rows = self.campaign_measurements()
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            input_path = directory / "raw.csv"
            manifest_path = directory / "manifest.csv"
            self.write_measurements(input_path, rows)
            self.write_manifest(manifest_path, manifest_rows)

            normalized, _, _ = summarize.reduce_results(
                input_path, directory / "output", manifest_path)
            self.assertEqual(len(normalized), summarize.EXPECTED_WORKLOAD_COUNT)

    def testValidatesCompileWorkloadWithoutHaystackLength(self):
        rows, manifest_rows = self.campaign_measurements()
        compile_name = rows[0]["name"]
        for row in rows:
            if row["name"] == compile_name:
                row["model"] = "compile"
                row["haystack_len"] = ""
        manifest_rows[0]["model"] = "compile"

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            input_path = directory / "raw.csv"
            manifest_path = directory / "manifest.csv"
            self.write_measurements(input_path, rows)
            self.write_manifest(manifest_path, manifest_rows)

            normalized, _, _ = summarize.reduce_results(
                input_path, directory / "output", manifest_path)
            self.assertEqual(len(normalized), summarize.EXPECTED_WORKLOAD_COUNT)

    def testValidatesFilteredCampaignUsingEngineManifest(self):
        rows, manifest_rows = self.campaign_measurements()
        selected_name = rows[0]["name"]
        selected_rows = [row for row in rows if row["name"] == selected_name]
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            input_path = directory / "raw.csv"
            manifest_path = directory / "manifest.csv"
            engine_manifest_path = directory / "engine-manifest.csv"
            self.write_measurements(input_path, selected_rows)
            self.write_manifest(manifest_path, manifest_rows)
            self.write_engine_manifest(engine_manifest_path, selected_rows)

            normalized, _, _ = summarize.reduce_results(
                input_path,
                directory / "output",
                manifest_path,
                engine_manifest_path)
            self.assertEqual([row["name"] for row in normalized], [selected_name])

    def testRejectsFilteredCampaignEngineVersionDrift(self):
        rows, manifest_rows = self.campaign_measurements()
        selected_name = rows[0]["name"]
        selected_rows = [row for row in rows if row["name"] == selected_name]
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            input_path = directory / "raw.csv"
            manifest_path = directory / "manifest.csv"
            engine_manifest_path = directory / "engine-manifest.csv"
            self.write_measurements(input_path, selected_rows)
            self.write_manifest(manifest_path, manifest_rows)
            self.write_engine_manifest(engine_manifest_path, selected_rows)

            selected_rows[0]["engine_version"] = "unexpected"
            self.write_measurements(input_path, selected_rows)
            with self.assertRaisesRegex(ValueError, "engine version differs from engine manifest"):
                summarize.reduce_results(
                    input_path,
                    directory / "output",
                    manifest_path,
                    engine_manifest_path)

    def testRejectsWorkloadManifestHaystackLengthDrift(self):
        rows, manifest_rows = self.campaign_measurements()
        manifest_rows[0]["haystack_length"] = "999"
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            input_path = directory / "raw.csv"
            manifest_path = directory / "manifest.csv"
            self.write_measurements(input_path, rows)
            self.write_manifest(manifest_path, manifest_rows)

            with self.assertRaisesRegex(ValueError, "haystack length differs from manifest"):
                summarize.reduce_results(input_path, directory / "output", manifest_path)

    def testRejectsWrongCampaignProvenance(self):
        rows, manifest_rows = self.campaign_measurements()
        object_row = next(row for row in rows if row["engine"] == summarize.SLICE_OBJECT_ENGINE)
        object_row["engine_version"] = "Slice RE2 (native-access=true)"
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            input_path = directory / "raw.csv"
            manifest_path = directory / "manifest.csv"
            self.write_measurements(input_path, rows)
            self.write_manifest(manifest_path, manifest_rows)
            with self.assertRaisesRegex(ValueError, "object Slice runner reported native access"):
                summarize.reduce_results(input_path, directory / "output", manifest_path)

    def complete_measurements(self, before="100ns", after="100ns"):
        return self.measurements("curated/complete", "count", "100", {
            summarize.BUNDLED_ENGINE: "100ns",
            summarize.PINNED_PORTABLE_ENGINE: "100ns",
            summarize.PINNED_HOST_BEFORE_ENGINE: before,
            summarize.PINNED_HOST_AFTER_ENGINE: after,
            summarize.SLICE_ENGINE: "100ns",
            summarize.SLICE_OBJECT_ENGINE: "100ns",
        })

    def campaign_measurements(self):
        rows = []
        manifest_rows = []
        versions = {
            summarize.BUNDLED_ENGINE: "2025-11-05",
            summarize.PINNED_PORTABLE_ENGINE: summarize.PINNED_RE2_COMMIT,
            summarize.PINNED_HOST_BEFORE_ENGINE: summarize.PINNED_RE2_COMMIT,
            summarize.PINNED_HOST_AFTER_ENGINE: summarize.PINNED_RE2_COMMIT,
            summarize.SLICE_ENGINE: "Slice RE2 (native-access=true)",
            summarize.SLICE_OBJECT_ENGINE: "Slice RE2 (native-access=false)",
        }
        for index in range(summarize.EXPECTED_WORKLOAD_COUNT):
            name = f"curated/workload-{index}"
            measurements = self.complete_measurements()
            for row in measurements:
                row["name"] = name
                row["rebar_version"] = summarize.PINNED_REBAR_VERSION
                row["engine_version"] = versions[row["engine"]]
            rows.extend(measurements)
            manifest_rows.append({
                "name": name,
                "model": "count",
                "native_access": "true",
                "expected_result": "1",
                "pattern_length": "1",
                "pattern_sha256": "0" * 64,
                "haystack_length": "100",
                "haystack_sha256": "1" * 64,
            })
        return rows, manifest_rows

    @staticmethod
    def measurements(name, model, haystack_len, medians):
        return [{
            "name": name,
            "model": model,
            "rebar_version": "0.0.1",
            "engine": engine,
            "engine_version": "test",
            "err": "",
            "haystack_len": haystack_len,
            "iters": "10",
            "total": "1s",
            "median": median,
            "mad": "0",
            "mean": median,
            "stddev": "0",
            "min": median,
            "max": median,
        } for engine, median in medians.items()]

    @staticmethod
    def write_measurements(path, rows):
        with path.open("w", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=REBAR_FIELDS)
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def write_manifest(path, rows):
        with path.open("w", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(
                output_file,
                fieldnames=(
                    "name",
                    "model",
                    "native_access",
                    "expected_result",
                    "pattern_length",
                    "pattern_sha256",
                    "haystack_length",
                    "haystack_sha256",
                ))
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def write_engine_manifest(path, rows):
        with path.open("w", newline="", encoding="utf-8") as output_file:
            writer = csv.writer(output_file)
            for row in rows:
                writer.writerow((row["name"], row["model"], row["engine"], row["engine_version"]))

    @staticmethod
    def read_csv(path):
        with path.open(newline="", encoding="utf-8") as input_file:
            return list(csv.DictReader(input_file))

    def assertReductionFails(self, rows, message):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            input_path = directory / "raw.csv"
            output_directory = directory / "output"
            self.write_measurements(input_path, rows)
            with self.assertRaisesRegex(ValueError, message):
                summarize.reduce_results(input_path, output_directory)
            self.assertFalse(output_directory.exists())


if __name__ == "__main__":
    unittest.main()
