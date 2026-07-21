import csv
import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("validate_verification.py")
SPEC = importlib.util.spec_from_file_location("validate_verification", MODULE_PATH)
validate_verification = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validate_verification)


class TestValidateVerification(unittest.TestCase):
    def testAcceptsCompleteVerification(self):
        rows = self.complete_rows()
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "verification.csv"
            self.write_rows(path, rows)
            validate_verification.validate(path)

    def testRejectsFailure(self):
        rows = self.complete_rows()
        rows[0][-1] = "wrong result"
        self.assertFails(rows, "wrong result")

    def testRejectsMissingEngine(self):
        rows = self.complete_rows()
        rows.pop()
        self.assertFails(rows, "missing verified engines")

    def testAcceptsFilteredVerificationUsingManifest(self):
        rows = self.complete_rows(workload_count=2)
        with tempfile.TemporaryDirectory() as temporary_directory:
            verification_path = Path(temporary_directory) / "verification.csv"
            manifest_path = Path(temporary_directory) / "manifest.csv"
            self.write_rows(verification_path, rows)
            self.write_rows(manifest_path, [row[:-1] for row in rows])
            validate_verification.validate(verification_path, manifest_path)

    def testRejectsFilteredVerificationMissingManifestWorkload(self):
        rows = self.complete_rows(workload_count=2)
        with tempfile.TemporaryDirectory() as temporary_directory:
            verification_path = Path(temporary_directory) / "verification.csv"
            manifest_path = Path(temporary_directory) / "manifest.csv"
            self.write_rows(verification_path, rows[:-len(validate_verification.EXPECTED_ENGINES)])
            self.write_rows(manifest_path, [row[:-1] for row in rows])
            with self.assertRaisesRegex(ValueError, "Verification workloads differ from manifest"):
                validate_verification.validate(verification_path, manifest_path)

    @staticmethod
    def complete_rows(workload_count=validate_verification.EXPECTED_WORKLOAD_COUNT):
        return [
            [f"curated/workload-{workload}", "count", engine, "version", "OK"]
            for workload in range(workload_count)
            for engine in sorted(validate_verification.EXPECTED_ENGINES)
        ]

    @staticmethod
    def write_rows(path, rows):
        with path.open("w", newline="", encoding="utf-8") as output_file:
            csv.writer(output_file).writerows(rows)

    def assertFails(self, rows, message):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "verification.csv"
            self.write_rows(path, rows)
            with self.assertRaisesRegex(ValueError, message):
                validate_verification.validate(path)


if __name__ == "__main__":
    unittest.main()
