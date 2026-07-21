import io
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import summarize_pipeline


class TestSummarizePipeline(unittest.TestCase):
    def test_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            raw = directory / "raw.csv"
            manifest = directory / "manifest.txt"
            raw.write_text(
                "workload,stage,duration_ns,result\n"
                "test,public-before,100,5\n"
                "test,public-after,110,5\n"
                "test,native-public-before,70,5\n"
                "test,native-bit-state,65,5\n"
                "test,native-public-after,72,5\n"
                "test,control,5,5\n"
                "test,composed,105,5\n",
                encoding="ascii",
            )
            manifest.write_text(
                "name=test\nmodel=count-captures\nattempts=3\nmatcher_resets=1\ncapture_calls=2\ncapture_bytes=8\npublic_result=5\n",
                encoding="ascii",
            )
            output = io.StringIO()
            summarize_pipeline.summarize(raw, manifest, output)
            rows = output.getvalue().splitlines()
            self.assertEqual(len(rows), 8)
            self.assertIn("component_accounting_ratio", rows[0])
            self.assertIn("0.9523809523809523", rows[-1])

            parsed_rows = list(__import__("csv").DictReader(rows))
            native_public = next(row for row in parsed_rows if row["stage"] == "native-public-before")
            native_bit_state = next(row for row in parsed_rows if row["stage"] == "native-bit-state")
            self.assertEqual(native_public["attempt_count"], "3")
            self.assertEqual(native_bit_state["attempt_count"], "2")


if __name__ == "__main__":
    unittest.main()
