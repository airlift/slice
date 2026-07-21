#!/usr/bin/env python3
import csv
import statistics
import sys
from collections import defaultdict


def read_manifest(path):
    values = {}
    with open(path, encoding="ascii") as manifest:
        for line in manifest:
            name, value = line.rstrip("\n").split("=", 1)
            values[name] = value
    return values


def summarize(raw_path, manifest_path, output):
    manifest = read_manifest(manifest_path)
    samples = defaultdict(list)
    results = {}
    with open(raw_path, newline="", encoding="ascii") as raw:
        for row in csv.DictReader(raw):
            samples[row["stage"]].append(int(row["duration_ns"]))
            results[row["stage"]] = int(row["result"])

    expected_result = int(manifest["public_result"])
    if any(result != expected_result for result in results.values()):
        raise ValueError("stage result differs from public result")

    public_before = statistics.median(samples["public-before"])
    public_after = statistics.median(samples["public-after"])
    public_median = (public_before + public_after) / 2
    control_median = statistics.median(samples["control"]) if "control" in samples else 0
    composed_median = statistics.median(samples["composed"]) if "composed" in samples else 0
    adjusted_composed = max(0, composed_median - control_median)
    accounting_ratio = adjusted_composed / public_median if public_median else 0
    public_drift = abs(public_after - public_before) / public_median if public_median else 0

    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([
        "name", "model", "stage", "sample_count", "median_ns", "mean_ns", "standard_deviation_ns",
        "result", "attempt_count", "processed_bytes", "nanoseconds_per_attempt", "nanoseconds_per_byte",
        "stage_to_public_ratio", "component_accounting_ratio", "public_before_after_drift",
    ])
    for stage, durations in samples.items():
        median = statistics.median(durations)
        attempt_count = stage_attempt_count(stage, manifest)
        processed_bytes = stage_byte_count(stage, manifest)
        writer.writerow([
            manifest["name"],
            manifest["model"],
            stage,
            len(durations),
            median,
            statistics.mean(durations),
            statistics.pstdev(durations),
            results[stage],
            attempt_count,
            processed_bytes,
            median / attempt_count if attempt_count else "",
            median / processed_bytes if processed_bytes else "",
            median / public_median if public_median else "",
            accounting_ratio,
            public_drift,
        ])


def stage_attempt_count(stage, manifest):
    base_stage = stage.removeprefix("public-")
    if base_stage in {"native-public-before", "native-public-after"}:
        return int(manifest["attempts"])
    if base_stage == "native-bit-state":
        return int(manifest["capture_calls"])
    if base_stage in {"before", "after", "public", "composed", "control", "result"}:
        return int(manifest["attempts"])
    if base_stage == "setup":
        return int(manifest["matcher_resets"])
    manifest_stage = base_stage.replace("-", "_")
    return int(manifest.get(f"{manifest_stage}_calls", 0))


def stage_byte_count(stage, manifest):
    base_stage = stage.removeprefix("public-")
    if base_stage == "native-bit-state":
        return int(manifest["capture_bytes"])
    manifest_stage = base_stage.replace("-", "_")
    return int(manifest.get(f"{manifest_stage}_bytes", 0))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: summarize_pipeline.py <raw.csv> <manifest.txt>")
    summarize(sys.argv[1], sys.argv[2], sys.stdout)
