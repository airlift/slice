#!/usr/bin/env python3

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


RESULT_FILE = "slice-dfa-layout-screen.json"
NATIVE_RESULT_FILE = "native-dfa-pointer-host.json"
OBJECT_METHOD = "objectReferences"


def method_name(benchmark):
    return benchmark.rsplit(".", 1)[-1]


def load_results(path):
    with path.open(encoding="utf-8") as input_file:
        return json.load(input_file)


def load_native_scores(path):
    if not path.exists():
        return {}

    scores = defaultdict(list)
    for result in load_results(path)["benchmarks"]:
        parts = result["run_name"].split("/")
        if len(parts) != 3 or parts[0] != "DfaAbsolutePointers":
            continue
        if result["time_unit"] != "ns":
            raise ValueError(f"Unexpected native time unit: {result['time_unit']}")
        scores[(int(parts[1]), int(parts[2]))].append(result["real_time"])
    return {key: statistics.median(values) for key, values in scores.items()}


def transition_payload_bytes(method, state_count, class_count):
    row_count = state_count + 1
    if method == "directByteStateIds":
        return 1 << 16
    if method == "compactDirectByteStateIds":
        return state_count << 8
    if method == "preShiftedCharacterRowOffsets":
        return (1 << 16) * 2
    if method == "everythingSegmentPointers":
        return (state_count + 1) * class_count * 8
    if method == OBJECT_METHOD:
        return row_count * (class_count + 1) * 4
    if method == "classMajorStateIds":
        return row_count * class_count
    if method == "classMajorCharacterStateIds":
        return row_count * class_count * 2
    if method == "rowMajorStateIds":
        padded_class_count = 1 << (class_count - 1).bit_length()
        return row_count * padded_class_count
    if method == "rowMajorCharacterStateIds":
        padded_class_count = 1 << (class_count - 1).bit_length()
        return row_count * padded_class_count * 2
    if method in {"powerOfTwoStateIds", "preShiftedHeapRowIndexes"}:
        padded_class_count = 1 << (class_count - 1).bit_length()
        return row_count * padded_class_count * 4
    return row_count * class_count * 4


def collect_rows(session):
    rows = []
    session_label = session.parent.name
    for architecture in ("intel", "arm"):
        result_file = session / architecture / "re2-results" / RESULT_FILE
        if not result_file.exists():
            raise FileNotFoundError(f"Missing {result_file}")

        results = load_results(result_file)
        native_scores = load_native_scores(
            session / architecture / "re2-results" / NATIVE_RESULT_FILE)
        controls = {}
        for result in results:
            method = method_name(result["benchmark"])
            if method != OBJECT_METHOD:
                continue
            params = result["params"]
            key = (int(params["stateCount"]), int(params["classCount"]), int(params["textLength"]))
            controls[key] = result["primaryMetric"]["score"]

        for result in results:
            params = result["params"]
            state_count = int(params["stateCount"])
            class_count = int(params["classCount"])
            text_length = int(params["textLength"])
            key = (state_count, class_count, text_length)
            method = method_name(result["benchmark"])
            metric = result["primaryMetric"]
            score = metric["score"]
            native_score = native_scores.get((state_count, class_count))
            rows.append({
                "session": session_label,
                "architecture": architecture,
                "method": method,
                "state_count": state_count,
                "class_count": class_count,
                "text_length": text_length,
                "transition_payload_bytes": transition_payload_bytes(method, state_count, class_count),
                "score_ns_per_operation": score,
                "score_error": metric.get("scoreError"),
                "ratio_to_object": score / controls[key],
                "native_score_ns_per_operation": native_score,
                "ratio_to_native": score / native_score if native_score is not None else None,
            })
    return rows


def write_csv(path, rows):
    fieldnames = list(rows[0])
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_summary(rows):
    ratios = defaultdict(list)
    for row in rows:
        if row["method"] != OBJECT_METHOD:
            ratios[(row["session"], row["architecture"], row["method"])].append(row["ratio_to_object"])

    print("| Session | Architecture | Method | Geomean/Object | Geomean/Native | Best | Worst | Points |")
    print("|---|---|---|---:|---:|---:|---:|---:|")
    for (session, architecture, method), values in sorted(ratios.items()):
        geometric_mean = math.exp(sum(math.log(value) for value in values) / len(values))
        native_values = [
            row["ratio_to_native"]
            for row in rows
            if row["session"] == session and
            row["architecture"] == architecture and
            row["method"] == method and
            row["ratio_to_native"] is not None
        ]
        native_geometric_mean = (
            math.exp(sum(math.log(value) for value in native_values) / len(native_values))
            if native_values else None)
        native_text = f"{native_geometric_mean:.3f}x" if native_geometric_mean is not None else "n/a"
        print(f"| {session} | {architecture} | {method} | {geometric_mean:.3f}x | {native_text} | {min(values):.3f}x | {max(values):.3f}x | {len(values)} |")


def main():
    parser = argparse.ArgumentParser(description="Normalize compact DFA layout screen sessions")
    parser.add_argument("session", nargs="+", type=Path, help="AWS campaign session directories")
    parser.add_argument("--output", type=Path, required=True, help="Detailed CSV output")
    args = parser.parse_args()

    rows = []
    for session in args.session:
        rows.extend(collect_rows(session.resolve()))
    rows.sort(key=lambda row: (
        row["session"],
        row["architecture"],
        row["method"],
        row["class_count"],
        row["state_count"],
        row["text_length"],
    ))
    write_csv(args.output, rows)
    print_summary(rows)


if __name__ == "__main__":
    main()
