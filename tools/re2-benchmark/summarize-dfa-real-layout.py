#!/usr/bin/env python3

import argparse
import csv
import json
import statistics
from pathlib import Path


JAVA_RESULT_FILE = "slice-dfa-real-layout-screen.json"
NATIVE_RESULT_FILE = "native-real-layout-host.json"
OBJECT_METHODS = {"compactObjectReferences", "objectCompactTransitions"}


def method_name(benchmark):
    return benchmark.rsplit(".", 1)[-1]


def load_json(path):
    with path.open(encoding="utf-8") as input_file:
        return json.load(input_file)


def native_pattern(name):
    for pattern in ("Hard", "Parens"):
        if name.startswith(f"Search_{pattern}_CachedDFA/"):
            return pattern.upper()
    return None


def collect_rows(session):
    rows = []
    session_label = session.parent.name
    for architecture in ("intel", "arm"):
        result_directory = session / architecture / "re2-results"
        java_file = result_directory / JAVA_RESULT_FILE
        if not java_file.exists():
            continue
        java_results = load_json(java_file)
        object_scores = {}
        for result in java_results:
            if method_name(result["benchmark"]) in OBJECT_METHODS:
                object_scores[result["params"]["pattern"]] = result["primaryMetric"]["score"]

        for result in java_results:
            pattern = result["params"]["pattern"]
            metric = result["primaryMetric"]
            score = metric["score"]
            rows.append({
                "session": session_label,
                "architecture": architecture,
                "pattern": pattern,
                "implementation": method_name(result["benchmark"]),
                "score_ns_per_operation": score,
                "score_error": metric.get("scoreError"),
                "ratio_to_object": score / object_scores[pattern],
                "ratio_to_native": "",
            })

        native_file = result_directory / NATIVE_RESULT_FILE
        if not native_file.exists():
            continue
        repetitions = {}
        for result in load_json(native_file)["benchmarks"]:
            pattern = native_pattern(result["name"])
            if pattern is None or result.get("aggregate_name") is not None:
                continue
            repetitions.setdefault(pattern, []).append(result["real_time"])

        for pattern, scores in repetitions.items():
            native_score = statistics.median(scores)
            rows.append({
                "session": session_label,
                "architecture": architecture,
                "pattern": pattern,
                "implementation": "nativeCachedDfa",
                "score_ns_per_operation": native_score,
                "score_error": "",
                "ratio_to_object": native_score / object_scores[pattern],
                "ratio_to_native": 1.0,
            })
            for row in rows:
                if (row["session"], row["architecture"], row["pattern"]) == (session_label, architecture, pattern):
                    row["ratio_to_native"] = row["score_ns_per_operation"] / native_score
    return rows


def write_csv(path, rows):
    if not rows:
        raise ValueError("No completed real-layout results found")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def print_summary(rows):
    print("| Architecture | Pattern | Implementation | vs Object | vs Native | ns/op |")
    print("|---|---|---|---:|---:|---:|")
    for row in rows:
        native_ratio = row["ratio_to_native"]
        native_display = "n/a" if native_ratio == "" else f"{native_ratio:.3f}x"
        print(
            f"| {row['architecture']} | {row['pattern']} | {row['implementation']} | "
            f"{row['ratio_to_object']:.3f}x | {native_display} | {row['score_ns_per_operation']:.0f} |")


def main():
    parser = argparse.ArgumentParser(description="Normalize real-graph DFA layout screen sessions")
    parser.add_argument("session", type=Path, help="AWS campaign session directory")
    parser.add_argument("--output", type=Path, required=True, help="Detailed CSV output")
    args = parser.parse_args()

    rows = collect_rows(args.session.resolve())
    rows.sort(key=lambda row: (row["architecture"], row["pattern"], row["implementation"]))
    write_csv(args.output, rows)
    print_summary(rows)


if __name__ == "__main__":
    main()
