#!/usr/bin/env python3

import argparse
import csv
import json
import math
from pathlib import Path


LABELS = ("candidate-before", "control", "candidate-after")


def load_results(path):
    with path.open(encoding="utf-8") as input_file:
        return json.load(input_file)


def result_key(result):
    return result["benchmark"], tuple(sorted(result.get("params", {}).items()))


def collect_result_files(result_directory):
    files = {}
    for path in result_directory.glob("*.json"):
        for label in LABELS:
            prefix = f"{label}-"
            if path.name.startswith(prefix):
                files[(label, path.name[len(prefix):])] = path
                break
    return files


def collect_rows(session):
    rows = []
    for architecture in ("intel", "arm"):
        result_directory = session / architecture / "re2-results"
        files = collect_result_files(result_directory)
        result_sets = sorted({suffix for label, suffix in files if label == "control"})
        for result_set in result_sets:
            paths = {label: files.get((label, result_set)) for label in LABELS}
            if any(path is None for path in paths.values()):
                continue
            results = {
                label: {result_key(result): result for result in load_results(path)}
                for label, path in paths.items()
            }
            common_keys = set.intersection(*(set(results[label]) for label in LABELS))
            for benchmark, parameters in common_keys:
                before = results["candidate-before"][(benchmark, parameters)]["primaryMetric"]
                control = results["control"][(benchmark, parameters)]["primaryMetric"]
                after = results["candidate-after"][(benchmark, parameters)]["primaryMetric"]
                before_score = before["score"]
                control_score = control["score"]
                after_score = after["score"]
                bracketed_score = math.sqrt(before_score * after_score)
                rows.append({
                    "architecture": architecture,
                    "result_set": result_set.removesuffix(".json"),
                    "benchmark": benchmark,
                    "parameters": json.dumps(dict(parameters), sort_keys=True, separators=(",", ":")),
                    "score_unit": control["scoreUnit"],
                    "candidate_before": before_score,
                    "control": control_score,
                    "candidate_after": after_score,
                    "bracketed_candidate": bracketed_score,
                    "ratio_to_control": bracketed_score / control_score,
                })
    return rows


def write_csv(path, rows):
    if not rows:
        raise ValueError("No completed source-comparison results found")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def print_summary(rows):
    print("| Architecture | Result set | Benchmark | Parameters | Candidate/control |")
    print("|---|---|---|---|---:|")
    for row in rows:
        benchmark = row["benchmark"].rsplit(".", 1)[-1]
        print(
            f"| {row['architecture']} | {row['result_set']} | {benchmark} | "
            f"`{row['parameters']}` | {row['ratio_to_control']:.3f}x |")


def main():
    parser = argparse.ArgumentParser(description="Normalize bracketed DFA source A/B campaigns")
    parser.add_argument("session", type=Path, help="AWS campaign session directory")
    parser.add_argument("--output", type=Path, required=True, help="Detailed CSV output")
    args = parser.parse_args()

    rows = collect_rows(args.session.resolve())
    rows.sort(key=lambda row: (row["architecture"], row["result_set"], row["benchmark"], row["parameters"]))
    write_csv(args.output, rows)
    print_summary(rows)


if __name__ == "__main__":
    main()
