#!/usr/bin/env python3

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


KEY_FIELDS = ("workload", "source_length", "lifecycle", "operation", "root_count")


def read_rows(path):
    with path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    for row in rows:
        for field in (
                "source_length",
                "root_count",
                "total_heap_bytes",
                "input_heap_bytes",
                "owned_matcher_heap_bytes",
                "off_heap_bytes",
                "accounted_dfa_bytes",
                "dfa_resets"):
            row[field] = int(row[field])
        row["average_heap_bytes"] = float(row["average_heap_bytes"])
    return rows


def pair_rows(rows):
    engines = defaultdict(dict)
    for row in rows:
        key = tuple(row[field] for field in KEY_FIELDS)
        if row["engine"] in engines[key]:
            raise ValueError(f"duplicate {row['engine']} row for {key}")
        engines[key][row["engine"]] = row

    comparisons = []
    for key, pair in sorted(engines.items()):
        if set(pair) != {"RE2", "Joni"}:
            raise ValueError(f"incomplete engine pair for {key}: {sorted(pair)}")
        re2 = pair["RE2"]
        joni = pair["Joni"]
        re2_bytes = comparable_bytes(re2)
        joni_bytes = comparable_bytes(joni)
        comparisons.append({
            **dict(zip(KEY_FIELDS, key)),
            "re2_bytes": re2_bytes,
            "joni_bytes": joni_bytes,
            "re2_to_joni_ratio": re2_bytes / joni_bytes,
            "absolute_difference_bytes": re2_bytes - joni_bytes,
            "re2_off_heap_bytes": re2["off_heap_bytes"],
            "re2_accounted_dfa_bytes": re2["accounted_dfa_bytes"],
            "re2_dfa_resets": re2["dfa_resets"],
            "re2_owned_matcher_bytes": re2["owned_matcher_heap_bytes"],
            "joni_owned_matcher_bytes": joni["owned_matcher_heap_bytes"],
        })
    return comparisons


def comparable_bytes(row):
    heap_bytes = row["total_heap_bytes"] - row["input_heap_bytes"]
    if row["lifecycle"] == "compiled":
        heap_bytes = row["average_heap_bytes"]
    return heap_bytes + row["off_heap_bytes"]


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        raise ValueError("percentile requires values")
    index = math.ceil(fraction * len(ordered)) - 1
    return ordered[max(0, index)]


def summarize(comparisons):
    groups = defaultdict(list)
    for comparison in comparisons:
        groups[comparison["lifecycle"]].append(comparison["re2_to_joni_ratio"])
    return {
        lifecycle: {
            "rows": len(ratios),
            "median_ratio": statistics.median(ratios),
            "p90_ratio": percentile(ratios, 0.90),
            "maximum_ratio": max(ratios),
        }
        for lifecycle, ratios in sorted(groups.items())
    }


def write_csv(path, comparisons):
    fieldnames = list(comparisons[0])
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(comparisons)


def write_markdown(path, comparisons, summary):
    outliers = [
        row for row in comparisons
        if row["re2_to_joni_ratio"] > 2 and row["absolute_difference_bytes"] > 1024 * 1024
    ]
    maximum_rows = sorted(comparisons, key=lambda row: row["re2_to_joni_ratio"], reverse=True)[:10]
    with path.open("w") as output:
        output.write("# RE2 And Joni Memory Census\n\n")
        output.write("Ratios are RE2 bytes divided by Joni bytes; lower is better. Caller-owned input bytes are excluded.\n\n")
        output.write("## Summary\n\n")
        output.write("| Lifecycle | Rows | Median | P90 | Maximum |\n")
        output.write("|---|---:|---:|---:|---:|\n")
        for lifecycle, values in summary.items():
            output.write(
                f"| {lifecycle} | {values['rows']} | {values['median_ratio']:.3f}x | "
                f"{values['p90_ratio']:.3f}x | {values['maximum_ratio']:.3f}x |\n")

        output.write("\n## Largest Ratios\n\n")
        output.write("| Workload | Source | Lifecycle | Operation | Roots | RE2 | Joni | Ratio | Difference |\n")
        output.write("|---|---:|---|---|---:|---:|---:|---:|---:|\n")
        for row in maximum_rows:
            output.write(
                f"| {row['workload']} | {row['source_length']} | {row['lifecycle']} | {row['operation']} | "
                f"{row['root_count']} | {row['re2_bytes']} | {row['joni_bytes']} | "
                f"{row['re2_to_joni_ratio']:.3f}x | {row['absolute_difference_bytes']} |\n")

        output.write("\n## Material Outliers\n\n")
        if not outliers:
            output.write("No row exceeded both 2x Joni memory and 1 MiB of absolute excess.\n")
        else:
            output.write("| Workload | Source | Lifecycle | Operation | RE2 | Joni | Ratio | Difference |\n")
            output.write("|---|---:|---|---|---:|---:|---:|---:|\n")
            for row in outliers:
                output.write(
                    f"| {row['workload']} | {row['source_length']} | {row['lifecycle']} | {row['operation']} | "
                    f"{row['re2_bytes']} | {row['joni_bytes']} | {row['re2_to_joni_ratio']:.3f}x | "
                    f"{row['absolute_difference_bytes']} |\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("census", type=Path)
    parser.add_argument("output_directory", type=Path)
    arguments = parser.parse_args()

    arguments.output_directory.mkdir(parents=True, exist_ok=True)
    comparisons = pair_rows(read_rows(arguments.census))
    summary = summarize(comparisons)
    write_csv(arguments.output_directory / "memory-comparison.csv", comparisons)
    (arguments.output_directory / "memory-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    write_markdown(arguments.output_directory / "memory-summary.md", comparisons, summary)


if __name__ == "__main__":
    main()
