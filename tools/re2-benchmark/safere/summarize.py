#!/usr/bin/env python3

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


def read_jmh(path, suffix):
    rows = {}
    for result in json.loads(path.read_text()):
        operation = result["benchmark"].rsplit(".", 1)[1]
        if suffix:
            if not operation.endswith(suffix):
                raise ValueError(f"unexpected benchmark without {suffix} suffix: {operation}")
            operation = operation[: -len(suffix)]
        key = (operation, result["params"]["workload"], int(result["params"]["sourceLength"]))
        if key in rows:
            raise ValueError(f"duplicate benchmark row: {key}")
        allocation = result["secondaryMetrics"].get("gc.alloc.rate.norm")
        if allocation is None:
            raise ValueError(f"missing normalized allocation metric: {key}")
        rows[key] = {
            "time_ns": float(result["primaryMetric"]["score"]),
            "allocation_bytes": float(allocation["score"]),
        }
    return rows


def combine_slice_bracket(before_rows, after_rows):
    if set(before_rows) != set(after_rows):
        raise ValueError("Slice before/after benchmark keys differ")

    combined = {}
    for key in before_rows:
        before = before_rows[key]
        after = after_rows[key]
        combined[key] = {
            "time_ns": math.sqrt(before["time_ns"] * after["time_ns"]),
            "allocation_bytes": statistics.mean([before["allocation_bytes"], after["allocation_bytes"]]),
            "before_time_ns": before["time_ns"],
            "after_time_ns": after["time_ns"],
        }
    return combined


def compare(slice_rows, safere_rows):
    if set(slice_rows) != set(safere_rows):
        missing_slice = sorted(set(safere_rows) - set(slice_rows))
        missing_safere = sorted(set(slice_rows) - set(safere_rows))
        raise ValueError(
            f"benchmark key mismatch: missing Slice={missing_slice}, missing SafeRE={missing_safere}"
        )

    rows = []
    for operation, workload, source_length in sorted(slice_rows):
        slice_result = slice_rows[(operation, workload, source_length)]
        safere_result = safere_rows[(operation, workload, source_length)]
        rows.append(
            {
                "operation": operation,
                "workload": workload,
                "source_length": source_length,
                "slice_time_ns": slice_result["time_ns"],
                "safere_time_ns": safere_result["time_ns"],
                "slice_to_safere_time_ratio": slice_result["time_ns"] / safere_result["time_ns"],
                "slice_allocation_bytes": slice_result["allocation_bytes"],
                "safere_allocation_bytes": safere_result["allocation_bytes"],
                "allocation_difference_bytes": slice_result["allocation_bytes"]
                - safere_result["allocation_bytes"],
                "slice_before_time_ns": slice_result["before_time_ns"],
                "slice_after_time_ns": slice_result["after_time_ns"],
                "slice_bracket_ratio": max(
                    slice_result["before_time_ns"] / slice_result["after_time_ns"],
                    slice_result["after_time_ns"] / slice_result["before_time_ns"],
                ),
            }
        )
    return rows


def geometric_mean(values):
    return math.exp(statistics.fmean(math.log(value) for value in values))


def percentile(values, fraction):
    ordered = sorted(values)
    index = math.ceil(fraction * len(ordered)) - 1
    return ordered[max(0, index)]


def summarize_rows(rows):
    ratios = [row["slice_to_safere_time_ratio"] for row in rows]
    return {
        "rows": len(rows),
        "wins": sum(ratio < 1 for ratio in ratios),
        "geometric_mean_time_ratio": geometric_mean(ratios),
        "median_time_ratio": statistics.median(ratios),
        "p90_time_ratio": percentile(ratios, 0.90),
        "maximum_time_ratio": max(ratios),
        "maximum_slice_bracket_ratio": max(row["slice_bracket_ratio"] for row in rows),
        "median_allocation_difference_bytes": statistics.median(
            row["allocation_difference_bytes"] for row in rows
        ),
    }


def summarize(rows):
    by_operation = defaultdict(list)
    for row in rows:
        by_operation[row["operation"]].append(row)
    return {
        "overall": summarize_rows(rows),
        "operations": {
            operation: summarize_rows(operation_rows)
            for operation, operation_rows in sorted(by_operation.items())
        },
    }


def write_csv(path, rows):
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys(), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path, rows, summary):
    largest_ratios = sorted(rows, key=lambda row: row["slice_to_safere_time_ratio"], reverse=True)[:12]
    with path.open("w") as output:
        output.write("# Slice RE2 And SafeRE Operation Comparison\n\n")
        output.write("Ratios are Slice elapsed time divided by SafeRE elapsed time; lower is better. ")
        output.write("Slice time is the geometric mean of runs immediately before and after SafeRE.\n\n")
        output.write("## Summary\n\n")
        output.write(
            "| Operation | Rows | Slice wins | Geometric mean | Median | P90 | Worst | Maximum Slice bracket | Median allocation difference |\n"
        )
        output.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for operation, values in [("overall", summary["overall"]), *summary["operations"].items()]:
            output.write(
                f"| {operation} | {values['rows']} | {values['wins']} | "
                f"{values['geometric_mean_time_ratio']:.3f}x | {values['median_time_ratio']:.3f}x | "
                f"{values['p90_time_ratio']:.3f}x | {values['maximum_time_ratio']:.3f}x | "
                f"{values['maximum_slice_bracket_ratio']:.3f}x | "
                f"{values['median_allocation_difference_bytes']:.0f} B |\n"
            )

        output.write("\n## Largest Slice Time Ratios\n\n")
        output.write(
            "| Operation | Workload | Source | Slice | SafeRE | Ratio | Slice allocation | SafeRE allocation |\n"
        )
        output.write("|---|---|---:|---:|---:|---:|---:|---:|\n")
        for row in largest_ratios:
            output.write(
                f"| {row['operation']} | {row['workload']} | {row['source_length']} | "
                f"{row['slice_time_ns']:.1f} ns | {row['safere_time_ns']:.1f} ns | "
                f"{row['slice_to_safere_time_ratio']:.3f}x | "
                f"{row['slice_allocation_bytes']:.0f} B | {row['safere_allocation_bytes']:.0f} B |\n"
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("slice_before", type=Path)
    parser.add_argument("safere", type=Path)
    parser.add_argument("output_directory", type=Path)
    parser.add_argument("--slice-after", type=Path, required=True)
    arguments = parser.parse_args()

    arguments.output_directory.mkdir(parents=True, exist_ok=True)
    slice_rows = combine_slice_bracket(
        read_jmh(arguments.slice_before, ""), read_jmh(arguments.slice_after, "")
    )
    rows = compare(slice_rows, read_jmh(arguments.safere, "SafeRe"))
    summary = summarize(rows)
    write_csv(arguments.output_directory / "operation-comparison.csv", rows)
    (arguments.output_directory / "operation-summary.json").write_text(
        json.dumps(summary, indent=2) + "\n"
    )
    write_markdown(arguments.output_directory / "operation-summary.md", rows, summary)


if __name__ == "__main__":
    main()
