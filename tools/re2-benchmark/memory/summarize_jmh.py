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
            operation = operation[:-len(suffix)]
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


def compare(slice_rows, joni_rows):
    if set(slice_rows) != set(joni_rows):
        missing_slice = sorted(set(joni_rows) - set(slice_rows))
        missing_joni = sorted(set(slice_rows) - set(joni_rows))
        raise ValueError(f"benchmark key mismatch: missing Slice={missing_slice}, missing Joni={missing_joni}")

    rows = []
    for (operation, workload, source_length) in sorted(slice_rows):
        slice_result = slice_rows[(operation, workload, source_length)]
        joni_result = joni_rows[(operation, workload, source_length)]
        rows.append({
            "operation": operation,
            "workload": workload,
            "source_length": source_length,
            "slice_time_ns": slice_result["time_ns"],
            "joni_time_ns": joni_result["time_ns"],
            "slice_to_joni_time_ratio": slice_result["time_ns"] / joni_result["time_ns"],
            "slice_allocation_bytes": slice_result["allocation_bytes"],
            "joni_allocation_bytes": joni_result["allocation_bytes"],
            "allocation_difference_bytes": slice_result["allocation_bytes"] - joni_result["allocation_bytes"],
            **({
                "slice_before_time_ns": slice_result["before_time_ns"],
                "slice_after_time_ns": slice_result["after_time_ns"],
                "slice_bracket_ratio": max(
                    slice_result["before_time_ns"] / slice_result["after_time_ns"],
                    slice_result["after_time_ns"] / slice_result["before_time_ns"]),
            } if "before_time_ns" in slice_result else {}),
        })
    return rows


def combine_slice_bracket(before_rows, after_rows):
    if set(before_rows) != set(after_rows):
        missing_before = sorted(set(after_rows) - set(before_rows))
        missing_after = sorted(set(before_rows) - set(after_rows))
        raise ValueError(f"Slice bracket key mismatch: missing before={missing_before}, missing after={missing_after}")

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


def percentile(values, fraction):
    ordered = sorted(values)
    index = math.ceil(fraction * len(ordered)) - 1
    return ordered[max(0, index)]


def statistics_for(rows):
    ratios = [row["slice_to_joni_time_ratio"] for row in rows]
    allocation_differences = [row["allocation_difference_bytes"] for row in rows]
    result = {
        "rows": len(rows),
        "median_time_ratio": statistics.median(ratios),
        "p90_time_ratio": percentile(ratios, 0.90),
        "maximum_time_ratio": max(ratios),
        "median_allocation_difference_bytes": statistics.median(allocation_differences),
        "p90_allocation_difference_bytes": percentile(allocation_differences, 0.90),
        "maximum_allocation_difference_bytes": max(allocation_differences),
    }
    if "slice_bracket_ratio" in rows[0]:
        result["maximum_slice_bracket_ratio"] = max(row["slice_bracket_ratio"] for row in rows)
    return result


def summarize(rows):
    by_operation = defaultdict(list)
    for row in rows:
        by_operation[row["operation"]].append(row)
    return {
        "overall": statistics_for(rows),
        "operations": {
            operation: statistics_for(operation_rows)
            for operation, operation_rows in sorted(by_operation.items())
        },
    }


def write_csv(path, rows):
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path, rows, summary):
    largest_time = sorted(rows, key=lambda row: row["slice_to_joni_time_ratio"], reverse=True)[:12]
    largest_allocation = sorted(rows, key=lambda row: row["allocation_difference_bytes"], reverse=True)[:12]
    with path.open("w") as output:
        output.write("# RE2 And Joni Operation Comparison\n\n")
        output.write("Time ratios are Slice RE2 divided by Joni; lower is better. Allocation is reported as absolute bytes per operation. ")
        if "slice_bracket_ratio" in rows[0]:
            output.write("Slice time is the geometric mean of the runs immediately before and after Joni.\n\n")
        else:
            output.write("\n\n")
        output.write("## Summary\n\n")
        bracketed = "slice_bracket_ratio" in rows[0]
        bracket_header = " Maximum Slice bracket |" if bracketed else ""
        output.write(f"| Operation | Rows | Median time | P90 time | Maximum time | Median allocation difference | P90 allocation difference | Maximum allocation difference |{bracket_header}\n")
        output.write(f"|---|---:|---:|---:|---:|---:|---:|---:|{'---:|' if bracketed else ''}\n")
        for operation, values in [("overall", summary["overall"]), *summary["operations"].items()]:
            bracket_value = f" {values['maximum_slice_bracket_ratio']:.3f}x |" if bracketed else ""
            output.write(
                f"| {operation} | {values['rows']} | {values['median_time_ratio']:.3f}x | "
                f"{values['p90_time_ratio']:.3f}x | {values['maximum_time_ratio']:.3f}x | "
                f"{values['median_allocation_difference_bytes']:.0f} B | "
                f"{values['p90_allocation_difference_bytes']:.0f} B | "
                f"{values['maximum_allocation_difference_bytes']:.0f} B |{bracket_value}\n")

        output.write("\n## Largest Time Ratios\n\n")
        write_rows(output, largest_time)
        output.write("\n## Largest Allocation Excess\n\n")
        write_rows(output, largest_allocation)


def write_rows(output, rows):
    output.write("| Operation | Workload | Source | Slice time | Joni time | Time ratio | Slice allocation | Joni allocation | Allocation difference |\n")
    output.write("|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
    for row in rows:
        output.write(
            f"| {row['operation']} | {row['workload']} | {row['source_length']} | "
            f"{row['slice_time_ns']:.1f} ns | {row['joni_time_ns']:.1f} ns | "
            f"{row['slice_to_joni_time_ratio']:.3f}x | {row['slice_allocation_bytes']:.0f} B | "
            f"{row['joni_allocation_bytes']:.0f} B | {row['allocation_difference_bytes']:.0f} B |\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("slice_json", type=Path)
    parser.add_argument("joni_json", type=Path)
    parser.add_argument("output_directory", type=Path)
    parser.add_argument("--slice-after", type=Path)
    arguments = parser.parse_args()

    arguments.output_directory.mkdir(parents=True, exist_ok=True)
    slice_rows = read_jmh(arguments.slice_json, "")
    if arguments.slice_after:
        slice_rows = combine_slice_bracket(slice_rows, read_jmh(arguments.slice_after, ""))
    rows = compare(slice_rows, read_jmh(arguments.joni_json, "Joni"))
    summary = summarize(rows)
    write_csv(arguments.output_directory / "operation-comparison.csv", rows)
    (arguments.output_directory / "operation-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    write_markdown(arguments.output_directory / "operation-summary.md", rows, summary)


if __name__ == "__main__":
    main()
