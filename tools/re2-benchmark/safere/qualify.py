#!/usr/bin/env python3

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


MAXIMUM_SLICE_BRACKET_RATIO = 1.02
MINIMUM_STABLE_SESSIONS = 2


def geometric_mean(values):
    return math.exp(statistics.fmean(math.log(value) for value in values))


def percentile(values, fraction):
    ordered = sorted(values)
    index = math.ceil(fraction * len(ordered)) - 1
    return ordered[max(0, index)]


def read_session(session, architecture):
    path = session / architecture / "re2-results" / "operation-summary" / "operation-comparison.csv"
    rows = {}
    with path.open(newline="") as source:
        for row in csv.DictReader(source):
            key = (row["operation"], row["workload"], int(row["source_length"]))
            rows[key] = {
                "slice_time_ns": float(row["slice_time_ns"]),
                "safere_time_ns": float(row["safere_time_ns"]),
                "time_ratio": float(row["slice_to_safere_time_ratio"]),
                "slice_allocation_bytes": float(row["slice_allocation_bytes"]),
                "safere_allocation_bytes": float(row["safere_allocation_bytes"]),
                "allocation_difference_bytes": float(row["allocation_difference_bytes"]),
                "slice_bracket_ratio": float(row["slice_bracket_ratio"]),
            }
    return rows


def qualify_architecture(sessions, architecture):
    session_rows = [read_session(session, architecture) for session in sessions]
    expected_keys = set(session_rows[0])
    if any(set(rows) != expected_keys for rows in session_rows[1:]):
        raise ValueError(f"session keys differ for {architecture}")

    qualified = []
    underqualified = []
    for operation, workload, source_length in sorted(expected_keys):
        measurements = [rows[(operation, workload, source_length)] for rows in session_rows]
        stable = [
            measurement
            for measurement in measurements
            if measurement["slice_bracket_ratio"] <= MAXIMUM_SLICE_BRACKET_RATIO
        ]
        base = {
            "architecture": architecture,
            "operation": operation,
            "workload": workload,
            "source_length": source_length,
            "stable_sessions": len(stable),
        }
        if len(stable) < MINIMUM_STABLE_SESSIONS:
            underqualified.append(
                {
                    **base,
                    "diagnostic_time_ratio": geometric_mean(
                        measurement["time_ratio"] for measurement in measurements
                    ),
                    "minimum_slice_bracket_ratio": min(
                        measurement["slice_bracket_ratio"] for measurement in measurements
                    ),
                }
            )
            continue

        qualified.append(
            {
                **base,
                "slice_time_ns": geometric_mean(
                    measurement["slice_time_ns"] for measurement in stable
                ),
                "safere_time_ns": geometric_mean(
                    measurement["safere_time_ns"] for measurement in stable
                ),
                "time_ratio": geometric_mean(measurement["time_ratio"] for measurement in stable),
                "slice_allocation_bytes": statistics.fmean(
                    measurement["slice_allocation_bytes"] for measurement in stable
                ),
                "safere_allocation_bytes": statistics.fmean(
                    measurement["safere_allocation_bytes"] for measurement in stable
                ),
                "allocation_difference_bytes": statistics.fmean(
                    measurement["allocation_difference_bytes"] for measurement in stable
                ),
                "maximum_slice_bracket_ratio": max(
                    measurement["slice_bracket_ratio"] for measurement in stable
                ),
            }
        )
    return qualified, underqualified


def summarize_rows(rows):
    ratios = [row["time_ratio"] for row in rows]
    return {
        "rows": len(rows),
        "wins": sum(ratio < 1 for ratio in ratios),
        "geometric_mean_time_ratio": geometric_mean(ratios),
        "median_time_ratio": statistics.median(ratios),
        "p90_time_ratio": percentile(ratios, 0.90),
        "maximum_time_ratio": max(ratios),
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
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys(), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path, sessions, qualified_by_architecture, underqualified_by_architecture, summaries):
    with path.open("w") as output:
        output.write("# Slice RE2 And SafeRE Qualification\n\n")
        output.write("Ratios are Slice elapsed time divided by SafeRE elapsed time; lower is better. ")
        output.write(
            f"Rows require at least {MINIMUM_STABLE_SESSIONS} sessions with a Slice before/after bracket no larger than {MAXIMUM_SLICE_BRACKET_RATIO:.2f}x.\n\n"
        )
        output.write("Sessions:\n\n")
        for session in sessions:
            output.write(f"- `{session.name}`\n")

        output.write("\n## Aggregate\n\n")
        output.write("| Architecture | Qualified | Slice wins | Geometric mean | Median | P90 | Worst |\n")
        output.write("|---|---:|---:|---:|---:|---:|---:|\n")
        for architecture in ("intel", "arm"):
            values = summaries[architecture]["overall"]
            output.write(
                f"| {architecture} | {values['rows']}/80 | {values['wins']}/{values['rows']} | "
                f"{values['geometric_mean_time_ratio']:.3f}x | {values['median_time_ratio']:.3f}x | "
                f"{values['p90_time_ratio']:.3f}x | {values['maximum_time_ratio']:.3f}x |\n"
            )

        for architecture in ("intel", "arm"):
            output.write(f"\n## {architecture.title()} Operations\n\n")
            output.write("| Operation | Rows | Slice wins | Geometric mean | Median | P90 | Worst |\n")
            output.write("|---|---:|---:|---:|---:|---:|---:|\n")
            for operation, values in summaries[architecture]["operations"].items():
                output.write(
                    f"| {operation} | {values['rows']} | {values['wins']} | "
                    f"{values['geometric_mean_time_ratio']:.3f}x | {values['median_time_ratio']:.3f}x | "
                    f"{values['p90_time_ratio']:.3f}x | {values['maximum_time_ratio']:.3f}x |\n"
                )

            regressions = [
                row for row in qualified_by_architecture[architecture] if row["time_ratio"] >= 1
            ]
            output.write(f"\n## {architecture.title()} Regressions\n\n")
            if not regressions:
                output.write("None.\n")
            else:
                output.write("| Operation | Workload | Source | Slice | SafeRE | Ratio |\n")
                output.write("|---|---|---:|---:|---:|---:|\n")
                for row in sorted(regressions, key=lambda value: value["time_ratio"], reverse=True):
                    output.write(
                        f"| {row['operation']} | {row['workload']} | {row['source_length']} | "
                        f"{row['slice_time_ns']:.1f} ns | {row['safere_time_ns']:.1f} ns | "
                        f"{row['time_ratio']:.3f}x |\n"
                    )

            underqualified = underqualified_by_architecture[architecture]
            output.write(f"\n## {architecture.title()} Underqualified\n\n")
            if not underqualified:
                output.write("None.\n")
            else:
                output.write("| Operation | Workload | Source | Stable sessions | Diagnostic ratio | Best bracket |\n")
                output.write("|---|---|---:|---:|---:|---:|\n")
                for row in underqualified:
                    output.write(
                        f"| {row['operation']} | {row['workload']} | {row['source_length']} | "
                        f"{row['stable_sessions']} | {row['diagnostic_time_ratio']:.3f}x | "
                        f"{row['minimum_slice_bracket_ratio']:.3f}x |\n"
                    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output_directory", type=Path)
    parser.add_argument("sessions", type=Path, nargs=3)
    arguments = parser.parse_args()

    arguments.output_directory.mkdir(parents=True, exist_ok=True)
    qualified_by_architecture = {}
    underqualified_by_architecture = {}
    summaries = {}
    for architecture in ("intel", "arm"):
        qualified, underqualified = qualify_architecture(arguments.sessions, architecture)
        qualified_by_architecture[architecture] = qualified
        underqualified_by_architecture[architecture] = underqualified
        summaries[architecture] = summarize(qualified)
        write_csv(arguments.output_directory / f"{architecture}-qualified.csv", qualified)
        write_csv(arguments.output_directory / f"{architecture}-underqualified.csv", underqualified)

    (arguments.output_directory / "summary.json").write_text(
        json.dumps(summaries, indent=2) + "\n"
    )
    write_markdown(
        arguments.output_directory / "summary.md",
        arguments.sessions,
        qualified_by_architecture,
        underqualified_by_architecture,
        summaries,
    )


if __name__ == "__main__":
    main()
