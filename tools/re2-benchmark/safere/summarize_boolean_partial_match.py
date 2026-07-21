#!/usr/bin/env python3

import argparse
import csv
import json
from pathlib import Path


def read_jmh(path):
    rows = {}
    for result in json.loads(path.read_text()):
        params = result["params"]
        key = (params["workload"], int(params["sourceLength"]))
        route = params["route"]
        if route in rows.setdefault(key, {}):
            raise ValueError(f"duplicate benchmark row: {key}, {route}")
        rows[key][route] = float(result["primaryMetric"]["score"])
    return rows


def compare(results):
    rows = []
    for (workload, source_length), routes in sorted(results.items()):
        if set(routes) != {"ENGINE", "OPTIMIZED"}:
            raise ValueError(f"expected ENGINE and OPTIMIZED routes for {(workload, source_length)}")
        rows.append(
            {
                "workload": workload,
                "source_length": source_length,
                "optimized_time_ns": routes["OPTIMIZED"],
                "engine_time_ns": routes["ENGINE"],
                "optimized_to_engine_ratio": routes["OPTIMIZED"] / routes["ENGINE"],
            }
        )
    return rows


def write_csv(path, rows):
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys(), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path, rows):
    with path.open("w") as output:
        output.write("# Boolean Partial-Match Route Comparison\n\n")
        output.write("Ratios are optimized elapsed time divided by engine elapsed time; lower is better.\n\n")
        output.write("| Workload | Source | Optimized | Engine | Ratio |\n")
        output.write("|---|---:|---:|---:|---:|\n")
        for row in rows:
            output.write(
                f"| {row['workload']} | {row['source_length']} | "
                f"{row['optimized_time_ns']:.1f} ns | {row['engine_time_ns']:.1f} ns | "
                f"{row['optimized_to_engine_ratio']:.3f}x |\n"
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("jmh_results", type=Path)
    parser.add_argument("output_directory", type=Path)
    arguments = parser.parse_args()

    arguments.output_directory.mkdir(parents=True, exist_ok=True)
    rows = compare(read_jmh(arguments.jmh_results))
    write_csv(arguments.output_directory / "comparison.csv", rows)
    write_markdown(arguments.output_directory / "summary.md", rows)


if __name__ == "__main__":
    main()
