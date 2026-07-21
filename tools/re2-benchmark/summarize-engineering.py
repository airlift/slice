#!/usr/bin/env python3

import argparse
import csv
import json
import re
from pathlib import Path


DFA_METHODS = {
    "searchEasy0Dfa": "Easy0",
    "searchEasy1Dfa": "Easy1",
    "searchHardDfa": "Hard",
    "searchParensDfa": "Parens",
}
NATIVE_DFA_NAME = re.compile(
    r"^Search_(Easy0|Easy1|Hard|Parens)_CachedDFA/(\d+)/threads:(\d+)$")
BIG_FIXED_METHODS = {
    "searchBigFixedDfa": "DFA",
    "searchBigFixedRe2": "RE2",
}
NATIVE_BIG_FIXED_NAME = re.compile(
    r"^Search_BigFixed_Cached(DFA|RE2)/(\d+)/threads:1$")
TRINO_ENGINES = ("Joni", "Re2J")


def load_json(path):
    with path.open(encoding="utf-8") as input_file:
        return json.load(input_file)


def method_name(benchmark):
    return benchmark.rsplit(".", 1)[-1]


def format_number(value):
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.12g}"
    return str(value)


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: format_number(row.get(name)) for name in fieldnames})


def jmh_rows(path):
    rows = []
    for result in load_json(path):
        metric = result["primaryMetric"]
        rows.append({
            "benchmark": result["benchmark"],
            "method": method_name(result["benchmark"]),
            "params": result.get("params", {}),
            "threads": result["threads"],
            "score": metric["score"],
            "score_error": metric.get("scoreError"),
            "score_unit": metric["scoreUnit"],
            "secondary_metrics": result.get("secondaryMetrics", {}),
        })
    return rows


def summarize_dfa(architecture, result_directory):
    java = {}
    for row in jmh_rows(result_directory / "slice-dfa.json"):
        family = DFA_METHODS.get(row["method"])
        if family is not None:
            java[(family, int(row["params"]["textSize"]))] = row

    native = {}
    native_rows = []
    for result in load_json(result_directory / "native-dfa.json")["benchmarks"]:
        if result.get("run_type") != "aggregate" or result.get("aggregate_name") != "median":
            continue
        match = NATIVE_DFA_NAME.fullmatch(result["run_name"])
        if match is None:
            continue
        family, text_size, threads = match.groups()
        key = (family, int(text_size), int(threads))
        native[key] = result
        native_rows.append({
            "architecture": architecture,
            "family": family,
            "text_size": int(text_size),
            "threads": int(threads),
            "native_median_real_time": result["real_time"],
            "native_median_cpu_time": result["cpu_time"],
            "time_unit": result["time_unit"],
            "bytes_per_second": result.get("bytes_per_second"),
        })

    comparison_rows = []
    for (family, text_size), java_result in sorted(java.items()):
        native_result = native[(family, text_size, 1)]
        comparison_rows.append({
            "architecture": architecture,
            "family": family,
            "text_size": text_size,
            "java_mean_ns_per_op": java_result["score"],
            "java_score_error": java_result["score_error"],
            "native_median_ns_per_op": native_result["real_time"],
            "java_to_native_ratio": java_result["score"] / native_result["real_time"],
        })
    return comparison_rows, native_rows


def summarize_public_api(architecture, result_directory):
    allocation = {}
    for row in jmh_rows(result_directory / "slice-public-api-allocation.json"):
        metric = row["secondary_metrics"].get("gc.alloc.rate.norm")
        if metric is not None:
            allocation[(row["method"], row["params"]["workload"])] = metric

    rows = []
    for row in jmh_rows(result_directory / "slice-public-api.json"):
        workload = row["params"]["workload"]
        allocation_metric = allocation.get((row["method"], workload), {})
        rows.append({
            "architecture": architecture,
            "method": row["method"],
            "workload": workload,
            "mean_ns_per_op": row["score"],
            "score_error": row["score_error"],
            "allocated_bytes_per_op": allocation_metric.get("score"),
            "allocation_score_error": allocation_metric.get("scoreError"),
        })
    return rows


def summarize_big_fixed(architecture, result_directory):
    java = {}
    for row in jmh_rows(result_directory / "slice-big-fixed.json"):
        engine = BIG_FIXED_METHODS.get(row["method"])
        if engine is not None:
            java[(engine, int(row["params"]["textSize"]))] = row

    native = {}
    for result in load_json(result_directory / "native-big-fixed.json")["benchmarks"]:
        if result.get("run_type") != "aggregate" or result.get("aggregate_name") != "median":
            continue
        match = NATIVE_BIG_FIXED_NAME.fullmatch(result["run_name"])
        if match is None:
            continue
        engine, text_size = match.groups()
        native[(engine, int(text_size))] = result

    rows = []
    for (engine, text_size), java_result in sorted(java.items()):
        native_result = native[(engine, text_size)]
        rows.append({
            "architecture": architecture,
            "engine": engine,
            "text_size": text_size,
            "java_mean_ns_per_op": java_result["score"],
            "java_score_error": java_result["score_error"],
            "native_median_ns_per_op": native_result["real_time"],
            "java_to_native_ratio": java_result["score"] / native_result["real_time"],
        })
    return rows


def summarize_trino(architecture, result_directory):
    slice_results = {}
    for row in jmh_rows(result_directory / "slice-trino-operations.json"):
        key = (row["method"], row["params"]["workload"], int(row["params"]["sourceLength"]))
        slice_results[key] = row

    comparator_results = {}
    for row in jmh_rows(result_directory / "trino-joni-re2j-operations.json"):
        for engine in TRINO_ENGINES:
            if row["method"].endswith(engine):
                operation = row["method"][:-len(engine)]
                key = (operation, row["params"]["workload"], int(row["params"]["sourceLength"]), engine)
                comparator_results[key] = row
                break

    rows = []
    for (operation, workload, source_length), slice_result in sorted(slice_results.items()):
        joni = comparator_results[(operation, workload, source_length, "Joni")]
        re2j = comparator_results[(operation, workload, source_length, "Re2J")]
        rows.append({
            "architecture": architecture,
            "operation": operation,
            "workload": workload,
            "source_length": source_length,
            "slice_mean_ns_per_op": slice_result["score"],
            "joni_mean_ns_per_op": joni["score"],
            "historical_re2j_mean_ns_per_op": re2j["score"],
            "slice_to_joni_ratio": slice_result["score"] / joni["score"],
            "slice_to_historical_re2j_ratio": slice_result["score"] / re2j["score"],
        })
    return rows


def summarize_cache(architecture, result_directory):
    files = {
        "shared_one_thread": "slice-dfa-shared-1.json",
        "shared_all_cores": "slice-dfa-shared-all-cores.json",
        "cold_wave": "slice-dfa-cold-wave.json",
    }
    rows = []
    for phase, filename in files.items():
        for row in jmh_rows(result_directory / filename):
            rows.append({
                "architecture": architecture,
                "phase": phase,
                "method": row["method"],
                "text_size": row["params"]["textSize"],
                "worker_count": row["params"].get("workerCount"),
                "threads": row["threads"],
                "score": row["score"],
                "score_error": row["score_error"],
                "score_unit": row["score_unit"],
            })
    return rows


def main():
    parser = argparse.ArgumentParser(description="Normalize one RE2 AWS engineering session")
    parser.add_argument("session_directory", type=Path)
    parser.add_argument("--output-directory", type=Path)
    parser.add_argument(
        "--architectures",
        nargs="+",
        choices=("intel", "arm"),
        default=("intel", "arm"),
        help="Architectures whose completed result archives should be normalized")
    arguments = parser.parse_args()

    output_directory = arguments.output_directory or arguments.session_directory / "normalized"
    dfa_rows = []
    native_rows = []
    public_api_rows = []
    big_fixed_rows = []
    trino_rows = []
    cache_rows = []

    for architecture in arguments.architectures:
        result_directory = arguments.session_directory / architecture / "re2-results"
        dfa, native = summarize_dfa(architecture, result_directory)
        dfa_rows.extend(dfa)
        native_rows.extend(native)
        public_api_rows.extend(summarize_public_api(architecture, result_directory))
        big_fixed_rows.extend(summarize_big_fixed(architecture, result_directory))
        trino_rows.extend(summarize_trino(architecture, result_directory))
        cache_rows.extend(summarize_cache(architecture, result_directory))

    write_csv(output_directory / "dfa-comparison.csv", [
        "architecture", "family", "text_size", "java_mean_ns_per_op",
        "java_score_error", "native_median_ns_per_op", "java_to_native_ratio",
    ], dfa_rows)
    write_csv(output_directory / "native-dfa.csv", [
        "architecture", "family", "text_size", "threads",
        "native_median_real_time", "native_median_cpu_time", "time_unit",
        "bytes_per_second",
    ], native_rows)
    write_csv(output_directory / "public-api.csv", [
        "architecture", "method", "workload", "mean_ns_per_op", "score_error",
        "allocated_bytes_per_op", "allocation_score_error",
    ], public_api_rows)
    write_csv(output_directory / "big-fixed-comparison.csv", [
        "architecture", "engine", "text_size", "java_mean_ns_per_op",
        "java_score_error", "native_median_ns_per_op", "java_to_native_ratio",
    ], big_fixed_rows)
    write_csv(output_directory / "trino-comparison.csv", [
        "architecture", "operation", "workload", "source_length",
        "slice_mean_ns_per_op", "joni_mean_ns_per_op",
        "historical_re2j_mean_ns_per_op", "slice_to_joni_ratio",
        "slice_to_historical_re2j_ratio",
    ], trino_rows)
    write_csv(output_directory / "dfa-cache.csv", [
        "architecture", "phase", "method", "text_size", "worker_count",
        "threads", "score", "score_error", "score_unit",
    ], cache_rows)


if __name__ == "__main__":
    main()
