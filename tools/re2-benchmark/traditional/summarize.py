#!/usr/bin/env python3

import argparse
import csv
import json
import math
import re
import statistics
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Pair:
    identifier: str
    family: str
    engine: str
    java_benchmark: str
    java_parameters: tuple[tuple[str, str], ...]
    native_benchmark: str


SEARCH_SIZES = (8, 64, 512, 4096, 32768, 262144, 2097152, 16777216)
NFA_SEARCH_SIZES = SEARCH_SIZES[:6]
ONE_PASS_SEARCH_SIZES = SEARCH_SIZES[:7]
BIG_FIXED_SIZES = (8, 64, 512, 4096, 32768, 262144, 1048576)
FULL_MATCH_SIZES = SEARCH_SIZES[:7]


def benchmark_name(class_name, method_name):
    return f"io.airlift.slice.re2.{class_name}.{method_name}"


def add_sized_pairs(pairs, family, case_name, engines, sizes, class_name, method_prefix, native_prefix):
    for engine, java_suffix, native_suffix in engines:
        for size in sizes:
            pairs.append(Pair(
                identifier=f"{family}/{case_name}/{engine}/{size}",
                family=family,
                engine=engine,
                java_benchmark=benchmark_name(class_name, f"{method_prefix}{java_suffix}"),
                java_parameters=(("textSize", str(size)),),
                native_benchmark=f"{native_prefix}{native_suffix}/{size}/threads:1"))


def add_fixed_pair(pairs, family, case_name, engine, class_name, method_name, native_benchmark):
    pairs.append(Pair(
        identifier=f"{family}/{case_name}/{engine}",
        family=family,
        engine=engine,
        java_benchmark=benchmark_name(class_name, method_name),
        java_parameters=(),
        native_benchmark=native_benchmark))


def build_pairs():
    pairs = []

    for case_name in ("Easy0", "Easy1", "Medium", "Hard", "Parens"):
        add_sized_pairs(
            pairs,
            "failed-search",
            case_name.lower(),
            (("dfa", "Dfa", "DFA"), ("re2", "Re2", "RE2")),
            SEARCH_SIZES,
            "BenchmarkRe2Search",
            f"search{case_name}",
            f"Search_{case_name}_Cached")
        add_sized_pairs(
            pairs,
            "failed-search",
            case_name.lower(),
            (("nfa", "Nfa", "NFA"),),
            NFA_SEARCH_SIZES,
            "BenchmarkRe2SearchNfa",
            f"search{case_name}",
            f"Search_{case_name}_Cached")

    for case_name in ("Easy2", "Fanout"):
        add_sized_pairs(
            pairs,
            "failed-search",
            case_name.lower(),
            (("dfa", "Dfa", "DFA"), ("re2", "Re2", "RE2")),
            SEARCH_SIZES,
            "BenchmarkRe2SearchExtra",
            f"search{case_name}",
            f"Search_{case_name}_Cached")

    add_sized_pairs(
        pairs,
        "failed-search",
        "big-fixed",
        (("dfa", "Dfa", "DFA"), ("re2", "Re2", "RE2")),
        BIG_FIXED_SIZES,
        "BenchmarkRe2SearchExtra",
        "searchBigFixed",
        "Search_BigFixed_Cached")

    for case_name, java_prefix, native_prefix, specialized_engine in (
            ("success", "searchSuccess", "Search_Success_Cached", ("one-pass", "OnePass", "OnePass")),
            ("success-one-byte", "searchSuccess1", "Search_Success1_Cached", ("bit-state", "BitState", "BitState"))):
        add_sized_pairs(
            pairs,
            "successful-search",
            case_name,
            (("dfa", "Dfa", "DFA"), ("re2", "Re2", "RE2")),
            SEARCH_SIZES,
            "BenchmarkRe2SearchExtra",
            java_prefix,
            native_prefix)
        add_sized_pairs(
            pairs,
            "successful-search",
            case_name,
            (specialized_engine,),
            ONE_PASS_SEARCH_SIZES,
            "BenchmarkRe2SearchExtra",
            java_prefix,
            native_prefix)

    add_sized_pairs(
        pairs,
        "successful-search",
        "alternate-match",
        (("dfa", "Dfa", "DFA"), ("re2", "Re2", "RE2"),
         ("one-pass", "OnePass", "OnePass"), ("bit-state", "BitState", "BitState")),
        SEARCH_SIZES,
        "BenchmarkRe2SearchExtra",
        "searchAltMatch",
        "Search_AltMatch_Cached")

    parse_cases = (
        ("digits", "3Digits", "Digits", ("nfa", "Nfa", "NFA"), ("one-pass", "OnePass", "OnePass"),
         ("bit-state", "BitState", "BitState"), ("backtrack", "Backtrack", "Backtrack"), ("re2", "Re2", "RE2")),
        ("digit-classes", "3DigitDs", "DigitDs", ("nfa", "Nfa", "NFA"), ("one-pass", "OnePass", "OnePass"),
         ("bit-state", "BitState", "BitState"), ("backtrack", "Backtrack", "Backtrack"), ("re2", "Re2", "RE2")),
        ("split", "1Split", "Split", ("nfa", "Nfa", "NFA"), ("one-pass", "OnePass", "OnePass"),
         ("bit-state", "BitState", "BitState"), ("re2", "Re2", "RE2")),
        ("split-hard", "SplitHard", "SplitHard", ("nfa", "Nfa", "NFA"),
         ("bit-state", "BitState", "BitState"), ("backtrack", "Backtrack", "Backtrack"), ("re2", "Re2", "RE2")),
    )
    for case_name, java_case, native_case, *engines in parse_cases:
        for engine, java_suffix, native_suffix in engines:
            add_fixed_pair(
                pairs,
                "capture",
                case_name,
                engine,
                "BenchmarkRe2Parse",
                f"parse{java_case}{java_suffix}",
                f"Parse_Cached{native_case}_{native_suffix}/threads:1")

    for case_name in ("SplitBig1", "SplitBig2"):
        add_fixed_pair(
            pairs,
            "capture",
            case_name.lower(),
            "re2",
            "BenchmarkRe2Parse",
            f"parse{case_name}Re2",
            f"Parse_Cached{case_name}_RE2/threads:1")

    add_sized_pairs(
        pairs,
        "successful-search",
        "phone",
        (("re2", "Re2", "RE2"),),
        SEARCH_SIZES,
        "BenchmarkRe2Parse",
        "searchPhone",
        "SearchPhone_Cached")

    for case_name, java_method, native_name in (
            ("parse", "compilePhaseParse", "BM_Regexp_Parse/threads:1"),
            ("simplify", "compilePhaseSimplify", "BM_Regexp_Simplify/threads:1"),
            ("compile-to-program", "compilePhaseCompileToProg", "BM_CompileToProg/threads:1"),
            ("simplify-compile", "compilePhaseSimplifyCompile", "BM_Regexp_SimplifyCompile/threads:1"),
            ("re2", "compilePhaseRe2Compile", "BM_RE2_Compile/threads:1")):
        add_fixed_pair(pairs, "compile", case_name, "compile", "BenchmarkRe2Practical", java_method, native_name)

    for case_name, java_method, native_name in (
            ("empty", "emptyPartialMatch", "EmptyPartialMatchRE2/threads:1"),
            ("simple", "simplePartialMatch", "SimplePartialMatchRE2/threads:1"),
            ("http", "httpPartialMatch", "HTTPPartialMatchRE2/threads:1"),
            ("small-http", "smallHttpPartialMatch", "SmallHTTPPartialMatchRE2/threads:1")):
        add_fixed_pair(pairs, "practical", case_name, "re2", "BenchmarkRe2Practical", java_method, native_name)

    for case_name, java_method, native_name in (
            ("dot", "dotMatch", "DotMatchRE2/threads:1"),
            ("ascii", "asciiMatch", "ASCIIMatchRE2/threads:1"),
            ("possible-trivial", "possibleMatchRangeTrivial", "PossibleMatchRange_Trivial"),
            ("possible-complex", "possibleMatchRangeComplex", "PossibleMatchRange_Complex"),
            ("possible-prefix", "possibleMatchRangePrefix", "PossibleMatchRange_Prefix"),
            ("possible-no-program", "possibleMatchRangeNoProg", "PossibleMatchRange_NoProg")):
        add_fixed_pair(pairs, "practical", case_name, "re2", "BenchmarkRe2Misc", java_method, native_name)

    for case_name, java_method, native_prefix in (
            ("dot-star", "fullMatchDotStarLatin1", "FullMatch_DotStar_CachedRE2"),
            ("dot-star-dollar", "fullMatchDotStarDollarLatin1", "FullMatch_DotStarDollar_CachedRE2"),
            ("dot-star-capture", "fullMatchDotStarCaptureLatin1", "FullMatch_DotStarCapture_CachedRE2")):
        for size in FULL_MATCH_SIZES:
            pairs.append(Pair(
                identifier=f"full-match/{case_name}/re2/{size}",
                family="full-match",
                engine="re2",
                java_benchmark=benchmark_name("BenchmarkRe2FullMatch", java_method),
                java_parameters=(("textSize", str(size)),),
                native_benchmark=f"{native_prefix}/{size}"))

    identifiers = {pair.identifier for pair in pairs}
    java_keys = {(pair.java_benchmark, pair.java_parameters) for pair in pairs}
    native_names = {pair.native_benchmark for pair in pairs}
    if len(identifiers) != len(pairs) or len(java_keys) != len(pairs) or len(native_names) != len(pairs):
        raise ValueError("Traditional benchmark pairs must be one-to-one")
    return pairs


def select_pairs(class_name=None):
    pairs = build_pairs()
    if class_name is None:
        return pairs

    class_prefix = f"io.airlift.slice.re2.{class_name}."
    selected = [pair for pair in pairs if pair.java_benchmark.startswith(class_prefix)]
    if not selected:
        raise ValueError(f"No paired benchmarks for class {class_name}")
    return selected


def select_unpaired_java_keys(class_name=None):
    keys = expected_unpaired_java_keys()
    if class_name is None:
        return keys

    class_prefix = f"io.airlift.slice.re2.{class_name}."
    return {key for key in keys if key[0].startswith(class_prefix)}


def write_manifest(path, pairs):
    fieldnames = (
        "identifier",
        "family",
        "engine",
        "java_benchmark",
        "java_parameters",
        "native_benchmark",
    )
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for pair in pairs:
            writer.writerow({
                "identifier": pair.identifier,
                "family": pair.family,
                "engine": pair.engine,
                "java_benchmark": pair.java_benchmark,
                "java_parameters": ";".join(f"{name}={value}" for name, value in pair.java_parameters),
                "native_benchmark": pair.native_benchmark,
            })


def exact_filter(values):
    return "^(" + "|".join(re.escape(value) for value in sorted(values)) + ")$"


def expected_unpaired_java_keys():
    return {
        (benchmark_name("BenchmarkRe2SearchExtra", "searchSuccessOnePass"), (("textSize", "16777216"),)),
        (benchmark_name("BenchmarkRe2SearchExtra", "searchSuccess1BitState"), (("textSize", "16777216"),)),
    }


def load_java(path):
    results = {}
    for row in json.loads(Path(path).read_text()):
        parameters = tuple(sorted((name, str(value)) for name, value in row.get("params", {}).items()))
        key = (row["benchmark"], parameters)
        metric = row["primaryMetric"]
        if metric["scoreUnit"] != "ns/op":
            raise ValueError(f"Unexpected Java unit for {key}: {metric['scoreUnit']}")
        samples = [sample for fork in metric["rawData"] for sample in fork]
        if not samples:
            raise ValueError(f"Java result has no raw samples: {key}")
        if key in results:
            raise ValueError(f"Duplicate Java result: {key}")
        results[key] = {
            "median": statistics.median(samples),
            "mean": statistics.mean(samples),
            "cv": statistics.stdev(samples) / statistics.mean(samples) if len(samples) > 1 else 0,
            "samples": len(samples),
        }
    return results


def load_native(path):
    document = json.loads(Path(path).read_text())
    results = {}
    for row in document["benchmarks"]:
        if row.get("run_type") != "aggregate" or row.get("aggregate_name") != "median":
            continue
        if row["time_unit"] != "ns":
            raise ValueError(f"Unexpected native unit for {row['run_name']}: {row['time_unit']}")
        if row["run_name"] in results:
            raise ValueError(f"Duplicate native result: {row['run_name']}")
        results[row["run_name"]] = row["real_time"]
    return results


def geometric_mean(values):
    return math.exp(sum(math.log(value) for value in values) / len(values))


def summarize(java_path, native_before_path, native_after_path, pairs=None, expected_unpaired_java=None):
    pairs = pairs or build_pairs()
    if expected_unpaired_java is None:
        expected_unpaired_java = expected_unpaired_java_keys()
    java = load_java(java_path)
    native_before = load_native(native_before_path)
    native_after = load_native(native_after_path)
    expected_java = {(pair.java_benchmark, pair.java_parameters) for pair in pairs}
    expected_native = {pair.native_benchmark for pair in pairs}
    unexpected = []
    unexpected.extend(f"Java {key}" for key in sorted(set(java) - expected_java - expected_unpaired_java))
    unexpected.extend(f"native before {name}" for name in sorted(set(native_before) - expected_native))
    unexpected.extend(f"native after {name}" for name in sorted(set(native_after) - expected_native))
    if unexpected:
        raise ValueError("Unexpected traditional benchmark results:\n" + "\n".join(unexpected))

    rows = []
    missing = []
    missing.extend(f"unpaired Java {key}" for key in sorted(expected_unpaired_java - set(java)))
    for pair in pairs:
        java_key = (pair.java_benchmark, pair.java_parameters)
        if java_key not in java:
            missing.append(f"Java {java_key}")
            continue
        if pair.native_benchmark not in native_before:
            missing.append(f"native before {pair.native_benchmark}")
            continue
        if pair.native_benchmark not in native_after:
            missing.append(f"native after {pair.native_benchmark}")
            continue
        before = native_before[pair.native_benchmark]
        after = native_after[pair.native_benchmark]
        native_reference = math.sqrt(before * after)
        java_result = java[java_key]
        rows.append({
            "identifier": pair.identifier,
            "family": pair.family,
            "engine": pair.engine,
            "java_benchmark": pair.java_benchmark,
            "java_parameters": ";".join(f"{name}={value}" for name, value in pair.java_parameters),
            "native_benchmark": pair.native_benchmark,
            "java_median_ns": java_result["median"],
            "java_cv": java_result["cv"],
            "java_samples": java_result["samples"],
            "native_before_ns": before,
            "native_after_ns": after,
            "native_drift": abs(after - before) / min(before, after),
            "native_reference_ns": native_reference,
            "slice_native_ratio": java_result["median"] / native_reference,
            "absolute_deficit_ns": java_result["median"] - native_reference,
        })
    if missing:
        raise ValueError("Missing traditional benchmark results:\n" + "\n".join(missing))
    if len(rows) != len(pairs):
        raise ValueError(f"Expected {len(pairs)} pairs, found {len(rows)}")
    return rows


def write_outputs(rows, output_directory):
    output_directory.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0])
    with (output_directory / "rows.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    groups = {}
    for row in rows:
        groups.setdefault((row["family"], row["engine"]), []).append(row)
    summaries = []
    for (family, engine), group in sorted(groups.items()):
        summaries.append({
            "family": family,
            "engine": engine,
            "rows": len(group),
            "geometric_ratio": geometric_mean([row["slice_native_ratio"] for row in group]),
            "median_ratio": statistics.median(row["slice_native_ratio"] for row in group),
            "maximum_ratio": max(row["slice_native_ratio"] for row in group),
            "maximum_native_drift": max(row["native_drift"] for row in group),
        })
    with (output_directory / "summary.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(summaries[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(summaries)

    document = {
        "pair_count": len(rows),
        "all_row_geometric_ratio": geometric_mean([row["slice_native_ratio"] for row in rows]),
        "maximum_native_drift": max(row["native_drift"] for row in rows),
        "rows_over_five_percent_java_cv": sum(row["java_cv"] > 0.05 for row in rows),
        "rows_over_five_percent_native_drift": sum(row["native_drift"] > 0.05 for row in rows),
        "groups": summaries,
    }
    (output_directory / "summary.json").write_text(json.dumps(document, indent=2) + "\n")


def write_unpaired_java(java_path, output_directory, expected_keys=None):
    if expected_keys is None:
        expected_keys = expected_unpaired_java_keys()
    java = load_java(java_path)
    rows = []
    for benchmark, parameters in sorted(expected_keys):
        result = java[(benchmark, parameters)]
        rows.append({
            "java_benchmark": benchmark,
            "java_parameters": ";".join(f"{name}={value}" for name, value in parameters),
            "java_median_ns": result["median"],
            "java_cv": result["cv"],
            "java_samples": result["samples"],
            "reason": "No equivalent pinned native benchmark registration",
        })
    fieldnames = (
        "java_benchmark",
        "java_parameters",
        "java_median_ns",
        "java_cv",
        "java_samples",
        "reason",
    )
    with (output_directory / "unpaired-java.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def merge_shards(shard_directories, output_directory):
    output_directory.mkdir(parents=True, exist_ok=True)
    java_rows = []
    native_before_rows = []
    native_after_rows = []
    for shard_directory in shard_directories:
        java_rows.extend(json.loads((shard_directory / "traditional-java-all.json").read_text()))
        native_before_rows.extend(json.loads(
            (shard_directory / "traditional-native-before.json").read_text())["benchmarks"])
        native_after_rows.extend(json.loads(
            (shard_directory / "traditional-native-after.json").read_text())["benchmarks"])

    java_path = output_directory / "traditional-java-all.json"
    native_before_path = output_directory / "traditional-native-before.json"
    native_after_path = output_directory / "traditional-native-after.json"
    java_path.write_text(json.dumps(java_rows) + "\n")
    native_before_path.write_text(json.dumps({"benchmarks": native_before_rows}) + "\n")
    native_after_path.write_text(json.dumps({"benchmarks": native_after_rows}) + "\n")

    normalized_directory = output_directory / "traditional-normalized"
    rows = summarize(java_path, native_before_path, native_after_path)
    write_outputs(rows, normalized_directory)
    write_unpaired_java(java_path, normalized_directory)
    return rows


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    summarize_parser = subparsers.add_parser("summarize")
    summarize_parser.add_argument("java_results", type=Path)
    summarize_parser.add_argument("native_before", type=Path)
    summarize_parser.add_argument("native_after", type=Path)
    summarize_parser.add_argument("output_directory", type=Path)
    summarize_parser.add_argument("--class-name")

    manifest_parser = subparsers.add_parser("manifest")
    manifest_parser.add_argument("output", type=Path)
    manifest_parser.add_argument("--class-name")

    java_filter_parser = subparsers.add_parser("java-filter")
    java_filter_parser.add_argument("--class-name")
    native_filter_parser = subparsers.add_parser("native-filter")
    native_filter_parser.add_argument("--class-name")

    merge_parser = subparsers.add_parser("merge")
    merge_parser.add_argument("output_directory", type=Path)
    merge_parser.add_argument("shard_directories", type=Path, nargs="+")
    arguments = parser.parse_args()

    if arguments.command == "manifest":
        write_manifest(arguments.output, select_pairs(arguments.class_name))
        return
    if arguments.command == "java-filter":
        benchmarks = {pair.java_benchmark for pair in build_pairs()}
        if arguments.class_name:
            class_prefix = f"io.airlift.slice.re2.{arguments.class_name}."
            benchmarks = {benchmark for benchmark in benchmarks if benchmark.startswith(class_prefix)}
            if not benchmarks:
                raise ValueError(f"No paired benchmarks for class {arguments.class_name}")
        print(exact_filter(benchmarks))
        return
    if arguments.command == "native-filter":
        print(exact_filter({pair.native_benchmark for pair in select_pairs(arguments.class_name)}))
        return
    if arguments.command == "merge":
        rows = merge_shards(arguments.shard_directories, arguments.output_directory)
        print(f"Validated {len(rows)} merged traditional Java/native benchmark pairs")
        return

    pairs = select_pairs(arguments.class_name)
    expected_unpaired_java = select_unpaired_java_keys(arguments.class_name)
    rows = summarize(
        arguments.java_results,
        arguments.native_before,
        arguments.native_after,
        pairs,
        expected_unpaired_java)
    write_outputs(rows, arguments.output_directory)
    write_unpaired_java(arguments.java_results, arguments.output_directory, expected_unpaired_java)
    print(f"Validated {len(rows)} traditional Java/native benchmark pairs")


if __name__ == "__main__":
    main()
