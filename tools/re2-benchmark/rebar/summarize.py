#!/usr/bin/env python3

import argparse
import csv
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path


BUNDLED_ENGINE = "re2"
PINNED_PORTABLE_ENGINE = "re2/pinned-portable"
PINNED_HOST_ENGINE = "re2/pinned-host-tuned"
PINNED_HOST_BEFORE_ENGINE = "re2/pinned-host-tuned-before"
PINNED_HOST_AFTER_ENGINE = "re2/pinned-host-tuned-after"
SLICE_ENGINE = "slice/re2"
SLICE_OBJECT_ENGINE = "slice/re2-object"
PINNED_RE2_COMMIT = "972a15cedd008d846f1a39b2e88ce48d7f166cbd"
PINNED_REBAR_VERSION = "0.1.0 (rev 463d00f318)"
EXPECTED_WORKLOAD_COUNT = 41

REQUIRED_ENGINES = (
    BUNDLED_ENGINE,
    PINNED_PORTABLE_ENGINE,
    PINNED_HOST_BEFORE_ENGINE,
    PINNED_HOST_AFTER_ENGINE,
    SLICE_ENGINE,
    SLICE_OBJECT_ENGINE,
)
COMPARATORS = (
    (BUNDLED_ENGINE, "slice_to_re2_bundled_ratio"),
    (PINNED_PORTABLE_ENGINE, "slice_to_re2_pinned_portable_ratio"),
    (PINNED_HOST_ENGINE, "slice_to_re2_pinned_host_tuned_ratio"),
)
RATIO_BUCKETS = (
    "faster-than-0.90x",
    "within-0.90x-1.10x",
    "1.10x-1.25x",
    "1.25x-1.50x",
    "1.50x-2.00x",
    "slower-than-2.00x",
)
DRIFT_LIMIT = 0.02
DURATION_PATTERN = re.compile(r"^(?:([0-9]+(?:\.[0-9]*)?|\.[0-9]+)(s|ms|us|ns)|0)$")
DURATION_TO_NS = {
    "s": 1_000_000_000,
    "ms": 1_000_000,
    "us": 1_000,
    "ns": 1,
}

ROW_FIELDS = (
    "name",
    "model",
    "haystack_len",
    "slice_median_ns",
    "slice_object_median_ns",
    "re2_bundled_median_ns",
    "re2_pinned_portable_median_ns",
    "re2_pinned_host_tuned_before_median_ns",
    "re2_pinned_host_tuned_after_median_ns",
    "re2_pinned_host_tuned_bracketed_median_ns",
    "slice_to_re2_bundled_ratio",
    "slice_to_re2_pinned_portable_ratio",
    "slice_to_re2_pinned_host_tuned_ratio",
    "slice_object_to_re2_pinned_host_tuned_ratio",
    "slice_to_slice_object_ratio",
    "native_before_after_drift",
    "slice_coefficient_of_variation",
    "native_before_coefficient_of_variation",
    "native_after_coefficient_of_variation",
    "absolute_deficit_ns",
    "material",
)
MODEL_FIELDS = (
    "model",
    "comparator",
    "row_count",
    "geometric_mean_ratio",
    "median_ratio",
    "min_ratio",
    "max_ratio",
    "wins",
    "parity",
    "losses",
)
BUCKET_FIELDS = ("model", "comparator", "bucket", "row_count")
DRIFT_FIELDS = (
    "name",
    "model",
    "re2_pinned_host_tuned_before_median_ns",
    "re2_pinned_host_tuned_after_median_ns",
    "native_before_after_drift",
)


def parse_duration_ns(value):
    match = DURATION_PATTERN.fullmatch(value.strip())
    if match is None:
        raise ValueError(
            f"Invalid duration {value!r}; expected <decimal>(s|ms|us|ns) or 0")
    if value.strip() == "0":
        return 0.0
    number, unit = match.groups()
    duration_ns = float(number) * DURATION_TO_NS[unit]
    if not math.isfinite(duration_ns):
        raise ValueError(f"Invalid duration {value!r}; value is not finite")
    return duration_ns


def load_measurements(path):
    workloads = defaultdict(dict)
    with path.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        required_columns = {
            "name", "model", "rebar_version", "engine", "engine_version", "err",
            "haystack_len", "median", "mean", "stddev"}
        missing_columns = required_columns - set(reader.fieldnames or ())
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Rebar CSV is missing required columns: {missing}")

        for line_number, measurement in enumerate(reader, start=2):
            engine = measurement["engine"]
            if engine not in REQUIRED_ENGINES:
                continue
            name = measurement["name"]
            if not name:
                raise ValueError(f"Line {line_number} has an empty workload name")
            if engine in workloads[name]:
                raise ValueError(f"Duplicate measurement for workload {name!r}, engine {engine!r}")
            if measurement["err"]:
                raise ValueError(
                    f"Measurement failed for workload {name!r}, engine {engine!r}: "
                    f"{measurement['err']}")
            try:
                median_ns = parse_duration_ns(measurement["median"])
            except ValueError as error:
                raise ValueError(
                    f"Invalid median for workload {name!r}, engine {engine!r}: {error}") from error
            if median_ns <= 0:
                raise ValueError(
                    f"Median must be positive for workload {name!r}, engine {engine!r}")
            try:
                mean_ns = parse_duration_ns(measurement["mean"])
                stddev_ns = parse_duration_ns(measurement["stddev"])
            except ValueError as error:
                raise ValueError(
                    f"Invalid variation for workload {name!r}, engine {engine!r}: {error}") from error
            if mean_ns <= 0:
                raise ValueError(
                    f"Mean must be positive for workload {name!r}, engine {engine!r}")
            workloads[name][engine] = {
                "model": measurement["model"],
                "haystack_len": measurement["haystack_len"],
                "median_ns": median_ns,
                "coefficient_of_variation": stddev_ns / mean_ns,
                "rebar_version": measurement["rebar_version"],
                "engine_version": measurement["engine_version"],
            }
    if not workloads:
        raise ValueError("Rebar CSV contains no campaign measurements")
    return workloads


def validate_workload(name, measurements):
    missing_engines = [engine for engine in REQUIRED_ENGINES if engine not in measurements]
    if missing_engines:
        raise ValueError(
            f"Workload {name!r} is missing required engines: {', '.join(missing_engines)}")

    models = {measurement["model"] for measurement in measurements.values()}
    if len(models) != 1:
        raise ValueError(f"Workload {name!r} has inconsistent models: {', '.join(sorted(models))}")
    haystack_lengths = {measurement["haystack_len"] for measurement in measurements.values()}
    if len(haystack_lengths) != 1:
        lengths = ", ".join(repr(length) for length in sorted(haystack_lengths))
        raise ValueError(f"Workload {name!r} has inconsistent haystack lengths: {lengths}")


def load_workload_manifest(path):
    workloads = {}
    with path.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        required_columns = {
            "name", "model", "native_access", "expected_result",
            "pattern_length", "pattern_sha256", "haystack_length", "haystack_sha256"}
        missing_columns = required_columns - set(reader.fieldnames or ())
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Workload manifest is missing required columns: {missing}")
        for line_number, row in enumerate(reader, start=2):
            name = row["name"]
            if not name:
                raise ValueError(f"Workload manifest line {line_number} has an empty name")
            if name in workloads:
                raise ValueError(f"Workload manifest contains duplicate name {name!r}")
            if row["native_access"] != "true":
                raise ValueError(f"Workload manifest did not enable native access for {name!r}")
            for length_field in ("pattern_length", "haystack_length"):
                try:
                    length = int(row[length_field])
                except ValueError as error:
                    raise ValueError(
                        f"Workload manifest has invalid {length_field} for {name!r}") from error
                if length < 0:
                    raise ValueError(f"Workload manifest has negative {length_field} for {name!r}")
            for hash_field in ("pattern_sha256", "haystack_sha256"):
                if re.fullmatch(r"[0-9a-f]{64}", row[hash_field]) is None:
                    raise ValueError(f"Workload manifest has invalid {hash_field} for {name!r}")
            try:
                int(row["expected_result"])
            except ValueError as error:
                raise ValueError(
                    f"Workload manifest has invalid expected_result for {name!r}") from error
            workloads[name] = row
    if len(workloads) != EXPECTED_WORKLOAD_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_WORKLOAD_COUNT} workload manifest rows but found {len(workloads)}")
    return workloads


def load_engine_manifest(path):
    workloads = defaultdict(dict)
    with path.open(newline="", encoding="utf-8") as input_file:
        for line_number, row in enumerate(csv.reader(input_file), start=1):
            if len(row) != 4:
                raise ValueError(
                    f"Engine manifest line {line_number} has {len(row)} fields instead of 4")
            name, model, engine, version = row
            if engine not in REQUIRED_ENGINES:
                raise ValueError(f"Engine manifest line {line_number} has unexpected engine {engine!r}")
            if engine in workloads[name]:
                raise ValueError(f"Duplicate engine manifest entry for workload {name!r}, engine {engine!r}")
            if not model or not version:
                raise ValueError(f"Engine manifest line {line_number} has missing metadata")
            workloads[name][engine] = (model, version)

    if not workloads:
        raise ValueError("Engine manifest contains no workloads")
    selected_workloads = {}
    for name, engines in workloads.items():
        missing_engines = [engine for engine in REQUIRED_ENGINES if engine not in engines]
        if missing_engines:
            raise ValueError(
                f"Engine manifest workload {name!r} is missing engines: {', '.join(missing_engines)}")
        models = {model for model, _ in engines.values()}
        if len(models) != 1:
            raise ValueError(f"Engine manifest workload {name!r} has inconsistent models")
        selected_workloads[name] = {
            "model": models.pop(),
            "versions": {engine: version for engine, (_, version) in engines.items()},
        }
    return selected_workloads


def validate_campaign_provenance(workloads, manifest, selected_manifest=None):
    measured_names = set(workloads)
    expected_names = set(manifest) if selected_manifest is None else set(selected_manifest)
    if not expected_names <= set(manifest):
        unknown = sorted(expected_names - set(manifest))
        raise ValueError(f"Engine manifest contains workloads absent from workload manifest: {unknown}")
    if measured_names != expected_names:
        missing = sorted(expected_names - measured_names)
        extra = sorted(measured_names - expected_names)
        raise ValueError(
            f"Measurements do not match workload manifest; missing={missing}, extra={extra}")

    for name, measurements in workloads.items():
        validate_workload(name, measurements)
        if measurements[SLICE_ENGINE]["model"] != manifest[name]["model"]:
            raise ValueError(f"Workload {name!r} model differs from manifest")
        if (
                measurements[SLICE_ENGINE]["model"] != "compile" and
                measurements[SLICE_ENGINE]["haystack_len"] != manifest[name]["haystack_length"]):
            raise ValueError(f"Workload {name!r} haystack length differs from manifest")
        if selected_manifest is not None and measurements[SLICE_ENGINE]["model"] != selected_manifest[name]["model"]:
            raise ValueError(f"Workload {name!r} model differs from engine manifest")
        if selected_manifest is not None:
            for engine, measurement in measurements.items():
                if measurement["engine_version"] != selected_manifest[name]["versions"][engine]:
                    raise ValueError(
                        f"Workload {name!r} engine {engine!r} engine version differs from engine manifest")
        rebar_versions = {measurement["rebar_version"] for measurement in measurements.values()}
        if rebar_versions != {PINNED_REBAR_VERSION}:
            raise ValueError(f"Workload {name!r} has unexpected Rebar versions: {sorted(rebar_versions)}")
        for engine in (PINNED_PORTABLE_ENGINE, PINNED_HOST_BEFORE_ENGINE, PINNED_HOST_AFTER_ENGINE):
            version = measurements[engine]["engine_version"]
            if version != PINNED_RE2_COMMIT:
                raise ValueError(f"Workload {name!r} engine {engine!r} has unexpected version {version!r}")
        if "native-access=true" not in measurements[SLICE_ENGINE]["engine_version"]:
            raise ValueError(f"Workload {name!r} primary Slice runner did not report native access")
        if "native-access=false" not in measurements[SLICE_OBJECT_ENGINE]["engine_version"]:
            raise ValueError(f"Workload {name!r} object Slice runner reported native access")


def normalize_rows(workloads, allow_native_drift=False, exclude_native_drift=False):
    if allow_native_drift and exclude_native_drift:
        raise ValueError("Native drift cannot be both allowed and excluded")

    rows = []
    drift_failures = []
    drift_rows = []
    for name, measurements in sorted(workloads.items()):
        validate_workload(name, measurements)
        medians = {engine: measurement["median_ns"] for engine, measurement in measurements.items()}
        host_before = medians[PINNED_HOST_BEFORE_ENGINE]
        host_after = medians[PINNED_HOST_AFTER_ENGINE]
        host_bracketed = math.sqrt(host_before * host_after)
        native_drift = max(host_before, host_after) / min(host_before, host_after) - 1
        if native_drift > DRIFT_LIMIT + 1e-12:
            drift_rows.append({
                "name": name,
                "model": measurements[SLICE_ENGINE]["model"],
                "re2_pinned_host_tuned_before_median_ns": host_before,
                "re2_pinned_host_tuned_after_median_ns": host_after,
                "native_before_after_drift": native_drift,
            })
            if exclude_native_drift:
                continue
            drift_failures.append(
                f"{name}: {PINNED_HOST_BEFORE_ENGINE}={format_number(host_before)}ns, "
                f"{PINNED_HOST_AFTER_ENGINE}={format_number(host_after)}ns, "
                f"drift={native_drift:.6f}")

        slice_median = medians[SLICE_ENGINE]
        slice_object_median = medians[SLICE_OBJECT_ENGINE]
        slice_to_host_ratio = slice_median / host_bracketed
        absolute_deficit_ns = slice_median - host_bracketed
        material = (
            (slice_to_host_ratio > 1.10 and absolute_deficit_ns > 1_000) or
            slice_to_host_ratio > 2 or
            slice_to_host_ratio < 0.5)
        rows.append({
            "name": name,
            "model": measurements[SLICE_ENGINE]["model"],
            "haystack_len": measurements[SLICE_ENGINE]["haystack_len"],
            "slice_median_ns": slice_median,
            "slice_object_median_ns": slice_object_median,
            "re2_bundled_median_ns": medians[BUNDLED_ENGINE],
            "re2_pinned_portable_median_ns": medians[PINNED_PORTABLE_ENGINE],
            "re2_pinned_host_tuned_before_median_ns": host_before,
            "re2_pinned_host_tuned_after_median_ns": host_after,
            "re2_pinned_host_tuned_bracketed_median_ns": host_bracketed,
            "slice_to_re2_bundled_ratio": slice_median / medians[BUNDLED_ENGINE],
            "slice_to_re2_pinned_portable_ratio": slice_median / medians[PINNED_PORTABLE_ENGINE],
            "slice_to_re2_pinned_host_tuned_ratio": slice_to_host_ratio,
            "slice_object_to_re2_pinned_host_tuned_ratio": slice_object_median / host_bracketed,
            "slice_to_slice_object_ratio": slice_median / slice_object_median,
            "native_before_after_drift": native_drift,
            "slice_coefficient_of_variation": measurements[SLICE_ENGINE]["coefficient_of_variation"],
            "native_before_coefficient_of_variation": measurements[PINNED_HOST_BEFORE_ENGINE]["coefficient_of_variation"],
            "native_after_coefficient_of_variation": measurements[PINNED_HOST_AFTER_ENGINE]["coefficient_of_variation"],
            "absolute_deficit_ns": absolute_deficit_ns,
            "material": material,
        })
    if drift_failures and not allow_native_drift:
        details = "\n".join(f"  {failure}" for failure in drift_failures)
        raise ValueError(f"Native before/after drift exceeded 2%:\n{details}")
    if not rows:
        raise ValueError("Native drift exclusion removed every measured workload")
    return rows, drift_rows


def ratio_bucket(ratio):
    if ratio < 0.90:
        return RATIO_BUCKETS[0]
    if ratio <= 1.10:
        return RATIO_BUCKETS[1]
    if ratio <= 1.25:
        return RATIO_BUCKETS[2]
    if ratio <= 1.50:
        return RATIO_BUCKETS[3]
    if ratio <= 2.00:
        return RATIO_BUCKETS[4]
    return RATIO_BUCKETS[5]


def summarize_models(rows):
    ratios_by_model = defaultdict(list)
    for row in rows:
        for comparator, ratio_field in COMPARATORS:
            ratios_by_model[(row["model"], comparator)].append(row[ratio_field])

    summaries = []
    for (model, comparator), ratios in sorted(ratios_by_model.items()):
        summaries.append({
            "model": model,
            "comparator": comparator,
            "row_count": len(ratios),
            "geometric_mean_ratio": math.exp(statistics.fmean(math.log(ratio) for ratio in ratios)),
            "median_ratio": statistics.median(ratios),
            "min_ratio": min(ratios),
            "max_ratio": max(ratios),
            "wins": sum(ratio < 0.90 for ratio in ratios),
            "parity": sum(0.90 <= ratio <= 1.10 for ratio in ratios),
            "losses": sum(ratio > 1.10 for ratio in ratios),
        })
    return summaries


def summarize_buckets(rows):
    counts = defaultdict(int)
    models = sorted({row["model"] for row in rows})
    for row in rows:
        for comparator, ratio_field in COMPARATORS:
            bucket = ratio_bucket(row[ratio_field])
            counts[(row["model"], comparator, bucket)] += 1
            counts[("all", comparator, bucket)] += 1

    return [
        {
            "model": model,
            "comparator": comparator,
            "bucket": bucket,
            "row_count": counts[(model, comparator, bucket)],
        }
        for model in [*models, "all"]
        for comparator, _ in COMPARATORS
        for bucket in RATIO_BUCKETS
    ]


def format_number(value):
    if isinstance(value, float):
        return f"{value:.12g}"
    return str(value)


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: format_number(row[field]) for field in fieldnames})


def reduce_results(
        input_path,
        output_directory,
        workload_manifest=None,
        engine_manifest=None,
        allow_native_drift=False,
        exclude_native_drift=False):
    workloads = load_measurements(input_path)
    if workload_manifest is not None:
        selected_manifest = None if engine_manifest is None else load_engine_manifest(engine_manifest)
        validate_campaign_provenance(
            workloads,
            load_workload_manifest(workload_manifest),
            selected_manifest)
    rows, drift_rows = normalize_rows(
        workloads,
        allow_native_drift,
        exclude_native_drift)
    model_summaries = summarize_models(rows)
    bucket_summaries = summarize_buckets(rows)
    write_csv(output_directory / "rows.csv", ROW_FIELDS, rows)
    write_csv(output_directory / "models.csv", MODEL_FIELDS, model_summaries)
    write_csv(output_directory / "ratio-buckets.csv", BUCKET_FIELDS, bucket_summaries)
    write_csv(output_directory / "native-drift.csv", DRIFT_FIELDS, drift_rows)
    return rows, model_summaries, bucket_summaries


def main():
    parser = argparse.ArgumentParser(description="Reduce a Rebar native comparison CSV")
    parser.add_argument("input", type=Path, help="Raw CSV emitted by rebar measure")
    parser.add_argument("--workload-manifest", type=Path)
    parser.add_argument("--engine-manifest", type=Path)
    drift_group = parser.add_mutually_exclusive_group()
    drift_group.add_argument("--allow-native-drift", action="store_true")
    drift_group.add_argument("--exclude-native-drift", action="store_true")
    parser.add_argument("--output-directory", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        manifest = arguments.workload_manifest.resolve() if arguments.workload_manifest else None
        engine_manifest = arguments.engine_manifest.resolve() if arguments.engine_manifest else None
        reduce_results(
            arguments.input.resolve(),
            arguments.output_directory.resolve(),
            manifest,
            engine_manifest,
            arguments.allow_native_drift,
            arguments.exclude_native_drift)
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
