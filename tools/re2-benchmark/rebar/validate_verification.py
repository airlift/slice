#!/usr/bin/env python3

import argparse
import csv
from collections import defaultdict
from pathlib import Path


EXPECTED_ENGINES = {
    "re2",
    "re2/pinned-portable",
    "re2/pinned-host-tuned-before",
    "re2/pinned-host-tuned-after",
    "slice/re2",
    "slice/re2-object",
}
EXPECTED_WORKLOAD_COUNT = 41


def read_manifest(path):
    workloads = defaultdict(dict)
    with path.open(newline="", encoding="utf-8") as input_file:
        for line_number, row in enumerate(csv.reader(input_file), start=1):
            if len(row) != 4:
                raise ValueError(
                    f"Manifest line {line_number} has {len(row)} fields instead of 4")
            name, model, engine, version = row
            if engine not in EXPECTED_ENGINES:
                raise ValueError(f"Manifest line {line_number} has unexpected engine {engine!r}")
            if engine in workloads[name]:
                raise ValueError(f"Duplicate manifest entry for workload {name!r}, engine {engine!r}")
            if not model or not version:
                raise ValueError(f"Manifest line {line_number} has missing metadata")
            workloads[name][engine] = (model, version)

    if not workloads:
        raise ValueError("Manifest contains no workloads")
    for name, engines in workloads.items():
        if set(engines) != EXPECTED_ENGINES:
            missing = sorted(EXPECTED_ENGINES - set(engines))
            raise ValueError(f"Manifest workload {name!r} is missing engines: {missing}")
        if len({model for model, _ in engines.values()}) != 1:
            raise ValueError(f"Manifest workload {name!r} has inconsistent models")
    return workloads


def validate(path, manifest_path=None):
    workloads = defaultdict(dict)
    with path.open(newline="", encoding="utf-8") as input_file:
        for line_number, row in enumerate(csv.reader(input_file), start=1):
            if len(row) != 5:
                raise ValueError(
                    f"Verification line {line_number} has {len(row)} fields instead of 5")
            name, model, engine, version, status = row
            if engine not in EXPECTED_ENGINES:
                raise ValueError(f"Verification line {line_number} has unexpected engine {engine!r}")
            if engine in workloads[name]:
                raise ValueError(f"Duplicate verification for workload {name!r}, engine {engine!r}")
            if not model or not version:
                raise ValueError(f"Verification line {line_number} has missing metadata")
            if status != "OK":
                raise ValueError(
                    f"Verification failed for workload {name!r}, engine {engine!r}: {status}")
            workloads[name][engine] = (model, version)

    expected_workloads = None if manifest_path is None else read_manifest(manifest_path)
    if expected_workloads is None and len(workloads) != EXPECTED_WORKLOAD_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_WORKLOAD_COUNT} verified workloads but found {len(workloads)}")
    if expected_workloads is not None and set(workloads) != set(expected_workloads):
        missing = sorted(set(expected_workloads) - set(workloads))
        unexpected = sorted(set(workloads) - set(expected_workloads))
        raise ValueError(
            f"Verification workloads differ from manifest; missing={missing}, unexpected={unexpected}")
    for name, engines in workloads.items():
        if set(engines) != EXPECTED_ENGINES:
            missing = sorted(EXPECTED_ENGINES - set(engines))
            raise ValueError(f"Workload {name!r} is missing verified engines: {missing}")
        if len({model for model, _ in engines.values()}) != 1:
            raise ValueError(f"Workload {name!r} has inconsistent verification models")
        if expected_workloads is not None and engines != expected_workloads[name]:
            raise ValueError(f"Workload {name!r} verification metadata differs from manifest")


def main():
    parser = argparse.ArgumentParser(description="Validate a Rebar verification CSV")
    parser.add_argument("input", type=Path)
    parser.add_argument("--manifest", type=Path)
    arguments = parser.parse_args()
    try:
        manifest = None if arguments.manifest is None else arguments.manifest.resolve()
        validate(arguments.input.resolve(), manifest)
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
