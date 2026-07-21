#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)
DEPENDENCY_DIR=${RE2_GOLDEN_DEPENDENCY_DIR:-"${ROOT_DIR}/target/re2-golden-dependencies"}
BUILD_DIR=${RE2_GOLDEN_BUILD_DIR:-"${ROOT_DIR}/target/re2-golden-build"}

if [[ -z "${RE2_DIR:-}" || -z "${ABSL_DIR:-}" ]]; then
    "${SCRIPT_DIR}/fetch-dependencies.sh"
fi

RE2_DIR=${RE2_DIR:-"${DEPENDENCY_DIR}/re2"}
ABSL_DIR=${ABSL_DIR:-"${DEPENDENCY_DIR}/abseil-cpp"}

cmake -S "${SCRIPT_DIR}" -B "${BUILD_DIR}" \
    -DRE2_DIR="${RE2_DIR}" \
    -DABSL_DIR="${ABSL_DIR}" \
    -DCMAKE_BUILD_TYPE=Release
cmake --build "${BUILD_DIR}" --target re2_golden --parallel

printf 'Built %s/re2_golden\n' "${BUILD_DIR}"
