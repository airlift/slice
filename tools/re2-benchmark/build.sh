#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)
DEPENDENCY_DIR=${RE2_GOLDEN_DEPENDENCY_DIR:-"${ROOT_DIR}/target/re2-golden-dependencies"}
BUILD_DIR=${RE2_BENCHMARK_BUILD_DIR:-"${ROOT_DIR}/target/re2-benchmark-build"}
NATIVE_TUNING=${RE2_NATIVE_TUNING:-generic}
BENCHMARK_TARGETS=${RE2_BENCHMARK_TARGETS:-regexp_benchmark}

case "${NATIVE_TUNING}" in
    generic)
        RELEASE_FLAGS='-O3 -DNDEBUG'
        ;;
    host)
        case "$(uname -m)" in
            x86_64) RELEASE_FLAGS='-O3 -DNDEBUG -march=native' ;;
            aarch64 | arm64) RELEASE_FLAGS='-O3 -DNDEBUG -mcpu=native' ;;
            *) echo "Unsupported architecture for host tuning: $(uname -m)" >&2; exit 1 ;;
        esac
        ;;
    *) echo "Unsupported native tuning mode: ${NATIVE_TUNING}" >&2; exit 1 ;;
esac

"${SCRIPT_DIR}/fetch-dependencies.sh" >/dev/null
RE2_DIR=${RE2_DIR:-"${DEPENDENCY_DIR}/re2"}
ABSL_DIR=${ABSL_DIR:-"${DEPENDENCY_DIR}/abseil-cpp"}
BENCHMARK_DIR=${BENCHMARK_DIR:-"${DEPENDENCY_DIR}/benchmark"}
GOOGLETEST_DIR=${GOOGLETEST_DIR:-"${DEPENDENCY_DIR}/googletest"}

git -C "${RE2_DIR}" checkout -q -- re2/testing/regexp_benchmark.cc
git -C "${RE2_DIR}" apply "${SCRIPT_DIR}/random-text.patch"

cmake -S "${SCRIPT_DIR}" -B "${BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DCMAKE_CXX_FLAGS_RELEASE="${RELEASE_FLAGS}" \
    -DRE2_DIR="${RE2_DIR}" \
    -DABSL_DIR="${ABSL_DIR}" \
    -DBENCHMARK_DIR="${BENCHMARK_DIR}" \
    -DGOOGLETEST_DIR="${GOOGLETEST_DIR}"
read -r -a benchmark_targets <<<"${BENCHMARK_TARGETS}"
cmake --build "${BUILD_DIR}" --target "${benchmark_targets[@]}" --parallel

printf 'Built %s targets %s with %s\n' "${BUILD_DIR}" "${BENCHMARK_TARGETS}" "${RELEASE_FLAGS}"
