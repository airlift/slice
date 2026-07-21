#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)
DEPENDENCY_DIR=${RE2_GOLDEN_DEPENDENCY_DIR:-"${ROOT_DIR}/target/re2-golden-dependencies"}

BENCHMARK_REPOSITORY=https://github.com/google/benchmark.git
BENCHMARK_COMMIT=192ef10025eb2c4cdd392bc502f0c852196baa48
GOOGLETEST_REPOSITORY=https://github.com/google/googletest.git
GOOGLETEST_COMMIT=52eb8108c5bdec04579160ae17225d66034bd723

fetch_commit()
{
    local repository=$1
    local commit=$2
    local directory=$3

    if [[ ! -d "${directory}/.git" ]]; then
        rm -rf "${directory}"
        git init -q "${directory}"
        git -C "${directory}" remote add origin "${repository}"
    fi

    if ! git -C "${directory}" cat-file -e "${commit}^{commit}" 2>/dev/null; then
        git -C "${directory}" fetch -q --depth=1 origin "${commit}"
    fi
    git -C "${directory}" checkout -q --detach "${commit}"

    local actual_commit
    actual_commit=$(git -C "${directory}" rev-parse HEAD)
    if [[ "${actual_commit}" != "${commit}" ]]; then
        echo "Expected ${commit}, found ${actual_commit} in ${directory}" >&2
        exit 1
    fi
}

"${ROOT_DIR}/tools/re2-golden/fetch-dependencies.sh" >/dev/null
fetch_commit "${BENCHMARK_REPOSITORY}" "${BENCHMARK_COMMIT}" "${DEPENDENCY_DIR}/benchmark"
fetch_commit "${GOOGLETEST_REPOSITORY}" "${GOOGLETEST_COMMIT}" "${DEPENDENCY_DIR}/googletest"

printf 'RE2_DIR=%s\n' "${DEPENDENCY_DIR}/re2"
printf 'ABSL_DIR=%s\n' "${DEPENDENCY_DIR}/abseil-cpp"
printf 'BENCHMARK_DIR=%s\n' "${DEPENDENCY_DIR}/benchmark"
printf 'GOOGLETEST_DIR=%s\n' "${DEPENDENCY_DIR}/googletest"
