#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)
DEPENDENCY_DIR=${RE2_GOLDEN_DEPENDENCY_DIR:-"${ROOT_DIR}/target/re2-golden-dependencies"}

RE2_REPOSITORY=https://github.com/google/re2.git
RE2_COMMIT=972a15cedd008d846f1a39b2e88ce48d7f166cbd
ABSL_REPOSITORY=https://github.com/abseil/abseil-cpp.git
ABSL_COMMIT=d38452e1ee03523a208362186fd42248ff2609f6

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

mkdir -p "${DEPENDENCY_DIR}"
fetch_commit "${RE2_REPOSITORY}" "${RE2_COMMIT}" "${DEPENDENCY_DIR}/re2"
fetch_commit "${ABSL_REPOSITORY}" "${ABSL_COMMIT}" "${DEPENDENCY_DIR}/abseil-cpp"

printf 'RE2_DIR=%s\n' "${DEPENDENCY_DIR}/re2"
printf 'ABSL_DIR=%s\n' "${DEPENDENCY_DIR}/abseil-cpp"
