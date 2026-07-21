#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)
GENERATOR="${ROOT_DIR}/target/re2-golden-build/re2_golden"
RESOURCE_DIR="${ROOT_DIR}/src/test/resources/io/airlift/slice/re2/testing"

groups=(
    small_egrep_literals
    big_egrep_literals
    small_egrep_captures
    big_egrep_captures
    complicated
)

"${SCRIPT_DIR}/build.sh"
mkdir -p "${RESOURCE_DIR}"

for group in "${groups[@]}"; do
    resource="${RESOURCE_DIR}/random_agreement_${group}.tsv.gzip"
    "${GENERATOR}" --mode=random_corpus --group="${group}" | gzip -9 -n > "${resource}"
    printf '%s\t%s bytes\t%s\n' \
        "$(shasum -a 256 "${resource}" | awk '{print $1}')" \
        "$(wc -c < "${resource}" | tr -d ' ')" \
        "${resource}"
done
