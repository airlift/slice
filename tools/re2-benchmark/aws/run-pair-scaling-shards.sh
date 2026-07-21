#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SLICE_DIR=$(cd "${SCRIPT_DIR}/../../.." && pwd)
RESULT_ROOT=${RESULT_ROOT:-"${SLICE_DIR}/benchmark-results/re2-engineering"}
SESSION_ID=$(date -u +%Y%m%dT%H%M%SZ)
SESSION_DIR="${RESULT_ROOT}/${SESSION_ID}-pair-scaling-shards"
mkdir -p "${SESSION_DIR}"

PIDS=()
LABELS=()

cleanup()
{
    local status=$?
    if [[ ${status} -ne 0 ]]; then
        for pid in "${PIDS[@]}"; do
            kill "${pid}" >/dev/null 2>&1 || true
        done
    fi
}
trap cleanup EXIT INT TERM

start_shard()
{
    local class_count=$1
    local state_counts=$2
    local state_label=$3
    local label="class-${class_count}-${state_label}"

    LABELS+=("${label}")
    CAMPAIGN_MODE=dfa-pair-scaling \
    RESULT_ROOT="${SESSION_DIR}/${label}" \
    INTEL_INSTANCE_TYPE=${INTEL_INSTANCE_TYPE:-c8i.xlarge} \
    ARM_INSTANCE_TYPE=${ARM_INSTANCE_TYPE:-c8g.xlarge} \
    BENCHMARK_HEAP_SIZE=${BENCHMARK_HEAP_SIZE:-2g} \
    PAIR_SCALING_CLASS_COUNTS="${class_count}" \
    PAIR_SCALING_STATE_COUNTS="${state_counts}" \
    TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-3600} \
        "${SCRIPT_DIR}/run-campaign.sh" > "${SESSION_DIR}/${label}.log" 2>&1 &
    PIDS+=("$!")
}

for class_count in 16 29 64; do
    start_shard "${class_count}" "4,8,16,32" small
    start_shard "${class_count}" "64,128,256" large
done

failed=0
for index in "${!PIDS[@]}"; do
    if ! wait "${PIDS[index]}"; then
        echo "Shard ${LABELS[index]} failed; inspect ${SESSION_DIR}/${LABELS[index]}.log" >&2
        failed=1
    fi
done
if [[ ${failed} -ne 0 ]]; then
    exit 1
fi

for architecture in intel arm; do
    result_files=()
    for label in "${LABELS[@]}"; do
        while IFS= read -r result_file; do
            result_files+=("${result_file}")
        done < <(find "${SESSION_DIR}/${label}" -path "*/${architecture}/re2-results/slice-dfa-pair-scaling.json" -type f)
    done
    if [[ ${#result_files[@]} -ne ${#LABELS[@]} ]]; then
        echo "Expected ${#LABELS[@]} ${architecture} result files, found ${#result_files[@]}" >&2
        exit 1
    fi
    jq -s 'add' "${result_files[@]}" > "${SESSION_DIR}/${architecture}-pair-scaling.json"
done

echo "Sharded pair-scaling campaign complete: ${SESSION_DIR}"
