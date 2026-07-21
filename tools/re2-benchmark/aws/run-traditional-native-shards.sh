#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SLICE_DIR=$(cd "${SCRIPT_DIR}/../../.." && pwd)
RESULT_ROOT=${RESULT_ROOT:-"${SLICE_DIR}/benchmark-results/re2-engineering"}
SESSION_ID="$(date -u +%Y%m%dT%H%M%SZ)-traditional-shards"
SESSION_DIR="${RESULT_ROOT}/${SESSION_ID}"
MAX_PARALLEL_SHARDS=${MAX_PARALLEL_SHARDS:-3}
INTEL_INSTANCE_TYPE=${INTEL_INSTANCE_TYPE:-c8i.2xlarge}
ARM_INSTANCE_TYPE=${ARM_INSTANCE_TYPE:-c8g.2xlarge}
BENCHMARK_CLASSES=(
    BenchmarkRe2Search
    BenchmarkRe2SearchNfa
    BenchmarkRe2SearchExtra
    BenchmarkRe2Parse
    BenchmarkRe2FullMatch
    BenchmarkRe2Practical
    BenchmarkRe2Misc
)
PIDS=()

if [[ ! ${MAX_PARALLEL_SHARDS} =~ ^[1-7]$ ]]; then
    echo "MAX_PARALLEL_SHARDS must be between 1 and 7: ${MAX_PARALLEL_SHARDS}" >&2
    exit 1
fi

mkdir -p "${SESSION_DIR}"

stop_campaigns()
{
    local process_id
    for process_id in "${PIDS[@]}"; do
        kill -TERM "${process_id}" 2>/dev/null || true
    done
    wait || true
}

terminate_campaigns()
{
    trap - EXIT INT TERM
    stop_campaigns
    exit 130
}

cleanup_campaigns()
{
    status=$?
    trap - EXIT INT TERM
    stop_campaigns
    exit "${status}"
}
trap cleanup_campaigns EXIT
trap terminate_campaigns INT TERM

wait_for_campaigns()
{
    local failed=0
    local process_id
    for process_id in "${PIDS[@]}"; do
        wait "${process_id}" || failed=1
    done
    PIDS=()
    return "${failed}"
}

for benchmark_class in "${BENCHMARK_CLASSES[@]}"; do
    shard_root="${SESSION_DIR}/${benchmark_class}"
    mkdir -p "${shard_root}"
    CAMPAIGN_MODE=traditional-native-comparison \
        TRADITIONAL_BENCHMARK_CLASS="${benchmark_class}" \
        TIMEOUT_SECONDS="${CAMPAIGN_TIMEOUT_SECONDS:-21600}" \
        INTEL_INSTANCE_TYPE="${INTEL_INSTANCE_TYPE}" \
        ARM_INSTANCE_TYPE="${ARM_INSTANCE_TYPE}" \
        RESULT_ROOT="${shard_root}" \
        "${SCRIPT_DIR}/run-campaign.sh" \
        > "${shard_root}/campaign.log" 2>&1 &
    PIDS+=("$!")
    sleep 3
    if [[ ${#PIDS[@]} -eq ${MAX_PARALLEL_SHARDS} ]]; then
        wait_for_campaigns || {
            echo "At least one traditional benchmark shard failed" >&2
            exit 1
        }
    fi
done

if ! wait_for_campaigns; then
    echo "At least one traditional benchmark shard failed" >&2
    exit 1
fi

expected_commit=
expected_content=
for benchmark_class in "${BENCHMARK_CLASSES[@]}"; do
    shard_directory=$(find "${SESSION_DIR}/${benchmark_class}" -mindepth 1 -maxdepth 1 -type d)
    shard_commit=$(awk -F= '$1 == "slice_commit" {print $2}' "${shard_directory}/session.txt")
    shard_content=$(awk -F= '$1 == "slice_content_sha256" {print $2}' "${shard_directory}/session.txt")
    if [[ -z "${expected_commit}" ]]; then
        expected_commit=${shard_commit}
        expected_content=${shard_content}
    fi
    if [[ "${shard_commit}" != "${expected_commit}" || "${shard_content}" != "${expected_content}" ]]; then
        echo "Traditional shards do not use one source snapshot" >&2
        exit 1
    fi
done

for architecture in intel arm; do
    shard_results=()
    for benchmark_class in "${BENCHMARK_CLASSES[@]}"; do
        shard_directory=$(find "${SESSION_DIR}/${benchmark_class}" -mindepth 1 -maxdepth 1 -type d)
        shard_results+=("${shard_directory}/${architecture}/re2-results")
    done
    python3 "${SLICE_DIR}/tools/re2-benchmark/traditional/summarize.py" merge \
        "${SESSION_DIR}/merged/${architecture}" \
        "${shard_results[@]}"
done

printf '%s\n' "${SESSION_DIR}"
