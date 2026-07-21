#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
slice_directory=$(cd "${script_directory}/../../.." && pwd)
result_root=${RESULT_ROOT:-"${slice_directory}/benchmark-results/re2-engineering"}
session_directory="${result_root}/$(date -u +%Y%m%dT%H%M%SZ)-capture-pipeline"
selected_shards=${CAPTURE_PIPELINE_SHARDS:-veryl,unicode,date,split-big-2}
mkdir -p "${session_directory}"

pids=()
labels=()

cleanup()
{
    local status=$?
    if [[ ${status} -ne 0 ]]; then
        for pid in "${pids[@]}"; do
            kill "${pid}" >/dev/null 2>&1 || true
        done
    fi
}
trap cleanup EXIT INT TERM

start_shard()
{
    local label=$1
    local workload=$2
    local stages=${CAPTURE_PIPELINE_STAGES_OVERRIDE:-$3}

    if [[ ",${selected_shards}," != *",${label},"* ]]; then
        return
    fi
    labels+=("${label}")
    CAMPAIGN_MODE=capture-pipeline \
    RESULT_ROOT="${session_directory}/${label}" \
    INTEL_INSTANCE_TYPE=${INTEL_INSTANCE_TYPE:-c8i.2xlarge} \
    ARM_INSTANCE_TYPE=${ARM_INSTANCE_TYPE:-c8g.2xlarge} \
    BENCHMARK_HEAP_SIZE=${BENCHMARK_HEAP_SIZE:-8g} \
    CAPTURE_PIPELINE_WORKLOAD="${workload}" \
    CAPTURE_PIPELINE_STAGES="${stages}" \
    TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-3600} \
        "${script_directory}/run-campaign.sh" > "${session_directory}/${label}.log" 2>&1 &
    pids+=("$!")
}

start_shard veryl 'curated/05-lexer-veryl/single' 'control,setup,forward,reverse,capture,direct-capture,composed,result'
start_shard unicode 'curated/07-unicode-character-data/parse-line' 'control,setup,forward,reverse,capture,direct-capture,composed,result'
start_shard date 'curated/03-date/ascii' 'control,setup,forward,reverse,composed,result'
start_shard split-big-2 'synthetic/split-big-2' 'control,setup,forward,reverse,capture,direct-capture,bit-state-capture,direct-bit-state-capture,composed,result'

failed=0
for index in "${!pids[@]}"; do
    if ! wait "${pids[index]}"; then
        echo "Shard ${labels[index]} failed; inspect ${session_directory}/${labels[index]}.log" >&2
        failed=1
    fi
done
if [[ ${failed} -ne 0 ]]; then
    exit 1
fi

for architecture in intel arm; do
    output="${session_directory}/${architecture}-capture-pipeline.csv"
    first=true
    for label in "${labels[@]}"; do
        summary=$(find "${session_directory}/${label}" -path "*/${architecture}/re2-results/capture-pipeline/summary.csv" -type f -print -quit)
        if [[ -z "${summary}" ]]; then
            echo "Missing ${architecture} summary for ${label}" >&2
            exit 1
        fi
        if ${first}; then
            cat "${summary}" > "${output}"
            first=false
        else
            tail -n +2 "${summary}" >> "${output}"
        fi
    done
done

echo "Capture-pipeline campaign complete: ${session_directory}"
