#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd "$(dirname "$0")" && pwd)
repository_root=$(cd "${script_directory}/../../.." && pwd)
rebar_root=${REBAR_ROOT:-"${repository_root}/target/rebar-corpus"}
rebar=${REBAR_EXECUTABLE:-"${rebar_root}/target/release/rebar"}
benchmark_directory=${REBAR_BENCHMARK_DIR:-"${repository_root}/target/rebar-slice-benchmarks"}
workload=${CAPTURE_PIPELINE_WORKLOAD:?CAPTURE_PIPELINE_WORKLOAD is required}
stages=${CAPTURE_PIPELINE_STAGES:-control,setup,forward,reverse,capture,composed,result}
output_directory=${CAPTURE_PIPELINE_OUTPUT_DIR:-"${repository_root}/target/capture-pipeline"}
native_diagnostic=${CAPTURE_PIPELINE_NATIVE_DIAGNOSTIC:-}
allow_extended_one_pass=${CAPTURE_PIPELINE_ALLOW_EXTENDED_ONE_PASS:-false}

mkdir -p "${output_directory}"
klv_file="${output_directory}/workload.klv"

if [[ "${workload}" == synthetic/split-big-2 ]]; then
    python3 - "${klv_file}" <<'PY'
import sys


def write_item(output, key, value):
    output.write(f"{key}:{len(value)}:".encode("ascii"))
    output.write(value)
    output.write(b"\n")


with open(sys.argv[1], "wb") as output:
    write_item(output, "name", b"synthetic/split-big-2")
    write_item(output, "model", b"count-captures")
    write_item(output, "case-insensitive", b"false")
    write_item(output, "unicode", b"true")
    write_item(output, "max-iters", b"20")
    write_item(output, "max-warmup-iters", b"20")
    write_item(output, "max-time", b"5000000000")
    write_item(output, "max-warmup-time", b"5000000000")
    write_item(output, "pattern", b"[0-9]+.(.*)")
    write_item(output, "haystack", b"650-253-" + (b"0" * 100_000))
PY
else
    "${rebar}" klv \
        -d "${benchmark_directory}" \
        --max-iters "${CAPTURE_PIPELINE_ITERATIONS:-20}" \
        --max-warmup-iters "${CAPTURE_PIPELINE_WARMUP_ITERATIONS:-20}" \
        --max-time "${CAPTURE_PIPELINE_MAXIMUM_TIME:-5s}" \
        --max-warmup-time "${CAPTURE_PIPELINE_WARMUP_TIME:-5s}" \
        "${workload}" \
        > "${klv_file}"
fi

REBAR_NATIVE_ACCESS=${REBAR_NATIVE_ACCESS:-enabled} \
REBAR_HEAP_SIZE=${REBAR_HEAP_SIZE:-8g} \
    "${script_directory}/run-slice.sh" --capture-pipeline-manifest \
    < "${klv_file}" \
    > "${output_directory}/manifest.txt"

if [[ -n "${native_diagnostic}" ]]; then
    "${native_diagnostic}" --manifest \
        < "${klv_file}" \
        > "${output_directory}/native-manifest.txt"

    route_comparison="${output_directory}/route-comparison.txt"
    : > "${route_comparison}"
    # Capture call and byte counts are informational: Java may reject a line with DFA before
    # extraction, while native RE2 performs extraction inside its monolithic match call.
    for key in name model groups program_instructions program_one_pass one_pass_capture_eligible \
            bit_state_eligible anchored_dfa_skipped bit_state_lists capture_engine public_result; do
        java_value=$(sed -n "s/^${key}=//p" "${output_directory}/manifest.txt")
        native_value=$(sed -n "s/^${key}=//p" "${output_directory}/native-manifest.txt")
        printf '%s,java=%s,native=%s\n' "${key}" "${java_value}" "${native_value}" >> "${route_comparison}"
        if [[ "${java_value}" != "${native_value}" ]]; then
            if [[ "${allow_extended_one_pass}" == true &&
                    "${key}:${java_value}:${native_value}" == "one_pass_capture_eligible:true:false" ]]; then
                continue
            fi
            if [[ "${allow_extended_one_pass}" == true &&
                    "${key}:${java_value}:${native_value}" == "capture_engine:ONE_PASS:BIT_STATE" ]]; then
                continue
            fi
            echo "Java/native capture route differs for ${key}: ${java_value} != ${native_value}" >&2
            exit 1
        fi
    done
fi

raw_results="${output_directory}/raw.csv"
printf 'workload,stage,duration_ns,result\n' > "${raw_results}"
cpu_command=()
if command -v taskset >/dev/null 2>&1; then
    cpu_command=(taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}")
fi

run_stage()
{
    local stage=$1
    local runner_stage=${stage}
    if [[ "${stage}" == public-before || "${stage}" == public-after ]]; then
        runner_stage=public
    fi
    local output_file="${output_directory}/${stage}.csv"
    REBAR_NATIVE_ACCESS=${REBAR_NATIVE_ACCESS:-enabled} \
    REBAR_HEAP_SIZE=${REBAR_HEAP_SIZE:-8g} \
        "${cpu_command[@]}" \
        "${script_directory}/run-slice.sh" --capture-pipeline-stage="${runner_stage}" \
        < "${klv_file}" \
        > "${output_file}"
    awk -F, -v workload="${workload}" -v stage="${stage}" \
        '{print workload "," stage "," $1 "," $2}' \
        "${output_file}" >> "${raw_results}"
}

run_native_stage()
{
    local stage=$1
    local runner_stage=$2
    local output_file="${output_directory}/${stage}.csv"
    "${cpu_command[@]}" \
        "${native_diagnostic}" --stage="${runner_stage}" \
        < "${klv_file}" \
        > "${output_file}"
    awk -F, -v workload="${workload}" -v stage="${stage}" \
        '{print workload "," stage "," $1 "," $2}' \
        "${output_file}" >> "${raw_results}"
}

run_stage public-before
if [[ -n "${native_diagnostic}" ]]; then
    run_native_stage native-public-before public
    if grep -Eq '^capture_engine=(BIT_STATE|.*\+BIT_STATE|BIT_STATE\+.*)$' "${output_directory}/native-manifest.txt"; then
        run_native_stage native-bit-state bit-state
    fi
fi
IFS=',' read -r -a stage_list <<< "${stages}"
for stage in "${stage_list[@]}"; do
    run_stage "${stage}"
done
if [[ -n "${native_diagnostic}" ]]; then
    run_native_stage native-public-after public
fi
run_stage public-after

python3 "${script_directory}/summarize_pipeline.py" \
    "${raw_results}" \
    "${output_directory}/manifest.txt" \
    > "${output_directory}/summary.csv"
