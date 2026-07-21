#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd "$(dirname "$0")" && pwd)
repository_root=$(cd "${script_directory}/../../.." && pwd)
rebar_root=${REBAR_ROOT:-"${repository_root}/target/rebar-corpus"}
rebar=${REBAR_EXECUTABLE:-"${rebar_root}/target/release/rebar"}
benchmark_directory=${REBAR_BENCHMARK_DIR:-"${repository_root}/target/rebar-slice-benchmarks"}
workload=${COUNT_PIPELINE_WORKLOAD:-curated/10-bounded-repeat/context}
expected_count=${COUNT_PIPELINE_EXPECTED_COUNT:?COUNT_PIPELINE_EXPECTED_COUNT is required}
memory_megabytes=${COUNT_PIPELINE_MEMORY_MEGABYTES:-8,16,32,64,128}
output_directory=${COUNT_PIPELINE_OUTPUT_DIR:-"${repository_root}/target/count-pipeline"}

mkdir -p "${output_directory}"
klv_file="${output_directory}/workload.klv"
"${rebar}" klv \
    -d "${benchmark_directory}" \
    --max-iters "${COUNT_PIPELINE_ITERATIONS:-5}" \
    --max-warmup-iters "${COUNT_PIPELINE_WARMUP_ITERATIONS:-3}" \
    --max-time "${COUNT_PIPELINE_MAXIMUM_TIME:-5s}" \
    --max-warmup-time "${COUNT_PIPELINE_WARMUP_TIME:-5s}" \
    "${workload}" \
    > "${klv_file}"

printf 'memory_megabytes,maximum_memory_bytes,duration_ns,count,cache_resets,state_count,cache_entries,state_budget_bytes,available_state_bytes,byte_scan_fallbacks,absolute_pointer_bytes,absolute_pointer_transitions,paired_bytes,paired_row_selections\n' \
    > "${output_directory}/java.csv"
IFS=',' read -r -a memory_sizes <<< "${memory_megabytes}"
for memory_size in "${memory_sizes[@]}"; do
    memory_bytes=$((memory_size * 1024 * 1024))
    REBAR_NATIVE_ACCESS=${REBAR_NATIVE_ACCESS:-enabled} \
    REBAR_HEAP_SIZE=${REBAR_HEAP_SIZE:-8g} \
        "${script_directory}/run-slice.sh" "--count-pipeline=${memory_bytes}" \
        < "${klv_file}" \
        | tail -n +2 \
        | awk -v memory_size="${memory_size}" '{print memory_size "," $0}' \
        >> "${output_directory}/java.csv"
done

awk -F, -v expected_count="${expected_count}" \
    'NR > 1 && $4 != expected_count {exit 1}' \
    "${output_directory}/java.csv" || {
        echo "Java count did not equal ${expected_count}" >&2
        exit 1
    }

python3 - "${klv_file}" "${output_directory}/pattern.txt" "${output_directory}/haystack.bin" <<'PY'
import pathlib
import sys


def read_items(data):
    position = 0
    while position < len(data):
        key_end = data.index(b":", position)
        length_end = data.index(b":", key_end + 1)
        key = data[position:key_end].decode("ascii")
        length = int(data[key_end + 1:length_end])
        value_start = length_end + 1
        value_end = value_start + length
        if data[value_end:value_end + 1] != b"\n":
            raise ValueError(f"invalid KLV item: {key}")
        yield key, data[value_start:value_end]
        position = value_end + 1


items = list(read_items(pathlib.Path(sys.argv[1]).read_bytes()))
patterns = [value for key, value in items if key == "pattern"]
haystacks = [value for key, value in items if key == "haystack"]
if len(patterns) != 1 or len(haystacks) != 1:
    raise ValueError("count diagnostics requires one pattern and one haystack")
pathlib.Path(sys.argv[2]).write_bytes(patterns[0])
pathlib.Path(sys.argv[3]).write_bytes(haystacks[0])
PY

{
    printf 'workload=%s\n' "${workload}"
    printf 'expected_count=%s\n' "${expected_count}"
    printf 'pattern_sha256=%s\n' "$(sha256sum "${output_directory}/pattern.txt" | awk '{print $1}')"
    printf 'haystack_sha256=%s\n' "$(sha256sum "${output_directory}/haystack.bin" | awk '{print $1}')"
    printf 'haystack_bytes=%s\n' "$(wc -c < "${output_directory}/haystack.bin" | tr -d ' ')"
} > "${output_directory}/manifest.txt"

native_diagnostics=${RE2_COUNT_DIAGNOSTICS:-"${repository_root}/target/re2-golden-build/re2_count_diagnostics"}
if [[ -x "${native_diagnostics}" ]]; then
    printf 'memory_megabytes,iteration,duration_ns,count,cache_resets,total_states_at_reset,maximum_states_at_reset,state_budget_bytes\n' \
        > "${output_directory}/native.csv"
    for memory_size in "${memory_sizes[@]}"; do
        memory_bytes=$((memory_size * 1024 * 1024))
        "${native_diagnostics}" \
            "$(cat "${output_directory}/pattern.txt")" \
            "${output_directory}/haystack.bin" \
            "${memory_bytes}" \
            "${COUNT_PIPELINE_NATIVE_ITERATIONS:-5}" \
            | tail -n +2 \
            | awk -v memory_size="${memory_size}" '{print memory_size "," $0}' \
            >> "${output_directory}/native.csv"
    done
    awk -F, -v expected_count="${expected_count}" \
        'NR > 1 && $4 != expected_count {exit 1}' \
        "${output_directory}/native.csv" || {
            echo "Native count did not equal ${expected_count}" >&2
            exit 1
        }
fi

echo "${output_directory}"
