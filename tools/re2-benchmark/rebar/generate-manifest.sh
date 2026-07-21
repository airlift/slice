#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <rebar-executable> <benchmark-directory> <output-file>" >&2
    exit 1
fi

rebar=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
benchmark_directory=$(cd "$2" && pwd)
output=$(mkdir -p "$(dirname "$3")" && cd "$(dirname "$3")" && pwd)/$(basename "$3")
repo_root=$(cd "$(dirname "$0")/../../.." && pwd)
model_filter='^(?:compile|count|count-spans|count-captures|grep|grep-captures)$'

printf '%s\n' 'name,model,case_insensitive,unicode,pattern_length,pattern_sha256,haystack_length,haystack_sha256,expected_result,result_demand,native_access,required_prefix,prefix_acceleration,one_pass_eligible,bit_state_eligible,forward_dfa_kinds,forward_absolute_pointer_bytes,forward_absolute_pointer_transitions,forward_paired_bytes,forward_paired_selections,forward_cache_resets,reverse_computed,reverse_dfa_kinds,reverse_absolute_pointer_bytes,reverse_absolute_pointer_transitions,reverse_paired_bytes,reverse_paired_selections,reverse_cache_resets' \
    > "$output"

while IFS=, read -r name _; do
    "$rebar" klv -d "$benchmark_directory" "$name" \
        | REBAR_NATIVE_ACCESS=enabled \
        "$repo_root/tools/re2-benchmark/rebar/run-slice.sh" --manifest \
        >> "$output"
done < <(
    "$rebar" measure \
        -d "$benchmark_directory" \
        -e '^slice/re2$' \
        -f '^curated/' \
        -m "$model_filter" \
        --list
)

row_count=$(($(wc -l < "$output") - 1))
if [[ $row_count -ne 41 ]]; then
    echo "Expected 41 manifest rows but found $row_count" >&2
    exit 1
fi
