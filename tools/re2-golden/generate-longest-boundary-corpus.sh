#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)
GENERATOR="${ROOT_DIR}/target/re2-golden-build/re2_golden"
RESOURCE_DIR="${ROOT_DIR}/src/test/resources/io/airlift/slice/re2/testing"
RESOURCE="${RESOURCE_DIR}/native_longest_boundary.jsonl"
INPUT=$(mktemp "${TMPDIR:-/tmp}/re2-longest-boundary.XXXXXX")
OUTPUT=$(mktemp "${TMPDIR:-/tmp}/re2-longest-boundary-output.XXXXXX")
trap 'rm -f "${INPUT}" "${OUTPUT}"' EXIT

emit_case()
{
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$@" >> "${INPUT}"
}

emit_pair()
{
    local id=$1
    local pattern=$2
    local text=$3
    local start=$4
    local end=$5
    local anchor=$6
    local encoding=$7
    local groups=$8

    emit_case "${id}-${encoding}-first" "${pattern}" "${text}" "${start}" "${end}" "${anchor}" "${encoding}" false "${groups}"
    emit_case "${id}-${encoding}-longest" "${pattern}" "${text}" "${start}" "${end}" "${anchor}" "${encoding}" true "${groups}"
}

# These pairs force the public option through cases where leftmost-first and
# leftmost-longest differ, plus controls where anchoring makes them equivalent.
while IFS=$'\t' read -r id pattern text start end anchor groups; do
    [[ -z "${id}" ]] && continue
    for encoding in utf8 latin1; do
        emit_pair "long-${id}" "${pattern}" "${text}" "${start}" "${end}" "${anchor}" "${encoding}" "${groups}"
    done
done <<'CASES'
alternation	28617c616129	6161	0	2	unanchored	2
alternation-start	28617c616129	6161	0	2	start	2
alternation-offset	28617c616129	7a61617a	0	4	unanchored	2
alternation-window	28617c616129	7a61617a	1	3	unanchored	2
alternation-full-control	28617c616129	6161	0	2	both	2
reluctant-star	612a3f	616161	0	3	unanchored	1
reluctant-capture	28612b3f29	616161	0	3	unanchored	2
reluctant-repetition	2828617c6161292b3f29	616161	0	3	unanchored	3
prefix-alternation	2861627c61626329	7a6162637a	0	5	unanchored	2
empty-alternation	287c616129	6161	0	2	unanchored	2
CASES

# Explicit windows preserve the distinction between API anchoring and regexp
# anchors, including boundaries with context outside the requested range.
while IFS=$'\t' read -r id pattern text start end anchor groups; do
    [[ -z "${id}" ]] && continue
    for encoding in utf8 latin1; do
        emit_pair "anchor-${id}" "${pattern}" "${text}" "${start}" "${end}" "${anchor}" "${encoding}" "${groups}"
    done
done <<'CASES'
unanchored-middle	61	786179	0	3	unanchored	1
start-rejects-middle	61	786179	0	3	start	1
both-rejects-context	61	786179	0	3	both	1
window-unanchored	61	786179	1	2	unanchored	1
window-start	61	786179	1	2	start	1
window-both	61	786179	1	2	both	1
begin-text-outside-window	5e61	7861	1	2	unanchored	1
end-text-outside-window	6124	6178	0	1	unanchored	1
absolute-begin-outside-window	5c4161	7861	1	2	unanchored	1
absolute-end-outside-window	615c7a	6178	0	1	unanchored	1
word-context-outside-window	5c62615c62	786179	1	2	both	1
multiline-window	283f6d3a5e612429	780a610a79	2	3	both	1
CASES

# Each row is evaluated in both encodings. UTF-8 must retain rune boundaries;
# Latin1 treats the same input as independent bytes.
while IFS=$'\t' read -r id pattern text start end anchor groups; do
    [[ -z "${id}" ]] && continue
    for encoding in utf8 latin1; do
        emit_case "byte-${id}-${encoding}" "${pattern}" "${text}" "${start}" "${end}" "${anchor}" "${encoding}" false "${groups}"
    done
done <<'CASES'
dot-valid-rune	2e	c3a9	0	2	both	1
dot-valid-leading-byte	2e	c3a9	0	1	both	1
dot-valid-continuation-byte	2e	c3a9	1	2	both	1
dot-lone-continuation	2e	80	0	1	both	1
dot-overlong	2e	c0af	0	2	both	1
dot-truncated	2e	e282	0	2	both	1
dot-invalid-four-byte	2e	f0808080	0	4	both	1
dot-invalid-middle	2e	61ff62	1	2	both	1
dot-valid-before-invalid	2e	61ff62	0	2	unanchored	1
dot-valid-after-invalid	2e	61ff62	1	3	unanchored	1
dotall-invalid	283f733a2e29	ff	0	1	both	1
dotall-truncated	283f733a2e29	e282	0	2	unanchored	1
any-byte-invalid	5c43	ff	0	1	both	1
any-byte-valid-leading	5c43	c3a9	0	1	both	1
any-byte-valid-continuation	5c43	c3a9	1	2	both	1
any-byte-pair-valid-rune	5c435c43	c3a9	0	2	both	1
any-byte-star-invalid	5c432a	61ff62	0	3	both	1
two-dots-valid-rune	2e2e	c3a9	0	2	both	1
optional-dot-empty-window	282e293f	c3a9	1	1	both	2
optional-dot-valid-rune	282e293f	c3a9	0	2	both	2
negated-class-invalid	5b5e615d	ff	0	1	both	1
negated-class-valid-rune	5b5e615d	c3a9	0	2	both	1
literal-valid-rune	c3a9	c3a9	0	2	both	1
literal-valid-rune-window	c3a9	78c3a979	1	3	both	1
literal-valid-rune-split-start	c3a9	78c3a979	2	3	both	1
literal-valid-rune-split-end	c3a9	78c3a979	1	2	both	1
empty-at-invalid-boundary	-	c3a9	1	1	both	1
nul-byte	5c43	00	0	1	both	1
newline-dot	2e	0a	0	1	both	1
newline-dotall	283f733a2e29	0a	0	1	both	1
CASES

"${SCRIPT_DIR}/build.sh"
mkdir -p "${RESOURCE_DIR}"
"${GENERATOR}" --mode=match < "${INPUT}" > "${OUTPUT}"
mv "${OUTPUT}" "${RESOURCE}"

printf '%s\t%s cases\t%s bytes\t%s\n' \
    "$(shasum -a 256 "${RESOURCE}" | awk '{print $1}')" \
    "$(wc -l < "${RESOURCE}" | tr -d ' ')" \
    "$(wc -c < "${RESOURCE}" | tr -d ' ')" \
    "${RESOURCE}"
