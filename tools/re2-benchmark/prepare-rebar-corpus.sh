#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)
REBAR_DIR=${REBAR_DIR:-"${ROOT_DIR}/target/rebar-corpus"}
OUTPUT_DIR=${REBAR_CORPUS_DIR:-"${ROOT_DIR}/target/rebar-selected"}
REBAR_EXECUTABLE="${REBAR_DIR}/target/release/rebar"

if [[ ! -x "${REBAR_EXECUTABLE}" ]]; then
    echo "Rebar executable does not exist: ${REBAR_EXECUTABLE}" >&2
    exit 1
fi

mkdir -p "${OUTPUT_DIR}"
while IFS=$'\t' read -r benchmark_name output_file; do
    (cd "${REBAR_DIR}" && "${REBAR_EXECUTABLE}" klv "${benchmark_name}") | gzip -9 > "${OUTPUT_DIR}/${output_file}"
done <<'EOF'
imported/leipzig/tom-sawyer-huckle-fin-prefix-long	imported_leipzig_tom-sawyer-huckle-fin-prefix-long.klv.gz
reported/i787-keywords/ascii	reported_i787-keywords_ascii.klv.gz
wild/url/search	wild_url_search.klv.gz
opt/reverse-inner/no-quadratic-forward	opt_reverse-inner_no-quadratic-forward.klv.gz
unicode/word/boundary-any-english	unicode_word_boundary-any-english.klv.gz
imported/leipzig/word-ending-nn	imported_leipzig_word-ending-nn.klv.gz
unicode/codepoints/any-one	unicode_codepoints_any-one.klv.gz
imported/leipzig/bounded-strings-ending-z	imported_leipzig_bounded-strings-ending-z.klv.gz
unicode/word/around-holmes-english	unicode_word_around-holmes-english.klv.gz
imported/sherlock/line-boundary-sherlock-holmes	imported_sherlock_line-boundary-sherlock-holmes.klv.gz
EOF

echo "Prepared Rebar corpus in ${OUTPUT_DIR}"
