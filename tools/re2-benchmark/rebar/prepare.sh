#!/usr/bin/env bash

set -euo pipefail

readonly pinned_rebar_commit=463d00f31887e84c38467805b9e3122c314b9521
repo_root=$(cd "$(dirname "$0")/../../.." && pwd)
rebar_root=${1:-"$repo_root/target/rebar-corpus"}
output=${2:-"$repo_root/target/rebar-slice-benchmarks"}
portable_native_root=${3:-"$repo_root/target/rebar-native-portable"}
tuned_native_root=${4:-"$repo_root/target/rebar-native-tuned"}
comparator_order=${REBAR_COMPARATOR_ORDER:-forward}

case "$comparator_order" in
    forward | reverse) ;;
    *)
        echo "REBAR_COMPARATOR_ORDER must be forward or reverse: $comparator_order" >&2
        exit 1
        ;;
esac

if [[ ! -d "$rebar_root/.git" ]]; then
    echo "Rebar checkout not found: $rebar_root" >&2
    exit 1
fi
actual_commit=$(git -C "$rebar_root" rev-parse HEAD)
if [[ "$actual_commit" != "$pinned_rebar_commit" ]]; then
    echo "Expected Rebar $pinned_rebar_commit but found $actual_commit" >&2
    exit 1
fi

rm -rf "$output"
mkdir -p "$output"
cp "$rebar_root/benchmarks/engines.toml" "$output/engines.toml"
cp -R "$rebar_root/benchmarks/definitions" "$output/definitions"
ln -s "$rebar_root/benchmarks/haystacks" "$output/haystacks"
ln -s "$rebar_root/benchmarks/regexes" "$output/regexes"

REBAR_NATIVE_CWD="$rebar_root/engines/re2" perl -pi -e '
    $is_re2 = 0 if /^\[\[engine\]\]/;
    $is_re2 = 1 if /^\s+name = "re2"$/;
    if ($is_re2 && /^\s+cwd = /) {
        $_ = qq{  cwd = "$ENV{REBAR_NATIVE_CWD}"\n};
        $is_re2 = 0;
    }
' "$output/engines.toml"

# Preserve only the curated native RE2 intersection and order the duplicate
# host-tuned controls around the primary Slice runner. The confirmation run
# reverses every comparator while retaining the native bracket.
if [[ "$comparator_order" == forward ]]; then
    find "$output/definitions/curated" -type f -name '*.toml' -exec \
        perl -0pi -e "s/^([ \\t]*)'re2',\\n/\$1're2\\/pinned-host-tuned-before',\\n\$1'slice\\/re2',\\n\$1're2\\/pinned-host-tuned-after',\\n\$1're2',\\n\$1're2\\/pinned-portable',\\n\$1'slice\\/re2-object',\\n/gm" {} +
else
    find "$output/definitions/curated" -type f -name '*.toml' -exec \
        perl -0pi -e "s/^([ \\t]*)'re2',\\n/\$1'slice\\/re2-object',\\n\$1're2\\/pinned-portable',\\n\$1're2',\\n\$1're2\\/pinned-host-tuned-after',\\n\$1'slice\\/re2',\\n\$1're2\\/pinned-host-tuned-before',\\n/gm" {} +
fi

cat >> "$output/engines.toml" <<EOF

# Slice's byte-oriented Java port of RE2.
[[engine]]
  name = "slice/re2"
  cwd = "$repo_root"
  [engine.version]
    bin = "tools/re2-benchmark/rebar/run-slice.sh"
    args = ["--version"]
    envs = [{ name = "REBAR_NATIVE_ACCESS", value = "enabled" }]
  [engine.run]
    bin = "tools/re2-benchmark/rebar/run-slice.sh"
    envs = [
      { name = "REBAR_HEAP_SIZE", value = "8g" },
      { name = "REBAR_NATIVE_ACCESS", value = "enabled" },
    ]
  [[engine.dependency]]
    bin = "java"
    args = ["--version"]
  [[engine.build]]
    bin = "tools/re2-benchmark/rebar/build-slice.sh"

# Slice's portable object-row control.
[[engine]]
  name = "slice/re2-object"
  cwd = "$repo_root"
  [engine.version]
    bin = "tools/re2-benchmark/rebar/run-slice.sh"
    args = ["--version"]
    envs = [{ name = "REBAR_NATIVE_ACCESS", value = "disabled" }]
  [engine.run]
    bin = "tools/re2-benchmark/rebar/run-slice.sh"
    envs = [
      { name = "REBAR_HEAP_SIZE", value = "8g" },
      { name = "REBAR_NATIVE_ACCESS", value = "disabled" },
    ]

# Exact pinned-upstream native control built with portable release flags.
[[engine]]
  name = "re2/pinned-portable"
  cwd = "$portable_native_root"
  [engine.version]
    bin = "./target/release/main"
    args = ["--version"]
  [engine.run]
    bin = "./target/release/main"

# Identical host-tuned binaries bracket Slice to expose host drift.
[[engine]]
  name = "re2/pinned-host-tuned-before"
  cwd = "$tuned_native_root"
  [engine.version]
    bin = "./target/release/main"
    args = ["--version"]
  [engine.run]
    bin = "./target/release/main"

[[engine]]
  name = "re2/pinned-host-tuned-after"
  cwd = "$tuned_native_root"
  [engine.version]
    bin = "./target/release/main"
    args = ["--version"]
  [engine.run]
    bin = "./target/release/main"
EOF

echo "$output"
