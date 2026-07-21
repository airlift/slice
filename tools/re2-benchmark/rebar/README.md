# Official Rebar Runner

`RebarRunner` implements the official Rebar KLV protocol over Slice byte inputs.
It supports the `compile`, `count`, `count-spans`, `count-captures`, `grep`, and
`grep-captures` models. `regex-redux` is intentionally excluded because it is a
composite application workload rather than a single compiled-regexp model.
The count and span models request only group-zero boundaries, while the capture
models request every explicit group. This matches the result demand of Rebar's
native RE2 runner instead of charging ordinary counting for capture extraction.

The qualification corpus is pinned to Rebar commit
`463d00f31887e84c38467805b9e3122c314b9521`. Prepare a generated benchmark
directory without modifying the checkout:

```bash
tools/re2-benchmark/rebar/prepare.sh
tools/re2-benchmark/rebar/build-slice.sh
```

The generated directory adds the primary native-access Slice runner, an
object-row Slice control, and exact pinned portable and host-tuned native RE2
runners only to curated definitions that already list native `re2`. This
defines the initial semantic intersection; execution failures must still be
classified rather than silently removed.

`generate-manifest.sh` expands each selected KLV workload and records input
hashes, flags, expected results, result demand, and cold route diagnostics. The
diagnostic replay is outside Rebar's timed process and does not instrument the
engine's hot loops. `summarize.py` validates the complete comparator matrix and
emits per-row, per-model, and ratio-bucket CSV files.

Model-filtered campaigns must pass both the complete workload manifest and the
selected engine manifest to `summarize.py`. The reducer verifies exact input
lengths, input hashes, models, engine versions, and the comparator matrix. It
rejects native before/after drift above 2% by default. Formal multi-session
reduction may use `--exclude-native-drift` to retain independently bracketed
stable workloads and record every rejected workload in `native-drift.csv`.
Diagnostic runs may instead use `--allow-native-drift`; drift-waived rows are
not final qualification evidence.

Build Rebar and its native RE2 runner from the pinned checkout, then measure both
engines from the same corpus and timer:

```bash
cargo build --release --manifest-path target/rebar-corpus/Cargo.toml
target/rebar-corpus/target/release/rebar build \
    -d target/rebar-slice-benchmarks \
    -e '^(?:re2|re2/pinned-portable|re2/pinned-host-tuned-before|re2/pinned-host-tuned-after|slice/re2|slice/re2-object)$'
target/rebar-corpus/target/release/rebar measure \
    -d target/rebar-slice-benchmarks \
    -e '^(?:re2|re2/pinned-portable|re2/pinned-host-tuned-before|re2/pinned-host-tuned-after|slice/re2|slice/re2-object)$' \
    -f '^curated/' \
    -m '^(?:compile|count|count-spans|count-captures|grep|grep-captures)$' \
    --timeout 30s \
    > target/rebar-slice-measurements.csv
```

Formal results must be collected on the dedicated Intel and Graviton hosts
described in `docs/benchmarks/REBAR_NATIVE_COMPARISON_PLAN.md`; local execution
is only a protocol smoke test.

## Capture Pipeline Decomposition

`CapturePipelineRunner` replays the exact match attempts from a Rebar capture
workload and times the public operation, trace control, matcher setup, forward
DFA, reverse DFA, capture engine, composed engine sequence, and result
consumption in isolated JVMs. The manifest records invocation and byte counts
without instrumenting production hot loops. A result is usable only when the
control-adjusted composed stage accounts for 90% to 110% of the bracketed
public time.

Run the deterministic `SplitBig2` workload locally with:

```bash
CAPTURE_PIPELINE_WORKLOAD='synthetic/split-big-2' \
REBAR_NATIVE_ACCESS=enabled \
tools/re2-benchmark/rebar/run-capture-pipeline.sh
```

The official Rebar workloads require the prepared benchmark directory. The
AWS shard runner measures Veryl, Unicode line captures, date group-zero spans,
and `SplitBig2` on Intel and Graviton in parallel:

```bash
AWS_PROFILE=dev tools/re2-benchmark/aws/run-capture-pipeline-shards.sh
```
