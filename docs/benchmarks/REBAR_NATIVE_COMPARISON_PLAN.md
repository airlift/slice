# Rebar Native RE2 Comparison Plan

**Status:** Executed with a residual strict drift-gate failure. See
[`history/2026-07-16-rebar-native-comparison.md`](history/2026-07-16-rebar-native-comparison.md).

**Engine checkpoint:** `ff59828e7ebfd87e983d2a8e5adf0805e00ad85e`

**Rebar revision:** `463d00f31887e84c38467805b9e3122c314b9521`

This is a bounded engineering comparison, not the later publication
qualification campaign. Its purpose is to establish where the current Slice
RE2 engine stands relative to native RE2 on Rebar's curated workloads and to
identify the responsible layer for material differences. It does not include
Joni, historical Trino RE2J, Trino integration, or speculative optimization.

## Questions

The campaign must answer these questions separately:

1. How does the public Slice API compare with native RE2 for each exact Rebar
   workload shared by both engines?
2. Does the answer differ between Intel and Graviton?
3. Does the answer differ by result demand: compile, count, match boundaries,
   captures, line matching, or line captures?
4. How much of each material difference belongs to algorithm selection, the
   public API/model adapter, allocation, or a core execution loop?
5. How much does the optional absolute-pointer DFA sidecar contribute on the
   workloads that select it?

There is deliberately no single pass/fail ratio for the entire corpus. Compile
and search are different operations, and a geometric mean over unrelated
result demands can hide the information needed to improve the engine.

## Existing Evidence

Sessions `20260716T132411Z-74989` and `20260716T140959Z-85510` ran the official
41-row curated RE2 intersection at Slice commit `378aa40`. The second session
verified all rows on both architectures and produced these geometric ratios of
Slice time to Rebar's native RE2 time:

| Model | Rows | Intel | Graviton |
|---|---:|---:|---:|
| Compile | 10 | 1.147x | 1.527x |
| Count | 18 | 1.497x | 1.346x |
| Count captures | 1 | 2.202x | 1.986x |
| Count spans | 6 | 1.972x | 1.672x |
| Grep | 1 | 1.050x | 0.890x |
| Grep captures | 5 | 1.485x | 1.305x |
| All rows, informational only | 41 | 1.460x | 1.426x |

Those results are not the current answer. They predate the final DFA sidecar,
JVM-sourced Unicode work, and other changes in checkpoint `ff59828`. They also
compare against Rebar's bundled RE2 snapshot rather than the exact upstream
commit used by this port. They remain useful as a stable historical baseline
and as a list of gaps that the new campaign must explain.

## Workload Boundary

Use only curated Rebar definitions that already list native `re2`. The current
intersection contains 41 rows across these models:

| Model | Rows | Required result |
|---|---:|---|
| `compile` | 10 | Compile and consume a verified search result outside the timed region |
| `count` | 18 | Number of non-overlapping matches |
| `count-spans` | 6 | Sum of group-zero match lengths |
| `count-captures` | 1 | Number of participating groups |
| `grep` | 1 | Number of matching lines |
| `grep-captures` | 5 | Participating groups across matching lines |

Keep Rebar's pattern bytes, flags, haystacks, expected counts, and model
definitions unchanged. Do not add unsupported rows to improve apparent
coverage, silently drop failures, or combine `regex-redux` with the single
compiled-pattern models.

Generate and retain a manifest containing every selected row, engine, model,
input length, pattern checksum, haystack checksum, flags, and expected result.
The verification and measurement passes must consume the same manifest.

## Comparators

Report three native references because they answer different questions:

| Comparator | Purpose |
|---|---|
| Rebar bundled `re2` | Preserves the untouched official Rebar ecosystem comparison |
| Pinned RE2 portable release | Compares the port with its exact upstream source revision using `-O3 -DNDEBUG` |
| Pinned RE2 host-tuned release | Establishes the strongest native ceiling using `-march=native` on Intel or `-mcpu=native` on Graviton |

The pinned runners must use RE2 commit
`972a15cedd008d846f1a39b2e88ce48d7f166cbd`. Reuse Rebar's native model
adapter so only the RE2 source revision and compiler flags differ. Record the
compiler version, complete command lines, linked Abseil revision, binary hash,
and reported RE2 revision. Confirm optimized generated code for one search and
one compile calibration case before accepting measurements.

The Slice runners are:

| Comparator | Purpose |
|---|---|
| `slice/re2` | Primary public API result with the accepted native-access configuration |
| `slice/re2-object` | Secondary control with native access unavailable and ordinary object rows retained |

The primary Java runner must use:

```text
--enable-native-access=ALL-UNNAMED
--illegal-native-access=deny
-Xms8g
-Xmx8g
-XX:+AlwaysPreTouch
```

The object control must omit native access without producing a warning or
initialization failure. Verify compressed oops and tier-4 compilation on both
hosts. JVM startup, heap pre-touch, and process creation remain outside each
runner's internally timed samples, as required by Rebar's protocol.

## Harness Preparation

Before target-host measurement:

1. Register the two pinned native variants and the two Slice variants in the
   generated Rebar configuration.
2. Make native-access mode explicit in `run-slice.sh`; do not depend on the
   launching shell's JVM options.
3. Add a reducer that validates complete engine pairs and emits per-row ratios,
   per-model geometric means, ratio buckets, and native bracketing drift.
4. Add route diagnostics outside the timed loop for Slice engine selection,
   boundary demand, DFA representation, cache resets, and fallback.
5. Run `rebar measure --test` for every selected row and comparator.
6. Run a local protocol smoke test, but retain no local timing as evidence.
7. Run the complete RE2 test selector with native access enabled and disabled.

Any expected-count mismatch, unsupported option, native build ambiguity, or
missing route classification is a hard gate failure. Stop and return with the
evidence instead of shrinking the intersection.

## Target Hosts

Run Intel and Graviton concurrently on dedicated, non-burstable AWS instances:

| Architecture | Initial host |
|---|---|
| Intel | `c8i.2xlarge` |
| Graviton | `c8g.2xlarge` |

Use the same AMI family, JDK build, 8 GiB Java heap, and benchmark scripts used
by the accepted DFA campaign. Pin the benchmark to one physical core and leave
its sibling idle. Record CPU model, caches, frequency policy, kernel, microcode,
NUMA topology, compressed-oops mode, JDK, compiler, and all source and binary
hashes.

Use explicit Rebar limits of five seconds of warmup and five seconds of
measurement per engine and row, with a 30-second process timeout. A calibration
pass must show that the relevant Java methods reach tier 4 within warmup. If
they do not, increase warmup before the campaign; do not reinterpret partially
compiled measurements afterward.

## Execution Order

Run one integrated campaign per architecture with this order for every row:

1. Pinned host-tuned native before control.
2. Slice with native access.
3. Pinned host-tuned native after control.
4. Rebar bundled native RE2.
5. Pinned portable native RE2.
6. Slice object-row control.

The two identical host-tuned native controls bracket the primary Java result.
Their medians must agree within 2%. This catches host drift without requiring a
second full campaign by default. The two architectures run in parallel, but no
two benchmark processes run concurrently on one host.

One optional confirmation campaign is allowed when native bracketing exceeds
2%, a material row has coefficient of variation above 5%, or Intel and
Graviton disagree in a way that may be noise. The confirmation repeats the
same manifest and reverses the comparator order. Do not open an optimization
cycle inside this goal.

## Analysis

The report must retain every individual row and provide these summaries:

1. Search and compile results in separate top-level sections.
2. Per-model geometric mean, median ratio, range, and number of wins, parity
   results, and losses.
3. Ratio buckets: faster than `0.90x`, within `0.90x-1.10x`, `1.10x-1.25x`,
   `1.25x-1.50x`, `1.50x-2.00x`, and slower than `2.00x`.
4. Separate ASCII, non-ASCII Unicode, case-insensitive, boundary, and capture
   views where the corpus contains enough rows.
5. Ratios against bundled RE2, pinned portable RE2, and pinned host-tuned RE2.
6. The absolute-pointer contribution from `slice/re2` versus
   `slice/re2-object`, limited to rows that actually select the sidecar.
7. A route-coverage matrix for literal and prefix scanning, OnePass, paired
   DFA, absolute-pointer DFA, object-row DFA, NFA, BitState, and capture paths.
8. An explicit list of important engine routes not represented by the 41 rows.
9. The ten largest absolute-time deficits and ten largest relative deficits.
10. The ten largest Slice wins, with result-demand and dead-work checks.

An all-row geometric mean may be shown only as an informational index. It must
not replace per-model or per-row results and must not be described as expected
real-world application performance. No conclusion may be generalized to an
engine route absent from the coverage matrix.

## Gap Classification

A row is material when Slice is more than 10% slower than pinned host-tuned
native and the absolute difference exceeds one microsecond, or when either
engine is more than 2x faster regardless of absolute time. Classify every
material row into one of these categories:

- different algorithm or acceleration path
- different result demand or model implementation
- public API or line-iteration overhead
- allocation or retained-state behavior
- DFA cache construction, reset, or fallback
- core execution-loop generated code
- native source-revision or compiler difference
- unresolved

Use route diagnostics first. Profile only the deduplicated top three families
by absolute deficit and top three by relative deficit, for at most six families.
Capture at most one CPU profile per family and architecture. Capture perfasm and
native disassembly only when profiles identify a core loop and the load or
dependency chain remains unexplained. Do not change production code during
this campaign.

Every difference larger than 5x in either direction is presumed to be a
semantic, benchmark, or algorithm-selection problem until proven otherwise.

## Deliverables

The campaign produces:

- one immutable raw result directory keyed by the harness commit and session
- environment, source, binary, corpus, and manifest hashes
- verification output for every comparator and row
- raw Rebar CSV for both architectures
- normalized per-row and per-model comparison CSV
- a route and gap-classification ledger
- a dated report under `docs/benchmarks/history/`
- a concise conclusion stating where Slice is faster, at parity, or slower
  than native RE2 and why

The report must keep official Rebar positioning separate from same-revision
native parity and from the object-row fallback control.

## Stopping Conditions

This goal ends after one harness preparation and smoke pass, one integrated
Intel/Graviton campaign, one optional confirmation campaign, and the bounded
diagnostics above.

Stop immediately for discussion when:

- any shared row produces a result mismatch
- either pinned native binary cannot be reproduced with confirmed release flags
- native access is not demonstrably active in the primary Slice runner
- the native before and after controls disagree after the optional confirmation
- a lifecycle, memory, or correctness failure appears
- a material result cannot be attributed within the bounded diagnostic set

The outcome is a trustworthy comparison and a prioritized gap list. Fixing the
gaps, if any, is a separate explicitly approved goal with its own tests,
benchmarks, and stopping conditions.
