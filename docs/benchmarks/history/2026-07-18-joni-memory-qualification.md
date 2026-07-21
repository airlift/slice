# Joni Memory Qualification

**Status:** Accepted and complete.

This bounded campaign determines whether the Java RE2 representation retains
unproductive memory relative to Joni in Trino-shaped use. It does not attempt to
match native RE2's denser C++ representation. The practical decision compares
retained bytes and throughput together: DFA storage is justified when it remains
reachable, is charged to the configured budget, and materially improves a
reused compiled pattern.

## Comparison Contract

- Joni is pinned to `io.airlift:joni:2.1.5.3`, matching the comparator checkout.
- Pattern bytes, source bytes, encoding, operation, and match counts are equal.
- Compiled patterns are reused; compilation time is not part of operation timing.
- Memory is measured separately for compiled patterns, warmed patterns, and
  active matchers. Caller-owned input bytes are excluded.
- RE2 totals include reachable Java heap and any owned native transition
  sidecar. Joni totals include its reachable Java heap.
- Operation timing is current Slice divided by Joni, so lower is better.
- Output-producing operation allocations are informative rather than exact
  representation comparisons: Slice returns Slice collections while Trino's
  Joni adapter constructs Blocks.

The target-host mode runs current Slice immediately before and after Joni on
one pinned physical core. The reported Slice time is the geometric mean of the
two phases. A separate retained-memory census uses the selected 96 MiB planning
budget and the exact bounded-context input preserved by the cache-capacity
campaign.

## Measurement Accuracy

The production `SizeOf` model was compared with temporary JOL lifecycle probes.
For the 75,850-state bounded-context graph, compressed-reference retained array
growth differed by 296 bytes and live per-state objects matched exactly.
Without compressed references, retained array growth differed by 312 bytes and
the custom live-state model was 1.436% conservative. JOL is resolved only by the
external census script and is not a project dependency.

The census subtracts the pattern graph after matcher traversal. This matters
because traversal can build a reverse DFA or reset and shrink an existing cache.
Using the pre-traversal graph produced an invalid negative matcher size in the
reset-heavy 64 MiB uncompressed control. The corrected control reports a
296-byte matcher and 28 resets.

## Ordinary Trino-Shaped Memory

The local census covers five deterministic workloads, two source lengths, all
eight adapter operations, active `find()` matchers, and 1, 8, and 64 independent
compiled roots. Ratios are Slice bytes divided by Joni bytes.

| Lifecycle | Rows | Median | P90 | Maximum |
|---|---:|---:|---:|---:|
| Compiled | 15 | 0.898x | 2.556x | 2.729x |
| Warm pattern | 80 | 0.474x | 0.998x | 1.004x |
| Active matcher | 10 | 0.481x | 0.958x | 0.961x |

The largest compiled ratios are small absolute objects. At 64 independent
roots, the maximum average excess is 1,527 bytes per capture pattern. No
ordinary row exceeds both twice Joni's memory and 1 MiB of absolute excess.

The bounded-context active matcher audit further separates pattern and matcher
ownership. After a complete traversal, the Slice matcher itself owns 208 bytes;
Joni's matcher owns 15,368 bytes. Slice's larger active total comes from shared
forward and lazily built reverse DFA state on the compiled pattern, not
per-matcher workspace.

## Route-Selection Waste Found

The initial operation matrix exposed two allocation and throughput outliers in
lambda replacement:

| Workload | Source | Slice/Joni before | Excess allocation |
|---|---:|---:|---:|
| Capture sparse | 32 KiB | 11.163x | 41.7 KiB |
| Unicode sparse | 32 KiB | 2.834x | 52.7 KiB |

Both patterns were eligible for OnePass, but the direct BitState capture route
was selected first. Direct BitState is useful when it avoids a forward DFA,
reverse DFA, and slower NFA extraction; it is wasteful when it supersedes the
single-pass capture engine. The route now excludes OnePass programs. Existing
non-OnePass direct BitState tests remain, and a new test locks the OnePass
precedence.

After the correction, Slice wins all 80 local timing rows:

| Operation | Rows | Median | P90 | Maximum |
|---|---:|---:|---:|---:|
| Overall | 80 | 0.372x | 0.861x | 0.966x |
| Contains | 10 | 0.447x | 0.901x | 0.912x |
| Count | 10 | 0.254x | 0.473x | 0.694x |
| Extract | 10 | 0.352x | 0.912x | 0.936x |
| Extract all | 10 | 0.324x | 0.630x | 0.966x |
| Position third | 10 | 0.185x | 0.441x | 0.554x |
| Replace | 10 | 0.387x | 0.861x | 0.866x |
| Lambda replace | 10 | 0.438x | 0.737x | 0.936x |
| Split | 10 | 0.343x | 0.634x | 0.782x |

The largest Slice allocation excess is 13,408 bytes for 32 KiB Unicode
`extractAll`, where Slice still takes 0.966x Joni time and the adapters return
different result representations. No local throughput loss remains.

## Large DFA Policy

The exact Latin-1 bounded-context pattern has 75,850 retained states and 53
matches in a 7,384,531-byte source. Joni retains no DFA and therefore has a much
smaller pattern graph. Slice spends memory to avoid repeated backtracking or
cache reconstruction.

| Reference mode and budget | Slice warm pattern | Joni warm pattern | Resets | Slice/Joni time |
|---|---:|---:|---:|---:|
| Compressed, 64 MiB | 21,427,744 B | 11,368 B | 0 | 0.173x |
| Compressed, 96 MiB | 21,842,216 B | 11,368 B | 0 | 0.174x |
| Uncompressed, 64 MiB | incomplete final graph | 11,496 B | 26 | 0.652x |
| Uncompressed, 96 MiB | 28,121,032 B | 11,496 B | 0 | 0.152x |

The compressed 96 MiB ceiling retains only 414,472 bytes more than 64 MiB for
the same state count. The difference is backing-array capacity, not additional
states or a 32 MiB reservation. Ordinary patterns continue to retain kilobytes.
Without compressed references, 64 MiB repeatedly resets while 96 MiB retains
the complete graph and is about four times faster. The 96 MiB policy therefore
provides reference-mode and growth headroom without changing the reset-free
compressed loop.

The bounded graph's roughly 22 MiB compressed footprint is much larger than
Joni's graph, but the local operation is about 5.7 times faster. That is an
intentional reusable-cache tradeoff rather than unexplained waste.

## Target Confirmation

Operation session `20260718T215058Z-67755` ran on a `c8i.2xlarge` Intel Xeon
6975P-C and a `c8g.2xlarge` Graviton4. Both hosts used Temurin 25.0.3, an 8 GiB
zero-based compressed-oops heap, and one pinned physical core. Both Slice modes
and the Trino comparator's semantic test passed before timing.

| Architecture | Rows | Median | P90 | Maximum | Maximum allocation excess | Maximum Slice bracket |
|---|---:|---:|---:|---:|---:|---:|
| Intel | 80 | 0.412x | 0.771x | 0.995x | 512 B | 1.111x |
| Graviton | 80 | 0.401x | 0.758x | 1.003x | 13,408 B | 1.156x |

Intel's slowest row is 32 KiB delimiter-dense `contains` at 0.995x Joni time.
Graviton's corresponding row is 1.003x, a 0.3% difference with a 1.001x Slice
before/after bracket. This is parity, not a repeatable 10% loss. The largest
Slice bracket variation occurs in rows that remain substantially faster than
Joni and does not affect the decision.

The largest allocation excess on Intel is 512 bytes. Graviton's 13,408-byte
maximum is 32 KiB Unicode `extractAll`, which takes 0.651x Joni time and returns
a different adapter representation. No ordinary target row combines an
allocation concern with a throughput loss.

Both hosts completed and summarized the operation matrix. The subsequent
memory census did not start because its Maven invocation did not carry the
snapshot runner's git-plugin skip flag into the intentionally `.git`-free
source archive. The operation artifacts remain valid. A corrected census-only
recovery supplies the missing retained-memory evidence without repeating the
accepted timing phase.

Recovery session `20260718T225427Z-71021` ran the corrected 96 MiB census on
the same instance families, JDK, heap shape, and physical-core pinning. Both
hosts passed focused Slice tests with native access disabled and enabled, and
both returned exact match counts.

Ordinary target memory is identical on the two reference-compatible VMs:

| Lifecycle | Rows | Median | P90 | Maximum |
|---|---:|---:|---:|---:|
| Compiled | 15 | 0.898x | 2.556x | 2.729x |
| Warm pattern | 80 | 0.474x | 1.000x | 5.151x |
| Active matcher | 10 | 0.481x | 0.958x | 0.961x |

The 5.151x warm-pattern maximum is `x*` containment. Slice owns a 4.1 KiB heap
graph plus the eligible fixed 48 KiB absolute-pointer sidecar, compared with a
10.4 KiB Joni graph. The absolute excess is about 43 KiB, remains under the
sidecar's explicit cap, and accelerates the hot transition loop. No ordinary
row exceeds both twice Joni memory and 1 MiB of excess.

The bounded-context result is:

| Architecture | Slice warm pattern | Joni warm pattern | Slice active total | Joni active total | Resets | Slice/Joni time |
|---|---:|---:|---:|---:|---:|---:|
| Intel | 21,842,216 B | 11,368 B | 23,593,056 B | 26,736 B | 0 | 0.140x |
| Graviton | 21,842,216 B | 11,368 B | 23,593,056 B | 26,736 B | 0 | 0.114x |

The large graph exceeds the absolute-pointer cap and uses ordinary object rows.
Its active total includes shared reverse-DFA state built during traversal; the
matcher itself owns 208 bytes. Slice is about 7.1 times faster than Joni on
Intel and 8.7 times faster on Graviton. This is a productive, bounded
compiled-pattern cache rather than per-thread or transient waste.

A later focused campaign supersedes this bounded graph's sidecar size and
throughput. The large absolute-pointer sidecar reduces the same route to
`0.908x/0.946x` current Intel/Graviton native time. The
first no-handoff policy eagerly reserved a maximum table and failed the
small-pattern lifecycle gate.

Recovery session `20260719T051215Z-5387` measures the accepted geometric policy
on both reference hosts. Excluding the explicit bounded-context graph, ordinary
warm patterns retain `0.473x` Joni memory at the median, `0.996x` at P90, and
`1.003x` at the maximum. The warmed `x*` containment graph now retains only a
144-byte sidecar and 4.3 KiB total, versus 10.4 KiB for Joni.

The bounded graph retains all 75,850 states, uses a 7,959,120-byte sidecar, and
resets zero times. Its warmed pattern is 29,801,528 bytes and its active total
is 31,552,360 bytes excluding caller-owned input. This is intentionally much
larger than Joni's compiled graph, but it is shared reusable state rather than
matcher-local memory. The bracketed Slice operation takes `0.070x` Joni time on
Intel and `0.065x` on Graviton, about 14.3x and 15.4x faster respectively. See
[`2026-07-18-dfa-large-pointer-sidecar.md`](2026-07-18-dfa-large-pointer-sidecar.md).

## Decision Gate

Retain the selected policy if target results preserve exact match counts, show
no material ordinary memory outlier, and do not show a repeatable operation more
than 10% slower than Joni. A correctness, lifecycle, accounting, or material
performance failure stops the campaign rather than opening another transition
layout experiment.

All gates pass. Retain the OnePass precedence correction, runtime-aware cache
accounting, demand-driven 96 MiB default, and bounded native sidecar. No timing
confirmation or additional memory-policy candidate is warranted.
