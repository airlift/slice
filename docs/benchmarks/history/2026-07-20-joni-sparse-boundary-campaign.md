# Joni Sparse Boundary Campaign

**Status:** Complete. The bounded bulk-scan candidate passes correctness and
target performance gates on Intel and Graviton and is retained.

**Baseline source:** `c212684c4ad0a700e18c12ffca66ec1c33ff2160`

**Candidate production diff SHA-256:**
`46cba5a3bed01e27ad3f4c349044fda3b958fef4539f929be811cd4f2392327d`

**Target session:** `20260720T182018Z-72726`

**Packaged source content SHA-256:**
`e0966b94c80b22dd51fbacf71466d3ffe169edbeb637973d70a3a674c3198765`

This is bounded engineering evidence, not release qualification. Ratios are
candidate time divided by comparator time, so lower is better.

## Question

Candidate qualification found five material Joni losses for the 32 KiB
`captureSparse` workload. The expression is `([a-z]+)-([0-9]+)`, the input is
otherwise filled with `.`, and three seven-byte matches are injected at one
quarter, one half, and three quarters of the input.

All five losing operations request only group-zero boundaries. Lambda
replacement requests the two explicit capture groups and remained at parity or
faster, so the NFA, OnePass, and BitState capture engines were not implicated.

## Corrected Route Diagnosis

The preliminary qualification report incorrectly attributed the loss to
forward-plus-reverse DFA replay. A deterministic trace of the exact workload
establishes the opposite:

- the candidate-start cursor is enabled;
- all three matches use the cursor;
- no cursor attempt falls back;
- the reverse program is never computed.

The cursor scanned every rejected byte itself and called `consumeWork()` for
each byte. That call mutates a `long` field and checks the remaining budget.
The ordinary DFA route instead uses the existing tight candidate-table scanner
and accounts outside that loop. The per-byte budget bookkeeping is especially
expensive on Intel and Graviton and explains why the regression was absent from
the earlier operation matrix before the cursor was introduced.

## Candidate

The candidate-start cursor now calls the existing candidate-table scanner to
find the next possible start byte. It charges the number of skipped bytes to
the same reset-scoped work budget in one operation. Candidate verification and
anchored transition work retain their separate charges.

The total budget is therefore unchanged:

- scanning `N` rejected bytes still consumes `N` units;
- finding a candidate still consumes one additional verification unit;
- anchored transitions retain their existing per-transition charge;
- insufficient work still disables the cursor and uses the established
  matcher fallback.

No DFA transition loop, state representation, cache lifecycle, or public API
changes.

## Deterministic Verification

`TestDfaCandidateStartCursor` covers the exact 32 KiB pattern and input. It
checks all three boundaries, three cursor routes, zero fallbacks, bounded scan
work, and absence of reverse-program construction.

The complete 915-test RE2 selector passes with zero failures or errors and one
intentional skip in both configurations:

- native access unavailable, using object DFA rows;
- native access enabled with denied undeclared native access.

Existing cursor tests additionally cover first-match priority, greedy and
reluctant matches, logical regions, UTF-8 and malformed input, bounded
fallback, reset reuse, and concurrent DFA cache resets.

## Local A/B Screen

The local screen used Temurin 25.0.2 on Apple Silicon with one fork, five
500 ms warmup iterations, and five 500 ms measurement iterations for the
32 KiB workload. These measurements select a candidate; they do not qualify
Intel or Graviton.

| Operation | Baseline | Candidate | Candidate/baseline |
|---|---:|---:|---:|
| Extract | 5,571 ns | 2,730 ns | 0.490x |
| Extract all | 14,684 ns | 10,661 ns | 0.726x |
| Third position | 16,637 ns | 14,015 ns | 0.842x |
| Replace | 16,248 ns | 11,898 ns | 0.732x |
| Split | 14,662 ns | 10,674 ns | 0.728x |

A same-JVM directional comparison against the existing Trino Joni benchmark
places all five candidate operations well ahead of Joni:

| Operation | Slice | Joni | Slice/Joni |
|---|---:|---:|---:|
| Extract | 2,730 ns | 6,508 ns | 0.419x |
| Extract all | 10,661 ns | 25,965 ns | 0.411x |
| Third position | 14,015 ns | 33,195 ns | 0.422x |
| Replace | 11,898 ns | 26,846 ns | 0.443x |
| Split | 10,674 ns | 28,864 ns | 0.370x |

This screen established that the loss was not an unavoidable
backtracking-versus-DFA architectural difference. The target campaign below is
the retained evidence.

## Historical Target Evidence

Session `20260718T215058Z-67755`, before the candidate cursor was introduced,
used the ordinary group-zero route and won the same five 32 KiB operations on
both architectures. Slice/Joni ratios were `0.359x`-`0.564x` on Intel and
`0.370x`-`0.435x` on Graviton. This is supporting evidence that removing the
cursor's per-byte bookkeeping should recover target performance, but it is not
evidence for the current source.

## Target Qualification

Session `20260720T182018Z-72726` ran interleaved Slice-before, Joni, and
Slice-after brackets concurrently on dedicated `c8i.2xlarge` and
`c8g.2xlarge` hosts. It measured all eight Trino-shaped operations for both
`captureSparse` and `unicodeSparse` at 1 KiB and 32 KiB. Each bracket used three
forks, five 500 ms warmup iterations, and five 500 ms measurement iterations.

All 32 rows on each architecture are Slice wins:

| Scope | Intel Slice/Joni | Graviton Slice/Joni |
|---|---:|---:|
| All 32 rows, median | 0.546x | 0.463x |
| All 32 rows, maximum | 0.995x | 0.953x |
| All 16 `captureSparse` rows, median | 0.516x | 0.437x |
| 32 KiB `captureSparse`, range | 0.335x-0.552x | 0.395x-0.469x |
| Maximum Slice bracket | 1.017x | 1.028x |

The five formerly losing 32 KiB group-zero operations now take
`0.335x`-`0.529x` Joni time on Intel and `0.395x`-`0.469x` on Graviton. The
true-capture lambda control takes `0.552x` and `0.443x` Joni time. The result
therefore closes the known Joni regression without changing the capture
engines or DFA architecture.

The host processes exited with status 1 only because the allocation-oriented
summary tool required `gc.alloc.rate.norm`, while this time-only campaign did
not enable the GC profiler. All three 32-row JMH JSON files had already been
written on both hosts. Product tests, measurement completeness, and time
brackets are unaffected. The complete row-level evidence is in
[`2026-07-20-joni-sparse-boundary-qualified.csv`](2026-07-20-joni-sparse-boundary-qualified.csv).

The candidate is retained. A complete Trino-shaped matrix remains part of the
final acceptance pass, but this focused regression is closed and does not need
another confirmation campaign.
