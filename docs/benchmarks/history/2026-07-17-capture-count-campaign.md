# Capture And Count Campaign

**Status:** Complete.

**Focused sessions:** `20260717T180551Z-45827` and
`20260717T190018Z-65475`

**Final native-comparison sessions:** Intel `20260717T202823Z-82766` and
Graviton `20260717T204410Z-85223`

The prior traditional and Rebar comparisons identified capture extraction and
repeated counting as the largest remaining execution gaps. This campaign
bounded the investigation to three rounds. Every retained change required a
direct semantic test, a local performance gate, and five-fork Intel and
Graviton controls. Lower times are better.

## Retained Design

Two count changes are retained:

1. A non-nullable DFA count holds one reader lease while finding all successive
   matches. This removes repeated reader registration and cache publication
   checks without changing the established transition loops.
2. A complete finite greedy repetition of one character class counts maximal
   matching runs in one decoded scan. A lazy exact-membership bitset replaces a
   complete matcher invocation per result.

The character-class table is fixed at 136 KiB for UTF-8 and 32 bytes for
Latin-1. It is built once under a cold synchronized path and retained by the
compiled pattern. Unsupported shapes continue through the ordinary DFA or
matcher path.

## Round 1: One DFA Reader Lease

`BenchmarkDfaCountMatches` compares the retained batched implementation with
the previous loop of independent `Dfa.search` calls over the same compiled
program and 32 KiB input. The second session confirms the first session's
result after all later correctness fixes.

| Architecture | Workload | Batched | Repeated | Change |
|---|---|---:|---:|---:|
| Intel | Dense words | 82.202 us | 110.228 us | 25.4% faster |
| Intel | Bounded context | 71.535 us | 74.002 us | 3.3% faster |
| Intel | Capture-shaped count | 33.579 us | 40.346 us | 16.8% faster |
| Graviton | Dense words | 121.110 us | 163.134 us | 25.8% faster |
| Graviton | Bounded context | 91.804 us | 93.876 us | 2.2% faster |
| Graviton | Capture-shaped count | 41.478 us | 48.056 us | 13.7% faster |

The gain follows matches per input. Dense and capture-shaped workloads avoid
many reader registrations; context-heavy matching spends most of its time in
the unchanged transition work.

## Round 2: Sparse Capture Histories

The capture candidate represented NFA capture updates as persistent primitive
history nodes and materialized the final capture array only at a match. A
synthetic Veryl-shaped benchmark compared it with the existing dense
`System.arraycopy` history.

| Capturing groups | Sparse history | Dense history | Sparse/dense |
|---:|---:|---:|---:|
| 16 | 319.823 ms | 80.906 ms | 3.95x |
| 32 | 761.758 ms | 160.416 ms | 4.75x |
| 64 | 1543.512 ms | 406.385 ms | 3.80x |

The serial history-pointer and reference-count bookkeeping costs much more than
the existing bulk array copies. This candidate failed the local hard gate, was
fully removed, and did not consume a target-host round. Capture remains an open
performance problem.

## Round 3: Bounded Character Classes

`BenchmarkBoundedCharacterClassCounter` compares one decoded run-counting scan
with repeated general matching for `\p{L}{8,13}` over 32 KiB inputs.

| Architecture | Workload | Specialized | General matcher | Change |
|---|---|---:|---:|---:|
| Intel | ASCII | 114.481 us | 246.706 us | 53.6% faster |
| Intel | Russian | 94.291 us | 196.511 us | 52.0% faster |
| Intel | Mixed UTF-8 | 99.975 us | 206.556 us | 51.6% faster |
| Graviton | ASCII | 115.740 us | 348.691 us | 66.8% faster |
| Graviton | Russian | 108.598 us | 337.227 us | 67.8% faster |
| Graviton | Mixed UTF-8 | 105.125 us | 327.085 us | 67.9% faster |

The improvement is algorithmic: classification performs one indexed bitset
load per decoded code point, and each maximal run is partitioned with arithmetic
instead of restarting the matcher for every result.

## Capture Regression Controls

The count changes do not alter capture engines. Four direct `Re2` controls were
rerun on both target architectures and compared with the pre-campaign focused
session.

| Control | Intel before | Intel after | Graviton before | Graviton after |
|---|---:|---:|---:|---:|
| One split capture | 204.692 ns | 203.616 ns | 260.773 ns | 259.878 ns |
| Three `\d` captures | 145.773 ns | 145.075 ns | 206.715 ns | 206.635 ns |
| Three digit-class captures | 146.114 ns | 145.097 ns | 205.513 ns | 204.584 ns |
| Split-hard capture | 559.898 ns | 554.650 ns | 806.826 ns | 807.492 ns |

Changes range from 0.9% faster to 0.1% slower. There is no capture regression,
but also no retained capture improvement.

## Correctness And Lifecycle Review

A read-only concurrency and semantic review found two defects before the final
target run:

- fixed-distance DFA metadata was initially created after reader publication,
  which could deadlock against an exclusive cache mutation; initialization now
  occurs before `beginSearch`, matching ordinary DFA search ordering;
- malformed UTF-8 initially decoded to U+FFFD and could match a negated class;
  invalid one-byte decode results are now non-matching, while a valid encoded
  U+FFFD remains an ordinary code point.

Direct tests cover first versus longest matching, non-zero Slice offsets,
word-boundary context across successive matches, every bounded-run residue
through multiple maxima, malformed UTF-8 including negated classes, valid
U+FFFD, and Latin-1.
The full Maven install passed 2,666 tests with zero failures or errors and one
intentional skip.

## Final Native Comparison

The final 41-workload Rebar comparison used pinned host-tuned native RE2 as the
denominator. A ratio below 1.0 means Slice is faster. The campaign found and
corrected a harness defect: Rebar's timed `count` model was still iterating a
matcher even though its correctness warmup used the retained count API. Both
final sessions measure the actual capture-free operation.

| Model | Intel | Graviton |
|---|---:|---:|
| Compile | 1.181x | 1.601x |
| Count | 0.949x | 0.651x |
| Count with captures | 2.291x | 2.150x |
| Count group-zero spans | 1.997x | 1.726x |
| Grep | 1.152x | 0.870x |
| Grep with captures | 1.482x | 1.241x |

Capture-free count improved from the prior `1.751x/1.203x` Intel/Graviton
baseline to `0.949x/0.651x`. The geometric aggregate is now 5% faster than
native on Intel and 35% faster on Graviton. Capture-bearing models did not
change structurally and remain the largest general execution gap.

### Material Count Rows

| Workload | Intel | Graviton | Interpretation |
|---|---:|---:|---|
| Bounded context | 1.480x | 1.534x | Only large absolute count deficit; transition work dominates |
| Dictionary | 0.018x | 0.021x | Batched boundary-free counting eliminates repeated matcher traversal |
| Russian bounded letters | 0.024x | 0.032x | Specialized decoded character-class scan |
| Bounded capitals | 0.764x | 0.656x | General batched DFA count win |

The large ratio losses for literal Russian and Chinese on Intel are 8.88x and
21.19x, but their absolute deficits are only 2.4 ms and 0.7 ms per complete
Rebar operation. They are listed as extreme ratio outliers rather than allowed
to obscure the dominant absolute workloads. Graviton does not reproduce those
losses: the corresponding Russian and Chinese rows are 0.94x and 0.94x.

### Capture Outliers

| Workload | Model | Intel | Graviton | Absolute deficit |
|---|---|---:|---:|---:|
| Veryl lexer | Count with captures | 2.291x | 2.150x | 199/257 ms |
| Unicode parse-line | Grep with captures | 2.685x | 2.215x | 35/36 ms |
| Ruff noqa real | Grep with captures | 1.440x | 1.145x | 32/14 ms |
| Date ASCII | Count spans | 1.675x | 1.395x | 2.6/2.0 ms |

The complete 82-row result is
[`2026-07-17-capture-count-native-comparison.csv`](2026-07-17-capture-count-native-comparison.csv).
Both hosts passed the complete RE2 selector with and without native access,
uploaded normalized artifacts, exited successfully, and were terminated. All
temporary transfer buckets were removed.
