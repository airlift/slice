# Final Joni Acceptance

**Status:** Complete. The current candidate is faster than Joni for every
qualified Trino-shaped operation on Intel and Graviton. No reproducible Joni
regression remains.

**Baseline source:** `c212684c4ad0a700e18c12ffca66ec1c33ff2160`

**Candidate production diff SHA-256:**
`46cba5a3bed01e27ad3f4c349044fda3b958fef4539f929be811cd4f2392327d`

**Packaged source content SHA-256:**
`1f419789f4bb82f820a1f7f615f183145ae9b70064d088c7dc1b11fa2cb97475`

Ratios are Slice time divided by Joni time, so lower is better. This is the
final Joni acceptance layer for the current candidate. It does not replace the
separate native RE2 and Rebar release gates.

## Question

Candidate qualification exposed five 32 KiB sparse-capture operations that
were slower than Joni. The focused follow-up traced those losses to per-byte
mutable work accounting in the candidate-start cursor, not to RE2's DFA
architecture or its capture engines. A bounded bulk candidate scan closed all
five focused losses.

This campaign asks the broader question: does that retained fix beat Joni
across the complete Trino-shaped operation matrix without moving the
regression to another workload?

## Method

Three independent sessions ran on each architecture:

| Session | Intel | Graviton |
|---|---|---|
| `20260720T192831Z-88319` | Complete | Complete |
| `20260720T193015Z-88461` | Complete | Complete |
| `20260720T193127Z-88631` | Complete | Complete |

Each session used dedicated `c8i.2xlarge` and `c8g.2xlarge` instances in
`us-west-2`. The Intel hosts report Xeon 6975P-C processors; the Graviton hosts
report Graviton4. Both used Temurin 25.0.3+9 LTS and an 8 GiB compressed-oops
heap. Joni is `io.airlift:joni:2.1.5.3` from Trino comparator commit
`695b824f87c1272849503486cabd58c719c63e2c`.

The matrix contains eight operations, five workloads, and two source sizes:

- operations: contains, count, third position, extract, extract all, split,
  replace, and lambda replace;
- workloads: sparse literal, sparse capture, dense delimiter, empty matches,
  and sparse Unicode;
- source sizes: 1 KiB and 32 KiB.

Every host ran Slice immediately before and after Joni. Each phase used three
forks, five 500 ms warmup iterations, five 500 ms measurement iterations, one
thread, and the JMH GC profiler. The Slice score for a session is the geometric
mean of its before and after phases.

A session row is stable when its Slice before/after bracket is at most 1.02x.
A matrix row qualifies when at least two of the three independent sessions are
stable. The reported row ratio is the geometric mean of its stable session
ratios. Underqualified rows are retained separately and do not contribute to
the aggregate.

## Correctness Gate

All six hosts completed the 915-test RE2 selector with zero failures or errors
and one intentional skip, both without native access and with strict native
access enabled. The local final tree additionally passes the complete Maven
build with 2,707 tests, zero failures or errors, and one intentional skip. The
local native-access selector also passes all 915 tests.

## Aggregate Results

| Architecture | Qualified | Wins | Geometric | Median | P90 | Worst |
|---|---:|---:|---:|---:|---:|---:|
| Intel | 78/80 | 78/78 | 0.384x | 0.453x | 0.780x | 0.991x |
| Graviton | 78/80 | 78/78 | 0.337x | 0.434x | 0.908x | 0.963x |

Every raw session also records 80/80 Slice wins. The qualification exclusions
therefore reflect host-phase drift, not a measured Joni loss.

### Operation Classes

These are geometric means over qualified rows in each operation class:

| Operation | Intel rows | Intel | Graviton rows | Graviton |
|---|---:|---:|---:|---:|
| Contains | 10 | 0.548x | 10 | 0.535x |
| Count | 10 | 0.179x | 9 | 0.245x |
| Extract | 10 | 0.566x | 10 | 0.609x |
| Extract all | 10 | 0.372x | 10 | 0.364x |
| Third position | 10 | 0.239x | 10 | 0.066x |
| Replace | 10 | 0.478x | 10 | 0.435x |
| Lambda replace | 8 | 0.589x | 9 | 0.485x |
| Split | 10 | 0.370x | 10 | 0.419x |

The closest qualified Intel rows are 32 KiB and 1 KiB sparse-Unicode extract
at 0.991x and 0.985x, followed by 1 KiB sparse-capture lambda replacement at
0.981x. The closest qualified Graviton rows are 1 KiB sparse-Unicode extract
at 0.963x, 32 KiB dense-delimiter contains at 0.960x, and 32 KiB
sparse-Unicode extract at 0.959x. These are ordinary near-parity rows, not
extreme outliers or regressions.

## Underqualified Rows

| Architecture | Operation | Workload | Source | Stable sessions | Diagnostic ratio |
|---|---|---|---:|---:|---:|
| Intel | Lambda replace | Empty matches | 1 KiB | 1 | 0.312x |
| Intel | Lambda replace | Empty matches | 32 KiB | 1 | 0.298x |
| Graviton | Count | Empty matches | 1 KiB | 0 | 0.041x |
| Graviton | Lambda replace | Empty matches | 32 KiB | 1 | 0.294x |

The excluded rows are 3.2x to 24.2x faster than Joni directionally. Repeating
them solely to turn strong wins into qualified wins would not change the
acceptance decision, so no confirmation campaign is warranted.

## Allocation

The median qualified allocation difference is approximately -152 B/op on both
architectures, and the 90th percentile is approximately +24 B/op. The largest
Intel excess is 512 B/op for 1 KiB empty-match split.

The largest Graviton excess is the output-producing 32 KiB sparse-Unicode
extract-all row: Slice allocates 27,440 B/op versus Joni's 14,017 B/op, while
remaining faster at 0.727x Joni time. The same Slice operation allocates the
same 27,440 B/op on Intel, where Joni allocates 34,497 B/op. This is a
JIT/platform-sensitive comparator allocation difference, not hidden input
copying or allocation in a non-capturing boolean path.

## Decision

The known Joni regression is closed. It was caused by the integration of
candidate discovery and work-budget accounting, not by an unavoidable
backtracking-versus-DFA tradeoff. The bounded bulk scan preserves RE2's
linear-time execution and fallback semantics while recovering the search-skip
efficiency that Joni previously exposed.

The current candidate is accepted for the Joni performance gate. Remaining
release performance work concerns native RE2 outliers and the broader official
Rebar intersection, not Joni.

Complete row-level evidence is in
[`2026-07-20-joni-final-qualified.csv`](2026-07-20-joni-final-qualified.csv).
Excluded rows are in
[`2026-07-20-joni-final-underqualified.csv`](2026-07-20-joni-final-underqualified.csv).
