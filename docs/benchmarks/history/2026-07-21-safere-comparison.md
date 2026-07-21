# SafeRE Comparison

**Status:** Complete for the Trino-shaped operation matrix. This is an
end-to-end Java API comparison, not a universal regex-engine ranking.

Ratios are Slice elapsed time divided by SafeRE elapsed time, so lower is
better.

## Comparator

The comparator is [SafeRE 0.9.0](https://github.com/eaftan/safere/releases/tag/v0.9.0),
commit `6fe67606d7c7724b9b1f1736c3bee04c674eaec5`. The exact Maven Central jar
used by every host has SHA-256
`b219af0a28041bcf5be077fb115f6c7b34aa0ae53a1b8bbc8773547fdc6dc5ee`.

SafeRE is another optimized Java RE2-family engine. It provides Java-oriented
matching semantics and a borrowed UTF-8 byte-array API, making it a materially
closer comparator for this port than RE2/J.

## Question

How does the accepted Slice baseline compare with SafeRE for operations and
data shapes representative of a Trino integration?

The matrix contains eight operations, five workloads, and two source sizes:

- operations: contains, count, third position, extract, extract all, split,
  replacement, and lambda replacement;
- workloads: sparse literal, sparse capture, dense delimiter, empty matches,
  and sparse Unicode;
- source sizes: 1 KiB and 32 KiB.

Pattern compilation is excluded because Trino compiles once and applies the
compiled expression to many values.

## Adapter

The benchmark uses SafeRE's public direct UTF-8 API. Each operation creates a
borrowed `Utf8Input.trusted` view over the `Slice` byte array, as a Trino
adapter would need to do for each input value. Pattern compilation occurs once
per JMH state. No timed path decodes the source to `String` or copies the source
bytes.

- contains uses `Pattern.find(Utf8Input)`, including SafeRE's public literal
  fast paths;
- iterative and capturing operations use `Utf8Matcher` and byte offsets;
- replacement uses `Utf8Matcher` with `Utf8Sink` or the benchmark's equivalent
  lambda adapter;
- Slice views are returned for extracted groups and split fields.

`TestBenchmarkSafeReTrinoRegexp` compares all 80 SafeRE results byte-for-byte
with `TrinoRegexp`. Every host passed this semantic gate both without native
access and with strict native access enabled.

## Method

Three independent sessions ran on each architecture:

| Session | Intel | Graviton |
|---|---|---|
| `20260721T164759Z-6446` | Complete | Complete |
| `20260721T164905Z-6827` | Complete | Complete |
| `20260721T164905Z-6829` | Complete | Complete |

Each session used dedicated on-demand `c8i.2xlarge` and `c8g.2xlarge`
instances in `us-west-2`. Intel hosts report Xeon 6975P-C processors and
Graviton hosts report Graviton4. All hosts used Temurin 25.0.3+9 LTS, an 8 GiB
zero-based compressed-oops heap, one pinned physical core, and JMH 1.37.

Every host ran Slice immediately before and after SafeRE. Each phase used three
forks, five 500 ms warmup iterations, five 500 ms measurement iterations, one
thread, and the JMH GC profiler. Slice used strict native access for its compact
DFA transition table; SafeRE used its normal Java implementation.

A session row is stable when the Slice before/after bracket is at most 1.02x.
A matrix row qualifies when at least two of three sessions are stable. The
reported row is the geometric mean of its stable session ratios.

The Slice source baseline is `cf100e40f94d33f44dd60042c784e34476df5741`.
All three campaign snapshots have content SHA-256
`4c420d33adffccc82e613018f89f31a124ddc71e8a91f765ef9d4d6b4f5f9040`.

## Aggregate

| Architecture | Qualified | Slice wins | Geometric mean | Median | P90 | Worst |
|---|---:|---:|---:|---:|---:|---:|
| Intel | 77/80 | 74/77 | 0.267x | 0.358x | 0.779x | 1.365x |
| Graviton | 78/80 | 74/78 | 0.241x | 0.408x | 0.728x | 1.391x |

Across this matrix, Slice is about 3.7x faster by geometric mean on Intel and
4.1x faster on Graviton. Aggregate ratios are strongly affected by specialized
count and empty-match paths, so the operation and workload classes below are
more informative than the single headline number.

## Operation Classes

| Operation | Intel rows | Intel | Graviton rows | Graviton |
|---|---:|---:|---:|---:|
| Contains | 9 | 0.687x | 10 | 0.658x |
| Count | 10 | 0.095x | 9 | 0.084x |
| Extract | 10 | 0.314x | 10 | 0.292x |
| Extract all | 9 | 0.283x | 9 | 0.210x |
| Third position | 10 | 0.218x | 10 | 0.216x |
| Replace | 10 | 0.273x | 10 | 0.230x |
| Lambda replace | 9 | 0.339x | 10 | 0.248x |
| Split | 10 | 0.249x | 10 | 0.244x |

These are geometric means over qualified rows. Slice wins every qualified row
outside `contains`.

## Workload Classes

| Workload | Intel rows | Intel | Graviton rows | Graviton |
|---|---:|---:|---:|---:|
| Sparse capture | 16 | 0.486x | 16 | 0.487x |
| Dense delimiter | 16 | 0.133x | 15 | 0.144x |
| Empty matches | 14 | 0.085x | 16 | 0.063x |
| Sparse literal | 16 | 0.344x | 16 | 0.280x |
| Sparse Unicode | 15 | 0.660x | 15 | 0.690x |

Slice wins every qualified sparse-capture, dense-delimiter, and sparse-literal
row. It wins all but the immediate-success contains rows in the empty-match and
Unicode classes.

## SafeRE Wins

| Architecture | Workload | Source | Slice | SafeRE | Ratio |
|---|---|---:|---:|---:|---:|
| Intel | Empty matches | 1 KiB | 21.2 ns | 15.5 ns | 1.365x |
| Intel | Empty matches | 32 KiB | 21.0 ns | 15.5 ns | 1.354x |
| Intel | Sparse Unicode | 32 KiB | 30.2 ns | 28.5 ns | 1.058x |
| Graviton | Empty matches | 1 KiB | 31.8 ns | 22.9 ns | 1.391x |
| Graviton | Empty matches | 32 KiB | 31.4 ns | 23.0 ns | 1.367x |
| Graviton | Sparse Unicode | 32 KiB | 49.7 ns | 42.9 ns | 1.158x |
| Graviton | Sparse Unicode | 1 KiB | 48.7 ns | 42.8 ns | 1.135x |

These are startup-dominated operations, not scan-throughput losses. `x*`
matches at byte zero, and the first Unicode match starts at byte 32 regardless
of source size. The largest absolute gap is 8.9 ns. No qualified operation that
iterates matches or materializes output is slower than SafeRE.

The underqualified Intel 1 KiB Unicode contains row is directionally similar
at 1.092x. The other underqualified rows all strongly favor Slice.

## Allocation

The median qualified Slice allocation difference is about -200 B/op on both
architectures. The 90th-percentile difference is approximately +16 B/op, which
is also the maximum measured excess. Those +16 B rows are sparse-literal
position or result operations. There is no input-sized conversion or copying
in either measured adapter.

## Qualification Exclusions

| Architecture | Operation | Workload | Source | Stable sessions | Diagnostic ratio |
|---|---|---|---:|---:|---:|
| Intel | Contains | Sparse Unicode | 1 KiB | 1 | 1.092x |
| Intel | Extract all | Empty matches | 32 KiB | 0 | 0.046x |
| Intel | Lambda replace | Empty matches | 1 KiB | 1 | 0.119x |
| Graviton | Count | Sparse Unicode | 32 KiB | 1 | 0.455x |
| Graviton | Extract all | Dense delimiter | 1 KiB | 1 | 0.160x |

These rows are excluded from aggregate claims. Repeating strong directional
Slice wins solely to satisfy the drift gate would not alter the conclusion.

## Scope

This result establishes the comparison for Trino-shaped, direct UTF-8,
steady-state operations. It does not compare compilation, multi-pattern APIs,
concurrent scaling, pathological expressions, or the complete Rebar corpus.
Those would answer different questions and should not be inferred from this
matrix.

## Conclusion

Slice is materially faster than SafeRE for the measured Trino use case. The
largest practical differences are in counting, dense match iteration, and
output-producing operations, where Slice's specialized byte-oriented paths
avoid repeated general matcher work. SafeRE has a smaller startup cost for a
few immediate-success boolean matches, but no measured scan or materialization
advantage.

Complete row-level evidence is retained in:

- [`2026-07-21-safere-intel-qualified.csv`](2026-07-21-safere-intel-qualified.csv)
- [`2026-07-21-safere-intel-underqualified.csv`](2026-07-21-safere-intel-underqualified.csv)
- [`2026-07-21-safere-graviton-qualified.csv`](2026-07-21-safere-graviton-qualified.csv)
- [`2026-07-21-safere-graviton-underqualified.csv`](2026-07-21-safere-graviton-underqualified.csv)
