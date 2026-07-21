# Traditional Native RE2 Comparison

**Status:** Engineering baseline complete; not formal release qualification.

**Engine checkpoint:** `e97d531dc94b551daf78e6b13143c1860d57339d`

**Source snapshot:** `e02048092b307233646f0ada7551581b04d061a3c569fcf8a8403d763aa081d1`

**Campaign:** `20260717T093143Z-96711`

**Pinned RE2 revision:** `972a15cedd008d846f1a39b2e88ce48d7f166cbd`

The companion
[`2026-07-17-traditional-native-comparison.csv`](2026-07-17-traditional-native-comparison.csv)
contains all 596 architecture/pair rows. Every ratio is Slice elapsed time
divided by host-tuned native elapsed time, so lower is better. The `stable`
column excludes rows with Java coefficient of variation or native bracket drift
above 5%.

## Conclusion

The traditional benchmark suite does not show a general search-performance
deficit. Across stable rows, Slice took geometrically `0.676x` native time on
Intel and `0.617x` on Graviton. Public `Re2` boolean search and full-match rows
are substantially faster than native in aggregate:

| Public operation family | Intel/Arm stable rows | Intel | Graviton |
|---|---:|---:|---:|
| Failed search | 55 / 63 | 0.493x | 0.447x |
| Successful search | 32 / 32 | 0.666x | 0.594x |
| Full match | 21 / 21 | 0.390x | 0.336x |

The remaining losses are concentrated rather than systemic:

- public capture extraction is `2.60x` native time on Intel and `2.69x` on
  Graviton across six rows;
- compilation is `1.24x` on Intel and `1.80x` on Graviton;
- direct BitState successful search is about `2.90-3.04x` on both hosts after
  unstable rows are excluded;
- direct NFA capture is `1.90x` on Intel and `2.19x` on Graviton;
- `PossibleMatchRange` is `6.9x-3302x`, although its absolute deficits are
  5.3-128 microseconds and it is compile-time analysis rather than a match hot
  path.

Direct DFA results are mixed. Failed direct-DFA search is `0.996x` native on
Intel and `0.748x` on Graviton geometrically after drift exclusions, while
case-insensitive Easy2 remains `1.92x` and `1.58x`. These losses generally do
not survive the public engine's route selection and accelerators: the paired
public failed-search groups are faster than native. The accepted one-byte DFA
layout therefore remains frozen; this campaign does not justify reopening it.

This result complements rather than replaces the
[`2026-07-16 Rebar comparison`](2026-07-16-rebar-native-comparison.md). The
traditional suite is a code-path census of one search or compile operation and
includes direct internal engines. Rebar measures repeated whole operations,
capture demand, and application-shaped corpora. Rebar remains the stronger
evidence for the known count, boundary, and capture deficits.

## Qualification Boundary

The audited intersection contains 298 one-to-one Java/native pairs. Pairing
requires the same operation, pattern, input bytes, parameter, engine route,
caching behavior, capture demand, encoding, and memory budget. The comparison
excludes PCRE registrations, uncached native rows without a Java equivalent,
thread-scaling registrations, Java-only UTF-8 FullMatch rows, and duplicate
Java implementations. Two 16 MiB Java OnePass/BitState parameter rows emitted
by paired methods have no pinned native registration and are recorded
separately as unpaired rows.

The benchmark harness was corrected before measurement to preserve equivalence:

- capture buffers are reused instead of allocated in every timed invocation;
- the HTTP benchmark extracts the same capture as native;
- simple-match, FullMatch suffix, and `PossibleMatchRange` length inputs match
  pinned native exactly;
- the generated native filter is preflighted against the real binary and must
  select exactly 298 registrations;
- reducers reject missing, duplicate, unexpected, zero, non-finite, or
  wrong-unit paired rows.

Each host ran the complete 863-test RE2 selector both with ordinary object rows
and with native access enabled. All four runs passed with zero failures and
errors and one intentional skip. Java used two JMH forks, ten 500 ms warmup
iterations, and seven 500 ms measurement iterations on one physical core with
an 8 GiB zero-based compressed-oops heap. Native used five repetitions with a
0.2 second minimum and ran immediately before and after Java. The native
reference is the geometric mean of the two native medians.

The native executable was built directly from pinned RE2 with GCC 11.5.0 and
`-O3 -DNDEBUG`, plus `-march=native` on Intel or `-mcpu=native` on Graviton.
Compile commands, source identity, and binary hashes were retained in the raw
campaign artifacts.

| Architecture | Instance | Processor | JDK |
|---|---|---|---|
| Intel | `c8i.2xlarge` | Intel Xeon 6975P-C | Temurin 25.0.3+9 LTS |
| Graviton | `c8g.2xlarge` | AWS Graviton4 | Temurin 25.0.3+9 LTS |

The host scripts exited nonzero after measurement because the source-snapshot
reducer initially rejected the two known unpaired Java rows. All raw Java and
native files were complete. The corrected reducer explicitly classifies those
rows and regenerated both 298-pair summaries without changing measurements.

## Stable Distribution

Sixteen Intel rows and three Graviton rows exceeded the 5% stability gate. The
stable distributions were:

| Slice/native ratio | Intel rows | Graviton rows |
|---|---:|---:|
| At most 0.80x | 159 | 178 |
| Above 0.80x through 1.00x | 31 | 21 |
| Above 1.00x through 1.20x | 21 | 35 |
| Above 1.20x through 1.50x | 17 | 18 |
| Above 1.50x through 2.00x | 16 | 13 |
| Above 2.00x | 38 | 30 |

Slice was at or faster than native on 190 of 282 stable Intel rows and 199 of
295 stable Graviton rows. This count is informational: rows cover different
input sizes and internal routes and are not weighted by production frequency.

## Stable Engine Families

| Family | Engine | Intel rows | Intel | Arm rows | Arm |
|---|---|---:|---:|---:|---:|
| Capture | Re2 | 6 | 2.601x | 6 | 2.693x |
| Capture | NFA | 4 | 1.899x | 4 | 2.191x |
| Capture | OnePass | 3 | 1.337x | 3 | 1.986x |
| Capture | BitState | 3 | 2.409x | 4 | 3.268x |
| Compile | Compile | 5 | 1.239x | 5 | 1.798x |
| Failed search | DFA | 60 | 0.996x | 61 | 0.748x |
| Failed search | NFA | 27 | 0.907x | 30 | 0.941x |
| Failed search | Re2 | 55 | 0.493x | 63 | 0.447x |
| Full match | Re2 | 21 | 0.390x | 21 | 0.336x |
| Successful search | DFA | 24 | 0.846x | 24 | 0.732x |
| Successful search | BitState | 14 | 2.901x | 14 | 3.039x |
| Successful search | Re2 | 32 | 0.666x | 32 | 0.594x |

The direct successful OnePass geometric index is about `0.007x` on both hosts
because Slice can answer several anchored no-capture shapes without traversing
the input, while pinned native OnePass scans it. This is a real algorithmic
difference, but it makes an all-row aggregate especially unsuitable as an
application-level claim.

## Next Bounded Work

The strongest user-relevant target is capture extraction, which agrees with the
independent Rebar result. Compilation is second and is especially weak on
Graviton. `PossibleMatchRange` is a clear isolated implementation gap but is a
lower operational priority. Any follow-up should remain outside the accepted
DFA transition loops unless a focused path test and both target architectures
prove the change.
