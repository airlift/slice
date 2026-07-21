# Traditional Native RE2 Comparison After Capture Work

**Status:** Engineering baseline complete; not formal release qualification.

**Source snapshot:**
`2e7a0ef42c03dd28e224b620ec2233179538ce50e6c7e9c081604e7353adc7f0`

**Campaign:** `20260718T023744Z-76513`

**Pinned RE2 revision:** `972a15cedd008d846f1a39b2e88ce48d7f166cbd`

The companion
[`2026-07-18-traditional-native-comparison.csv`](2026-07-18-traditional-native-comparison.csv)
contains all 596 Intel/Graviton rows. Ratios are Slice elapsed time divided by
host-tuned native elapsed time, so lower is better. Stable rows have Java
coefficient of variation and native bracket drift at or below 5%.

## Conclusion

The settled engine remains substantially faster than native for public boolean
matching. Equivalent public capture is now near parity in aggregate:

| Public operation family | Intel stable rows | Intel | Graviton stable rows | Graviton |
|---|---:|---:|---:|---:|
| Failed search | 46 | 0.499x | 62 | 0.452x |
| Successful search | 32 | 0.670x | 32 | 0.587x |
| Full match | 21 | 0.387x | 21 | 0.343x |
| Capture | 5 | 1.035x | 6 | 1.144x |

Across every stable traditional row, Slice takes geometrically `0.652x`
native time on Intel and `0.574x` on Graviton. This aggregate is informational:
the suite mixes public operations, direct internal engines, compile-time
analysis, input sizes, and specialized constant-time routes.

The serious public capture residual is `SplitBig2`, which takes `1.973x`
native time on Intel and `1.984x` on Graviton. Its absolute deficit is about
2.5/3.1 ms per operation. The short digit captures are faster than native on
both hosts; split capture is at parity; the Graviton split-hard ratio is
`1.386x` but its absolute deficit is below one microsecond.

## Corrected Capture Equivalence

The pre-campaign traditional capture headline of `2.60x/2.69x` is not a valid
before/after comparator. Four fixed-size Java registrations used unanchored
first-match semantics while their native counterparts measured anchored full
match. That changed routing as well as semantics.

The current benchmark uses anchored `FULL_MATCH` for NFA, OnePass, BitState,
Backtrack, and public `Re2`, matching the native registrations. A data-driven
test verifies every traditional capture input matches completely and records
the expected groups. The two large split registrations were already
equivalent; `SplitBig1` remains about `1.07x`, while `SplitBig2` improves from
about `2.49x` to `1.97x` on Intel. The independent Rebar comparison is the valid
before/after evidence for application-level capture improvement.

## Direct Capture Engines

| Engine | Intel stable rows | Intel | Graviton stable rows | Graviton |
|---|---:|---:|---:|---:|
| NFA | 4 | 1.005x | 4 | 1.295x |
| OnePass | 3 | 0.932x | 3 | 1.229x |
| BitState | 4 | 1.153x | 3 | 1.458x |
| Testing Backtrack | 3 | 1.355x | 3 | 2.219x |

These results agree with the focused campaign. NFA and OnePass are at parity
or faster on Intel. Graviton retains a 23%-29% direct NFA/OnePass gap, and
BitState remains 15% slower on Intel and 46% slower on Graviton. Backtrack is a
test oracle, not a production route.

## Extreme Outliers

The general public results above exclude these qualitatively different rows:

- `PossibleMatchRange` takes `11.9x-2698x` native time on Intel. The absolute
  deficits are 5-102 microseconds and this is compile-time analysis, not a
  reusable match hot path.
- Direct BitState alternate-match rows reach `9.53x` on Intel and `2.98x` on
  Graviton. Public routing does not expose these ratios, but they remain
  direct-engine evidence.
- Several direct DFA Easy0/Easy2 rows reach `2.2x-5.5x` on Intel. The paired
  public failed-search families remain about twice as fast as native.
- Public `SplitBig2` is the material capture outlier at roughly `1.98x` on both
  hosts and warrants a separate future investigation.

Of 276 stable Intel rows, 182 are at or faster than native. Of 293 stable
Graviton rows, 205 are at or faster. Rows are not weighted by production
frequency, so this count is a coverage view rather than an engine ranking.

## Method And Provenance

The audited intersection contains 298 one-to-one Java/native pairs. Java used
two JMH forks, ten 500 ms warmup iterations, and seven 500 ms measurement
iterations on one physical core with an 8 GiB zero-based compressed-oops heap.
Native used five repetitions with a 0.2 second minimum immediately before and
after Java; the reference is the geometric mean of the two native medians.

Native RE2 was built with GCC 11.5.0 and `-O3 -DNDEBUG`, plus `-march=native`
on Intel and `-mcpu=native` on Graviton. Intel used a `c8i.8xlarge` with Xeon
6975P-C; Graviton used a `c8g.4xlarge` with Graviton4. Both ran Temurin
25.0.3+9 LTS.

Both hosts passed the complete RE2 selector with and without native access,
uploaded complete artifacts, exited with status zero, and were terminated.
The private transfer bucket and temporary IAM resources were removed.

## Next Bounded Work

The next capture target is the long `SplitBig2` public path. Group-zero span
counting remains a separate Rebar-level target because it does not request
subgroup captures. Compilation and `PossibleMatchRange` remain lower-priority
gaps for a compile-once, reuse-many workload.
