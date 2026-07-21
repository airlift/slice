# Current-Source Native RE2 Census

**Status:** Engineering baseline complete; not formal release qualification.

**Engine checkpoint:** `23d21694c0cf94d1d9e3dc06ab4f0bc61ceadfde`

**Primary source snapshot:**
`10433946d70fcf1f27d36aa5cbeb59ac8e7c7805e8d3f0747535582d84ed8375`

**Primary campaign:** `20260719T071144Z-current-census`

**Pinned RE2 revision:** `972a15cedd008d846f1a39b2e88ce48d7f166cbd`

Ratios are Slice elapsed time divided by host-tuned native RE2 elapsed time, so
lower is better. The companion CSV files retain all traditional and Rebar rows,
the selected primary or confirmation measurement, both native-bracket drift
values where a confirmation was needed, and the strict 2% stability decision.
Unless a row is identified as diagnostic, aggregate ratios are geometric means
over rows that pass that stability gate.

## Conclusion

The current engine is faster than native RE2 for the normal reusable matching
paths that matter most to the intended Trino use case. Public boolean operations
are materially faster on both architectures, public capture is at parity, and
the application-shaped Rebar corpus is generally faster once a small set of
qualitatively different outliers is separated.

| Public traditional operation | Intel rows | Intel | Graviton rows | Graviton |
|---|---:|---:|---:|---:|
| Failed search | 63 | 0.507x | 63 | 0.456x |
| Successful search | 32 | 0.655x | 32 | 0.589x |
| Full match | 21 | 0.389x | 21 | 0.326x |
| Capture | 5 | 0.984x | 6 | 1.012x |

The Rebar model results show where work remains:

| Rebar model | Intel | Graviton |
|---|---:|---:|
| Compile | 1.198x | 1.584x |
| Count | 0.827x | 0.569x |
| Count captures | 1.058x diagnostic | 0.964x |
| Count spans | 1.997x | 1.704x |
| Grep | 1.054x | 0.883x |
| Grep captures | 1.089x | 0.920x |

The Intel count-captures row is not qualified because native drift remained
2.89% in the single confirmation. Its primary and confirmation ratios were
1.036x and 1.058x, so both measurements point to parity despite the failed
strict gate.

Compared with the July 16 baseline, count moved from 1.751x/1.203x to
0.827x/0.569x, count captures moved from 2.365x/2.156x to approximately
1.058x/0.964x, and grep captures moved from 1.463x/1.235x to 1.089x/0.920x.
Count spans remains the model-level execution gap, concentrated in the date and
tiny Cloudflare shapes. Compilation is effectively unchanged and remains lower
priority for compile-once, reuse-many workloads.

## Normal Cases

The complete Rebar corpus contains 41 rows per architecture. For a symmetric
normal-case view, the same five workload shapes are removed on both hosts: the
two host-tuned literal-scan anomalies observed on Intel and the three tiny
Cloudflare relative-ratio anomalies. The 35/36 remaining stable rows take
geometrically 0.837x/0.841x native time on Intel/Graviton; Intel also omits the
unstable Veryl row. This aggregate is informational because it weights compile,
count, boundaries, and capture equally.

The model-level results are more useful:

- ordinary non-capturing count is faster than native in aggregate;
- full capture counting is at parity;
- boolean grep is at parity on Intel and faster on Graviton;
- capture grep is near parity in aggregate, with two material Intel rows called
  out below;
- group-zero span counting remains about 2x/1.7x native time before relative
  outliers are removed;
- after removing the three tiny Cloudflare rows, count spans is 1.041x/0.926x.

## Material Residuals

These stable rows have the largest absolute deficits:

| Workload | Model | Intel | Graviton | Intel deficit | Graviton deficit |
|---|---|---:|---:|---:|---:|
| Ruff `tweaked` | Grep captures | 1.299x | 1.078x | 12.32 ms | 4.09 ms |
| Unicode character data | Grep captures | 1.459x | 1.053x | 9.53 ms | 1.55 ms |
| Dictionary compile | Compile | 1.613x | 1.742x | 3.29 ms | 4.26 ms |
| Date ASCII | Count spans | 1.630x | 1.385x | 2.43 ms | 2.00 ms |

Ruff `real` is 1.011x/0.962x in this corpus and remains closed.
The `tweaked` result is a line-window integration issue rather than evidence
that the core capture engines are still 2x slower.

Direct traditional capture engines remain architecture-sensitive:

| Engine | Intel | Graviton |
|---|---:|---:|
| NFA | 1.014x | 1.318x |
| OnePass | 0.930x | 1.227x |
| BitState | 1.168x | 1.459x |
| Testing Backtrack | 1.352x | 2.240x |

The public capture pipeline hides most of those direct Graviton deficits. The
testing backtracker is an oracle and is not a production route.

## Extreme Outliers

The following relative outliers are separated from the normal summaries:

- Intel host-tuned native literal scans make `sherlock-zh` and `sherlock-ru`
  21.278x and 8.798x relative outliers. Their absolute deficits are 0.66 ms and
  2.38 ms. The same Java implementation does not show this shape on Graviton.
- Three Cloudflare count-span rows reach 3.29x-5.17x on Intel and up to 4.14x
  on Graviton, but their absolute deficits are at most 0.11 ms. Removing these
  rows brings the remaining count-span model to parity.
- Traditional `PossibleMatchRange` analysis reaches approximately 2,700x/3,300x
  native time on Intel/Graviton, but the largest absolute deficit is only
  0.13 ms and this is compile-time analysis rather than reusable matching.
- Direct internal DFA and BitState rows retain isolated 2x-9x ratios. The paired
  public operation families are faster than native, so these are diagnostic
  engine-shape evidence rather than public API regressions.

Three Intel traditional rows remain unqualified after confirmation:
`success-one-byte/bit-state` at 4 KiB and 2 MiB, and public `SplitBig2` capture.
Their latest ratios were 1.231x, 1.226x, and 1.251x, with native drift of 2.10%,
11.13%, and 2.26%. All 298 Graviton traditional rows qualified.

## Method And Provenance

The traditional campaign contains 298 one-to-one Java/native pairs on each
architecture. Seven disjoint benchmark classes ran on independent host pairs,
while every class preserved the native-before/Java/native-after bracket. The
final dataset has 593 stable rows and three explicitly retained drift failures.

The Rebar campaign contains 41 workloads and six engines per workload. Every
one of the 246 engine/workload verification rows passed on each architecture.
The complete 913-test RE2 selector passed with and without native access on
every primary host, with zero failures or errors and one intentional skip. The
final Rebar dataset has 81 stable rows and the one Intel Veryl drift failure.
Both primary hosts exited with status 2 only during post-processing because the
original reducer incorrectly required compile measurements to report a
haystack length. Commit `4e20e00a0fb35a079a4460adcf368b25a27b7f66` corrects
that metadata rule with a regression test. The published primary rows were
regenerated locally from the recovered raw CSVs and exact manifests using that
reducer; no measurements were rerun or altered for this repair.

Native RE2 was compiled from the pinned source with GCC 11.5.0 and
`-O3 -DNDEBUG`, plus `-march=native` on Intel and `-mcpu=native` on Graviton.
The hosts were `c8i.2xlarge` Intel Xeon 6975P-C and `c8g.2xlarge` Graviton4
instances running Temurin 25.0.3+9 LTS with an 8 GiB compressed-oops heap.

The primary benchmark source commit is `be8a61e99cb0f5275f3a3cfd4feaa8ea399a7fc4`.
Confirmation runs use `4e20e00a0fb35a079a4460adcf368b25a27b7f66`, which
changes only the Rebar compile-metadata reducer and its test. The engine
checkpoint is identical in both snapshots.

## Next Bounded Work

1. Investigate group-zero span counting as one focused campaign. First isolate
   whether the remaining cost is the forward/reverse composition or the span
   enumeration contract; do not reopen generic DFA layout exploration.
2. Investigate Ruff `tweaked` and Unicode line capture as integration pipelines,
   not as another NFA/OnePass/BitState rewrite campaign.
3. Treat Intel literal scanning as a separate specialized-prefix campaign only
   if the absolute deficits justify it.
4. Keep compilation and direct Graviton engine tuning below these application
   paths unless production routing evidence changes their priority.
