# Rebar Native RE2 Comparison

**Status:** Bounded comparison complete; strict native-drift gate remains open
on three small rows after the single allowed confirmation.

**Engine checkpoint:** `ff59828e7ebfd87e983d2a8e5adf0805e00ad85e`

**Primary campaign:** `20260717T020622Z-84985`

**Reverse-order confirmation:** `20260717T033108Z-58579`

**Rebar revision:** `463d00f31887e84c38467805b9e3122c314b9521`

**Pinned RE2 revision:** `972a15cedd008d846f1a39b2e88ce48d7f166cbd`

The companion
[`2026-07-16-rebar-native-comparison.csv`](2026-07-16-rebar-native-comparison.csv)
retains every workload's primary and confirmation ratios, route evidence,
object-row control, absolute deficit, materiality, and provisional gap class.

## Conclusion

The current Slice engine does not yet have whole-engine native parity. Its
accepted one-byte absolute-pointer DFA is useful, but the remaining dominant
gaps are outside that isolated transition-loop result:

- compilation is about `1.19x` host-tuned native time on Intel and
  `1.58-1.66x` on Graviton;
- non-capturing count is `1.75x` on Intel and `1.20-1.21x` on Graviton, with
  the Intel mean strongly affected by host-tuned native literal scanning;
- group-zero boundary demand is about `2.00x` on Intel and `1.72-1.73x` on
  Graviton;
- full capture counting is `2.37-2.40x` on Intel and `2.16-2.17x` on
  Graviton;
- line capture demand is `1.46-1.48x` on Intel and `1.24x` on Graviton;
- boolean line matching is `1.11x` on Intel and `0.87-0.88x` on Graviton.

The confirmation reproduced every per-model geometric ratio closely. The
stable priority gaps are capture extraction, Unicode bounded repetition,
dictionary matching, group-zero boundary processing, and line-capture work.
Compile variation is high at the individual-sample level, but its aggregate
architecture gap also reproduced.

This does not invalidate the accepted absolute-pointer design. On the ten rows
that populated it, the native-access Slice runner took `0.892x` object-row time
on Intel and `0.882x` on Graviton geometrically. The known extra address-add in
the Graviton hot loop remains an accepted architecture cost. It is not treated
as a new unresolved regression in this comparison.

## Qualification Boundary

Both campaigns used the same 41-workload manifest; its SHA-256 was
`50340ac8e73524c99f8a033b69405d98f036ff70bfdc3707184998951864579f`
on both architectures and in both orders. Each campaign ran six engines per
workload:

1. exact pinned host-tuned RE2 before control;
2. Slice with native access;
3. exact pinned host-tuned RE2 after control;
4. Rebar's bundled RE2;
5. exact pinned portable RE2;
6. Slice without native access.

The confirmation reversed that complete order. Every one of the 246
engine/workload pairs passed Rebar verification in both campaigns and on both
architectures. The complete RE2 selector also passed in both Slice modes on
every host: 859 tests, zero failures, zero errors, and one intentional skip.

The native runners were compiled from the pinned source with GCC 11.5.0.
Portable commands contained `-O3 -DNDEBUG`; tuned commands additionally used
`-march=native` on Intel or `-mcpu=native` on Graviton. Compiler-wrapper logs,
binary hashes, symbols, and disassembly are retained in each raw session.
Calibration showed tier-4 compilation of the hot Java compile path and the
absolute-pointer DFA search. Slice used an 8 GiB zero-based compressed-oops
heap.

The hosts were:

| Architecture | Instance | Processor | JDK |
|---|---|---|---|
| Intel | `c8i.2xlarge` | Intel Xeon 6975P-C | Temurin 25.0.3+9 LTS |
| Graviton | `c8g.2xlarge` | AWS Graviton4 | Temurin 25.0.3+9 LTS |

Each engine/workload pair received five seconds of warmup and five seconds of
measurement on one physical core, with no concurrent benchmark on that host.

## Confirmed Model Results

Ratios below are Slice time divided by native time from the reverse-order
confirmation. Lower is better.

| Model | Intel bundled | Intel portable | Intel tuned | Arm bundled | Arm portable | Arm tuned |
|---|---:|---:|---:|---:|---:|---:|
| Compile | 1.122x | 1.197x | 1.189x | 1.480x | 1.578x | 1.583x |
| Count | 1.228x | 1.323x | 1.751x | 1.090x | 1.206x | 1.203x |
| Count captures | 2.175x | 2.337x | 2.365x | 1.994x | 2.149x | 2.156x |
| Count spans | 1.989x | 2.006x | 2.001x | 1.671x | 1.734x | 1.730x |
| Grep | 1.114x | 1.114x | 1.111x | 0.878x | 0.862x | 0.867x |
| Grep captures | 1.401x | 1.369x | 1.463x | 1.177x | 1.232x | 1.235x |

The host-tuned primary and confirmation model ratios were:

| Model | Intel primary | Intel confirmation | Arm primary | Arm confirmation |
|---|---:|---:|---:|---:|
| Compile | 1.190x | 1.189x | 1.661x | 1.583x |
| Count | 1.755x | 1.751x | 1.213x | 1.203x |
| Count captures | 2.396x | 2.365x | 2.171x | 2.156x |
| Count spans | 1.999x | 2.001x | 1.721x | 1.730x |
| Grep | 1.109x | 1.111x | 0.885x | 0.867x |
| Grep captures | 1.480x | 1.463x | 1.243x | 1.235x |

## Distribution

The confirmation's 41 host-tuned ratios fell into these buckets:

| Slice/native ratio | Intel rows | Arm rows |
|---|---:|---:|
| Faster than 0.90x | 4 | 5 |
| 0.90x through 1.10x | 4 | 8 |
| 1.10x through 1.25x | 11 | 5 |
| 1.25x through 1.50x | 8 | 5 |
| 1.50x through 2.00x | 3 | 10 |
| Slower than 2.00x | 11 | 8 |

The all-row geometric index was `1.58x` on Intel and `1.37x` on Graviton in
the confirmation. It is informational only: compile, count, boundaries, and
captures have different result demands and should not be represented by one
application-level number.

## Workload Views

These confirmation-only geometric summaries are also informational:

| View | Rows | Intel | Arm |
|---|---:|---:|---:|
| ASCII mode | 34 | 1.416x | 1.382x |
| Unicode mode | 7 | 2.721x | 1.308x |
| Case-insensitive | 6 | 1.391x | 1.261x |
| Case-sensitive | 35 | 1.619x | 1.389x |
| Capture demand | 6 | 1.585x | 1.355x |
| Group-zero boundary demand | 6 | 2.001x | 1.730x |
| Search without captures | 19 | 1.709x | 1.183x |
| Absolute-pointer populated | 10 | 1.458x | 1.424x |
| Paired rows selected | 9 | 1.384x | 1.276x |
| Prefix acceleration active | 7 | 2.625x | 1.113x |

The Intel Unicode and prefix views are dominated by `sherlock-ru` and
`sherlock-zh`. Slice was `8.93x` and `20.99x` host-tuned Intel native time but
`0.93x` and `0.94x` on Graviton. The same Java code therefore does not show a
portable DFA-loop failure. The evidence points to host-tuned Intel native
literal scanning, and the absolute deficits were only 2.42 ms and 0.66 ms.

## Absolute-Pointer Contribution

Ten rows populated forward absolute-pointer transitions. Relative to the
ordinary object-row Slice control:

| Architecture | Geometric ratio | Median | Range |
|---|---:|---:|---:|
| Intel | 0.892x | 0.948x | 0.743x-1.016x |
| Graviton | 0.882x | 0.930x | 0.737x-1.051x |

The largest gains occur in literal-alternate and case-insensitive literal
graphs. Capture and large Unicode bounded-repeat rows are approximately neutral
between the two Slice layouts, showing that their native gap belongs elsewhere.
The accepted Graviton extra instruction remains part of the absolute-pointer
loop's known generated-code shape; no additional layout work is opened here.

## Largest Deficits

Rows are ordered by the larger architecture's absolute deficit.

| Workload | Model | Intel ratio | Arm ratio | Intel deficit | Arm deficit |
|---|---|---:|---:|---:|---:|
| `curated/05-lexer-veryl/single` | Count captures | 2.365x | 2.156x | 204.715 ms | 257.055 ms |
| `curated/10-bounded-repeat/letters-ru` | Count | 3.519x | 5.276x | 94.790 ms | 155.765 ms |
| `curated/10-bounded-repeat/context` | Count | 1.454x | 1.510x | 42.005 ms | 55.801 ms |
| `curated/07-unicode-character-data/parse-line` | Grep captures | 2.693x | 2.185x | 34.890 ms | 34.600 ms |
| `curated/04-ruff-noqa/real` | Grep captures | 1.438x | 1.113x | 31.780 ms | 11.130 ms |
| `curated/12-dictionary/single` | Count | 2.069x | 2.299x | 11.305 ms | 14.960 ms |
| `curated/04-ruff-noqa/tweaked` | Grep captures | 1.234x | 1.191x | 9.600 ms | 9.985 ms |
| `curated/09-aws-keys/quick` | Grep | 1.111x | 0.867x | 4.285 ms | -6.617 ms |
| `curated/12-dictionary/compile-single` | Compile | 1.513x | 1.757x | 2.815 ms | 4.270 ms |
| `curated/03-date/ascii` | Count spans | 1.663x | 1.414x | 2.560 ms | 2.150 ms |

The largest relative-only deficits include the two Intel literal scans above
and three tiny Cloudflare boundary rows. The latter are `2.73x-5.18x`, but
their absolute deficits are at most 0.11 ms. They remain important as path
evidence, not as dominant elapsed-time gaps.

## Largest Wins

The strongest cross-architecture wins are:

| Workload | Model | Intel ratio | Arm ratio |
|---|---|---:|---:|
| `curated/09-aws-keys/full` | Grep captures | 0.504x | 0.436x |
| `curated/02-literal-alternate/sherlock-en` | Count | 0.676x | 0.468x |
| `curated/10-bounded-repeat/capitals` | Count | 0.774x | 0.619x |
| `curated/08-words/long-english` | Count spans | 0.724x | 0.631x |
| `curated/08-words/all-english` | Count spans | 0.921x | 0.927x |

The AWS-key result is real line-capture work, not dead work: all engines
returned the same verified participating-group count. The literal-alternate
and bounded-repeat wins likewise retained identical match counts.

## Route Coverage

The cold manifest observed:

| Route evidence | Rows |
|---|---:|
| Prefix acceleration active | 7 |
| OnePass eligible | 19 |
| BitState eligible | 32 |
| Forward DFA retained | 39 |
| Forward absolute-pointer transitions populated | 10 |
| Paired DFA rows selected | 9 |
| Reverse program computed | 9 |
| Capture models | 6 |
| Forward cache resets observed | 30 total |

OnePass and BitState values are eligibility, not hot-path selection counters.
Two line-capture rows retained no forward DFA after execution, which is useful
negative evidence but not enough to identify every internal matcher branch.

Important routes absent or inadequately isolated by these 41 rows include
anchored full matching, required-prefix stripping, empty-match iteration,
explicit DFA budget exhaustion and repeated thrashing, reverse
absolute-pointer integration, concurrent shared-DFA warm-state throughput,
very large cache-pressure graphs, direct low-level `matchInto` use, and rewrite
operations. No claim in this report extends to those routes.

## Gap Classification

The companion CSV assigns every row a provisional code-path class. The
confirmed material families reduce to:

- parser, simplifier, and compiler work for compile rows;
- capture result demand and allocation for Veryl;
- line iteration plus capture extraction for Unicode data and Ruff `noqa`;
- literal or prefix acceleration for the architecture-specific Sherlock rows;
- group-zero boundary and reverse-search work for date, words, and Cloudflare;
- paired or absolute-pointer DFA construction/execution for the rows that
  selected those representations;
- general forward-DFA algorithm or adapter work for dictionary and remaining
  count rows.

These are route-level attributions, not completed CPU-profile diagnoses. The
formal plan required stopping after the optional confirmation when native
bracketing still exceeded 2%, so the bounded top-family profiler captures were
not opened.

## Residual Drift Gate

The confirmation reduced Intel drift failures from three rows to one and left
two different Graviton rows above 2%:

| Architecture | Workload | Native controls | Drift |
|---|---|---:|---:|
| Intel | `curated/01-literal/sherlock-zh` | 32.74 us / 33.56 us | 2.50% |
| Graviton | `curated/01-literal/sherlock-zh` | 773.52 us / 789.93 us | 2.12% |
| Graviton | `curated/09-aws-keys/quick` | 50.61 ms / 48.82 ms | 3.67% |

The Intel absolute difference is only 0.82 us, and every model-level result
reproduced. Nevertheless, the written plan has no absolute tolerance and
allows no third campaign. This report therefore records strong engineering
confidence in the model conclusions but does not claim that the strict
per-row qualification gate passed.

## Priority Order

The next performance goal, if approved, should be separate from this bounded
comparison:

1. capture counting and capture-result allocation;
2. Unicode bounded repetition;
3. dictionary and context count paths;
4. line-capture iteration and extraction;
5. group-zero boundary and reverse-search overhead;
6. parser/compiler work, especially on Graviton;
7. Intel host-tuned literal scanning as an architecture-specific study.

The accepted one-byte DFA layout should remain frozen. Any work on these gaps
must first isolate the responsible route with deterministic path tests and a
targeted benchmark, rather than altering the transition loop based on an
aggregate Rebar result.
