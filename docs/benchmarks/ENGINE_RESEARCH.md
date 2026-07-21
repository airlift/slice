# Regex Engine Optimization Research

This document records optimization ideas from other regular-expression engines,
their fit with this RE2 port, and the evidence required before adopting them.
It is a design reference, not a claim that every listed technique should be
implemented.

Last updated: 2026-07-16

## Constraints

An imported technique must preserve:

- pinned RE2 leftmost-first and longest-match behavior
- linear-time execution and bounded memory
- UTF-8 and Latin1 correctness, including malformed input behavior
- shared compiled-pattern safety
- the compact DFA path when an optimization is ineligible or unproductive
- no material regression on Intel or AWS Graviton

The target workload is a compiled pattern applied repeatedly to Slice values,
often through Trino columnar execution. Cold behavior still matters, but a
throughput optimization may use lazy one-time analysis when it avoids compile
regressions and is charged to the existing memory budget.

## Existing Strengths

The port already combines the main architecture found in modern linear-time
engines:

- forward DFA search for a match end
- reverse DFA recovery for the leftmost start
- OnePass, BitState, or NFA extraction when captures are requested
- exact prefix scanners, sparse first-byte scanning, and one-byte-language
  specialization
- compact object-reference transitions and bounded paired transitions
- adaptive stable-self-loop scanning for long unpaired searches
- cache reset and compact fallback under the configured memory budget

The useful external ideas are therefore selective scanners, dispatch policies,
and bounded metadata. Replacing the engine is not the near-term opportunity.

## Engine Survey Summary

| Engine | Relevant techniques | Decision for this port |
|---|---|---|
| Native RE2 | Shared DFA cache, forward match-end search, reverse boundary recovery, OnePass and NFA capture extraction | Preserve as the semantic and architectural baseline. Optimize Java representation and dispatch without replacing the engine cascade. |
| Rust `regex-automata` | Meta-engine strategy selection, literal prefilters, reverse suffix search, DFA state acceleration, and automatic fallback | Retain selective prefilters and bounded fallback policies. Reject its boundary-producing reverse-suffix shape because the leftmost counterexample below also reproduces there. |
| .NET `System.Text.RegularExpressions` | Ranked fixed-distance sets, literal-after-loop search, vectorized `IndexOf` dispatch, bounds-only execution, and DFA-to-NFA continuation | Fixed-distance search and operation-specific capture demand are retained. Continuation after cache failure remains profile-driven future work. Runtime code generation and backtracking-specific optimizations do not transfer. |
| Airlift Joni 2.1.5.3 | Exact-string and byte-map search, Boyer-Moore skipping, minimum and maximum distance propagation, and threshold-based dispatch | Use as the direct Trino comparator and as evidence for selective literal and distance analysis. Do not import its backtracking VM or unsupported syntax. See its pinned [`Analyser`](https://github.com/airlift/joni/blob/2.1.5.3/src/org/joni/Analyser.java) and [`SearchAlgorithm`](https://github.com/airlift/joni/blob/2.1.5.3/src/org/joni/SearchAlgorithm.java). |
| PCRE2 | Minimum-length and start-bit study, required code units, auto-possessification, and optional native JIT | Retain bounded minimum-width and required-literal ideas. Backtracking and native-code generation are outside RE2 semantics and duplicate the native-binding alternative. |
| Hyperscan and VectorScan | Multi-pattern literal filtering, SIMD scanners, state-local acceleration, and fully compiled automata | Reuse only isolated scanner ideas. Static entry-state acceleration was rejected, but guarded sampling of actual stable self-loops is retained. The full reporting-oriented architecture is not a match for single-pattern leftmost boundaries. |

The survey supports a stable strategy: keep the linear-time RE2 engine cascade,
move less input through it with semantically conservative scanners, specialize
operations that do not need captures, and require every extra dispatch or
metadata load to pass source-disabled Intel and Graviton controls. The
experiments below test those ideas independently so a local win cannot hide a
regression in an existing route.

## Ranked Candidates

### 1. Fixed-Distance Selective Bytes

.NET analyzes literal bytes and character sets at fixed offsets, ranks them by
selectivity, and uses the strongest candidates to find possible starts. Rust
disables prefilters that do not skip enough input. See
[`RegexFindOptimizations`](https://github.com/dotnet/runtime/blob/f41747370bfdd6887c4ae4a27c5bc219f838eedc/src/libraries/System.Text.RegularExpressions/src/System/Text/RegularExpressions/RegexFindOptimizations.cs)
and
[`regex-automata` prefilter state](https://github.com/BurntSushi/regex-automata/blob/33b73c5ada010e9925000938aa9ddc74fa868d1c/src/util/prefilter.rs).

This port can extend its existing start-byte scanner when every possible match
has a more selective byte set at one fixed offset. For example,
`[a-z]{8}-[0-9]{4}` can scan for `-` and rewind eight bytes instead of handing
every letter to the DFA.

Required controls:

- analyze only a bounded prefix of the program
- reject assertions, nullable patterns, and variable-distance candidates
- require a strictly more selective set than the existing start-byte set
- keep scanning and DFA traversal monotonic
- abandon the scanner for the rest of a search after a short skip
- preserve the existing path below a source-length threshold
- build metadata lazily and charge it to the DFA budget

This scanner is retained. Source-disabled session `20260715T164348Z` reproduced
the sparse and no-match gains on Intel and Graviton while the protected dense
shape remained neutral on Intel and improved on Graviton. Implementation and
route-census details are recorded below and in `ENGINEERING_RESULTS.md`.

### 2. Complete Finite-Language Bypass

Rust and Hyperscan bypass general automata for exact literals and small finite
literal languages. See the Rust
[`meta` strategy](https://github.com/rust-lang/regex/blob/926af2e68eca3ce089815790541cf50759ba2c59/regex-automata/src/meta/strategy.rs)
and Hyperscan's
[`compile_lit` interface](https://intel.github.io/hyperscan/dev-reference/compilation.html#compile-pure-literals).

The port already specializes complete one-byte languages. A bounded extension
could handle one multi-byte literal or a small set of literals and drive count,
split, extraction, and replacement from one reusable scanner cursor.

Initial eligibility should exclude empty alternatives, assertions, general
case folding, and captures whose boundaries are not statically derivable.
Alternation order and longest-match behavior must be tested directly.

A benchmark-only prototype tested three equal-length, distinct-first-byte
literals with `ALPHA|BRAVO|CHIME`. This shape has no required prefix and no
more-selective fixed-distance byte, so it isolated direct literal verification
from existing accelerators. At 32 KiB, candidate/baseline ratios were:

| Operation | Sparse match | Dense false positive | No match |
|---|---:|---:|---:|
| contains | 1.04x | 0.92x | 1.02x |
| count | 1.02x | 0.94x | 1.02x |
| match boundary | 1.03x | 0.81x | 1.02x |

The warm start-byte and DFA path is already better for the common sparse and
no-match shapes. The direct matcher helps only when candidate first bytes are
deliberately dense, which does not justify permanent production machinery or a
regression elsewhere. The initial finite-language bypass is rejected. Revisit
only if a representative corpus demonstrates dense false candidates or a
scanner that also improves sparse and no-match inputs.

### 3. State-Local Self-Loop Scanning

Rust and VectorScan accelerate DFA states whose common bytes are exact,
non-reporting self-loops, scanning directly for one of the few bytes that can
leave the state. See Rust's
[`dfa::accel`](https://github.com/BurntSushi/regex-automata/blob/33b73c5ada010e9925000938aa9ddc74fa868d1c/src/dfa/accel.rs)
and VectorScan's
[`McClellan` acceleration](https://github.com/VectorCamp/vectorscan/blob/a1c107ed92b6cc811a6fbd6b7dfcc7f181e5ab85/src/nfa/mcclellan.c).

The first prototype statically recognized an unanchored entry state whose
common bytes entered one dominant self-loop state. It reduced sparse and
no-match inputs substantially, but selected only one of 246 successful Rebar
search rows and regressed protected Intel 32 KiB dense inputs by 7.3-8.0%.
That entry-state implementation remains rejected.

The compact-layout campaign found a broader and safer code-shape specialization
by observing actual transitions instead of predicting a program topology. For
a long end-constrained search that has not entered paired execution, the engine
samples 64 normal transitions. At least 58 exact self-loops dispatch to an
isolated identity-first scanner. A rejected sample resumes the original compact
loop, and paired execution bypasses the sampling method entirely.

The physical separation matters. Putting the identity check in every
transition regressed state-changing Intel topologies by about 13%. Leaving the
sampling branch in the shared paired continuation regressed Graviton `HARD` by
5.6% even when the predicate rejected it. The retained dispatcher leaves the
old hot loop unchanged and measured state-changing compact traversal at 0.999x
control on Intel and 1.001x on Graviton. Paired `HARD` and `PARENS` remained
within 1.0% of control on both architectures.

On the integrated unpaired workload with more than 500 warmed states, session
`20260716T051735Z-81238` measured 0.432x compact control and 0.599x native RE2
time on Intel, and 0.366x control and 0.515x native time on Graviton. The native
comparison used equivalent input and expressions and host-tuned `-O3 -DNDEBUG`
builds. The scanner is retained for this guarded shape. Complete representation
and policy evidence is in
[`DFA_LAYOUT_EXPLORATION.md`](DFA_LAYOUT_EXPLORATION.md).

### 4. Required Literal And Selective Suffix Search

Joni propagates exact strings, byte maps, and minimum/maximum distances through
the expression. Rust can scan a selective suffix and use reverse and forward
verification to recover exact match boundaries. PCRE2 also uses required code
units as start optimizations.

See Airlift Joni's pinned
[`Analyser`](https://github.com/airlift/joni/blob/2.1.5.3/src/org/joni/Analyser.java),
Rust's
[`reverse suffix` strategy](https://github.com/rust-lang/regex/blob/926af2e68eca3ce089815790541cf50759ba2c59/regex-automata/src/meta/strategy.rs),
and PCRE2's
[`study` analysis](https://github.com/PCRE2Project/pcre2/blob/ff92e0b9cea5b5ae3af12ba930d03556684f098b/src/pcre2_study.c).

An absence-only required-literal filter is low risk because it performs at most
one extra linear pass. Reusing a suffix location to recover a start is more
powerful but must process candidate intervals monotonically; running a complete
anchored verification for every literal occurrence can become quadratic.

A benchmark-only absence filter tested `[a-z]+NEEDLE[0-9]+`, a variable-distance
shape not covered by the fixed-distance or start-byte scanners. The filter
uses Slice's exact byte search and invokes the ordinary DFA only when `NEEDLE`
is present. Directional one-fork Apple Arm results were:

| Input bytes | Shape | Filter/DFA |
|---:|---|---:|
| 32 KiB | sparse late match | 1.17x |
| 32 KiB | dense false positives | 1.01x |
| 32 KiB | no literal | 0.10x |
| 2 MiB | sparse late match | 0.94x |
| 2 MiB | dense false positives | 0.92x |
| 2 MiB | no literal | 0.09x |

The no-match result is an algorithmic win, but the ordinary 32 KiB successful
case proves that an unconditional extra pass is not acceptable. A production
design needs either a large-input gate, a contention-free adaptive policy, or a
suffix strategy that reuses the located literal instead of rescanning from the
beginning. The benchmark-only implementation is not retained.

The current Rebar census found required atoms in 110 of 194 successful Latin1
rows. Existing prefix, start-byte, fixed-distance, and paired routes already
cover most of them; only 12 rows with required atoms remain on compact routes,
and one of those is case-insensitive. This is enough representative coverage to
keep suffix-driven verification as a ranked residual candidate, but not enough
to put a required-literal pass on every search.

A second benchmark-only prototype tested reverse suffix verification with
`[a-z]+(?:NEEDLE)?NEEDLE`. It scans for the required suffix, runs the existing
reverse DFA through that suffix to recover a candidate start, and runs an
anchored forward DFA from the recovered start so greediness still selects the
correct end. Literal absence proves no match. If the first suffix occurrence
cannot complete a match, the prototype immediately delegates to the ordinary
engine instead of scanning another suffix. This deliberately gives up some
opportunities to keep all speculative work bounded by one literal scan and one
reverse attempt.

The initial prototype allowed repeated reverse attempts under a cumulative
two-input-length budget. It was 4.3-11.6x faster on absent and sparse literals,
but 1.32-1.33x slower on dense 32 KiB false candidates. Immediate fallback
removed that tradeoff in a three-fork local screen:

| API | Input shape | 32 KiB candidate/control | 2 MiB candidate/control |
|---|---|---:|---:|
| Boolean | No suffix | 0.091x | 0.088x |
| Group-zero bounds | No suffix | 0.092x | 0.087x |
| Boolean | Sparse late match | 0.257x | 0.207x |
| Group-zero bounds | Sparse late match | 0.257x | 0.235x |
| Boolean | Dense false candidates | 0.998x | 0.992x |
| Group-zero bounds | Dense false candidates | 0.971x | 0.962x |

The boundary-producing matcher is rejected. A required common suffix does not
prove that the first suffix completing a match belongs to the leftmost match.
For example, `[a-z].{10}foo|[a-z]bfoo` can complete the short alternative at an
earlier `foo`, while a later `foo` completes a match beginning four bytes
earlier. Returning the first successful reverse result therefore violates
leftmost-first semantics. The same counterexample reproduces in Rust's current
reverse-suffix strategy with its automatic prefilter enabled. The prototype
also hard-coded first-match forward recovery and was therefore incorrect for
leftmost-longest mode.

Boolean existence is narrower and safe: a successful reverse verification
proves that some match exists without exposing its boundary. Literal absence
still proves no match, while a failed or unavailable reverse search delegates
to the ordinary engine because a later suffix may match. Any caller requesting
group zero or captures must bypass this optimization until a leftmost proof is
available.

Target session `20260715T194218Z` confirms that the boolean opportunity is
material. Intel no-suffix input took 0.037-0.038x control, sparse late matches
took 0.248-0.266x, and dense false candidates took 1.000-1.005x. Graviton took
0.035-0.040x, 0.280-0.294x, and 0.979-1.009x respectively. Candidate-before and
candidate-after agreement was not part of this benchmark-only prototype, so it
motivated the exact source-disabled production A/B below. Production evaluation
was limited to a conservative case-sensitive ASCII suffix extractor, the
existing shared reverse program and cache, unanchored boolean searches, and
ordinary fallback. The unsafe boundary path was excluded regardless of its
benchmark result.

The production candidate retains at most the final 64 exact ASCII bytes, so a
large trailing literal cannot create a second unbounded allocation. It activates
only for unanchored boolean searches over at least 4 KiB and only when the
existing required-prefix routes do not apply. A missing suffix rejects without
building the reverse program. A successful first reverse verification proves
existence. A failed verification delegates to the ordinary engine rather than
trying another suffix.

The 285-row Rebar census contains ten eligible rows and seven distinct useful
patterns. Nine already select start-byte acceleration and one selects the
fixed-distance scanner, so displacement of a good route is the primary risk.
Eight rows use Rebar's count-spans model and two use count; none directly calls
the boolean API. A dedicated benchmark therefore reuses their exact patterns
and inputs while comparing boolean search with the protected group-zero route.
Locally, the production boolean path took 0.064-0.512x group-zero time on six
inputs and 0.992x on the seventh. This is a directional screen, not the
source-disabled acceptance comparison.

Exact source-disabled session `20260715T211620Z` rejects the production design
on both target architectures. Candidate-before and candidate-after agree, and
both hosts passed the complete RE2 selector. Candidate/control ratios were:

| Boolean input shape | Intel 32 KiB / 2 MiB | Graviton 32 KiB / 2 MiB |
|---|---:|---:|
| No suffix | 0.040x / 0.040x | 0.031x / 0.042x |
| Sparse late match | 0.295x / 0.267x | 0.302x / 0.284x |
| Late false suffix | 1.046x / 1.043x | 1.051x / 1.077x |
| Late false suffix, then match | 1.032x / 1.031x | 1.037x / 1.068x |
| Long spanning match | 5.667x / 5.563x | 4.441x / 4.457x |

The absence and sparse-match wins reproduce, but a present suffix adds a
literal scan and reverse attempt before ordinary fallback. That duplicates a
large fraction of the search for late false candidates and nearly the complete
search for a long-spanning match. Representative Intel Rebar boolean rows also
regressed by 2.4-6.9%. Protected 16 MiB direct-DFA rows took 1.014x control on
Intel and 1.007-1.017x on Graviton, while compilation took 1.006x and 1.017x.
The Graviton group-zero `REALLY_HARD` control, which bypasses the dispatch,
still took 1.350x after the added metadata changed the compiled object shape.

These costs violate the 2% gate and show that apparently dormant analysis can
disturb protected routes. The extractor, dispatch, tests, selected corpus, and
campaign mode were removed. Required-literal search remains useful evidence for
future route-specific work, but an unconditional production suffix field and
pre-search reverse attempt should not be repeated.

### 5. Minimum And Maximum Byte Width

Joni and PCRE2 compute minimum lengths, and Joni tracks bounded distances from
candidate literals. This port's fixed-length analysis now also computes minimum
encoded byte length in the same stack-safe AST walk. It rejects short windows
before reverse-program construction, while retaining exact fixed length for
boundary recovery.

The retained local implementation consults minimum width only below 4 KiB. An
unconditional metadata load added about 0.7% to an approximately 11.5 ns
anchored rejection; the bounded check reduced that to about 0.4% with
overlapping intervals. On eight-byte windows, minimum-width rejection improved
five representative paths by about 4.7x. A three-fork case-fold compile control
was 1.006x baseline with overlapping intervals. Widths are encoded-byte widths,
saturate on overflow, and are computed after any required prefix is stripped.
A finite maximum remains unimplemented and must demonstrate a concrete reverse
recovery gap before adding more metadata.

### 6. Operation-Specific Capture Demand

.NET separates existence-only, bounds-only, and full-capture runner modes. Rust
also delays capture work until captures are requested. Historical Trino RE2J
initially requested group zero and reran a bounded match when a subgroup was
read.

The local experiment below showed that a global lazy design regresses short
repeated matches. The retained prototype instead uses the operation-specific
form: count, position, group-zero extraction, and split construct a matcher with
only group zero, while subgroup extraction and lambda replacement retain the
full capture buffer. Literal replacement scans its replacement shape once and
uses group-zero matching only when no subgroup can be referenced. No match is
replayed.

A provisional local screen reduced Unicode group-zero extract to 0.61x control,
extract-all to 0.84x, and split to 0.74x. Source-disabled session
`20260715T184445Z` accepted the complete operation-specific change on both
architectures: the 15 protected 32 KiB rows had a 0.851x candidate/control
geomean, affected Unicode operations took 0.684-0.789x control, and the largest
protected ratio was 1.005x. Boundary-equivalence tests cover greedy, reluctant,
optional, nested, anchored, empty, and UTF-8 captures.

### 7. Continue From DFA Cache Failure

.NET's symbolic engine can continue from the current NFA state set when its DFA
node limit is reached instead of restarting the search. See
[`SymbolicRegexMatcher`](https://github.com/dotnet/runtime/blob/f41747370bfdd6887c4ae4a27c5bc219f838eedc/src/libraries/System.Text.RegularExpressions/src/System/Text/RegularExpressions/Symbolic/SymbolicRegexMatcher.cs).

This could avoid rescanning a long prefix after DFA cache exhaustion, but it is
high risk: ordered threads, empty-width context, prior accepted matches, and
leftmost semantics must all survive the transition. Profile cache-failure
restarts before attempting it.

## Capture Findings

The earlier cross-engine report overstated the current capture deficit because
it predated retained sparse start-byte acceleration. A same-host development
run at checkpoint `aa4d842` measured the 32 KiB `captureSparse` workload as
follows:

| Operation | Slice ns/op | Joni ns/op | Historical Trino RE2J ns/op |
|---|---:|---:|---:|
| contains | 2,822 | 6,892 | 2,803 |
| count | 11,248 | 27,811 | 11,694 |
| extract | 2,887 | 6,937 | 2,913 |
| extract all | 11,542 | 28,462 | 11,958 |
| third position | 23,375 | 36,690 | 23,603 |
| replace | 13,196 | 29,219 | 12,392 |
| lambda replace | 13,174 | 29,640 | 13,888 |
| split | 11,880 | 30,562 | 14,202 |

This is developmental evidence, not formal qualification. It shows Slice at
0.39-0.64x Joni and near historical RE2J parity for this workload; plain replace
is the only listed operation more than 2% slower than historical RE2J.

Two capture experiments were rejected:

1. Limiting extraction to the subgroup requested by a Trino operation was
   neutral for the seven-byte sparse match and added complexity.
2. Globally finding group-zero bounds and replaying capture extraction improved
   long boundary-only matches but penalized short repeated matches and callers
   that read subgroups.

The controlled second experiment produced:

| Operation | Match bytes | Eager ns/op | Bounds-only ns/op | Bounds/eager |
|---|---:|---:|---:|---:|
| first boundary | 8 | 1,418 | 1,391 | 0.98x |
| first boundary | 256 | 5,352 | 4,207 | 0.79x |
| first boundary | 4,096 | 21,906 | 17,008 | 0.78x |
| all three boundaries | 8 | 10,607 | 11,122 | 1.05x |
| all three boundaries | 256 | 22,766 | 18,492 | 0.81x |
| all three boundaries | 4,096 | 69,768 | 54,522 | 0.78x |

The right conclusion is not that bounds-only execution is bad. Global lazy
replay is a long-match specialization without a reliable gate; statically
bounds-only operations avoid that tradeoff entirely.

## Nullable Single-Byte Repetition Counting

The remaining measured Joni loss was empty-match counting for `x*`. Allocation
profiles were already negligible. The cost came from invoking one DFA search at
every input boundary: each search paid reader registration and start-state
analysis even when it immediately returned an empty match.

The retained local specialization recognizes only greedy zero-or-more repetition
of a language proven to consume exactly one byte. It counts maximal matching
runs, unmatched UTF-8 code points, and the final empty boundary in one pass.
Reluctant repetition, assertions, multi-byte languages, and other nullable
structures retain ordinary matcher iteration. Direct tests compare both paths
for captures, case folding, UTF-8, Latin1, malformed bytes, nonzero Slice
offsets, empty inputs, and the equivalent `{0,}` syntax.

A controlled one-fork local comparison at 32 KiB measured:

| Input shape | Regular matcher ns/op | Specialized counter ns/op | Candidate/baseline |
|---|---:|---:|---:|
| All misses | 124,725 | 17,978 | 0.14x |
| One matching run | 101,986 | 15,346 | 0.15x |
| Alternating match/miss | 881,081 | 26,423 | 0.03x |
| 64-byte runs | 226,241 | 28,030 | 0.12x |
| Four-byte UTF-8 misses | 42,205 | 12,029 | 0.29x |

The complete Trino `count` benchmark improved from 125.15 microseconds to
17.50 microseconds locally. The matching same-host Joni benchmark took 358.38
microseconds. These are directional development results from separate but
byte-identical harnesses. Target session `20260715T164403Z` subsequently
confirmed 32 KiB candidate/control ratios of 0.02-0.26x on Intel and 0.03-0.30x
on Graviton across the five controlled shapes. Exact-current cursor session
`20260715T195116Z` accepts both counting and boundary iteration on both
architectures: direct paths take 0.014-0.473x ordinary matcher time,
empty-match operations improve, and protected exact-one operations remain
neutral. Captures now disable the boundary cursor only when the operation reads
subgroups; position, split, group-zero extraction, and group-zero-only
replacement retain it. The refreshed operation matrix takes 0.355x Joni time
on Intel with 79/80 wins and 0.294x on Graviton with 80/80 wins. Same-host
focused session `20260715T211625Z` resolves the lone nominal Intel loss:
interleaved Slice takes
0.993x Joni time with overlapping uncertainty on Intel and 0.843x on Graviton.
There is no reproducible current Joni loss.

## Fixed-Distance Prototype

The prototype uses `[a-z]{8}-[0-9]{4}` over 1 KiB and 32 KiB inputs with three
shapes:

- sparse valid matches
- dense `-` false positives
- no `-` and therefore no match

The retained local design:

- walks at most the first 16 consumed bytes
- selects `-` at offset eight instead of the 26-letter start set
- activates only for inputs of at least 4 KiB
- switches permanently to the existing start-byte scanner when a candidate
  advances fewer than 16 bytes
- analyzes lazily under the DFA mutation lock
- charges 304 retained bytes plus the DFA bookkeeping fields and preserves the
  plan charge across cache resets

A directional local A/B on 32 KiB inputs measured:

| API | Input | Candidate/baseline |
|---|---|---:|
| match boundary | sparse match | 0.16x |
| match boundary | dense false positive | 0.96x |
| match boundary | no match | 0.57x |
| boolean partial match | sparse match | 0.16x |
| boolean partial match | dense false positive | 1.00x |
| boolean partial match | no match | 0.58x |

DFA construction was 0.998x baseline and total `Re2.compile` was 1.003x after
moving analysis out of construction. These local results motivated the
source-disabled target-host gate; they did not establish acceptance by
themselves.

Target session `20260715T164348Z` subsequently accepted the scanner on both
architectures. Sparse and no-match boundary and boolean searches took
0.14-0.22x disabled time. Dense false-positive controls took 0.98-1.00x on
Intel and 0.74-0.89x on Graviton. Candidate-before and candidate-after runs
agreed, and both hosts passed the complete RE2 selector.

The current official Rebar checkout at
`463d00f31887e84c38467805b9e3122c314b9521` expands to 285 analyzable
RE2-compatible rows and 198 distinct pattern/flag combinations. All compiled;
26 rows, representing 15 distinct patterns, contain a qualifying fixed-distance
candidate. Twenty-two of those rows execute searches and four are compile-only.
The earlier selected ten-workload paired corpus contained no candidates because
it was deliberately chosen around paired-table boundaries, not scanner
coverage. Real eligible shapes include literal alternations, dictionary
patterns, bounded context, `[a-z]shing`, `[a-q][^u-z]{13}x`, and
`[XYZ]ABCDEFGHIJKLMNOPQRSTUVWXYZ$`.

After normal route precedence and execution, 18 of the 246 successful search
rows actually select fixed-distance acceleration. For context, 82 select
start-byte acceleration, 47 use an existing prefix path, 23 use complete
pairing, and six use partial pairing. The row formerly handled by the rejected
self-loop scanner returns to complete pairing.

Lazy initialization occurs before DFA reader registration. The first version
initialized while a shared search was active, which could deadlock with an
exclusive cache reset holding the mutation lock and waiting for that reader.
Semantic ineligibility is cached permanently; temporary budget pressure is
retried only after a cache reset rather than evicting a warm DFA.

## Deferred Compact DFA Research

The following work is intentionally deferred until the existing DFA engine and
its current transition representation are integrated, stable, and qualified.
These are independent follow-up investigations, not optional extensions of the
active DFA work. Do not start them automatically when an active experiment
stops or fails.

Two independent decisions define this research space:

| Decision | Alternatives |
|---|---|
| Graph construction | Lazy RE2 cache, bounded completion after use, or bounded eager completion |
| Transition representation | Object references, absolute native pointers, or compact state identifiers |

### Compact Relative Transitions

Native RE2 stores full-width atomic state pointers, which gives its lazy DFA a
minimal dependent chain but uses eight bytes per transition on the target
64-bit platforms. Other high-performance engines commonly use compact numeric
state identifiers: Rust `regex-automata` uses 32-bit premultiplied state IDs,
Hyperscan uses eight- or sixteen-bit IDs for qualifying automata, and .NET's
non-backtracking engine uses integer IDs in a flat transition table.

A future FFM experiment should compare the qualified absolute-pointer baseline
with 32-bit base-relative byte offsets. The relative loop would load a compact
offset and add the allocation base before the next transition. This adds one
dependent arithmetic operation but halves the transition payload, potentially
extending the cache-friendly range substantially. Eligibility and crossover
must be expressed in table bytes, not a fixed state count:

```text
table bytes = state count * transition class count * transition width
```

Test the 32-bit representation before considering narrower IDs. If it
qualifies, complete graphs can additionally select byte or unsigned-short state
IDs when their state and table bounds permit. Narrow representations must
reserve unambiguous encodings for dead, match, full-match, and unavailable
transitions.

### Bounded Complete Determinization

A complete DFA can require exponential time and space, so unbounded eager
determinization is not acceptable. A bounded attempt can still be useful for
the compile-once, scan-many workload:

```text
attempt complete DFA construction
    complete graph fits final and temporary memory limits -> publish it
    either limit is exceeded -> abandon the attempt and retain lazy RE2 DFA
```

The builder must independently cap final table bytes, temporary determinization
memory, state count, and construction work. Exceeding any limit is an ordinary
eligibility result, not a matching failure. The lazy DFA and its normal
OnePass, BitState, and NFA fallback cascade remain authoritative.

Building every DFA variant during `Re2.compile()` would waste work because
first-match, longest-match, reverse, and many-match automata are requested
independently. A likely alternative is demand-triggered bounded completion:
begin with the lazy cache, identify a repeatedly used DFA kind, attempt complete
construction once, and publish an immutable table only on success. Failed
completion must not be retried repeatedly.

### Immutable Table Layouts

A completed graph removes uncomputed transitions, state-cache lookup, mutable
publication, cache reset, reader quiescence, and missing-transition recovery
from its search route. Evaluate the graph construction benefit separately from
the physical table layout by comparing:

- heap `int[]` transitions
- heap `char[]` transitions for qualifying state ranges
- bounded `MemorySegment` transitions
- everything-segment relative offsets
- the qualified absolute-pointer baseline

Completeness does not guarantee that HotSpot can eliminate a heap-array bounds
check: each next state is data loaded from the table, and the compiler may not
prove that every stored value identifies a valid row. Conversely, a primitive
heap table may be close enough to native performance that restricted FFM access
is unnecessary. Decide from target-host assembly and measurements rather than
assuming either outcome.

Rust and .NET use power-of-two row strides and premultiplied state identifiers
to simplify indexing. Compare exact-width rows with pre-scaled byte offsets
against power-of-two rows only after a compact complete table qualifies. The
extra padding must be included in cache-footprint and memory-budget accounting.

### Multi-Byte Transition Composition

A small complete graph can precompose multiple transitions and process more
than one input byte per dependent table lookup. A two-byte table grows roughly
as:

```text
state count * transition class count^2 * transition width
```

This is practical only for tightly bounded state and alphabet sizes. Treat it
as a specialized follow-up to a successful complete-table representation, not
as a universal DFA expansion. Compare it with the existing paired-transition
design rather than adding an overlapping unbounded route.

### Future Engine Tiers

If the investigations qualify, the eventual routing model could be:

```text
complete compact DFA   -> small graph, immutable table
lazy bounded DFA       -> large theoretical graph, small observed subset
OnePass/BitState/NFA   -> DFA cache cannot make productive progress
```

Useful diagnostics include complete-DFA eligibility, final state and byte-class
counts, final and temporary bytes, completion abort reason, promotion count,
bytes processed by each route, and ordinary DFA reset/fallback count. Corpus
coverage is informational; it must not justify broadening a cache-hostile
representation.

### Exploration Order And Gates

1. Establish a stable, qualified baseline for the current DFA representation.
2. Compare 32-bit relative FFM offsets with that exact baseline.
3. Measure bounded complete-DFA sizes without changing production routing.
4. Prototype bounded completion with deterministic limit tests.
5. Compare immutable heap and FFM table layouts.
6. Decide eager versus demand-triggered completion.
7. Consider narrower IDs only after the 32-bit representation qualifies.
8. Consider multi-byte composition only for a qualified tiny-DFA population.

Every experiment must retain the existing memory budget, object-DFA fallback,
linear-time behavior, and Intel and Graviton no-regression gates. A failed gate
ends that experiment; it does not authorize the next representation
automatically.

## Ideas Not To Import Directly

- General backtracking, auto-possessification, and backtracking JIT techniques
  do not fit RE2's execution model.
- Hyperscan's full Rose/FDR multi-pattern architecture is designed around a
  different reporting contract and start-of-match tradeoff. Small literal and
  scanner components are relevant; the complete architecture is not.
- A fully expanded transition table has already shown cache and memory
  regressions in this port. Reconsider only with materially different evidence.
- Earlier bounded-segment FFM and buffer-based DFA layouts were neutral or
  slower across the target architectures. Do not generalize those results to a
  materially different access shape, and do not revive `Unsafe`; it remains
  diagnostic-only and unshippable.
- Shared adaptive policy across Trino workers can add contention and
  nondeterminism. Prefer one-way decisions local to one search.

## Next Order

1. Finish the official Rebar-compatible search and compile comparison.
2. Do not start the deferred relative-offset, complete-DFA, absolute-pointer,
   or multi-byte work automatically. Reassess only after the user explicitly
   selects a materially different investigation.
