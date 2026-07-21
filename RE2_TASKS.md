# RE2 Port Status and Work List

This is the authoritative status and task list for the RE2 port. Campaign logs,
historical audits, and benchmark results record useful evidence, but they do not
override this document.

Last organized: 2026-07-20

## Current Status

| Item | Status |
|---|---|
| Ported surface | Engine, Slice API, reusable matcher, set/filtered APIs, and Trino operation adapter are present |
| Build | Verified 2026-07-20: the latest Maven install passes with 2,714 tests, zero failures or errors, and one intentional skip; the current 922-test RE2 selector passes with and without native access |
| Release readiness | Not ready |
| Correctness | Complete upstream case-table, exhaustive, randomized, native differential, public/direct-engine, and compatibility coverage passes |
| Upstream parity | Applicable pinned upstream test tables and active generated parameters are ported |
| Performance | Candidate qualification places normal public execution at 0.730x/0.638x native time and public capture at 1.028x/1.021x on Intel/Graviton. Normal Rebar execution is 0.991x/0.970x. Final Joni acceptance qualifies 78/80 rows per architecture and all qualify as wins, with 0.384x/0.337x geometric Slice/Joni time and 0.991x/0.963x worst rows. The final search-outlier campaign closes long Cloudflare at 0.950x/0.942x native time and reduces case-insensitive English to 1.157x/0.855x. The vector literal campaign reduces the former Russian and Chinese literal outliers by 15x/12x and 30x/5x on Intel/Graviton; final same-session ratios are 0.592x/0.072x native for Russian and 0.726x/0.176x for Chinese. The smaller Intel case-insensitive residual and `SplitBig2` capture remain. |
| Cleanup | Public-surface, package encapsulation, naming, and cold-path structural cleanup are complete; protected loops are frozen at the benchmark-qualified capture-engine baseline |
| Review history | Development log consolidated into five independently buildable reviewer-facing commits |

The baseline review that initiated this correctness campaign is
[`docs/audits/history/2026-07-13-port-readiness-review.md`](docs/audits/history/2026-07-13-port-readiness-review.md).

## Resume Point

The most recent full-census source archive is snapshot
`10433946d70fcf1f27d36aa5cbeb59ac8e7c7805e8d3f0747535582d84ed8375`
at engine checkpoint `23d21694c0cf94d1d9e3dc06ab4f0bc61ceadfde`.
It retains a primitive sparse NFA queue, compiled capture metadata, shared
compact NFA/BitState instructions, direct OnePass capture finalization, and a
bounded initial BitState job capacity. Reusable matchers additionally retain
private NFA and BitState scratch, and eligible long unanchored captures use
BitState directly. Eligible finite-budget forward compact DFAs use the
geometrically grown absolute-pointer sidecar. Repeated short eligible searches
promote to that sidecar after 16 calls, while isolated short calls preserve
later paired-table eligibility.

The current focused-campaign source additionally routes deterministic programs
beyond native RE2's five-group OnePass limit through a budgeted 64-bit
capture-mask sidecar for up to 32 groups, while smaller requests retain the
original packed hot loop. Reusable matchers can also bind a logical input region
without allocating a Slice view for every failed line search. Eligible repeated
group-zero searches additionally use a bounded candidate-start cursor instead
of replaying every match through forward and reverse DFA searches. That cursor
bulk-scans rejected bytes while preserving its reset-scoped work budget. The
complete local RE2 selector passes 922 tests with and without native access,
and the full Maven install passes 2,714 tests with zero failures or errors and
one intentional skip.

The latest source also scans matching DFA self-loops directly after the first
paired match and replaces folded-prefix ShiftDFA with fused first/last candidate
masks. Target session `20260720T224822Z-6754` closes Cloudflare simplified-long
at `0.950x/0.942x` native time and moves case-insensitive English from
`2.163x/1.628x` to `1.157x/0.855x` on Intel/Graviton. See
`docs/benchmarks/history/2026-07-20-final-outlier-closure.md`.

Case-sensitive non-ASCII multi-byte prefixes now use fused front/back masks so
common UTF-8 leading bytes do not repeatedly return to scalar verification.
Searches below 1 KiB use SWAR and longer searches use the platform's preferred
vector species. Exact candidate/control brackets reduce Russian and Chinese
literal count from 2.710 ms to 0.171-0.181 ms and 0.696 ms to 0.0227-0.0230 ms
on Intel, and from 4.300 ms to 0.353-0.363 ms and 0.734 ms to 0.132-0.138 ms on
Graviton. Apple Silicon qualification shows the same 12x and 5.5x improvements.
See `docs/benchmarks/history/2026-07-20-vector-literal-scanning.md`.

Focused Java/native ratios from the July 18 capture-engine campaign are retained
for path-specific context; they are not the current census aggregates:

| Engine | Intel | Graviton |
|---|---:|---:|
| NFA | 1.013x | 1.302x |
| OnePass | 0.935x | 1.216x |
| BitState | 1.180x | 1.565x |

The final capture-pipeline session reduces Veryl count captures by 40% on both
architectures, Unicode capture grep by 15%/39%, and `SplitBig2` count captures
by 83%/84% on Intel/Graviton. Capture-free count is unaffected. Group-zero
spans remain a separate forward-plus-reverse DFA path because they do not run a
capture engine.

Exact affected-model session `20260718T074110Z-72844` places count with captures
at `1.033x/0.960x` native time and five-row capture grep at `1.180x/0.968x` on
Intel/Graviton. The aggregate grep result is mixed: AWS-keys capture grep is
`0.540x/0.447x`, while the two Ruff rows remain `1.423x/1.161x` and
`1.262x/1.188x`. Unicode is `1.495x/1.013x`; its remaining Intel deficit is
10.2 ms. The unstructured row is `1.576x/1.362x` but only about 0.1 ms slower
per complete operation.

Post-audit session `20260718T083119Z-79902` used the current source and strict
2% native-bracket drift rejection. Graviton passed, with count captures at
`0.971x` and five-row capture grep at `0.949x`. Intel completed correctness and
the full matrix, but Unicode native drifted 3.50% and unstructured native
drifted 4.60%, so the host result was rejected as qualification evidence. Its
diagnostic `1.060x` count and `1.172x` grep ratios are consistent with the
retained baseline, but they must not be presented as qualified current-source
results. The bounded optional confirmation is exhausted; do not rerun it as
part of this campaign.

The pre-workspace traditional session `20260718T023744Z-76513` places stable
public capture at `1.035x/1.144x`. The prior `2.60x/2.69x` headline is
superseded because four old Java rows used unanchored first-match while native
measured anchored full-match. Public failed search, successful search, and full
match remain faster than native at `0.499x/0.452x`, `0.670x/0.587x`, and
`0.387x/0.343x`. That full census predates the retained direct BitState route;
the focused exact-source campaign reduces its `SplitBig2` control by 83%-84%.

The bounded engine and pipeline campaigns are closed. Rejected sparse histories,
primitive BitState traversal stacks, public offset folding, direct unanchored
NFA, and a group-zero fused-DFA assumption must not be repeated unchanged.
Detailed evidence is in
`docs/benchmarks/history/2026-07-18-capture-engine-campaign.md` and
`docs/benchmarks/history/2026-07-18-capture-pipeline-campaign.md`.

Sessions `20260718T191726Z-61590` and `20260718T192636Z-67046` close the
bounded-context cache-capacity issue. Runtime-aware retained-memory accounting
and a demand-driven 96 MiB default eliminate target resets and improve the
operation by 6.7x/5.8x relative to 8 MiB on Intel/Graviton, while controls remain
within 0.2%. That campaign isolated a `1.386x/1.590x` native object-row deficit;
the final large-pointer campaign closes it at `0.908x/0.946x` native time. See
`docs/benchmarks/history/2026-07-18-dfa-cache-capacity-policy.md` and
`docs/benchmarks/history/2026-07-18-dfa-large-pointer-sidecar.md`.

Candidate `c212684` initially superseded the earlier Joni operation headline. The
multi-session Intel aggregate wins 73 of 79 qualified rows with a `0.481x`
median, while current two-session Graviton evidence wins 67 of 72 rows with a
`0.435x` median. That snapshot exposed five 32 KiB `captureSparse`
repeated-boundary losses. Ordinary warm-pattern memory remains `0.474x`
Joni at the median. The 75,850-state bounded graph retains a 7.96 MiB sidecar
and roughly 30 MiB total because Joni does not retain an equivalent DFA. See
`docs/benchmarks/history/2026-07-20-candidate-qualification.md`.

The completed sparse-boundary campaign corrects the initial diagnosis of those
five losses. They use the candidate-start cursor, record zero fallbacks, and do
not construct the reverse program. The retained candidate replaces per-byte
mutable work-budget accounting with the existing tight candidate scanner while
charging the same total work. It passes the 915-test RE2 selector with and
without native access. Target session `20260720T182018Z-72726` wins all 32
focused rows on each architecture, with `0.546x/0.463x` median Slice/Joni time
and `0.335x`-`0.552x`/`0.395x`-`0.469x` across the 32 KiB
`captureSparse` rows on Intel/Graviton. See
`docs/benchmarks/history/2026-07-20-joni-sparse-boundary-campaign.md`.

The final three-session Joni acceptance covers the complete 80-row operation
matrix on each architecture. At the strict 2% bracket gate, 78 rows qualify per
architecture and every qualified row wins. Intel takes geometrically 0.384x
Joni time with a 0.991x worst row; Graviton takes 0.337x with a 0.963x worst
row. The four architecture-specific exclusions are strong directional wins but
lack two stable brackets. See
`docs/benchmarks/history/2026-07-20-joni-final-acceptance.md`.

Sessions `20260719T054652Z-ruff-pipeline` through
`20260719T061153Z-33390` close Ruff `real`. Capture extraction was immaterial:
the workload makes 890,926 forward searches but only 20 reverse and OnePass
capture passes. Removing redundant matcher-buffer clears and promoting repeated
short searches to FFM reduces the operation from 105.37/115.33 ms to
72.96/96.83 ms on Intel/Graviton, or `0.998x/0.983x` retained native time.
Ruff `tweaked` remains a line-window integration outlier; its forward stage is
already faster than native's complete operation. See
`docs/benchmarks/history/2026-07-18-ruff-short-dfa.md`.

Session `20260719T071144Z-current-census` was the prior current-source native
baseline and is superseded by candidate `c212684`.
The sharded traditional comparison qualifies 593 of 596 rows at a strict 2%
native-bracket drift limit. Public failed search, successful search, full match,
and capture take `0.507x/0.456x`, `0.655x/0.589x`, `0.389x/0.326x`, and
`0.984x/1.012x` native time on Intel/Graviton. The Rebar comparison qualifies
81 of 82 rows: count is `0.827x/0.569x`, count captures is approximately
`1.058x/0.964x`, grep captures is `1.089x/0.920x`, and count spans remains
`1.997x/1.704x`. See
`docs/benchmarks/history/2026-07-19-current-native-census.md`.

Sessions `20260719T173556Z-30414` through `20260719T173611Z-30577`
decompose all six group-zero span workloads on Intel and Graviton. Date's
composed forward/reverse replay accounts for 98.6%/95.7% of public time and no
capture engine runs. Public integration cleanup cannot close its
`1.630x/1.385x` native gap. The tiny Cloudflare ratios are fixed-cost outliers,
while both English controls are already native wins. No production change is
retained. Any follow-up must be a separate repeated-boundary cursor or tagged
DFA campaign with shared-cache safety and English controls, not another
matcher cleanup. See
`docs/benchmarks/history/2026-07-19-group-zero-pipeline.md`.

Sessions under `20260719T184554Z-line-region-baseline` and
`20260719T185603Z-line-swar-candidate` close the scalar-newline hypothesis for
line capture. Offset-aware SWAR discovery changes Ruff `tweaked` public time by
`+2.13%/+4.68%` and Unicode by `-0.18%/-1.06%` on Intel/Graviton. The stable
Ruff Graviton regression rejects the candidate. Unicode setup is less than 0.7
ms after the change and remains immaterial beside its BitState capture stage.
No production change is retained. See
`docs/benchmarks/history/2026-07-19-line-capture-integration.md`.

Sessions `20260719T193619Z-35711`, `20260719T195845Z-53093`, and
`20260719T200502Z-56646` close the Unicode BitState question. Java and native
compile the workload to the same structurally one-pass 78-instruction program,
but native's packed OnePass action supports only five groups and routes the
16-group workload through BitState. The retained 64-bit capture-mask sidecar
routes it through OnePass and reduces Java public time by 63.9%/53.6% on
Intel/Graviton. The complete operation now takes `0.579x/0.496x` native time,
with stable public brackets and identical results. See
`docs/benchmarks/history/2026-07-19-extended-onepass-captures.md`.

Sessions `20260720T000944Z-98916`, `20260720T001609Z-3715`, and
`20260720T001609Z-3716` close the Ruff logical-region question. Carrying region
bounds through DFA setup and allocating a Slice view only when execution reaches
a capture engine reduces Ruff `tweaked` Java time by 3.0%/6.2% and setup by
18.3%/16.1% on Intel/Graviton. Ruff `real` changes by +1.3%/-2.8%, and Unicode
by +1.7%/+1.7%, within the protected gate. The DFA transition loops are unchanged. Ruff
`tweaked` remains at `1.487x/1.135x` same-session native time, so formal
qualification must revisit it. See
`docs/benchmarks/history/2026-07-20-logical-region-matcher.md`.

Session `20260720T012825Z-60724` closes the bounded Date candidate-start
campaign. The cursor reduces Date from `1.630x/1.385x` to `1.218x/0.888x`
host-tuned native time on Intel/Graviton. Date therefore improves materially
and is faster than native on Graviton, but retains a 21.8% Intel deficit. The
English controls remain native wins at `0.959x/0.876x` and `0.725x/0.639x`;
they are ineligible for the cursor because they use empty-width word boundaries.
The three Cloudflare rows remain extreme relative-ratio outliers with absolute
deficits from 0.7 to 108 microseconds. See
`docs/benchmarks/history/2026-07-20-date-candidate-start-cursor.md`.

Actual Trino module integration remains a separate project and is not a release
gate. The public API and engineering source are frozen, and the reviewer-facing
history is reconstructed. Resume with formal cross-architecture qualification
on the final source, explicitly revisiting Intel Date and Ruff `tweaked`, then
perform the final release documentation and criteria audit.

Consider Intel literal scanning only as a separate specialized-prefix campaign
if its absolute deficits justify the work. Keep compilation and direct Graviton
engine tuning lower priority unless production routing evidence changes their
importance.

Do not resume a queue found in a historical campaign notebook. Any unfinished
work that still matters is represented below.

## Implemented Surface

The following major components exist and have substantial tests:

- Slice-oriented public pattern and text handling over internal byte ranges
- regexp AST, parser, simplifier, Unicode groups, and character classes
- program representation and compiler
- NFA, DFA, OnePass, and BitState execution engines
- `Re2`, `Re2Matcher`, `MatchResult`, capture buffers, rewrite operations, and options
- `Re2Set`, `FilteredRe2`, and prefilter support
- Trino-compatible count, position, extraction, split, and replacement operations
- upstream-derived golden tests, generated exhaustive tests, and randomized tests
- benchmark harnesses and historical Java/C++ comparison data
- Java naming, exception, API, and test-source cleanup

Presence is not the same as verified parity. Completion claims require the
release criteria at the end of this document.

## P0: Correctness and Safety

These are release blockers and should be fixed before further cleanup or
performance work.

- [x] **P0.1 Correct full-match semantics.** Distinct first, longest, full, and
  many-match execution modes now cover alternation and reluctant repetitions.
- [x] **P0.2 Make shared compiled patterns thread-safe.** Searches register as
  readers, cold construction and reset wait for readers to quiesce, and cache
  generations prevent queued builders from repeating published work. Warm
  transition reads remain plain loads.
- [x] **P0.3 Preserve UTF-8 validity in match-every-byte optimizations.** The
  shortcut applies only to byte-universal expressions; UTF-8 dot-all still
  validates its input.
- [x] **P0.4 Restore linear-time `Re2Set` matching.** Sets compile and execute as
  one upstream-style many-match DFA traversal.
- [x] **P0.5 Make parsing stack-safe.** Group parsing and nested repetition
  accounting use explicit stacks. Parse-time simplification uses recursion for
  shallow ASTs and switches to explicit post-order traversal at a bounded depth,
  with 10,000-level and constrained-stack regression coverage.
- [x] **P0.6 Preserve OnePass captures at priority matches.** Java's loop
  `break` incorrectly continued into end-of-input capture processing, unlike
  upstream's jump to the common completion path. An exact pinned-random case,
  a focused OnePass test, and a public range-match test protect the corrected
  branch target.

Every P0 fix requires a failing semantic test before implementation and a
comparison against pinned upstream behavior.

## P1: Parity and Test Assurance

- [x] **P1.1 Exercise the public engine cascade in differential tests.** The
  generated matrix compares the public API, DFA, NFA, BitState, OnePass, and
  testing Backtrack across parse modes, anchors, match kinds, and context windows.
- [x] **P1.2 Audit every upstream test mapping.** `docs/porting/TEST_MAP.md`
  records exact coverage categories, local parameters, API substitutions, and
  remaining case-table work without unsupported percentages.
- [x] **P1.3 Restore realistic concurrency tests.** Port the shared-DFA contract
  instead of compiling one `Prog` per worker, and document any deliberately
  reduced upstream stress parameters.
- [x] **P1.4 Correct compile memory accounting.** All public compile paths use
  the upstream default and forward/reverse split; compiler and DFA storage are
  deducted before cache allocation.
- [x] **P1.5 Remove the 1,000-capture limit.** The parser follows pinned upstream
  and has direct coverage for 10,000 nested captures.
- [x] **P1.6 Make the pinned upstream source reproducible.** A reproducible
  native oracle fetches and verifies
  `972a15cedd008d846f1a39b2e88ce48d7f166cbd` before generating fixtures.
- [x] **P1.7 Validate unsupported-byte and boundary behavior.** The pinned native
  oracle and public/direct-engine matrices cover malformed UTF-8, empty and
  non-zero windows, unmatched captures, anchors, and end-of-text transitions.
- [x] **P1.8 Complete parser and AST case-table ports.** All 175 primary parser
  rows, non-default flag groups, invalid expressions, exact error arguments, and
  unique character-class builder rows are covered. Pairwise AST equality and
  equal-hash checks use upstream's operation-specific flag semantics. The full
  parser table also verifies formatter/reparse fixed points, and exact AST tests
  cover 90,000-child concatenation and duplicate named captures. This work
  corrected `RegexpWalker` so its visit limit counts nodes, as upstream does,
  rather than internal traversal iterations.
- [x] **P1.9 Complete compiler and program case-table ports.** All simple
  compile dumps, memory failures, byte maps, pinned bug cases, handwritten
  possible-match rows, and failure rows are covered. Possible-match exhaustive
  generation uses upstream's 3-atom, 3-operator, 5-byte parameters, asserts all
  27,012 regexp callbacks, and checks 364 strings for every bounded range.
- [x] **P1.10 Complete search and DFA case-table ports.** All 236 search rows run
  through the complete Java engine matrix. DFA build, search, concurrency,
  memory, callback, and reverse tests retain the pinned parameters and shared
  cache behavior. Seven callback rows assert exact graph dumps; three rows use
  Java's internal full-match sentinel and instead assert the upstream state count
  and longest-match boundary. No stress-count reductions remain.
- [x] **P1.11 Complete public and aggregate API case-table ports.** Every
  applicable `re2_test.cc`, `set_test.cc`, and `filtered_re2_test.cc` case is
  represented. Numeric extraction includes exact signed and unsigned overflow,
  leading-zero, hexadecimal, octal, and decimal boundaries. C++ consume APIs map
  to repeated anchored matching and `Re2Matcher.find()`; out-parameter, move,
  malloc-counter, and lazy-wrapper mechanics are not applicable to the Java API.
- [x] **P1.12 Reconcile exhaustive and randomized parameters.** The four active
  exhaustive suites use the pinned upstream operator sets and parameters. Five
  reproducible native random shards preserve seed 404, 100 constructions, four
  wrappers, and 100 texts per construction for 200,000 cases. Each case records
  native match and capture results and also runs through the complete Java
  engine/public matrix.
- [x] **P1.13 Expand native differential fixtures.** A reproducible pinned-native
  corpus covers 148 deterministic cases: 40 first/longest comparisons including
  18 differing boundaries, 48 anchor and non-zero-window cases, and 60 malformed,
  truncated, or split-byte cases across UTF-8 and Latin-1. The public
  `matchInto`, `matchResult`, partial-match, and full-match paths consume the same
  generated expectations.

The active RE2 test selector is:

```bash
./mvnw "-Dtest=**/re2/**/Test*" test
```

## P2: Performance and Resource Use

Do not benchmark speculative fixes until P0 correctness is restored. Each
optimization must preserve all supported modes and include a direct code-path
test in addition to benchmarks.

- [x] **P2.1 Remove boolean full-match capture allocation.** Non-capturing
  `fullMatch` now runs without creating an `int[2]` capture buffer.
- [x] **P2.2 Remove repeated synchronized OnePass analysis.** Forward compilation
  now determines eligibility once, applies upstream's memory gate, and accounts
  for retained OnePass storage before assigning the DFA budget.
- [x] **P2.3 Reconcile required-prefix architecture with upstream.** Anchored
  prefixes are checked separately, the forward and reverse programs compile the
  suffix, and native-golden tests cover capture and group-zero offsets.
- [ ] **P2.4 Complete formal cross-architecture qualification.** After correctness
  and public API work, rerun the complete matrix on dedicated AWS Intel and Arm
  Graviton machines. Compare pinned native RE2, the core and Slice APIs, Joni,
  and historical Trino RE2J at both engine and operation-adapter levels. Add an
  official Rebar-compatible runner and report the curated search and compile
  intersections against the engines in the pinned Rebar revision; the existing
  Rebar-derived route benchmarks are not an overall engine ranking. Do not carry
  February 2026 measurements forward. See
  `docs/benchmarks/REBAR_NATIVE_COMPARISON_PLAN.md` for the initial native
  comparison and `docs/benchmarks/QUALIFICATION_PLAN.md` for the release
  protocol. Candidate `c212684` completed three-session traditional and bounded
  Rebar-native campaigns plus the Trino-shaped Joni campaign. The long full
  matrix did not produce three complete sessions per architecture after Spot
  interruptions and the original controller timeout, and the broader official
  Rebar intersection remains open. The candidate report records material native
  outliers that require disposition; the later final Joni acceptance closes its
  superseded Joni finding. See
  `docs/benchmarks/history/2026-07-20-candidate-qualification.md`.
- [x] **P2.5 Reassess recorded gaps.** BigFixed public matching, direct-DFA
  benchmark validity, fixed-width boundary recovery, and dense single-byte Trino
  operations are resolved. Flat primitive, byte-offset, and direct 256-entry
  expansion controls were slower or neutral. Bounded depth-two composition
  resolves the recursive object-array decode/checkcast gap for small graphs; the
  integrated Hard/Parens path is faster than native on both target
  architectures. An Intel crossover run regressed once compressed reference
  payload exceeded 64-108 KiB, so the implementation uses a conservative 64 KiB
  estimate with eight-byte references, charges it to the DFA budget, discards it
  before normal state allocation fails, and preserves compact fallback. A full
  Rebar route census rejected broad partial expansion: only the two-row,
  at-most-16-KiB Hard/ReallyHard shape is retained when a complete table does
  not fit. Do not infer a globally expanded representation from the small-DFA
  result. A local correctness-protected start-byte accelerator reduces
  capture-sparse operations by 49-53%. Exact-current controls rejected adding a
  separate direct single-byte containment route; see P2.10.
  Representative Rebar validation retains complete pairing for productive
  searches, rejects it after 16 repeated sub-16-byte results, and restores
  compact performance on both Intel and Graviton. Sparse start-byte acceleration
  is validated cross-architecture. A retained one-pass matcher for greedy
  nullable single-byte repetition makes 32 KiB `x*` count 7.15x faster locally
  and passes its Intel and Graviton source-disabled count, boundary, and
  operation controls. The focused same-host comparison resolves the sole
  nominal Intel/Joni loss as statistically equivalent and confirms the
  Graviton win. A subsequent compact-layout campaign rejects bounded FFM,
  relative-offset buffers, and the tested primitive heap layouts as universal
  replacements. It retains the existing object rows plus a guarded
  stable-self-loop scanner: the integrated route takes 0.432x compact-control
  and 0.599x native time on Intel, and 0.366x control and 0.515x native time on
  Graviton. Physically bypassing sampling for paired execution keeps paired and
  state-changing compact routes within the 2% regression gate. The later
  supported FFM everything-segment result is retained under P2.11 after the
  integrated production route reached native parity on both targets.
- [x] **P2.6 Revalidate allocations and retained memory.** Boolean search and
  caller-owned capture buffers have exact zero-allocation guards and
  cross-architecture profiles. JOL graph measurements cover compiled patterns,
  lazy reverse compilation, many-pattern ownership, warm/reset DFA caches, and
  100 reset/rebuild cycles. The representative DFA graph returns from 16,544
  warm bytes to a stable 9,256-byte reset plateau. A 10,000-virtual-thread wave
  exposed a 65,640-byte empty `ConcurrentHashMap` high-water table after weak
  references were drained. The cold registry now compacts stale-heavy waves and
  returns to its 64-byte empty graph. Deterministic tests protect state-root
  clearing, capacity stability, one registration per persistent worker,
  primitive-only reader slots, registry compaction, and concurrent reset/search
  behavior. Target-host concurrency throughput remains part of formal
  qualification rather than this retained-size closure.
- [x] **P2.7 Align DFA cache accounting with upstream.** Use the maximum state
  size only for the initialization gate, then charge actual instruction storage
  per state. The complete upstream BigFixed range now executes through the DFA
  instead of returning the failure sentinel.
- [x] **P2.8 Validate imported bounded optimizations.** Minimum encoded byte
  width is locally accepted with short-window and compile controls. The retained
  fixed-distance scanner qualifies 26 of 285 analyzed Rebar rows and passes its
  source-disabled Intel and Graviton gate: sparse and no-match searches take
  0.14-0.22x control, while dense false positives are neutral-to-faster. The
  retry-after-reset budget branch has a deterministic direct test. The
  dominant start self-loop scanner was rejected despite 6.2-6.4x sparse and
  no-match gains because the source-disabled Intel control exposed a 7.3-8.0%
  dense 32 KiB regression. The multi-stage Rebar repeat remains on complete
  pairing. Required-suffix boolean search was also removed: absent inputs took
  0.03-0.04x control, but late false candidates regressed by 3-8% and a
  long-spanning match by 4.4-5.7x on Intel and Graviton. The later adaptive
  self-loop scanner is a different design: it samples actual transitions only
  on long unpaired end-constrained searches and enters a physically isolated
  loop after at least 58 of 64 exact self-loops. Source-disabled Intel and
  Graviton controls accept it without a protected regression.
- [x] **P2.9 Qualify nullable single-byte repetition matching.** The analyzer and
  one-pass matcher have direct UTF-8, Latin1, malformed-input, Slice-offset,
  capture, folding, and rejection tests. Controlled local shapes improve by
  3.5-33x and close the measured Joni `x*` count deficit. The Intel and Graviton
  controls pass with 32 KiB count ratios of 0.02-0.30x and boundary ratios of
  0.01-0.47x across all shapes. The refreshed operation matrix and exact-current
  source-disabled operation controls close the measured Joni deficit without a
  protected regression. Capture-demand routing retains boundary specialization
  for group-zero-only operations, with malformed UTF-8, Latin1, Slice-offset,
  and non-default-position differential coverage.
- [x] **P2.10 Resolve single-byte containment dispatch overhead.** Exact-current
  session `20260716T114214Z-51237` rejects the direct containment subtype. The
  main operation matrix remained within 0.6% of control, but Graviton generic
  routes reached 1.033x for single-byte no-match and 1.030x for an unsupported
  boundary pattern, failing the 2% no-regression gate. The 0.139-0.143x dense
  delimiter result is too narrow to justify that shared dispatch cost. Compile
  control session `20260716T114420Z-52459` passed on both architectures. The
  specialized subtype and its eligibility metadata are removed; `contains`
  uses the established generic partial-match route, while the one-byte matcher
  remains available to count and repeated-operation paths that already qualify.
- [x] **P2.11 Qualify a forward absolute-pointer DFA sidecar.** The isolated
  cutoff campaign measured 0.716-0.776x object-row time on Intel and
  0.716-0.724x on Graviton through useful footprints, but exposed a known
  C2/AArch64 dependent-address limitation against a minimal native loop. The
  original production design used a 48 KiB row-rounded cap, gave pairing the
  first selection opportunity, permanently charged its automatic-arena
  allocation, and silently retained object rows without native access.
  Integrated session `20260716T233209Z-7064` measured 0.709x object and 0.996x
  native time on Intel, and 0.728x object and 1.006x native time on Graviton.
  Paired controls remained within 2%. This was the original bounded integration;
  P2.17 supersedes its fixed-cap policy without changing the hot pointer loop. See
  `docs/benchmarks/DFA_ABSOLUTE_POINTER_PLAN.md` and
  `docs/benchmarks/history/2026-07-16-dfa-absolute-pointer-integrated.md`.
- [x] **P2.12 Qualify capture execution engines.** Three bounded NFA rounds,
  two OnePass rounds, three BitState rounds, and one public integration round
  retained only same-host Intel/Graviton wins that passed a 2% protected gate.
  NFA now uses compiled opcode counts, a primitive sparse queue, and compact
  instructions; OnePass finalizes captures directly when safe; BitState uses a
  bounded initial object stack and compact instructions. Final Rebar capture
  counting improves to `1.922x/1.644x` native time and capture grep to
  `1.182x/1.051x`. Corrected traditional public capture is
  `1.035x/1.144x`. See
  `docs/benchmarks/history/2026-07-18-capture-engine-campaign.md`.
- [x] **P2.13 Decompose and optimize capture/count composition.** A stage-level
  runner accounts for at least 90% of representative public operations without
  instrumenting production hot loops. Reusable matchers now own NFA and BitState
  workspaces, and eligible long unanchored subgroup searches use a bounded
  direct BitState route. Veryl improves by 40%, Unicode capture grep by 15%/39%,
  and `SplitBig2` by 83%/84% on Intel/Graviton. Direct unanchored NFA was
  49%-50% slower and rejected. Group-zero date spans do not run a capture engine
  and require a separate tagged-state algorithm to avoid reverse traversal.
  Exact affected-model confirmation places count captures at `1.033x/0.960x`
  and capture grep at `1.180x/0.968x` on Intel/Graviton. See
  `docs/benchmarks/history/2026-07-18-capture-pipeline-campaign.md`.
- [x] **P2.14 Diagnose bounded-context capture-free counting.** A focused
  8-128 MiB cutoff matrix shows that native becomes reset-free at 48 MiB while
  Java becomes reset-free at 96 MiB. Removing Java's 31 default-budget resets
  improves the operation by 7.2x/6.0x on Intel/Graviton, but the reset-free
  object-row loop remains `1.393x/1.531x` native time. Controls remain within
  1%. See
  `docs/benchmarks/history/2026-07-18-bounded-context-count-diagnostic.md`.
- [x] **P2.15 Qualify DFA cache-capacity policy.** Runtime-aware `SizeOf`
  accounting now charges exact retained array growth, state objects, map entries,
  and retained reset capacity. Temporary JOL measurements matched compressed
  live-state growth exactly and found only a 1.4% conservative overestimate
  without compressed references; JOL was then removed. The default planning
  budget is 96 MiB and remains demand-driven. Confirmation eliminates all target
  resets, improves bounded-context count by 6.7x/5.8x on Intel/Graviton relative
  to 8 MiB, and keeps both controls within 0.2%. The reset-free loop remains
  `1.386x/1.590x` native time and is a separate transition-throughput problem.
  See `docs/benchmarks/history/2026-07-18-dfa-cache-capacity-policy.md`.
- [x] **P2.16 Qualify retained memory against Joni.** A lifecycle census covers
  compiled roots, all 80 warmed Trino-shaped operations, active matchers, and
  the large bounded-context graph. Ordinary warm patterns retain 0.474x Joni
  memory at the median, and no ordinary row exceeds both 2x and 1 MiB of
  excess. The bounded graph retains 21.8 MiB, resets zero times, and runs at
  `0.140x/0.114x` Joni time on Intel/Graviton. Its matcher owns 208 bytes; the
  remaining state is reusable on the compiled pattern. Its original paired
  operation matrix is superseded by the candidate qualification, which exposes
  a material long sparse repeated-boundary loss subsequently closed by the
  final Joni acceptance. See
  `docs/benchmarks/history/2026-07-18-joni-memory-qualification.md` and
  `docs/benchmarks/history/2026-07-20-joni-final-acceptance.md`.
- [x] **P2.17 Extend the absolute-pointer sidecar to large DFAs.** The exact
  75,850-state bounded-context graph selects a geometrically grown sidecar.
  Original session `20260719T003857Z-7477` measured `0.903x/0.964x` native time
  on Intel/Graviton. Intel follow-up `20260719T042441Z-48534` removes the size
  cutoff while preserving 53 matches, 75,850 states, zero resets, and `0.904x`
  native time. Final two-host session `20260719T052408Z-11066` confirms the
  geometric policy at `0.908x/0.946x` native and `0.648x/0.619x` bracketed
  object-row time on Intel/Graviton. The current allocation is charged
  permanently, grows only under exclusive cache mutation, and remains FFM
  rather than switching to object traversal. It is optional when native access
  or a finite budget is unavailable. See
  `docs/benchmarks/history/2026-07-18-dfa-large-pointer-sidecar.md`.
- [x] **P2.18 Refresh retained memory against Joni.** Two-host session
  `20260719T051215Z-5387` measures the geometric sidecar policy. Ordinary warm
  patterns retain `0.473x` Joni memory at the median and `1.003x` at the
  maximum. Tiny `x*` containment uses a 144-byte sidecar; the explicit
  75,850-state graph uses 7,959,120 bytes, resets zero times, and runs at
  `0.070x/0.065x` Joni time on Intel/Graviton.
Rejected campaign experiments are evidence against repeating the same patch,
not evidence that the underlying gap is resolved. See
`docs/benchmarks/history/` before retrying an approach.

## P3: API, Documentation, and Cleanup

- [x] **P3.1 Correct `RE2_DECISIONS.md`.** Remove superseded exception, logging,
  memory, allocation, and thread-safety claims; record only current intentional
  deviations from upstream.
- [x] **P3.2 Rebuild `docs/porting/TEST_MAP.md`.** It records current class names,
  corpus scope, engine matrices, substitutions, and remaining gaps.
- [x] **P3.3 Minimize the public API.** The RE2 implementation and its tests are
  flattened into `io.airlift.slice.re2`, which makes parser, AST, compiler,
  program, Unicode, utility, and execution-engine types genuinely
  package-private on the ordinary classpath without an indirection layer. The
  public surface is limited to patterns, matchers, results, set/filter APIs,
  the operation adapter, and typed parse/compile failures.
- [x] **P3.4 Finish cold-path structural cleanup.** Complete the naming,
  obsolete qualification, dead-code, and package-private encapsulation sweep in
  parser, compiler, AST, Unicode-construction, and support code. Assess large
  methods by responsibility and readability rather than splitting them merely
  by size. Do not include DFA, NFA, OnePass, BitState, scanning, or
  engine-selection loops in this cleanup; P3.5 governs any necessary change to
  those paths. The final audit retained only precise upstream algorithm anchors
  and protected-loop comments. At that capture-engine checkpoint, the complete
  889-test RE2 selector and 2,679-test Maven install passed; the current counts
  are recorded in the status section above.
- [x] **P3.5 Protect hot paths during cleanup.** Treat the established DFA, NFA,
  OnePass, BitState, scanning, and engine-selection loops as frozen performance
  code. Keep explicit warnings at their boundaries and do not apply
  readability-only refactors. Any necessary change requires direct code-path
  tests, allocation evidence, and targeted Intel and Graviton benchmarks. The
  protected boundaries now carry explicit performance-sensitive warnings.
- [x] **P3.6 Remove or relocate development artifacts.** Keep durable benchmark
  conclusions and reproducible inputs; exclude large raw output, assembly
  dumps, and campaign scratch data from the initial PR unless they are needed
  to reproduce a result.
- [x] **P3.7 Reconstruct reviewer-facing Git history.** Drop experiment/revert
  pairs, fold cleanup into the code it clarifies, and retain only coherent
  independently buildable layers.
- [x] **P3.8 Run a final documentation consistency audit.** Reconcile current
  status, test counts, commands, file names, Unicode authority, rejected
  experiments, and performance claims after the Rebar and qualification runs.
  Keep actual Trino integration outside this project's release criteria and
  ensure historical benchmark documents remain explicitly labeled. The
  2026-07-18 capture campaign updates the active ledger and task resume point,
  explicitly supersedes invalid old capture equivalence, and separates general
  results from extreme ratio outliers. Formal qualification will require a
  final release-specific refresh.
- [x] **P3.9 Research Trino integration requirements.** Audit Trino's SQL regexp
  contract, historical RE2J fork, relevant fixes and prototypes, syntax risks,
  API needs, and future correctness and benchmark qualification. The findings
  are recorded in `docs/integrations/TRINO.md`.
- [x] **P3.10 Implement a Slice-oriented public facade.** Accept and return
  `Slice` throughout the public API and internals. Represent execution windows
  with explicit offsets into the original Slice so empty matches retain their
  surrounding context without allocating range wrappers. Keep a caller-owned
  capture-buffer path.
- [x] **P3.11 Add allocation-conscious repeated matching.** Design a reusable
  matcher or cursor that preserves full-input context, advances empty matches
  by encoding units correctly, reuses capture storage, and supports Trino's
  count, position, extraction, split, and replacement adapters.
- [x] **P3.12 Define the Trino compatibility boundary.** Build data-driven Trino
  function and syntax corpora, classify Java-only constructs, and identify
  semantic collisions that must be rejected or translated. Complete this before
  finalizing public API names.
- [x] **P3.13 Organize current and historical documentation.** Make this file
  the sole progress tracker, document the resume point, move dated campaigns
  and measurements under subsystem-specific `history/` directories, and add a
  lifecycle-aware documentation index.
- [x] **P3.14 Implement the Trino adapter.** Build count, position, extraction,
  split, and replacement semantics on the public facade and repeated matcher.
  Keep SQL positions, Trino replacement syntax, block construction, and error
  translation out of the core engine.
- [x] **P3.16 Implement JVM-sourced Unicode data.** The executing JVM now supplies
  categories, scripts, blocks, binary and Java properties, and simple case
  equivalence according to `docs/integrations/JVM_UNICODE.md`.
  Properties become immutable cached ranges before compilation reaches an
  execution engine, and the generated upstream range files are removed.
  Exhaustive authority tests cover every category, script, supported binary and
  Java property, and complete case-fold cycle; focused `Pattern` comparisons
  cover special folds. The complete native differential and RE2 suites protect
  unaffected syntax. Related regex-language extensions such as Unicode shorthand
  modes and named-character syntax remain separate decisions.

## Historical Campaign Summary

- The bounded 2026-07-18 capture-engine campaign retained structural NFA,
  OnePass, and BitState improvements and completed corrected traditional and
  Rebar native comparisons on Intel and Graviton. See
  `docs/benchmarks/history/2026-07-18-capture-engine-campaign.md`.
- The follow-up capture-pipeline campaign established matcher-owned workspace
  lifecycle as the dominant remaining Veryl and Unicode cost, retained direct
  bounded BitState for long eligible searches, and isolated group-zero spans as
  a different DFA algorithm problem. Its exact affected-model confirmation
  places capture counting at parity on both architectures and leaves the Ruff
  rows as the principal public capture losses. See
  `docs/benchmarks/history/2026-07-18-capture-pipeline-campaign.md`.

- The main engine campaign is closed. Its retained code was already incorporated;
  v3 retained no new candidates and reverted all tested changes. See
  `docs/benchmarks/history/2026-02-13-engine-campaign-v3.md`.
- Parser/compiler campaign v1 retained the full-range subtext allocation fast
  path and rejected the remaining candidates. See
  `docs/benchmarks/history/2026-02-13-parser-compiler-campaign-v1.md`.
- Parser/compiler campaign v2 retained six compiler/parser changes and the
  Simplifier coalescer change after its historical benchmark gates. See
  `docs/benchmarks/history/2026-02-14-parser-compiler-campaign-v2.md`.
- The cleanup campaign completed broad naming and test-source work. Its stated
  structural phases remain unfinished and are represented by P3.4 and P3.5.
  See `docs/cleanup/history/2026-02-14-code-cleanup-campaign.md`.
- Historical benchmark results are useful baselines but are not a current
  release qualification.

## Review Restack

The original development log is consolidated into five independently buildable
reviewer-facing commits:

1. Add the final-form Java RE2 engine and core tests.
2. Add pinned-upstream golden, exhaustive, and randomized verification.
3. Add the Trino regexp compatibility layer and behavior corpora.
4. Add reproducible benchmark and qualification tooling.
5. Add current design, status, process, and historical evidence documentation.

The engine is intentionally presented in final form rather than replaying
obsolete intermediate representations and optimizations. The compiler, program,
and execution engines are mutually dependent and do not form useful smaller
independently buildable commits.

## Release Criteria

The port is ready for initial review only when all of the following are true:

- all P0 defects are fixed with upstream-grounded regression tests
- the public API and every selected engine agree over exhaustive and randomized
  corpora, including full-match and invalid UTF-8 cases
- one compiled pattern is safe for concurrent use
- unanchored matching remains linear for both `Re2` and `Re2Set`
- memory limits bound programs and DFA caches in all public compile modes
- the upstream test map records exact coverage and remaining substitutions
- JVM-sourced Unicode authority and case equivalence pass their exhaustive
  checks on the final supported JDK
- the full Maven build passes
- the complete native RE2, core, Slice API, Joni, and historical Trino RE2J
  benchmark matrix has been rerun on dedicated AWS Intel and Arm Graviton
  machines
- an official Rebar-compatible runner reports the curated common search and
  compile intersections, with unsupported syntax and semantic differences
  classified rather than silently omitted
- every supported benchmark is at least as fast as native RE2, unless the user
  explicitly approves and documents an exception
- public API, decisions, task status, and documentation agree with the code
- the Slice facade and Trino compatibility boundary satisfy the correctness
  corpora recorded in `docs/integrations/TRINO.md`
- actual Trino module wiring, SQL registration, and deployment remain a
  separate integration project and are not release criteria for Slice
- the reviewer-facing commit stack is coherent and each commit passes its
  proportional verification gate
