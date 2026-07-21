# RE2 Performance Engineering Campaign

**Status:** Complete. Measurements from this campaign are engineering evidence,
not release qualification.

This campaign built strong internal confidence in the current RE2 implementation
and recovered performance lost during correctness, API, and cleanup work. Formal
Intel and Arm release qualification remains a separate campaign described in
[`QUALIFICATION_PLAN.md`](QUALIFICATION_PLAN.md).

## Objective

Demonstrate that matching native RE2 performance is achievable in the current
implementation, identify every material regression or unexplained gap, and
improve the implementation without trading one important workload for another.

The campaign optimizes the public `Re2`, `Re2Matcher`, and Trino operation paths.
Internal engine benchmarks diagnose those paths; an isolated internal win is not
sufficient evidence to retain a change.

The bounded forward compact-DFA absolute-pointer sidecar subgoal is complete and
accepted as the one-byte DFA design. Graviton retains the documented extra
address-generation instruction until the corresponding JVM optimization is
available. See
[`DFA_ABSOLUTE_POINTER_PLAN.md`](DFA_ABSOLUTE_POINTER_PLAN.md). This result does
not reopen general transition-layout exploration.

## Established Performance Model

The earlier optimization campaign established how to analyze this engine. New
work must start from these findings rather than rediscovering them:

- The steady-state DFA loop is dominated by the serial dependent load chain, not
  total instruction count.
- The target DFA chain is three loads: input byte, byte-map class, and next state.
  The Java and C++ loops previously reached the same load count and critical-path
  shape.
- Bounds checks, predicted branches, loop bookkeeping, and ordinary arithmetic
  are usually hidden under load latency. They matter only when generated code or
  measurements show that they escape that shadow or exhaust execution width.
- Reducing generated instructions without shortening the critical path is not an
  optimization. Earlier reductions from 55 to 35 instructions did not improve
  throughput; removing one dependent state load did.
- JIT assembly is most useful for finding wrong loop shapes, failed inlining,
  unexpected calls, extra dependent loads, barriers, lost unrolling, register
  spills, or casts that survive optimization. Annotating every instruction after
  the load graph is understood is not useful by itself.
- Prefix-accelerated workloads must include the transition between modes. `Easy1`
  repeatedly alternates between single-byte prefix scanning and short DFA bursts;
  a path optimized for uninterrupted DFA execution regressed this handoff shape.
- An additional instruction can matter when it no longer fits under the dependent
  loads. Earlier prefix-path changes demonstrated persistent regressions in the
  approximately 5% range even when the main load count was unchanged.
- Results from Apple Arm are directional. Intel and Graviton can have different
  load latency, execution width, branch, memory-ordering, and JIT code shapes.

The detailed evidence remains in
[`history/2026-02-09-dfa-assembly-analysis.md`](history/2026-02-09-dfa-assembly-analysis.md)
and
[`history/2026-02-14-optimization-log.md`](history/2026-02-14-optimization-log.md).

## Protected Workloads

Every DFA change must measure these workloads together:

| Workload | Behavior protected |
|---|---|
| `Easy0` | Multi-byte prefix acceleration with infrequent DFA entry |
| `Easy1` | Single-byte prefix acceleration with frequent scan/DFA handoff |
| `Easy2` | Case-folded prefix handling |
| `Hard` | Long uninterrupted DFA traversal |
| `Parens` | Long DFA traversal with capture-heavy program shape |
| `BigFixed` | Anchored long-prefix behavior and public engine selection |

The broader guard set must also cover NFA, OnePass, BitState, small captured
matches, `SplitBig`, phone search, full match, malformed UTF-8, cache reset, and
the practical HTTP patterns used by the earlier campaign.

## Campaign Layers

### Direct Engine

Use the existing DFA, NFA, OnePass, BitState, parser, compiler, prefix scan, and
utility benchmarks to locate costs. Compare direct Java engines only with the
corresponding direct pinned C++ engines doing equivalent work. Native timings
must invoke C++ RE2 directly; JNI or another Java/native bridge is not part of
the comparison.

### Public Java API

Measure boolean partial and full matching, caller-owned capture buffers,
`MatchResult`, matcher creation and reset, and repeated `find`. Include small
inputs where dispatch dominates and large inputs where the transition loop
dominates. Allocation-free APIs must be measured with reused caller storage;
allocation-inclusive convenience APIs must be reported separately.

### Trino Operations

Measure contains, count, position, extract, extract-all, split, template
replacement, and lambda-replacement iteration through the Slice operation
adapter. Compare the new implementation with Joni and historical Trino RE2J
through equivalent direct scalar operations. Compiled Trino page projections
and distributed query execution belong to the separate Trino adoption project.

## Iteration Protocol

1. Establish a short, well-warmed baseline for the complete protected matrix.
2. Verify that compared implementations use the same pattern, bytes, anchor,
   captures, expected result, and cache state.
3. Classify each material gap as scaling, engine selection, dependent loads,
   mode handoff, allocation, dispatch, synchronization, code generation, or
   benchmark error.
4. Profile the complete public operation before isolating a component.
5. Inspect generated code only after profiles and scaling identify the hot loop.
6. Add a correctness test that directly exercises the path before changing it.
7. Make one focused change and rerun the target plus the complete protected set.
8. Retain the change only when the public or Trino path improves and no protected
   regression persists after rerun.
9. Run the relevant correctness selector after each retained optimization and a
   clean build before closing a campaign phase.

A difference greater than 5x in either direction is presumed to be a benchmark
or algorithmic problem until explained. A persistent protected regression above
2% is rejected unless an explicit tradeoff is reviewed and recorded.

## DFA Generated-Code Audit

For each architecture and JDK used for serious measurements, record:

- the serial dependent load graph and measured cycles per byte
- whether byte-map and transition arrays remain in registers
- whether the loop is counted, unrolled, and free of hot calls and safepoints
- loads, acquire operations, barriers, casts, spills, and uncommon traps on the
  loop-carried dependency chain
- the instruction or dependency sequence between the byte-map result and the
  transition load
- the generated shape of uninterrupted scanning and prefix/DFA handoff paths
- whether cold cache construction changes the optimized warm-loop compilation

An instruction is actionable only when this audit shows why it affects the
critical path or available execution width.

### Transition Expansion Guardrail

Do not generalize a win from a small DFA into a fully expanded transition
representation. Earlier work found that full transition expansion became much
slower as its cache footprint grew and required excessive memory for more
complex DFAs. The current direct 256-entry-row control is also neutral or slower
than compact byte-class rows on Intel and Graviton.

A bounded multi-byte transition specialization is a separate hypothesis, not a
reconsideration of global expansion. Before retaining one, measure:

- compact, specialized, and native loops on the same generated DFA graph
- several state counts and byte-class counts, including cache-hostile graphs
- absolute retained bytes as well as the ratio to compact storage
- construction, reset, and fallback behavior under the configured DFA budget
- the complete protected workload set on both Intel and Graviton

Eligibility must use a hard absolute memory cap and preserve the compact
representation as the fallback. A result from a small graph that fits in L1 is
evidence only for that graph size until the cache-footprint boundary is measured.

## Concurrency And Cache Decision

Thread safety is a required behavior today. The read-lock implementation that
started this campaign established the correctness baseline but caused a material
performance regression. The retained design uses reader registration with
exclusive cold construction and reset. Searches use plain transition loads; its
evidence and remaining resource gates are recorded in
[`ENGINEERING_RESULTS.md`](ENGINEERING_RESULTS.md).

Current facts:

- `Re2` is documented as immutable and safe to share; `Re2Matcher` is mutable and
  thread-confined.
- Trino may share a compiled constant pattern across driver threads.
- A compiled program owns lazily populated mutable DFA caches and can reset them
  when the configured memory budget is exhausted.
- Pinned C++ RE2 publishes cached transitions with acquire/release atomics and
  synchronizes cache lifetime.
- The retained design registers a reader once per DFA search, uses plain loads in
  the transition loop, and waits for registered readers before cold construction
  or cache reset replaces storage.
- Removing reader registration is not a valid isolated optimization: an unsafe
  local ablation improved a repeated-search operation by about 10%, but shared
  searches without a cache-lifetime protocol previously produced incorrect
  results.

The retained design must be measured in three places:

1. per-search reader-registration overhead on tiny and repeatedly invoked inputs
2. generated-code shape of the plain-load steady-state DFA loop
3. contention, retained memory, and reset behavior with one pattern shared by
   many threads

Compare complete ownership designs rather than removing individual memory-ordering
operations in isolation:

| Design | Expected strength | Principal risk |
|---|---|---|
| Shared mutable DFA cache | One warmup shared by all threads and bounded aggregate memory | Publication cost, per-search lifetime lock, and cold-build contention |
| Matcher-private DFA cache | Plain hot-path access and conventional Java ownership | Repeated warmup and cache memory multiplied by active matchers |
| Immutable cache generations | Shared warm state without readers blocking reset | More complicated state replacement and temporarily retained generations |
| Shared warm cache plus matcher-local state | Amortized deterministic-state construction with private captures and transient work | Two cache paths and more complex budgeting |

Measure each viable design for first search, warm single-thread search, matcher
creation and reset, many matchers using one pattern, concurrent throughput, cache
reset, and retained memory. Include both one frequently used pattern and many
low-frequency patterns; the best design for a single hot expression may be
unacceptable for a Trino query containing many expressions.

API ergonomics are part of this study. The preferred Java surface remains an
immutable compiled `Re2` and a reusable thread-confined `Re2Matcher`. Cache
ownership is an implementation decision unless callers genuinely need to choose
between memory and warmup policies. A future Trino integration can determine
whether a matcher naturally remains associated with a driver or function
instance without adding user-visible lifecycle management to this library.

If the cost is material, evaluate alternatives in this order:

1. preserve shared-pattern semantics while reducing publication or cache-lifetime
   overhead
2. move mutable execution caches to a thread-confined matcher or execution object
   while keeping the compiled program immutable and reusable
3. use generation or snapshot ownership so active searches retain old cache data
   without a read lock
4. deliberately remove shared-pattern thread safety only if the faster ownership
   contract is explicit, practical for Trino, and worth its compile and memory cost

Do not remove synchronization in place while retaining shared mutable caches.
That configuration previously produced reproducible incorrect results. Feature
removal is permitted in this unpublished codebase, but it must be an explicit API
and architecture decision supported by single-thread performance, concurrent
correctness, memory, and operation-adapter measurements.

## Historical Revision Controls

When a current result contradicts the earlier load model or recorded benchmark,
rerun the historical implementation on the same machine, JDK, compiler, inputs,
and benchmark settings. Do not compare a fresh current number directly with an
old number from another environment.

The pre-restack implementation history remains available from
`archive/user-dain/re2-port/pre-restack` at
`c9e46be5dced63b6c0b67cd7eb92d3eea8496ba8`.
Recorded optimization commits, including `5d53269`, `81c05c0`, `887442a`,
`c0af967`, `ca202f0`, `d568e57`, `a055220`, and `b1156ca`, are currently present
in the repository object database. The pinned native RE2 commit is external and
is fetched and verified by `tools/re2-golden/fetch-dependencies.sh`.

Before removing or pruning historical refs, preserve durable refs for:

- the final pre-correctness performance baseline
- the flat `int[]` DFA transition implementation
- the `Object[]` transition implementation
- the split single-byte prefix implementation
- the accepted NFA campaign baseline
- representative rejected experiments needed to validate regression gates

Use a detached worktree to run a historical revision. Build its own classes and
dependencies in that worktree, but feed it the same benchmark inputs and native
baseline used by the current revision. A historical replay answers whether a
difference comes from implementation changes, JDK code generation, host behavior,
or the benchmark itself.

## Execution Phases

### Phase 1: Local Regression Survey

- Build pinned C++ RE2 directly with release optimization.
- Run short Java and C++ sweeps over historical gaps and protected workloads.
- Audit old benchmarks for allocation or semantic mismatches.
- Replay selected historical revisions beside the current code when results do
  not match the established critical-path model.
- Establish which recent changes altered performance shape.

### Phase 2: Hot-Path Recovery

- Investigate the largest public-path gaps one at a time.
- Audit current DFA publication loads and search locks first because they were
  added after the earlier loop campaign.
- Compare shared, matcher-private, generation-based, and hybrid cache ownership
  before changing the public concurrency contract.
- Revalidate engine selection, prefix acceleration, captures, and repeated search.
- Preserve unsuccessful experiments in the historical log so they are not
  repeated.

### Phase 3: Trino-Shaped API Confidence

- Expand Trino's narrow existing benchmark to the complete operation set.
- Use deterministic data and compare Joni, historical RE2J, and this port.
- Keep comparisons at the equivalent direct scalar-operation boundary so they
  measure this library rather than Trino's page, block, or expression machinery.
- Leave compiled page projections, SQL registration, block construction, and
  distributed query execution to the separate Trino adoption project.

### Phase 4: Cross-Architecture Engineering Run

After local gaps settle, run one serious engineering session on current Intel and
Graviton hosts. Use these results to catch architecture-specific regressions and
guide final changes. This is not the later multi-session qualification campaign.

## Campaign Output

Maintain the current ledger in
[`ENGINEERING_RESULTS.md`](ENGINEERING_RESULTS.md), containing:

- baseline and current measurements for protected workloads
- public and Trino-level results, not only internal engine numbers
- allocation results for each API shape
- every investigated gap with its scaling shape and root cause
- retained and rejected experiments
- current generated-code load graphs for each architecture examined
- unresolved gaps and the exact next experiment

The campaign is complete when all important scaling shapes match native RE2,
every material remaining gap is explained, the public and Trino paths meet their
performance goals, and no accepted optimization regresses a protected workload.
