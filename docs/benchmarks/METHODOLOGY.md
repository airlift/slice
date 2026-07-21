# RE2 Benchmarking Methodology

This guide covers how to run benchmarks, interpret results, and investigate performance gaps.

For prior measurements and root-cause work, see the
[historical benchmark archive](history/README.md). Those numbers are not current
release qualification.

---

## Principle: Know Why, Not Just What

A benchmark number without an explanation is worse than no benchmark at all - it creates false confidence. Every performance difference between Java and C++ RE2 must be **explained at the instruction or algorithm level**, not merely observed.

**Bad:** "Java is 4.6x slower on Easy1. This is JVM overhead."
**Good:** "Java is 3.7x slower on Easy1 because single-byte prefix `'A'` triggers ~176,000 indexOf calls per 16MB. Each call costs ~15-20ns in Java SWAR (loop bounds + chunk processing) vs ~5ns in Apple's hand-tuned NEON memchr. Mask hoisting (Run 002) reduced the gap from 4.6x to 3.7x by saving ~10ns per call. For long scans (Easy0, 8,600-byte average), setup amortizes and the gap closes to 1.08x."

Rules:
1. **Convert nanoseconds to cycles.** At 4 GHz, 1 ns = 4 cycles. If a 12-byte scan takes 1,850 ns (7,400 cycles), something is wrong - a table-lookup state machine needs ~300 cycles for 12 bytes.
2. **Account for every cycle.** If the measured cost is 600 cycles/byte and you can only explain 20 cycles/byte, the explanation is incomplete. Find the missing 580.
3. **Never blame "JVM overhead" without specifics.** Name the overhead: object allocation? bounds checks? lock acquisition? Quantify in cycles. "JVM overhead" is not an explanation - it's giving up.
4. **Check the benchmark before blaming the code.** Most "10x gaps" turn out to be measurement artifacts: interpreter-speed runs (insufficient warmup), dead code elimination, wrong text sizes.
5. **Require O(n) agreement.** If C++ is O(1) and Java is O(n), that's a missing optimization, not a language gap. Fix it.

---

## 1. Running Benchmarks

### JMH Benchmark Classes

| Class | What it measures | Params |
|-------|-----------------|--------|
| `BenchmarkRe2Search` | DFA + Re2 API search (failed match) | textSize: 8, 64, 512, 4K, 32K, 256K, 2M, 16M |
| `BenchmarkRe2SearchNfa` | NFA search (failed match) | textSize: 8, 64, 512, 4K, 32K, 256K |
| `BenchmarkRe2SearchExtra` | Additional failed and successful DFA, Re2, OnePass, and BitState searches | textSize: 8 through 16M, route dependent |
| `BenchmarkRe2Parse` | Parse/extract with submatch capture | fixed 12-byte input |
| `BenchmarkRe2FullMatch` | FullMatch scaling | textSize: 8, 64, 512, 4K, 32K, 256K, 2M |
| `BenchmarkRe2Practical` | Practical patterns + compile cost | fixed inputs |
| `BenchmarkRe2Misc` | Small matches and possible-match-range analysis | fixed inputs |
| `BenchmarkRe2CompileFocused` | Parser, compiler, total compile, and compile-plus-cold-DFA costs | pattern |
| `BenchmarkDfaCache` | Warm sharing, cold waves, and late transition construction | textSize and workerCount |
| `BenchmarkDfaFixedDistanceByte` | Fixed-distance selective-byte scanner and dense fallback | inputShape and sourceLength |
| `BenchmarkDfaCompactTransitionLayout` | Synthetic compact transition representations | stateCount and classCount |
| `BenchmarkDfaRealTransitionLayout` | Transition representations over warmed DFA graphs | pattern |
| `BenchmarkDfaSelfLoopSearch` | Integrated unpaired stable-self-loop DFA route | textLength |
| `BenchmarkSingleByteRepeatMatcher` | Nullable one-byte repetition count and boundary specialization | shape and sourceLength |
| `BenchmarkDfaCountMatches` | Successive DFA count searches with one reader lease versus repeated searches | workload and sourceLength |
| `BenchmarkBoundedCharacterClassCounter` | Bounded character-class counting versus repeated matching | workload and sourceLength |
| `BenchmarkRe2PublicApi` | Boolean, caller-buffer, result, and reusable matcher costs | tiny/large match/no-match workloads |
| `BenchmarkRe2BooleanPartialMatch` | Optimized boolean partial-match route versus the ordinary engine | workload and sourceLength |
| `BenchmarkTrinoRegexp` | Complete Trino adapter operation set | workload and sourceLength |

Shared utilities live in `Re2BenchmarkRunner.java` (CLI argument parsing,
byte-identical Java/C++ corpus generation, `compileProg()`, and `compileRe2()`).

### How to Run

```bash
# Compile
./mvnw test-compile -q

# Set classpath via Maven (reliable, handles all dependencies)
CP="target/test-classes:target/classes:$(./mvnw -q dependency:build-classpath -DincludeScope=test -Dmdep.outputFile=/dev/stdout 2>/dev/null)"

# Full run of one class (annotation defaults: 3 forks, 10x1s warmup, 5x1s measurement):
java --add-modules jdk.incubator.vector -cp "$CP" \
  io.airlift.slice.re2.BenchmarkRe2Search

# Smoke test (1 fork):
java --add-modules jdk.incubator.vector -cp "$CP" \
  io.airlift.slice.re2.BenchmarkRe2Search BenchmarkRe2Search 1

# Calibrate a single method (1 fork, 20 warmup, 10 measurement, 200ms iterations):
java --add-modules jdk.incubator.vector -cp "$CP" \
  io.airlift.slice.re2.BenchmarkRe2Search searchEasy0Re2 1 20 10 200
```

CLI args: `[filter] [forks] [warmupIters] [measIters] [iterTimeMs]`

### Direct C++ Baseline

```bash
tools/re2-benchmark/build.sh
target/re2-benchmark-build/regexp_benchmark \
  --benchmark_filter='threads:1$' \
  --benchmark_repetitions=5 \
  --benchmark_min_time=0.5s
```

The build script fetches pinned RE2, Abseil, Google Benchmark, and GoogleTest
sources and builds them together without host-installed C++ libraries. It
patches RE2's benchmark random-text generator to match
`Re2BenchmarkRunner.randomText()` byte for byte and adds benchmark-only
equivalent workload definitions; RE2 engine code is unchanged. Native results
come from this direct C++ executable, not from JNI.

Pinned dependency commits are:

| Dependency | Commit |
|---|---|
| RE2 | `972a15cedd008d846f1a39b2e88ce48d7f166cbd` |
| Abseil | `d38452e1ee03523a208362186fd42248ff2609f6` |
| Google Benchmark | `192ef10025eb2c4cdd392bc502f0c852196baa48` |
| GoogleTest | `52eb8108c5bdec04579160ae17225d66034bd723` |

The complete traditional comparison is the audited 298-row intersection, not
the union of every registration in both harnesses. Run it on Intel and Graviton
with `CAMPAIGN_MODE=traditional-native-comparison`. The pair manifest generated
by `tools/re2-benchmark/traditional/summarize.py` requires matching operation,
input, engine route, caching, capture behavior, encoding, and memory budget.
The native executable runs immediately before and after Java; reports use the
geometric mean of those native medians and express every result as Java elapsed
time divided by native elapsed time, where lower is better.

### Trino Comparators

`BenchmarkTrinoRegexp` measures this port's complete adapter operations. Its
paired Trino comparator is `BenchmarkRegexpOperations` in an isolated Trino
worktree based on `origin/master` commit
`695b824f87c1272849503486cabd58c719c63e2c`. Both harnesses use the same five
workloads, two source sizes, patterns, source bytes, occurrence arguments, and
lambda behavior. The Trino class exposes separate Joni and historical RE2J
methods for contains, count, third position, extract, extract-all, split,
replacement, and lambda replacement.

Before accepting timings, run the semantic smoke tests:

```bash
# This repository
./mvnw "-Dtest=TestBenchmarkTrinoRegexp,TestBenchmarkRe2PublicApi,TestBenchmarkSingleByteRepeatMatcher" test

# Trino worktree
./mvnw -pl core/trino-main -Dtest=TestBenchmarkRegexpOperations test
```

`TestBenchmarkRegexpOperations` also locks the deterministic corpus prefix and
compares every Joni result with historical RE2J for all workload/size pairs.
This protects the comparator itself; it is not performance evidence.

`BenchmarkSafeReTrinoRegexp` applies the same matrix to SafeRE's borrowed UTF-8
API. Its test-only adapter includes the per-value `Utf8Input.trusted` view that
a Trino integration requires, but does not decode or copy source bytes.
`TestBenchmarkSafeReTrinoRegexp` compares all results with `TrinoRegexp` before
timing. Run the paired Intel and Graviton comparison with
`CAMPAIGN_MODE=safere-comparator`; the host brackets SafeRE with Slice before
and after and records normalized allocation.

### AWS Engineering Session

`tools/re2-benchmark/aws/run-campaign.sh` runs one internal engineering session
on an Intel and Graviton host pair. It snapshots dirty worktrees,
uses a private temporary S3 bucket and a per-campaign least-privilege instance
role, launches instances with no inbound access, pins benchmark processes to
physical cores, downloads raw artifacts, and terminates all cloud resources.
Host transfers therefore survive expiration of the operator's SSO session. See
`tools/re2-benchmark/aws/README.md` for the exact workload and overrides.

Independent shards should use concurrent campaign invocations and independent
host pairs. Candidate and control measurements for one shard must stay on the
same host; results from different hosts are not interchangeable repetitions.

### Profiling

```bash
# CPU profiling with async-profiler
java -agentpath:/path/to/libasyncProfiler.dylib=start,event=cpu,file=profile.html \
  --add-modules jdk.incubator.vector -cp "$CP" \
  io.airlift.slice.re2.BenchmarkRe2Search

# JIT compilation logging
java -XX:+PrintCompilation --add-modules jdk.incubator.vector -cp "$CP" \
  io.airlift.slice.re2.BenchmarkRe2Search 2>&1 | grep "io.airlift.slice.re2"

# Assembly output (requires hsdis)
java -XX:+UnlockDiagnosticVMOptions -XX:+PrintAssembly \
  -XX:CompileCommand=print,*Dfa.searchLoop \
  --add-modules jdk.incubator.vector -cp "$CP" \
  io.airlift.slice.re2.BenchmarkRe2Search
```

### Retained Memory Diagnostics

Production DFA cache accounting uses `SizeOf` to derive object, reference-array,
and primitive-array sizes from the running VM. Tests verify that construction,
growth, and reset charge all retained backing storage to the DFA budget.

JOL was used temporarily to qualify this model on Intel and Graviton with
compressed references enabled and disabled. The retained per-state model matched
JOL exactly with compressed references and conservatively overestimated it by
1.4% without compressed references. JOL is deliberately not a project
dependency because its diagnostic access relies on restricted JVM mechanisms.
The qualification evidence is retained in the
[`DFA cache capacity policy`](history/2026-07-18-dfa-cache-capacity-policy.md).
The Trino-shaped lifecycle census and same-host Joni comparison are retained in
the [`Joni memory qualification`](history/2026-07-18-joni-memory-qualification.md).

---

## 2. Benchmarking Methodology

### Think in Cycles, Not Nanoseconds

On a 4 GHz machine: **1 ns = 4 cycles.** Always convert to cycle counts and ask whether the instruction count makes sense.

**Bad analysis:** "OnePass takes 1,850 ns vs C++ 154 ns (12x gap). This is inherent JVM overhead."

**Why it's bad:** 1,850 ns = 7,400 cycles. For 12 bytes with a table-lookup state machine, you need ~300 cycles. If you measure 7,400, either the benchmark is wrong, or there's a bug. Never accept a large number without accounting for the cycles.

Approximate cycle costs (JIT-compiled, 4 GHz):

| Operation | Cycles |
|-----------|--------|
| Array access (L1 hit) | 4 |
| Array access (L2 hit) | 12 |
| `new int[8]` (TLAB fast path) | 15-25 |
| Virtual call (monomorphic, inlined) | 0 |
| Virtual call (megamorphic) | 15-30 |
| `synchronized` enter/exit (uncontended, JDK 15+) | 40-100 |
| Branch (mispredicted) | 15-20 |

### JIT Warmup is Non-Negotiable

HotSpot uses tiered compilation: Interpreter -> C1 -> C2. The C2 compiler needs ~10,000 invocations and profiling data. **5 warmup iterations = measuring interpreter speed (10-50x slower than reality).**

**Calibration workflow:** Use short iterations (200ms) with many warmup rounds (20) to observe the C2 compilation cliff:

```
Iteration   1: 1850.234 ns/op   <- interpreter
Iteration   2: 1812.456 ns/op   <- interpreter
Iteration   3:  145.678 ns/op   <- C2 compiled!
Iteration   4:   14.234 ns/op   <- stable
```

JMH annotation defaults (10 x 1000ms warmup, 5 x 1000ms measurement, 3 forks) should be comfortably past C2 for most methods. If calibration shows late compilation, increase warmup.

**Red flags in results:**
- Java >10x faster than C++ at 10-20 ns: suspect constant folding / dead code elimination. Verify return values are consumed.
- All sizes report the same time: suspect JIT eliminated the work.
- Massive variance between iterations: GC pauses or background compilation. Increase fork count.

### Micro-benchmarks vs End-to-End

Micro-benchmark performance does not always predict end-to-end performance. Always validate optimizations end-to-end.

Example from this project:

| Implementation | Isolated byte scan | End-to-end (EASY0 16MB) |
|----------------|-------------------|------------------------|
| SWAR unroll-2 | 33.7 GB/s | **8.16 GB/s** (winner) |
| Vector API | **36.4 GB/s** (winner) | 7.72 GB/s |

The reversal happened because: code size affects I-cache, JIT optimizes differently in full context, register pressure from surrounding code changes.

**Rule:** Micro-benchmarks guide exploration; end-to-end benchmarks make decisions.

JIT method boundaries are part of the tested implementation. A branch that is
always rejected at runtime can still change inlining, register allocation, and
loop shape. The compact DFA exploration measured a 5.6% Graviton regression
when a sampling branch remained in a shared paired continuation, then removed
it by dispatching around a physically separate method. Protect rejected paths
with source-disabled end-to-end benchmarks; do not infer neutrality from a
predicate value or an isolated table walker.

### Bounded Engine Campaigns

Performance investigations must have a round limit and stopping rules before
implementation starts. The 2026-07-18 capture campaign used these ceilings:

- NFA: three rounds
- OnePass: two rounds
- BitState: three rounds
- public-path integration: two rounds

Each round tests one measured hypothesis. Direct-engine benchmarks select
mechanisms, but public integration and protected capture-free benchmarks decide
retention. Local measurements are diagnostic; Intel and Graviton determine the
decision.

Every retained-source comparison uses candidate-before, exact source control,
and candidate-after on the same host. Direct native comparisons also bracket
Java with native-before and native-after. The control must pass semantic tests,
and reversing it must restore the candidate source hash exactly.

Both candidate/control and Java/native timing ratios are elapsed-time ratios,
so lower is better. `1.00` is parity, values below `1.00` are faster, and values
above `1.00` are slower. Allocation is reported separately and cannot justify a
timing regression by itself.

Model-filtered Rebar campaigns must validate the selected engine/workload pairs
against their exact engine manifest. They must still retain and validate the
complete workload provenance manifest, so filtering performance measurements
does not silently narrow semantic or input provenance.

Native before/after drift above 2% is a hard qualification failure. The AWS
runner defaults `REBAR_ALLOW_NATIVE_DRIFT` to `false`; enabling it is reserved
for explicitly labeled diagnostics and does not produce final evidence.

Stop an engine when any of these conditions is met:

- the representative paths reach parity;
- the round ceiling is exhausted;
- no measured mechanism is likely to recover at least 5%;
- correctness, lifecycle, or memory accounting fails;
- an important protected path has a stable regression above 2%.

One protected regression fails the candidate even when a geometric aggregate
looks favorable. Do not open an extra round merely because a failed candidate
improved a different workload.

### Capture Pipeline Decomposition

When a public capture operation is slow, first identify its actual route and
measure the stages independently: control, matcher setup, forward DFA, reverse
DFA, capture engine, composed operation, and result consumption. The adjusted
components must explain at least 90% of bracketed public time before assigning a
root cause.

Do not infer public performance from a stateless direct-engine benchmark. A
reusable Java matcher can safely own scratch that a low-level stateless call must
allocate for each search. The 2026-07-18 pipeline campaign found that reusable
NFA and BitState workspaces recovered 15%-40% on capture-dominated public paths
without changing either matching algorithm.

Workspace tests must prove all of the following:

- scratch arrays and arena rows are reused after warmup;
- stale matches and unmatched groups are reset;
- no input `Slice`, backing byte array, or caller group array remains reachable;
- the caller-buffer API preserves its zero-allocation contract;
- no-match, nonzero backing offsets, and matcher reset retain semantics.

Routing changes require a complete public-operation win. Direct unanchored NFA
was rejected despite removing two DFA stages because the wider NFA search was
49%-50% slower. Direct BitState was retained only where its complete bounded
search replaced all three stages and improved the public operation by more than
80%.

---

## 3. Investigation Guide

When you find a performance gap between C++ RE2 and Java RE2:

### Step 1: Verify the Benchmark

- [ ] JVM warmed up properly? (Seeing thousands of iterations, not dozens)
- [ ] Identical inputs? (Same pattern, text generation, random seed)
- [ ] JIT compiled? (`-XX:+PrintCompilation` shows Tier 4 for hot methods)
- [ ] Return values consumed? (Not eliminated by dead code)

### Step 2: Check Scaling Behavior

Test multiple sizes (Java benchmarks now cover the full C++ range: 8B to 16MB for DFA/RE2, 8B to 256KB for NFA):

| Pattern | Diagnosis | Action |
|---------|-----------|--------|
| C++ constant time, Java linear | Missing early-exit optimization | Find the missing optimization |
| Both linear, same slope | Algorithmic parity | Profile hot paths if gap >2x |
| Both linear, Java 5-10x steeper | Algorithmic bug | Find missing optimization |

**Smoking gun:** C++ time constant across sizes, Java scales linearly = **algorithmic bug**, not a low-level issue.

### Step 3: Read the C++ Source

```bash
cd re2_upstream
grep -n "PrefixAccel\|PartialMatch\|anchor_end" re2/re2.cc re2/dfa.cc re2/prog.cc
```

Read the actual implementation. Check for early-exit conditions, caching, optimizations not yet ported to Java.

### Step 4: Instrument Both Sides

If reading alone isn't enough, add `printf` / `System.err.println` at matching decision points in both C++ and Java, then compare traces side-by-side:

```
C++ trace:                              Java trace:
Match: anchor_end=1, anchor=0          Match: anchorEnd=true, anchor=UNANCHORED
Match: running reverse DFA first        Match: running forward DFA first    <-- DIVERGENCE!
```

The divergence point tells you exactly what's missing. Clean up trace statements before committing.

### Step 5: Profile, Then Optimize

Profile to find where time is actually spent (async-profiler, JFR). Optimize in order of impact:
1. **Algorithmic** (10-1000x) -- fix bugs, port missing optimizations
2. **Data structure** (2-10x) -- cache-friendly layouts, reduce allocations
3. **Low-level** (1.5-3x) -- SWAR, Vector API, branch elimination

### Red Flags

- **"That's just how Java is"** -- Large gaps have reasons; find them. A 287,463x gap on `(?s).*` is not "how Java is" - it's a missing optimization.
- **"JVM overhead" without specifics** -- Name the overhead, quantify it in cycles. "30ns from 4 heap allocations (SearchResult x 2 + SearchParams x 2) at ~7ns each" is an explanation. "JVM overhead" is not.
- **"Remaining gap is inherent"** -- This is giving up disguised as analysis. If you can't account for the cycles, keep investigating.
- **Micro-benchmark tunnel vision** -- "This function is 10x faster but end-to-end is the same."
- **Optimizing without profiling** -- Profile first, optimize hot paths.
- **Not reading C++ source** -- Don't guess what C++ does; read it.
- **FUD ratios** -- Reporting a ratio without explaining WHY creates fear, uncertainty, and doubt. Every ratio in a report must have a root cause paragraph. If you can't write the paragraph, you don't understand the gap yet.

---

## 4. Lessons Learned

Hard-won insights from optimizing this codebase. Each cost hours or days to discover.

### 4.1 Zombie State Bug (Dfa.workqToCachedState)

In `Dfa.workqToCachedState()`, states with `ninst=0` and `flag=FLAG_LAST_WORD` (0x200) were not collapsed to DEAD because the check was `n == 0 && flag == 0`. The `FLAG_LAST_WORD` bit survives `needFlags` cleanup (which only clears `FLAG_EMPTY_MASK=0x00FF`).

**Fix:** `n == 0 && (flag & FLAG_MATCH) == 0` -- only keep states alive when FLAG_MATCH is set.

**Symptom:** Reverse DFA scanned the entire text linearly instead of stopping at DEAD after the first non-matching byte. This made end-anchor rejection O(n) instead of O(1).

### 4.2 Instance Method Cascade Bypass

The byte-oriented `Re2.partialMatch` and `fullMatch` instance methods routed directly to `Nfa.search()`, bypassing the DFA -> OnePass -> BitState cascade entirely.

**Fix:** Route through `match(input, anchor, null)` like the static methods and like C++ does (`PartialMatchN` -> `DoMatch(UNANCHORED)` -> `Match()`).

**Symptom:** 8-23x slower on FullMatch patterns. The benchmark `Re2Benchmark` was using instance methods, so all cascade optimizations appeared to have no effect.

### 4.3 JIT Warmup: 5 Iterations = Interpreter

The original `Re2Benchmark` used 5 warmup + 10 measured iterations. HotSpot C2 needs ~10,000 invocations. We were measuring **interpreter speed**, not JIT-compiled code.

**Fix:** Time-based warmup (10+ seconds) or proper JMH annotations.

**Impact:** Every number in the optimization log was wrong by 25-130x. A "12.6x OnePass gap" was actually a measurement artifact -- with proper warmup, Java OnePass is 2.1x **faster** than C++ (72 ns vs 154 ns).

**Rule:** If warmup completes in <1,000 iterations, the warmup is too short.

### 4.4 SWAR vs Vector API: Isolated Winner != Integrated Winner

Vector API (36.4 GB/s) beat SWAR (33.7 GB/s) in isolated byte scanning by 8%. But in end-to-end RE2 benchmarks, SWAR (8.16 GB/s) beat Vector API (7.72 GB/s) by 6%.

**Cause:** Code size affects I-cache; JIT optimizes differently in full context; register pressure changes with surrounding code.

**Rule:** Always validate with end-to-end benchmarks. Micro-benchmarks guide exploration, not decisions.

### 4.5 Amdahl's Law: 5.3x Byte Scan -> 1.81x End-to-End

SWAR byte scanning achieved 5.3x isolated speedup, but only 1.81x in EASY0 16MB. Byte scanning was ~50% of total DFA time. Amdahl's Law: `1 / (0.5 + 0.5/5.3) = 1.81x`.

**Rule:** Before optimizing a component, estimate what fraction of total time it represents. A 10x speedup on 5% of runtime = 1.05x end-to-end.

### 4.6 JDK Built-ins Don't Help for Byte Arrays

- `Arrays.mismatch()`: Finds differences, not matches. Wrong primitive for indexOf.
- `String.indexOf()`: Fast intrinsic (2.3x when String already exists), but creating a String from byte[] costs allocation + copy, making it **0.5-0.7x slower** overall.

**Conclusion:** For byte array scanning, custom SWAR/Vector API is the right approach.

### 4.7 ARM memchr Gap is Platform-Specific

C++ RE2 on ARM calls `memchr()`, which is hand-written Apple assembly using NEON, optimized over decades. The ~2x throughput gap between Java SWAR (33 GB/s) and Apple's memchr (~50-100 GB/s) is a platform limitation, not a code bug.

On x86, C++ RE2 uses AVX2 (256-bit). ARM NEON is only 128-bit. The gap varies by platform.

**Rule:** A 2x gap in a specific vectorized primitive is acceptable if end-to-end performance matches. Focus on algorithmic differences, not memchr parity.

### 4.8 End-Anchor Reverse DFA: Anchor Swapping for Constant-Time Rejection

For `$`-anchored patterns (e.g., `ABCDEFGHIJKLMNOPQRSTUVWXYZ$`):
- Forward program: `anchorStart=false, anchorEnd=true`
- Reverse program: `anchorStart=true, anchorEnd=false` (swapped by Compiler)
- Reverse DFA with `anchored=true` starts at physical text end and scans backward
- Non-matching text: DEAD state after 1 byte = O(1) rejection regardless of text size

This turned EASY0/EASY1/MEDIUM/HARD Re2 API benchmarks from O(n) linear scans into O(1) constant-time rejection, yielding 6,645x-4,731,279x speedups on 16MB non-matching text.

The zombie state bug (4.1) had to be fixed first -- without it, the reverse DFA never reached DEAD.
