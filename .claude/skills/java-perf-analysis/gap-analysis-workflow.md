# Gap Analysis Workflow

A structured approach to investigating performance gaps between Java and a native baseline (C, C++, Rust, etc.).

Java can match C/C++ performance in most cases — gaps always have specific, explainable reasons. The goal of this workflow is to find those reasons.

## The Workflow

### Step 1: Verify the Benchmark

Before blaming the code, verify the measurement is valid:

- [ ] JVM warmed up properly? (Seeing thousands of iterations, not dozens)
- [ ] Identical inputs? (Same pattern, text, random seed)
- [ ] JIT compiled? (`-XX:+PrintCompilation` shows tier 4 for hot methods)
- [ ] Return values consumed? (Not eliminated by dead code)
- [ ] Same algorithm? (Both using the same approach, not different engines)
- [ ] C/C++ compiled with optimizations? (`-O2` or `-O3`, not a debug build)

Most "10x gaps" turn out to be measurement artifacts: interpreter-speed runs, dead code elimination, wrong input sizes. This applies in both directions — we saw cases where Java appeared 5x faster than C++ because the C++ code had been compiled without `-O3`. An unoptimized C++ build is the equivalent of running Java in the interpreter.

**If either system is more than 5x faster**, something is almost certainly wrong — either an algorithmic difference or a bad benchmark. Investigate the measurement before investigating the code.

### Step 2: Check the Performance Formula

Every piece of code has a performance shape. Before analyzing the gap, verify both implementations match the expected shape:

- **Constant time**: Result independent of input size (e.g., hash lookup, optimized rejection)
- **Setup + per-unit cost**: `T = overhead + (n * cost_per_byte)` (e.g., scanning, parsing)
- **Per-node or per-edge**: `T = f(nodes, edges)` (e.g., graph traversal)

Run at multiple input sizes. The scaling pattern reveals the category of the gap:

| Pattern | Diagnosis | Category |
|---------|-----------|----------|
| Native O(1), Java O(n) | Missing optimization | Algorithmic |
| Both O(n), Java 5-10x steeper | Missing fast path | Algorithmic |
| Both O(n), same slope, constant offset | Per-call overhead | Data structure / dispatch |
| Both O(n), Java 1.2-1.5x steeper | Inner loop overhead | Low-level |

**Key insight**: If the ratio changes with input size, the gap is algorithmic. If the ratio is constant across sizes, the gap is per-operation overhead.

**Most common bug**: Code that should be O(1) is running O(n) because an optimization isn't firing. We hit this repeatedly — reverse DFA rejection should be constant-time, but zombie states prevented the DEAD sentinel from being reached, causing full linear scans.

### Step 3: Read the Native Source

Don't guess what the native code does. Read it.

```bash
grep -n "relevant_function\|fast_path\|optimization" native_source.cc
```

Look for:
- Early-exit conditions not ported to Java
- Caching / memoization not present in Java
- Template specialization producing multiple variants
- SIMD intrinsics (memchr, memmove, etc.)
- Compile-time constants that are runtime checks in Java

### Step 4: Instrument Both Sides

If reading alone isn't enough, add trace output at matching decision points in both implementations and compare side-by-side:

```
C++ trace:                              Java trace:
Match: anchor_end=1, anchor=0          Match: anchorEnd=true, anchor=UNANCHORED
Match: running reverse DFA first        Match: running forward DFA first    <-- DIVERGENCE
```

The first divergence point tells you exactly what's missing. Clean up trace output before committing.

### Step 5: Categorize and Prioritize

| Category | Typical speedup | Effort | Priority |
|----------|----------------|--------|----------|
| Algorithmic | 10-1000x | Medium | **Fix first** |
| Data structure | 2-10x | Medium-High | Fix second |
| Low-level | 1.2-1.5x | High | Fix only if high-impact path |

**Algorithmic gaps** (missing early-exit, wrong engine selection, O(n) vs O(1)):
- These are bugs. The Java code is doing fundamentally more work.
- Often caused by a missed optimization from the native code.
- Fix by porting the missing optimization.

**Data structure gaps** (object indirection, cache layout, allocation overhead):
- Java objects add pointer chases that don't exist with C structs/arrays.
- These only matter when on the critical path of a tight loop (most code is load-bound, so extra instructions and bounds checks are free).
- Fix by flattening to primitive arrays or caching references — but only when profiling shows it matters.

**Low-level gaps** (SIMD, platform-specific intrinsics):
- Some native code uses hand-tuned SIMD (e.g., Apple's NEON memchr) that Java can't match directly.
- These are platform-specific, not language-inherent — Java on the same hardware with the same algorithm should be close.
- Only optimize if the path dominates total runtime.

### Step 6: Profile, Then Optimize

Profile to find where time is actually spent (async-profiler, JFR). Optimize in order of impact, validating each change end-to-end.

## Anti-Patterns

### "That's just how Java is"

Large gaps have reasons. Find them. A 287,000x gap is not "how Java is" — it's a missing optimization. Java matches C/C++ in libraries like Aircompressor (xxhash, LZ4, Zstd, Snappy).

### "JVM overhead" without specifics

Name the overhead: object allocation? method dispatch? Quantify it. "30ns from 4 heap allocations at ~7ns each" is an explanation. "JVM overhead" is not. Note that bounds checks and compressed oops are generally free in load-bound code — don't blame them without evidence.

### "Remaining gap is inherent"

This is giving up disguised as analysis. If you can't account for the cycles, keep investigating.

### FUD ratios

Reporting a ratio without explaining WHY creates fear, uncertainty, and doubt. Every ratio in a report must have a root-cause paragraph. If you can't write the paragraph, you don't understand the gap yet.

### Micro-benchmark tunnel vision

"This function is 10x faster but end-to-end is the same." Always validate with end-to-end benchmarks.

### Optimizing without profiling

Profile first, optimize hot paths. A 10x speedup on 5% of runtime = 1.05x end-to-end.

## Worked Example: Per-Call Overhead

**Symptom**: Java 5x slower on 12-byte input, but only 1.3x slower when running the inner engine directly.

**Scaling check**: Gap shrinks at larger inputs (5x at 12B, 1.2x at 2MB). This means the gap is constant overhead, not per-byte cost.

**Root cause**: Java dispatch path has ~220ns overhead (method dispatch, parameter validation, two DFA phases at 45ns each). C++ inlines everything and uses stack allocation (~4ns overhead).

**Cost breakdown**:
- Method dispatch: ~8 calls x 5ns = ~40ns
- Two DFA phases: 2 x 37ns extra = ~74ns
- Parameter validation, wrapping: ~30ns

**Fix options**: Skip DFA for small anchored matches; inline dispatch path; reduce phase overhead.

## Worked Example: Missing Optimization

**Symptom**: C++ constant time across all input sizes (8B to 16MB). Java time scales linearly.

**Scaling check**: O(1) vs O(n) = algorithmic gap. Not a low-level issue.

**Native source**: C++ runs reverse DFA from text end, hits DEAD state after 1 byte for non-matching input.

**Instrumentation**: Java's reverse DFA never reaches DEAD state — scans the entire text linearly.

**Root cause**: States with zero instructions but non-zero flags were not collapsed to DEAD because the check was `n == 0 && flag == 0`. A `FLAG_LAST_WORD` bit (0x200) survived flag cleanup, keeping zombie states alive.

**Fix**: `n == 0 && (flag & FLAG_MATCH) == 0` — only keep states alive when FLAG_MATCH is set.

**Result**: 6,645x to 4,731,279x speedup on 16MB non-matching text (O(n) -> O(1)).
