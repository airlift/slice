---
name: java-perf-analysis
description: Advanced Java performance analysis — critical path analysis, JIT assembly, cycle budgets, benchmarking methodology, and optimization patterns. Use when investigating performance gaps, analyzing JIT output, or planning optimizations.
---

# Java Performance Analysis

## When to Use Which Technique

```
Performance problem
  |
  |-- "Why is Java Nx slower than C/C++?"
  |     \--> Gap Analysis Workflow (gap-analysis-workflow.md)
  |
  |-- "Where are the cycles going in this hot loop?"
  |     \--> Critical Path Analysis (critical-path-analysis.md)
  |
  |-- "Is the JIT generating good code?"
  |     \--> JIT Assembly Guide (jit-assembly-guide.md)
  |
  |-- "Are my benchmarks measuring the right thing?"
  |     \--> Benchmarking Guide (benchmarking-guide.md)
  |
  |-- "How do I make this faster?"
        \--> Optimization Patterns (optimization-patterns.md)
```

## Starting Point: Java Is Not Slow

Java can match C/C++ performance in most cases. The Aircompressor library proves this — xxhash, LZ4, Zstd, and Snappy in pure Java run at native speed. When there is a gap, there
is always a specific, explainable reason. **A performance number without an understanding of WHY is useless.** Every measurement needs an explanation at the instruction, loads, or
algorithm level.

The gut instinct that "Java is just slow" is wrong. When you see a gap, investigate it. You will find missing optimizations, algorithmic differences, or benchmark bugs — not an
inherent language tax.

## Quick Reference

### Think in Instructions and Loads

At 4 GHz: **1 ns = 4 cycles.** Converting measurements to cycles is useful for sanity-checking whether a number is physically plausible — if a 12-byte table lookup takes 7,400
cycles, something is wrong.

But don't take cycle-counting too far. On modern out-of-order cores, **the instructions come free with the loads** — especially main memory loads. Most hot loops are load-bound,
not instruction-bound. The critical path is the serial chain of dependent loads, and everything else executes in their shadow.
See [Critical Path Analysis](critical-path-analysis.md) for how to identify the true bottleneck.

That said, thinking in instructions is a powerful tool for reasoning about performance. It helps build accurate mental models and counteracts the vague "Java is just slow" instinct
with concrete, quantifiable explanations.

| Operation                          | Cycles  |
|------------------------------------|---------|
| L1 array access                    | 3-4     |
| L2 cache hit                       | 12      |
| L3 cache hit                       | 30-40   |
| Main memory                        | 150-300 |
| TLAB allocation (`new int[8]`)     | 15-25   |
| Monomorphic virtual call (inlined) | 0       |
| Megamorphic virtual call           | 15-30   |
| Branch misprediction               | 15-20   |

### Java-Specific Realities

**Bounds checks don't matter in practice.** In load-bound code (which most hot loops are), bounds checks execute in the shadow of the memory loads and add zero latency. Do not use
`Unsafe` to avoid them — `Unsafe` is unnecessary and unsupported. For extracting multi-byte values (int, long, float, double) from byte arrays, use VarHandles, which are just as
fast.

**Compressed oops cannot be avoided.** This is a reality of modern Java (default for heaps under 32 GB). The OOP decompression instructions (a shift + add) are real, but like
bounds checks, they generally don't matter because you are load-bound. Don't fight compressed oops — work with them.

### Know Your Performance Formula

Every piece of code has a performance shape. Before benchmarking, have an expectation:

- **Constant time**: Result independent of input size (e.g., hash lookup, optimized rejection)
- **Setup + per-unit cost**: `T = overhead + (n * cost_per_byte)` (e.g., scanning, parsing)
- **Per-node or per-edge**: `T = f(nodes, edges)` (e.g., graph traversal)

If the measured shape doesn't match your expectation, something is wrong. The most common case: code that should be O(1) is running O(n) because an optimization isn't firing. We
had multiple instances of this — reverse DFA rejection should be constant-time, but zombie states prevented the DEAD sentinel from being reached, causing linear scans of the entire
input.

### Optimization Impact Tiers

| Tier           | Typical speedup | Examples                                                 |
|----------------|-----------------|----------------------------------------------------------|
| Algorithmic    | 10-1000x        | Missing early-exit, O(n) vs O(1), wrong engine selection |
| Data structure | 2-10x           | Object indirection vs flat array, cache layout           |
| Low-level      | 1.5-3x          | SWAR, branch elimination, loop restructuring             |

### Red Flags

- **Either system > 5x faster than the other**: Something is wrong. It's either an algorithmic difference (missing optimization, wrong code path) or a bad benchmark (dead code
  elimination, insufficient warmup, wrong inputs). Investigate before trusting the number.
- **Java >10x faster than C++ at 10-20 ns**: Almost certainly dead code elimination — return values not consumed.
- **All input sizes report the same time**: JIT eliminated the work entirely.
- **Measured shape doesn't match expected shape**: O(1) code running O(n), or linear code showing constant time — an optimization is either missing or the benchmark is broken.
- **Massive inter-iteration variance**: GC pauses or background compilation — increase fork count.
- **"JVM overhead" without specifics**: Not an explanation. Name the overhead and quantify it in cycles. If you can't, you don't understand the gap yet.

## Supporting Files

- [Critical Path Analysis](critical-path-analysis.md) — Cycle-by-cycle dependency chain analysis on out-of-order cores
- [JIT Assembly Guide](jit-assembly-guide.md) — hsdis setup, assembly annotation, C2 compiler patterns
- [Benchmarking Guide](benchmarking-guide.md) — JMH methodology, cycle budget validation, scaling analysis
- [Gap Analysis Workflow](gap-analysis-workflow.md) — Structured root-cause decomposition for Java vs C++ gaps
- [Optimization Patterns](optimization-patterns.md) — C2 loop recognition, inlining thresholds, data structure choices
