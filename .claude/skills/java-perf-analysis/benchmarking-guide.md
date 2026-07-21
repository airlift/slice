# Benchmarking Guide

## Principle: Know Why, Not Just What

A benchmark number without an explanation is worse than no number — it creates false confidence. Every performance difference must be **explained at the instruction or algorithm level**, not merely observed.

**Bad:** "Java is 4.6x slower on this pattern. This is JVM overhead."

**Good:** "Java is 3.7x slower because single-byte prefix triggers ~176,000 indexOf calls per 16MB. Each call costs ~15-20ns in Java SWAR vs ~5ns in Apple's hand-tuned NEON memchr. Mask hoisting reduced the gap from 4.6x to 3.7x by saving ~10ns per call."

## Sanity-Check with Cycles

At 4 GHz: **1 ns = 4 cycles.** Use this conversion to sanity-check whether a measurement is physically plausible.

```
cycles_per_operation = measured_ns * GHz
```

If a 12-byte scan takes 1,850 ns (7,400 cycles), something is wrong — a table-lookup state machine needs ~300 cycles for 12 bytes. Never accept a large number without accounting for the cycles.

But remember: most hot loops are load-bound. The instructions come free with the loads. Cycle math is for catching nonsense numbers, not for micro-optimizing instruction counts. See [Critical Path Analysis](critical-path-analysis.md) for identifying the true bottleneck.

## JMH Best Practices

### Warmup is Non-Negotiable

HotSpot uses tiered compilation: Interpreter -> C1 -> C2. C2 needs ~10,000 invocations and profiling data. **5 warmup iterations = measuring interpreter speed (10-50x slower than reality).**

**Calibration workflow**: Use short iterations (200ms) with many warmup rounds (20) to observe the C2 compilation cliff:

```
Iteration   1: 1850.234 ns/op   <- interpreter
Iteration   2: 1812.456 ns/op   <- interpreter
Iteration   3:  145.678 ns/op   <- C2 compiled!
Iteration   4:   14.234 ns/op   <- stable
```

**Rule**: If warmup completes in < 1,000 iterations, the warmup is too short.

### Standard JMH Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| Forks | 3 | Eliminates JIT compilation ordering effects |
| Warmup iterations | 10 x 1s | Ensures C2 compilation |
| Measurement iterations | 5 x 1s | Captures stable performance |

For calibration/exploration: `1 fork, 20 warmup x 200ms, 10 measurement x 200ms`

### Verifying JIT Compilation

```bash
java -XX:+PrintCompilation -cp "$CP" MyBenchmark 2>&1 | grep "my.package"
```

Look for tier 4 compilations of hot methods. If you only see tier 1-3, warmup is insufficient.

## Know Your Performance Formula

Before benchmarking, have an expectation of the performance shape:

- **Constant time**: Result independent of input size (e.g., hash lookup, optimized rejection)
- **Setup + per-unit cost**: `T = overhead + (n * cost_per_byte)` (e.g., scanning, parsing)
- **Per-node or per-edge**: `T = f(nodes, edges)` (e.g., graph traversal)

If the measured shape doesn't match your expectation, something is wrong. The most common bug: code that should be O(1) is running O(n) because an optimization isn't firing.

Test at multiple input sizes to reveal the shape and distinguish algorithmic differences from constant-factor overhead:

| Pattern | Diagnosis | Action |
|---------|-----------|--------|
| Native O(1), Java O(n) | Missing optimization — algorithmic bug | Port the missing early-exit or short-circuit |
| Both O(n), same slope | Algorithmic parity | Profile hot paths if gap > 2x |
| Both O(n), Java 5-10x steeper | Missing fast path — algorithmic bug | Find the missing optimization |
| Both constant | Both optimized | Compare absolute cost, profile dispatch overhead |

**Key insight**: If the ratio changes with input size, the gap is algorithmic. If the ratio is constant across sizes, the gap is per-operation overhead.

## Red Flags

- **Either system > 5x faster than the other**: Something is wrong — either an algorithmic difference (missing optimization, wrong code path) or a bad benchmark. Investigate before trusting the number.
- **Java >10x faster than C++ at 10-20 ns**: Almost certainly dead code elimination — return values not consumed.
- **All input sizes report the same time**: JIT eliminated the work entirely.
- **Measured shape doesn't match expected shape**: O(1) code running O(n), or linear code showing constant time — an optimization is either missing or the benchmark is broken.
- **Massive variance between iterations**: GC pauses or background compilation — increase fork count.
- **Large gap that doesn't scale with input**: Per-call overhead, not algorithmic — profile the dispatch path.
- **"JVM overhead" cited as root cause**: Not an explanation. Name the specific overhead and quantify it.

## Amdahl's Law Estimation

Before optimizing a component, estimate what fraction of total time it represents:

```
speedup = 1 / (1 - fraction + fraction / component_speedup)
```

Example: Byte scanning is 50% of total time. A 5.3x scanning speedup yields: `1 / (0.5 + 0.5/5.3) = 1.81x` end-to-end. A 10x speedup on 5% of runtime = 1.05x end-to-end.

**Rule**: Before optimizing, estimate the maximum possible end-to-end impact. If the ceiling is < 1.1x, the optimization isn't worth pursuing.

## Micro-benchmarks vs End-to-End

Micro-benchmark performance does not always predict end-to-end performance. Always validate optimizations in the full context.

Real example:

| Implementation | Isolated byte scan | End-to-end |
|----------------|-------------------|------------|
| SWAR unroll-2 | 33.7 GB/s | **8.16 GB/s** (winner) |
| Vector API | **36.4 GB/s** (winner) | 7.72 GB/s |

The reversal happens because: code size affects I-cache, JIT optimizes differently in full context, register pressure from surrounding code changes.

Note: this was on ARM NEON (128-bit vectors), where SWAR processing 64 bits at a time is in the same ballpark as Vector API processing 128 bits. On x86 with AVX2 (256-bit) or AVX-512, the Vector API advantage in isolated throughput would be much larger — potentially enough to overcome the I-cache and register pressure costs and flip the end-to-end result the other way.

**Rule**: Micro-benchmarks guide exploration; end-to-end benchmarks make decisions.

## Understand Your Testing Platform

Performance results are platform-specific. What wins on one CPU may lose on another:

- **Vector width**: ARM NEON is 128-bit; x86 AVX2 is 256-bit; AVX-512 is 512-bit. Vectorized code scales with width, SWAR does not.
- **Branch prediction**: Different CPUs have different predictor designs. Branch-heavy code may perform differently.
- **Cache hierarchy**: L1/L2/L3 sizes and latencies vary. Working set size matters.
- **Memory subsystem**: Bandwidth and latency differ across platforms and NUMA topologies.

When possible, test on multiple CPU architectures before committing to an implementation strategy. An optimization validated only on one platform may regress on another.
