# Critical Path Analysis on Out-of-Order Cores

## Core Principle

On modern out-of-order (OoO) CPUs, **the instructions come free with the loads.** The bottleneck is the longest serial dependency chain — the critical path — which is almost always a chain of dependent memory loads. All other instructions (bounds checks, comparisons, branches, ALU ops) execute in the shadow of this chain.

## Why This Matters

A loop with 55 instructions per iteration can have the same throughput as one with 35 instructions if both have the same critical path length. Reducing instruction count without shortening the critical path yields zero throughput improvement.

Real example: An inner loop went through four optimization phases that reduced instructions from 55 to 35 (36% reduction) with **zero throughput change**. Only when the critical path was shortened (from 4 serial loads to 3) did throughput actually improve — by 39%.

## How to Identify the Critical Path

### Step 1: Find the Loop-Carried Dependency

The critical path is the chain of operations where **each operation depends on the result of the previous one**, and the chain spans from one iteration to the next.

Ask: "What value from iteration N is needed to start iteration N+1?"

For a state machine (DFA, parser, interpreter), this is typically the **state variable**:
```
iteration N computes: next_state = transition[current_state + input_class]
iteration N+1 needs:  current_state (= next_state from N)
```

### Step 2: Trace the Dependent Load Chain

Map each Java expression in the dependency chain to its hardware cost:

```
Expression                     Hardware operation         Latency
─────────────────────────────  ─────────────────────────  ───────
bytes[p]                       L1 array load              ~3 cycles
bytemap[c]                     L1 array load (c ready)    ~3 cycles
transitions[state + cls]       L1 array load              ~3 cycles
                                                    Total: ~9 cycles
```

Key latencies (modern ARM/x86, L1 hit):
- **Array load**: 3-4 cycles
- **Pointer chase** (object field load): 3-4 cycles
- **OOP decompression** (`add reg, base, compressed, lsl #3`): ~1 cycle
- **Integer ALU** (add, shift, compare): ~1 cycle
- **Branch** (correctly predicted): ~1 cycle

### Step 3: Identify What Runs in Shadow

Everything NOT on the critical path executes in parallel. On a 4-6 wide OoO core, this includes:
- Bounds checks (`cmp` + `b.cs` to uncommon trap)
- Loop counter increment and comparison
- Match/sentinel checks
- Safepoint polls (if not hoisted)
- Register shuffling

These consume decode bandwidth and execution ports but don't add to latency if the core can schedule them alongside the critical loads.

### Step 4: Verify Against Measured Throughput

Convert measured throughput to cycles and compare with the critical path prediction:

```
measured_ns_per_unit * GHz = cycles_per_unit
```

Example: 2.09 ns/byte at ~3.2 GHz = ~6.7 cycles/byte. With a 3-load critical path (9 cycles) and 2x loop unrolling enabling iteration overlap, this is consistent.

If measured cycles >> predicted cycles, there's a hidden dependency or the analysis is wrong. If measured cycles << predicted cycles, the benchmark may be broken.

## Object Indirection and Flat Data Structures

In Java, a `State` object with an `int[] transitions` field requires two pointer chases to reach the transition data: one to follow the `transitions` reference, one to access the array element. Normal C++ code has the same structure — a `State` struct with a `std::vector<int>` or `int*` member has an identical load chain (struct → pointer → data).

**Most C++ code has the same indirection as Java.** The load count only differs when the C++ author has deliberately inlined the array into the struct itself — using a flexible array member, a fixed-size array, or a flat arena. This is a manual optimization that most C++ codebases don't do. For example, Google's RE2 library stores the `next` transition array directly inside the `State` allocation as a trailing flexible array, turning `state→next[cls]` into a single offset load. This is not a language difference — it's a data structure design choice that happens to be easier to express in C/C++ than in Java (Java has no equivalent of flexible array members or inline arrays, at least until Project Valhalla).

When profiling shows that object indirection is on the critical path of a tight loop, flatten the data structure:

```
Before: trans = states[stateIndex].transitionArray[byteClass]
  loads: states[stateIndex] → .transitionArray → array[byteClass] = 3 serial loads

After:  trans = flatArray[stateOffset + byteClass]
  loads: flatArray[stateOffset + byteClass] = 1 load
```

Both Java and C++ benefit from this transformation. In this case, flattening all state transitions into a single `int[]` indexed by `stateOffset + byteClass` gave Java effectively the same data layout as RE2's inlined flexible array — and the same performance.

C++ has a handful of specialized techniques that Java doesn't have direct equivalents for: flexible array members, placement new into arenas, `__attribute__((packed))`, SIMD intrinsics via inline assembly. These are exceedingly rare in normal C++ code, but they show up in extremely optimized libraries written by very skilled engineers (like Google's RE2). When you encounter them, don't panic — there is usually a creative workaround that achieves the same effective data layout. But it may require a bigger step than a straightforward port (e.g., redesigning the state representation rather than just translating the struct to a class).

The key is to understand **what the C++ technique actually achieves** at the hardware level (fewer loads? better cache locality? wider SIMD?), then find a Java-idiomatic way to get the same result. The answer is almost never "Java can't do this" — it's "Java does this differently."

For most code, the standard object model is fine in both languages. Only invest in flattening when profiling shows indirection on the critical path of a tight inner loop.

## Think of the Loop as a Continuous Stream

A critical insight: **the loop body is not a series of isolated iterations — it is a continuous stream of instructions where loads from one iteration overlap with loads from the next.** The OoO core doesn't see loop boundaries. It sees a stream of dependent and independent operations and schedules them as aggressively as possible.

This means a load that appears at the "beginning" of the loop body can start executing during the "end" of the previous iteration, as long as its inputs are ready. The effective critical path per iteration is not the total of all serial loads within one iteration — it is only the loads that form a chain where each depends on the result of the previous one **including the wrap-around**.

## Worked Example: DFA Inner Loop

The DFA inner loop has three loads per byte:

```java
for (; p < textEnd; p++) {
    ns = transitions[s + (bytemap[bytes[p] & 0xFF] & 0xFF)];
    s = ns;
}
```

Naively, this looks like 3 dependent loads × 3 cycles = 9 cycles per byte. But the actual measured throughput is ~2 ns/byte (~6.7 cycles). The naive count is wrong because the loads don't all depend on each other in a straight line.

### The Three Loads

```
Load 1: bytes[p]              → c       (depends on: p, which is just an increment — always ready)
Load 2: bytemap[c]            → cls     (depends on: Load 1's result)
Load 3: transitions[s + cls]  → new_s   (depends on: Load 2's result AND previous iteration's Load 3)
```

### Why It's 2 Dependent Loads, Not 3

The key: **Load 1 does not depend on the previous iteration's state.** It only depends on `p`, which is a simple integer increment available immediately. So Load 1 of iteration N+1 can start executing while Load 3 of iteration N is still in flight:

```
Iteration N:                          Iteration N+1:

  Load 1: bytes[p]  ───→ c             Load 1: bytes[p+1]  ───→ c'     ← starts immediately!
                          │                                      │
  Load 2: bytemap[c] ──→ cls           Load 2: bytemap[c'] ──→ cls'
                          │                                      │
  Load 3: trans[s+cls] → new_s ──────→ Load 3: trans[new_s+cls'] → ...
          ↑                                     ↑
          s from prev iteration                 new_s from this iteration
```

By the time Load 3 of iteration N produces `new_s`, Load 1 and Load 2 of iteration N+1 have already completed (they started 6 cycles ago). Load 3 of iteration N+1 needs both `new_s` (just arrived) and `cls'` (already waiting). So the only serial dependency that spans iterations is **Load 3 → Load 3**: one load, ~3 cycles.

In steady state, the effective critical path per byte is:
- Load 2 + Load 3 = **2 dependent loads, ~6 cycles** (Load 1 overlaps with previous iteration)
- At ~3.2 GHz: 6 cycles / 3.2 = ~1.9 ns/byte theoretical
- Measured: ~2.09 ns/byte — consistent

With 2x loop unrolling, the JIT makes this overlap explicit by interleaving instructions from consecutive iterations, giving the OoO core even more scheduling freedom.

### The General Principle

When analyzing any loop, don't just count loads within one iteration. Draw the dependency graph **across iteration boundaries**:

1. Which loads depend on results from the **same iteration**? (These are serial within the iteration)
2. Which loads depend on results from the **previous iteration**? (These form the loop-carried chain)
3. Which loads have inputs that are **always ready** (simple increments, constants, values computed from independent chains)? (These overlap freely with the previous iteration)

The true critical path per iteration is the longest chain of loads where each load's input depends on the previous load's output, **wrapping around the loop boundary**. Loads with independent inputs execute in parallel with that chain, even if they appear "before" dependent loads in the source code.

## Decision Framework

| Situation | Action |
|-----------|--------|
| Throughput doesn't improve despite fewer instructions | Critical path unchanged — find and shorten the dependency chain |
| Throughput matches cycle prediction from load chain | At the hardware limit — need algorithmic change to improve further |
| Measured cycles >> predicted cycles | Hidden dependency, cache miss, or benchmark artifact |
| Multiple loads on critical path through objects | Flatten to primitive arrays to reduce serial loads |
| Bounds checks in loop | Check if on critical path (usually not) — focus on loads instead |
