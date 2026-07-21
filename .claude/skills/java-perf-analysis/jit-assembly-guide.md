# JIT Assembly Guide

## Installing hsdis

hsdis (HotSpot Disassembler) is required to view JIT-compiled assembly.

```bash
# ARM64 (Apple Silicon)
curl -O https://chriswhocodes.com/hsdis/hsdis-aarch64.dylib

# x86_64
curl -O https://chriswhocodes.com/hsdis/hsdis-amd64.so   # Linux
curl -O https://chriswhocodes.com/hsdis/hsdis-amd64.dylib # macOS

# Place in JDK lib/server/ or use -Djava.library.path=.
```

## Generating Assembly Output

JMH forks don't inherit JVM flags. Use a standalone driver class to trigger C2 compilation:

```java
public class AssemblyDump {
    public static void main(String[] args) {
        // Set up your hot loop's inputs
        MyClass obj = new MyClass(...);
        byte[] input = new byte[16_000_000];

        // Run enough iterations to trigger C2 (~10,000+)
        for (int i = 0; i < 200; i++) {
            obj.hotMethod(input);
        }
    }
}
```

Run with assembly output:

```bash
java -XX:+UnlockDiagnosticVMOptions \
     -Djava.library.path=. \
     -XX:+PrintAssembly \
     -XX:CompileCommand=print,*MyClass.hotMethod \
     -cp "$CP" \
     AssemblyDump > assembly.txt 2>&1
```

### Finding the Optimized Compilation

C2 may compile a method multiple times (OSR, deoptimization, recompilation). You want the final tier-4 non-OSR compilation:

```bash
# Find all C2 (tier 4) compilations, excluding OSR (marked with '@')
grep -n "Compiled method (c2).*MyClass::hotMethod" assembly.txt | grep -v "@"

# The last match is the most optimized version
# Extract it between its start line and the next "Compiled method" marker
```

## Helping C2 Generate Good Code

C2 is a powerful optimizer, but it needs help. The quality of the generated assembly depends heavily on how the Java code is structured. When C2's heuristics decide your code is too complex, it silently disables optimizations — and performance falls off a cliff with no warning.

### Counted Loops Are Critical

Counted loops are the single most important structural pattern for Java performance. A counted loop is one where C2 can prove, in its internal IR, that the loop has a fixed trip count. Note that C2 doesn't work on Java source code or even bytecode — it works on an IR with a control flow graph. So the source syntax (`for` vs `while`) doesn't matter per se. What matters is whether C2 can prove the loop has a constant stride with no modifications to the counter on any code path.

For C2 to recognize a counted loop:

1. **`int` induction variable** — must be 32-bit, not `long`
2. **Constant stride** — the increment must be a compile-time constant (`i++`, `i += 2`). A variable stride makes it a dependent loop.
3. **No counter modification on cold paths** — if any branch inside the loop modifies the counter (e.g., `p = handleCacheMiss(p)` on a rare error path), C2 can't prove constant stride and gives up. This is the most insidious case because the loop *looks* counted but isn't.

Without a counted loop, you lose:
- **Bounds check hoisting** — checks happen per iteration instead of once
- **Safepoint elimination** — polls happen per iteration instead of per chunk
- **Loop unrolling** — no interleaving of iterations

These compound: a non-counted loop can be 2-3x slower than the same logic in a counted loop, entirely due to lost optimizations. If a hot loop isn't performing as expected, the first thing to check is whether C2 recognizes it as counted.

The fix for all three problems is the same: use an inner/outer loop. The inner loop is a pure counted loop (`int` counter, constant stride, no cold paths). The outer loop handles everything else — `long` range chunking, cold-path recovery, index adjustment. See [Optimization Patterns](optimization-patterns.md) for the full pattern.

### Keep the Hot Path Small and Isolated

C2 makes better optimization decisions when the hot path is small. The optimizer has finite budgets for inlining, unrolling, and register allocation. When a method is too large or has too many branches, C2 starts making trade-offs — and the hot path suffers.

**Keep slow paths out of hot loops.** Error handling, rare branches, degenerate cases, cache-miss recovery — all of this should be extracted out of the inner loop. Options:

1. **Branch to a method call**: An `if` check inside the loop calls a separate method for the slow path. The fast path stays inline.
2. **Break out and re-enter**: The inner loop breaks when it hits the slow case, the outer loop handles it, then the inner loop resumes. This is what we did for DFA cache-miss handling — the inner counted loop processes bytes at full speed, breaks on a cache miss, the outer loop resolves it, and re-enters the inner loop.

The goal is to keep the inner loop's bytecode small and simple so C2 can apply its best optimizations. The slow path code can be as complex as it needs to be — it just can't live inside the hot loop.

### FreqInlineSize: The 325-Byte Cliff

C2 inlines frequently-called methods up to **325 bytes of bytecode** (`-XX:FreqInlineSize=325`). If a hot method exceeds this, C2 emits a full function call per invocation instead of inlining — a massive regression in tight loops.

This is a hard cliff, not a gradual degradation. At 324 bytes: fully inlined, fast. At 326 bytes: function call per iteration, slow.

**Real example from this project**: An attempt to inline `byteMap()` directly into `runStateOnByte` pushed the method from 317 to 336 bytes. C2 refused to inline it, emitting a full function call per byte processed. The fix was to keep `byteMap()` as a separate small method — `runStateOnByte` stayed at 317 bytes and remained inlined.

**How to check bytecode size**:
```bash
javap -c -p MyClass.class | grep -A1 "methodName"
# Look for "Code:" section, count bytes
```

**Fix**: Keep hot methods small. Extract cold paths into separate methods. Smaller methods give C2 more inlining budget to spend on what matters.

## Verifying JIT Optimization Decisions

Before diving into assembly annotation, verify that C2 is actually applying the optimizations you expect. If optimizations are being disabled, assembly analysis of the unoptimized code is a waste of time.

### Check Inlining Decisions

```bash
java -XX:+UnlockDiagnosticVMOptions \
     -XX:+PrintInlining \
     -cp "$CP" MyDriver 2>&1 | grep "mypackage"
```

Look for:
- `inline` — method was inlined (good)
- `too big` — method exceeded FreqInlineSize (bad — shrink it)
- `hot method too big` — similar, method is called frequently but too large to inline
- `callee is too large` — the callee method is too big
- `already compiled into a big method` — the caller got too large from other inlines

### Check Compilation Tier

```bash
java -XX:+PrintCompilation -cp "$CP" MyDriver 2>&1 | grep "mypackage"
```

Look for tier 4 compilations of your hot methods. If you only see tier 1-3, warmup is insufficient. If you see `made not entrant` or `made zombie`, the JIT is deoptimizing — investigate why (class loading, uncommon trap hit, etc.).

### Verify Key Optimizations in Assembly

Once you have the assembly, check for these patterns:

| Optimization | How to verify | If missing |
|---|---|---|
| Bounds check hoisting | No `cmp`/`b.cs` for array[i] inside inner loop | Restructure to counted `for` loop |
| Safepoint elimination | No `ldr [x28,#48]`/`ldr wzr,[x10]` inside inner loop | Restructure to counted `for` loop |
| Loop unrolling | Loop increment is `#2` or `#4`, interleaved iterations | Check loop is counted, method not too large |
| Method inlining | No `bl`/`call` instructions in hot path | Check FreqInlineSize, extract cold code |
| Pre-loop OOP decompression | `add xN, x27, xM, lsl #3` appears before loop, not inside | Cache array references before loop entry |

## Annotating the Inner Loop

### What to Look For

1. **The back-edge**: `b.lt loop_top` (ARM) or `jl loop_top` (x86) — marks the loop boundary
2. **Count instructions** between back-edge target and the branch — this is instructions per iteration
3. **Identify loads** (`ldr`, `ldrb` on ARM; `mov reg, [mem]` on x86) — these are potential critical path nodes
4. **Map each instruction back to Java source** — annotate with the Java expression it implements

### Annotation Template

```asm
; Inst  Assembly                         Java Expression              Chain    Critical?
; ----  --------                         ---------------              -----    ---------
  1     add   x10, x0, w19, sxtw        &array[i] (address calc)     C        no
  2     ldrb  w17, [x10, #16]           array[i] → value             C        YES
  3     cmp   w17, w15                   bounds check                 off-path no
  4     b.cs  uncommon_trap              (trap on OOB)                off-path no
  5     ldrb  w3, [x3, #16]             lookup[value] → result       B        YES
  ...
```

Chains: Label independent dependency chains (A, B, C...). Mark loads on the serial dependency chain as "YES" for critical. Everything else (bounds checks, sentinel tests, loop bookkeeping) runs in the shadow of the loads and is "no".

## C2 Compiler Patterns to Recognize

### Bounds Check Hoisting

C2 hoists bounds checks out of **counted loops** (loops with `for (int i = start; i < end; i++)`):

```
Good (hoisted):     No cmp/b.cs for array[i] inside inner loop
Bad (not hoisted):  cmp/b.cs pair before every array load
```

**Requirement**: The loop must be a counted `for` loop. A `while` loop with manual index increment often prevents hoisting. Restructure `while` to `for` if bounds checks appear in the hot path.

Note: Even when bounds checks are NOT hoisted, they typically don't affect throughput in load-bound loops — they execute in the shadow of the memory loads. Hoisting matters most when it enables other optimizations (safepoint elimination, unrolling) or when the loop is compute-bound rather than load-bound.

### Safepoint Elimination

C2 eliminates safepoint polls from counted loop inner bodies. Safepoints appear as:

```asm
; ARM64 safepoint poll
ldr  x10, [x28, #48]    ; load safepoint page address from Thread
ldr  wzr, [x10]          ; touch the page (triggers if page is protected)
```

If you see this pattern inside the inner loop, the loop is not recognized as counted. Restructure to a counted `for` loop.

### Loop Unrolling

C2 unrolls counted loops (typically 2x or 4x). Signs of unrolling:
- Loop increment is `add reg, reg, #2` (or #4) instead of `#1`
- Instructions for iteration N+1 interleaved with iteration N
- `bytes[p+1]` prefetch load appearing in the middle of the `bytes[p]` processing

Unrolling helps by allowing the OoO core to overlap work across iterations.

### OOP Decompression

With compressed oops (default for heaps < 32 GB), object references are stored as 32-bit values. Accessing a field requires decompression:

```asm
; Decompress compressed oop to full 64-bit address
add  x22, x27, x25, lsl #3    ; base + compressed_oop << 3
```

Where `x27` is the heap base register. This adds ~1 cycle per object reference access.

Compressed oops are a fact of life in modern Java — they cannot be avoided and generally don't affect throughput because the decompression executes in the shadow of loads. The case where it matters is when an object reference changes every iteration on the loop-carried critical path (e.g., `state.next` where `state` changes each iteration). The fix is to flatten the data structure (use int offsets into a flat array instead of object references), not to fight compressed oops.

**Pre-loop decompression**: When array references are loaded before the loop and kept in registers, decompression happens once (setup cost only). This is the common case and is essentially free.

**Per-iteration decompression**: If an object reference changes each iteration, decompression happens every iteration and lands on the critical path. This is the rare case worth optimizing.

### Uncommon Traps

Bounds checks, null checks, and type checks compile to:

```asm
cmp   w17, w15           ; compare index against array length
b.cs  0x...              ; branch to uncommon trap (never taken in normal execution)
```

The branch is predicted not-taken. The `cmp` and `b.cs` consume decode bandwidth but don't add latency on the critical path (assuming correct prediction). In load-bound loops, these are effectively free.

## Assembly Analysis Workflow

1. **Verify JIT optimization decisions** — check inlining (`-XX:+PrintInlining`) and compilation tier (`-XX:+PrintCompilation`) before looking at assembly. If methods aren't being inlined or optimizations are disabled, fix that first.
2. **Generate assembly** for the hot method using a standalone driver
3. **Find the C2 tier-4 non-OSR compilation** (last match in output)
4. **Verify key optimizations** — bounds check hoisting, safepoint elimination, unrolling, inlining (see table above)
5. **Locate the inner loop** by finding the back-edge branch
6. **Count instructions per iteration** (per byte, per element, etc.)
7. **Annotate each instruction** with the Java expression it implements
8. **Identify the critical path** — the serial load chain (the instructions come free with the loads)
9. **Classify non-critical instructions** — bounds checks, match tests, loop bookkeeping (these run in shadow)
10. **Verify**: does `critical_path_cycles` match `measured_ns * GHz`?
11. **Compare with baseline** (C++ or previous version) — where are the extra *loads*, not just extra instructions?
