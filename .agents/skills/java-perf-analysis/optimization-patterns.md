# Optimization Patterns

## C2 Compiler: Getting the Best JIT Output

### Counted Loop Recognition

C2 applies critical optimizations only to **counted loops** — loops where it can prove the iteration count at entry. Important: C2 doesn't work on Java source code or even bytecode directly — it works on an internal IR with a control flow graph. So the source-level syntax (`for` vs `while`) doesn't matter per se. What matters is whether C2 can prove, in its IR, that the loop has a fixed trip count.

For C2 to recognize a counted loop, it needs:

1. **An `int` induction variable.** Must be 32-bit `int`, not `long`.
2. **A constant stride.** The increment must be a compile-time constant (e.g., `i++`, `i += 2`). A variable stride makes it a dependent loop and defeats the optimization.
3. **No modification of the counter on cold paths.** If any code path inside the loop modifies the counter in a way C2 can't prove is equivalent to the constant stride, the loop is not counted.

```java
// GOOD: Counted loop — int counter, constant stride, no surprises
for (int i = start; i < end; i++) {
    result = array[i];
}

// BAD: long induction variable — C2 won't recognize as counted
for (long i = start; i < end; i++) {
    result = array[(int) i];
}

// BAD: Cold path modifies the index — C2 can't prove constant stride
for (int p = start; p < end; p++) {
    int ns = transition[state + bytemap[bytes[p]]];
    if (ns == CACHE_MISS) {
        // Cold path adjusts p — breaks counted loop recognition
        p = handleCacheMiss(p);
    }
    state = ns;
}
```

The last case is the most insidious because it looks like a normal `for` loop. But the `p = handleCacheMiss(p)` on the cold path means C2 can't prove the loop always increments by 1, so it falls back to the unoptimized version — losing bounds check hoisting, safepoint elimination, and unrolling.

What counted loops enable:
- **Bounds check hoisting**: Array bounds verified once before the loop, not per iteration
- **Safepoint elimination**: No safepoint poll inside the inner loop (poll per chunk instead)
- **Loop unrolling**: 2x or 4x unrolling to overlap work across iterations
- **Range check elimination**: If `i < array.length` is proven by the loop bounds

**Pattern**: If you see bounds checks or safepoint polls in the hot inner loop assembly, verify that C2 recognizes it as counted. Check `-XX:+PrintCompilation` and `-XX:+TraceLoopOpts` if needed.

### Inner/Outer Loop Restructuring

The fix for the cold-path-modifies-counter problem (and for `long` ranges) is the same: split into an inner counted loop and an outer control loop. The inner loop is pure — constant stride, `int` counter, no cold-path surprises. The outer loop handles everything else: cold-path recovery, index adjustment, safepoint polls, range chunking.

```java
// Inner loop is a pure counted loop — gets all optimizations
// Outer loop handles cold paths, index adjustment, and re-entry
for (int p = start; p < end; ) {
    int chunkEnd = Math.min(p + CHUNK_SIZE, end);
    for (; p < chunkEnd; p++) {
        int ns = transition[state + bytemap[bytes[p]]];
        if (ns == CACHE_MISS) {
            break;  // fall out to outer loop — don't touch p here
        }
        state = ns;
    }
    // Outer loop: handle cache miss, adjust p if needed, safepoint poll
    if (needsCacheMissRecovery) {
        handleCacheMiss();
    }
}
```

This is exactly the pattern we used for the DFA search loop. The inner loop processes bytes at full speed with all C2 optimizations. When a rare event happens (cache miss, sentinel state), it breaks to the outer loop, which handles the slow path and re-enters the inner loop. The slow path code can be arbitrarily complex — it doesn't affect the inner loop's optimization.

### FreqInlineSize Threshold (325 Bytes)

C2 inlines frequently-called methods up to **325 bytes of bytecode** (`-XX:FreqInlineSize=325`). Exceeding this causes C2 to emit a full function call instead of inlining.

**Symptoms**: Adding a few lines to a hot method causes sudden performance regression. Assembly shows `bl`/`call` instead of inlined code.

**Diagnosis**:
```bash
javap -c -p MyClass.class | grep -c "bytecode_instruction"
# Or use: javap -verbose to see Code attribute length
```

**Fix strategies**:
1. Keep the hot method small — extract cold paths (error handling, rare branches) into separate methods
2. If a method is 310 bytes and you need to add 20 bytes, extract 30 bytes of cold code first
3. For diagnostics, use `-XX:CompileCommand=dontinline,*MyClass.coldHelper` to test inlining effects

### CompileCommand for Debugging

```bash
# Force compilation of specific method
-XX:CompileCommand=compileonly,*MyClass.hotMethod

# Print assembly for specific method
-XX:CompileCommand=print,*MyClass.hotMethod

# Prevent inlining (for isolation testing)
-XX:CompileCommand=dontinline,*MyClass.coldHelper
```

## Data Structure Patterns

### Flat Array vs Object Indirection

Object references on the heap create pointer chases. Each chase adds a serial load to the critical path. In most code this doesn't matter because you're load-bound and extra instructions are free. But when object references are on the **loop-carried dependency chain** of a tight loop, they become the bottleneck.

```java
// SLOW when on critical path: 3 serial loads (states[i] → .transitions → transitions[cls])
class State {
    int[] transitions;
}
State[] states;
int next = states[current].transitions[byteClass];

// FAST: 1 serial load
int[] flatTransitions;  // all states packed sequentially
int next = flatTransitions[currentOffset + byteClass];
```

**When to flatten**: Only when profiling shows the object indirection is on the critical path of a tight inner loop. For infrequent access, the object model is fine and provides better readability.

### Field Hoisting / Reference Caching

Cache frequently-accessed field references as local variables or instance fields to shorten load chains:

```java
// SLOWER: 3 loads each time (this.prog → prog.bytemap → bytemap[c])
int cls = this.prog.bytemap()[c];

// FASTER: 1 load (cachedBytemap already in register)
// Cache once at construction or before loop entry:
private final byte[] cachedBytemap = prog.bytemapArray();
int cls = cachedBytemap[c];
```

Again, this only matters on the critical path. Don't pre-optimize field access patterns in cold code.

### Byte Array Access: VarHandles, Not Unsafe

For reading multi-byte values (int, long, float, double) from byte arrays, use `VarHandle` (or `MethodHandle`). These are just as fast as `Unsafe` and are the supported API:

```java
private static final VarHandle LONG_HANDLE =
    MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

long word = (long) LONG_HANDLE.get(array, offset);
```

Do not use `sun.misc.Unsafe` — it is unnecessary and unsupported. VarHandles compile to the same machine instructions.

### SWAR (SIMD Within A Register)

Process 8 bytes at once using long arithmetic for byte-scanning operations:

```java
// Scan for a specific byte value in a byte array
long broadcastByte = targetByte * 0x0101010101010101L;
for (int i = 0; i + 7 < length; i += 8) {
    long word = (long) LONG_HANDLE.get(array, offset + i);
    long xor = word ^ broadcastByte;
    long hasZero = (xor - 0x0101010101010101L) & ~xor & 0x8080808080808080L;
    if (hasZero != 0) {
        return i + Long.numberOfTrailingZeros(hasZero) / 8;
    }
}
```

**When to use**: Byte scanning in arrays where you need to find a specific byte value. SWAR gives ~4-8x speedup over byte-at-a-time loops.

**vs Vector API**: SWAR often wins in end-to-end benchmarks despite slower isolated throughput, because it has smaller code size (better I-cache) and lower register pressure.

### Sentinel Values vs Object Identity

Replace object identity checks (`state == DEAD_STATE`) with integer sentinel checks:

```java
// SLOWER: Requires materializing 64-bit constant, potential cache miss on reference
if (state == DEAD_STATE) { ... }

// FASTER: Integer comparison, fits in immediate
static final int T_DEAD = -1;
if (stateOffset <= 0) { ... }  // all sentinels are non-positive
```

## Java-Specific Realities

**Bounds checks are free in practice.** In load-bound code (which most hot loops are), bounds checks execute in the shadow of memory loads. Even when they remain in the inner loop (not hoisted by C2), they consume decode bandwidth but add zero latency to the critical path. Don't waste effort trying to eliminate them.

**Compressed oops are a fact of life.** Default for heaps under 32 GB, they add a shift+add per object reference decompression. Like bounds checks, these are generally free in load-bound code. The exception is when an object reference changes every iteration on the critical path — then the decompression lands in the serial chain. The fix is flattening (see above), not fighting compressed oops.

## Test-First Optimization Workflow

Never optimize without a test that proves the optimization works.

### Test Types (Preferred Order)

**Type A: Direct Verification (strongly preferred)**

Assert on internal state or return values. Fast, deterministic, diagnostic.

```java
@Test
void testOptimizationTriggers() {
    // The optimization should return a specific sentinel
    int result = engine.search(input);
    assertEquals(SENTINEL_FULL_MATCH, result);
}
```

**Type B: Golden Tests**

Run the native implementation, capture output, assert Java matches exactly.

```java
@Test
void testMatchesNativeOutput() {
    // Expected from running: native_tool --pattern "abc" --input "xabcy"
    // match[0] = "abc", match[1] = ...
    String[] match = engine.match("xabcy");
    assertEquals("abc", match[0]);
}
```

**Type C: Generated Expected Results**

Run native code to compute expected values, hardcode in Java test.

**Type D: Complexity Tests (last resort)**

Measure ratio of small vs large input time. Only use when direct verification is not possible.

```java
@Test
void testIsConstantTime() {
    // Warm up
    for (int i = 0; i < 1000; i++) { engine.run(small); engine.run(large); }

    long smallTime = measure(() -> engine.run(small), 10000);
    long largeTime = measure(() -> engine.run(large), 10000);

    // O(1) means ratio close to 1, not proportional to size ratio
    assertTrue(ratio < 100);
}
```

Why last resort: flaky (timing varies), non-diagnostic (doesn't say WHY), slow (needs warmup).

### Process

1. **Write failing test first** — based on expected native behavior
2. **Verify test fails** — if it passes, the test isn't testing the right thing
3. **Investigate root cause** — trace through the code, don't guess
4. **Implement minimal fix**
5. **Verify test passes** — fix is NOT complete until the test passes
6. **Run full test suite** — no regressions
7. **Validate end-to-end** — benchmark confirms improvement in context

### Adding Test Hooks

When internal state needs verification, add minimal hooks:

1. **Package-private accessor**: `State getLastTerminalState()` — test in same package
2. **Sentinel return value**: Return a value that indicates which code path was taken
3. **Package-private counter**: `int optimizationTriggerCount` — check that it incremented

Choose the least invasive option that allows verification.
