# DFA Hot Loop Assembly Analysis

This is a historical investigation record, not current benchmark qualification.
Raw machine-specific assembly dumps are intentionally not retained; regenerate
them with the maintained driver when validating a current JDK and machine.

**Platform:** Apple M-series (aarch64)
**JDK:** Temurin 25
**Pattern:** `[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$` (HARD pattern)
**Input:** 16 MB

```
Benchmark                         (textSize)  Mode  Cnt         Score        Error  Units
BenchmarkRe2Search.searchHardDfa    16777216  avgt   15  35036185.319 ± 110528.312  ns/op
```

**Per-byte throughput at 16M:** 35.0 ms / 16.8 MB = **2.09 ns/byte**

## Addendum: Easy1 Prefix-Accel (Run 010)

The analysis below focuses on the generic `searchForward` hot loop (Hard/Parens shape).
For Easy1, current HEAD uses a dedicated single-byte prefix path:

- `Dfa.searchForwardPrefixAccelSingleByte` (fused `int[]` transitions)
- `Prog.prefixAccelSingleByteNoFoldcase` (prefix scan helper)

Latest 16M JMH (3 forks, 10x1s warmup, 5x1s measurement):
- `searchEasy0Dfa`: `2,086,718 ns/op`
- `searchEasy1Dfa`: `2,365,278 ns/op`

## Critical Path in Java

On an out-of-order core, the bottleneck is the serial dependent load chain (~9 cycles), not instruction count (~18.5 instructions per byte after 2x unrolling). This section traces that chain through the Java source.

### The Inner Loop

The hot loop is `searchForward` (`Dfa.java:168-183`):

```java
for (; p < textEnd; p++) {
    ns = trans[s + (bmap[bytes[p] & 0xFF] & 0xFF)];
    if (ns <= 0) break;
    s = ns;
    if ((s & T_MATCH_BIT) != 0) { ... }
}
```

No more `runStateOnByte` — the fast path is a single flat array lookup. `trans` is the flat `int[]` transition table, `bmap` is the cached bytemap `byte[]`, both loaded once before the loop.

### Three Dependency Chains

Each iteration has three independent chains that merge at the end:

```
Chain A (state → transition):       trans[s + cls]  → new state offset
Chain B (bytemap → class):          bmap[c]         → cls
Chain C (position → byte):          bytes[p]        → c

Merge: chains B+C produce cls, chain A uses s (from previous iteration) + cls.
       trans[s + cls] produces the new state offset → feeds next iteration.
```

### Critical Path Per Byte (Java → Loads)

```
bytes[p]            → array load                    (~3 cycles)  [fast, overlaps with prev iter]
  bmap[c]           → array load (c ready)           (~3 cycles)
    = cls
trans[s + cls]      → array load (s from prev iter)  (~3 cycles)
  = new state offset → feeds trans[s + cls] in next iteration
```

The longest serial chain: `bytes[p] → bmap[c] → trans[s + cls]` = **3 dependent loads ~ 9 cycles**. With 2x unrolling, `bytes[p]` for the next iteration is prefetched during the current iteration, effectively reducing the serial chain to 2 loads in steady state.

### Why Java Has More Loads Than C++

In C++ these are also **3 loads** (`bytes[p]`, `bytemap[c]`, `next[cls]`) but with less overhead:
- `bytemap` is a direct `uint8_t*` pointer — one load to get the class
- `next` is a flat array pointer — one load to get the next state
- No bounds checks (raw pointer arithmetic)

| | C++ | Java |
|---|-----|------|
| bytes[p] | 1 load | 1 load |
| bytemap lookup | 1 load (direct pointer) | 1 load (array element via cached decompressed ref) |
| transition lookup | 1 load (flat array) | 1 load (flat `int[]` via cached decompressed ref) |
| **Total serial loads** | **3** | **3** |
| **Estimated critical path** | **~9 cycles** | **~9 cycles** |

The critical path length is now identical. The remaining ~1.1x gap comes from bounds check instructions and OOP decompression setup that consume decode bandwidth, not from extra serial loads.

## Cycle-by-Cycle Timeline

How the three chains execute on an out-of-order core. Cycles assume L1 hit latency of 3 cycles.

| Cycle | Java Expression | Assembly | Chain |
|-------|----------------|----------|-------|
| 0 | `bytes[p]` | `ldrb w17, [x10, #16]` | C |
| 0 | (prev state `s` ready) | — | A (from prev iter) |
| 3 | `bmap[c]` addr calc | `add x3, x22, w17, sxtw` | B |
| 3 | bmap bounds check | `cmp w17, w15` / `b.cs` | off-path |
| 4 | `bmap[c]` = cls | `ldrb w3, [x3, #16]` | **B→merge** |
| 5 | `s + cls` compute | `add w10, w16, w3` | merge |
| 5 | trans addr calc | `add x16, x12, x17, uxtx #2` | merge |
| 5 | trans bounds check | `cmp w10, w14` / `b.cs` | off-path |
| 6 | `trans[s + cls]` = ns | `ldr w29, [x26, #16]` | **result** |
| 7 | `ns > 0` check | `cmp w29, #0x0` / `b.le` | off-path |
| 7 | T_MATCH_BIT test | `and w10, w29, #0x40000000` | off-path |
| 7 | `cbnz` match handler | `cbnz w10, match_handler` | off-path |

**Total: ~7-9 cycles on the critical path.** With 2x unrolling, the JIT overlaps `bytes[p+1]` load with the current iteration, enabling further pipelining.

All bounds checks, sentinel guards, match tests, and loop bookkeeping execute in shadow of the load chain.

## Annotated Inner Loop Assembly

~18.5 instructions per byte (37 instructions per 2 bytes, 2x unrolled). The JIT unrolls the for-loop 2x and processes 2000-byte chunks with a safepoint poll between chunks.

### One Iteration (first of unrolled pair)

```asm
; Input: x0 = bytes base, w19 = p, x22 = bmap (decompressed), x12 = trans (decompressed)
;        w16 = current state offset (s), w15 = bmap.length, w14 = trans.length
;
; Inst  Assembly                         Java Expression              Chain    Critical?
; ----  --------                         ---------------              -----    ---------
  1     add   x10, x0, w19, sxtw        &bytes[p] (address calc)     C        no (fast)
  2     ldrb  w17, [x10, #16]           bytes[p] → c                 C        YES
  3     add   x3, x22, w17, sxtw        &bmap[c] (address calc)      B        no (fast)
  4     cmp   w17, w15                   c < bmap.length              off-path no
  5     b.cs  uncommon_trap              (bounds check trap)          off-path no
  6     ldrb  w3, [x3, #16]             bmap[c] → cls                B        YES
  7     sxtw  x17, w3                    (sign extend cls)            merge    no (fast)
  8     add   x17, x17, w16, sxtw       s + cls (64-bit)             merge    YES
  9     add   x26, x12, x17, uxtx #2    &trans[s+cls] (word scale)   merge    no (fast)
  10    ldrb  w17, [x10, #17]           bytes[p+1] → c_next          C        no (prefetch)
  11    add   w10, w16, w3              s + cls (32-bit bounds check) off-path no
  12    cmp   w10, w14                   (s+cls) < trans.length       off-path no
  13    b.cs  uncommon_trap              (bounds check trap)          off-path no
  14    ldr   w29, [x26, #16]           trans[s+cls] → ns             result   YES
  15    cmp   w29, #0x0                  ns > 0 ?                     off-path no
  16    b.le  exit_or_sentinel           (break if ns <= 0)           off-path no
  17    and   w10, w29, #0x40000000      T_MATCH_BIT test             off-path no
  18    cbnz  w10, match_handler         (branch if match)            off-path no
```

**Second iteration** (instructions 19-37) is identical but uses the prefetched `c_next` from instruction 10 and `w29` as the new state offset. The unrolled pair ends with:

```asm
  35    add   w19, w19, #0x2             p += 2
  36    cmp   w19, w2                    p < chunk_end
  37    b.lt  loop_top                   (back to instruction 1)
```

**Critical path instructions per iteration:** 2 → 6 → 8 → 14 (4 instructions, of which 3 are dependent loads). The other 14 instructions execute in parallel.

## Why Instruction Count and Load Count Don't Predict Throughput

The loop has gone through five phases. Phases 1-4 reduced instructions by 36% and critical path loads by 20% with zero throughput change. Phase 5 (flat transition table) finally broke through the bottleneck:

| Phase | Instructions | Critical loads | Throughput |
|-------|-------------|---------------|------------|
| Original | 55 | 5 | 3.44 ns/byte |
| Negative ninst | 49 | 5 | 3.44 ns/byte |
| For-loop restructure | 38 | 5 | 3.42 ns/byte |
| Bytemap hoist | 35 | 4 | 3.43 ns/byte |
| **Flat transition table** | **~18.5** | **3** | **2.09 ns/byte** |

Phases 1-4 showed no throughput change despite significant instruction and load reduction. The bottleneck was the `State` object pointer chase: loading `s.next` required an OOP decompression followed by an array indirection, creating a 3-load serial chain just for the state transition. Phase 5 eliminated this entirely by replacing per-state `State` objects with a flat `int[]` transition table where state identity is an offset, not an object reference. This removed the `State.next` field load, its OOP decompression, and the `next[]` array header load — collapsing the state transition from 3 serial loads to 1.

At ~3.2 GHz, 2.09 ns/byte = ~6.7 cycles/byte. With 3 serial loads × 3 cycles = 9 cycles theoretical and 2x unrolling enabling iteration overlap, this throughput is consistent with the critical path analysis.

## Remaining Structural Overhead

Overhead that cannot be eliminated through Java-level optimization:

### OOP decompression (setup cost only)

The `bytemap` and `transitions` array references are decompressed from compressed oops once before the loop (`add x22, x27, x25, lsl #3` and `add x12, x27, x21, lsl #3`). Within the inner loop, no OOP decompression occurs — all array accesses use the pre-decompressed pointers in registers.

This is a major improvement from the previous architecture where 4 OOP decompressions occurred per byte (for `bytemap`, `State`, `next[]`, and the new `State`).

### Bounds checks

Bounds checks for `bmap[c]` and `trans[s+cls]` remain in the loop (2 `cmp` + 2 `b.cs` = 4 instructions per byte). These are not on the critical path but consume decode bandwidth. The `bytes[p]` bounds check is hoisted out of the inner loop by C2.

### Bytemap array indirection

C++ uses an inline `uint8_t bytemap_[256]` in the DFA struct (direct pointer, no header). Java needs a `byte[]` heap object with a 16-byte header, requiring the `+16` offset on every access. The reference itself is cached in a register, but the array header offset adds one cycle of address computation that C++ avoids.

## Comparison with C++

| Operation | C++ | Java (bytemap hoist) | Java (flat transition table) |
|-----------|-----|----------------------|------------------------------|
| Load byte | 1 | 1 | 1 |
| p++ | 1 | 1 | 0.5 (amortized, 2x unrolled) |
| Bytemap lookup | 2 | 3 | 3 |
| State transition | 2 | 5 | 3 |
| NULL/cache check | 2 | 1 | 1 |
| Sentinel checks | 2 | 8 | 2 |
| isMatch check | 2 | 1 | 1 |
| bytes[p] bounds check | 0 | 0 | 0 (hoisted) |
| Safepoint poll | 0 | 0 | 0 (per-chunk only) |
| Loop condition + bookkeeping | 1 | 4 | 2.5 (amortized) |
| Compressed OOP decompress | 0 | 4 | 0 (pre-loop only) |
| Invariant field re-loads | 0 | 1 | 0 |
| Register shuffling | 0 | 2 | 2 |
| Other overhead | 0 | 4 | 3 |
| **Total instructions** | **~13** | **~35** | **~18.5** |
| **Critical path loads** | **~3** | **~4** | **~3** |
| **Measured throughput** | **~1.88 ns/byte** | **3.43 ns/byte** | **2.09 ns/byte** |

The flat transition table reduced instruction count by 47% (35 → 18.5) and matched C++'s critical path load count (3). The remaining ~1.1x gap is accounted for by bounds check instructions and the bytemap array header offset.

## Optimization History

### Phase 1: Negative ninst sentinels

Replaced reference equality checks (`s == DEAD`, `s == FULL_MATCH`) with integer field comparisons (`s.ninst == -1`, `s.ninst == -2`). Eliminated all `mov`/`movk` constant materialization sequences. **Reduced instruction count (55 → 49) but not critical path.**

### Phase 2: Inner/outer loop restructuring

Converted `while` loops to nested inner `for` / outer `while`. C2 recognized the counted loop pattern and:
- **Hoisted `bytes[p]` bounds check** out of the loop (2 instructions/byte saved)
- **Eliminated safepoint poll** from inner loop (2 instructions/byte saved)
- **Removed invariant `q0` load** that was only needed for cache-miss path (4 instructions/byte saved)
- **Reduced register shuffling** (3 fewer `mov` instructions/byte)

| Metric | while-loop | for-loop | Change |
|--------|-----------|----------|--------|
| Instructions per byte | 49 | 38 | **-11 (-22%)** |
| `bytes[p]` bounds check | per iteration | hoisted | **eliminated** |
| Safepoint poll | per iteration | none | **eliminated** |
| Throughput (16M) | 3.44 ns/byte | 3.42 ns/byte | ~0% |

### Phase 3: Bytemap field hoisting

Cached the `prog.bytemapArray()` reference and `prog.bytemapRange()` value as fields on `DfaInstance`, and updated `byteMap()` to use them instead of going through `this.prog`. This shortens the bytemap lookup chain from `dfa.prog → prog.bytemap → bytemap[c]` (3 loads) to `dfa.bytemap → bytemap[c]` (2 loads).

**Key constraint discovered:** `runStateOnByte` bytecode must stay ≤ 325 bytes (`FreqInlineSize` default) for C2 to inline it. The original attempt to inline `byteMap()` directly into `runStateOnByte` pushed it from 317 → 336 bytes, causing C2 to refuse inlining and emit a full function call per byte — a massive regression. Keeping `byteMap()` as a separate method keeps `runStateOnByte` at 317 bytes, within the threshold.

| Metric | for-loop (phase 2) | bytemap hoist (phase 3) | Change |
|--------|-------------------|------------------------|--------|
| Instructions per byte | 38 | 35 | **-3 (-8%)** |
| Critical path loads | 5 | 4 | **-1** |
| Bytemap chain loads | 3 (`dfa.prog → prog.bytemap → bytemap[c]`) | 2 (`dfa.bytemap → bytemap[c]`) | **-1** |
| OOP decompressions | 5 | 4 | **-1** |
| Throughput (16M) | 3.42 ns/byte | 3.43 ns/byte | ~0% |

### Phase 4: Flat transition table

Replaced per-state `State` objects with `StateData` records + flat `int[] transitions` array. State identity is now an `int` offset into the transitions array, not an object reference. The transitions for all states are packed into a single flat array, matching C++'s layout.

Key changes:
- **Eliminated `State.next` field load** — `trans` is a local `int[]` reference, not a field on a heap object
- **Eliminated `next[]` OOP decompression** — no separate `next[]` array per state
- **Eliminated `State` OOP decompression** — state is an `int` offset, not a compressed oop
- **C2 unrolls 2x** — processes 2 bytes per loop iteration, prefetching `bytes[p+1]` during iteration for `bytes[p]`
- **Fixed cache exhaustion bug** — dead code in `StateSaver` path prevented proper state restoration after cache reset
- **Added thrashing detection** — DFA returns `SEARCH_FAILED` if cache resets don't make forward progress

| Metric | bytemap hoist (phase 3) | flat transition table (phase 4) | Change |
|--------|------------------------|---------------------------------|--------|
| Instructions per byte | 35 | ~18.5 | **-16.5 (-47%)** |
| Critical path loads | 4 | 3 | **-1** |
| OOP decompressions in loop | 4 | 0 | **-4** |
| Throughput (16M) | 3.43 ns/byte | 2.09 ns/byte | **39% faster** |

**Why this phase finally improved throughput:** Phases 1-3 reduced instruction count and removed one serial load, but the `State` object pointer chase remained the true bottleneck. The OoO core could not overlap the `s.next → decompress → next[cls] → decompress → new State` chain across iterations because each iteration depended on the previous iteration's state object. By replacing object references with integer offsets into a flat array, the state-to-state dependency became a single array load (`trans[s + cls]`), which the core can pipeline much more effectively.

## How to Reproduce

### 1. Install hsdis

```bash
curl -O https://chriswhocodes.com/hsdis/hsdis-aarch64.dylib
# Either copy to JDK's lib/server/ or use -Djava.library.path=.
```

### 2. Compile the maintained driver

```bash
./mvnw test-compile -q
CP="target/test-classes:target/classes:$(./mvnw -q dependency:build-classpath -DincludeScope=test -Dmdep.outputFile=/dev/stdout 2>/dev/null)"
```

`io.airlift.slice.re2.DfaAssemblyDump` supplies the hard pattern, deterministic
input, warmup, and current `Dfa.search` invocation.

### 3. Run with assembly output

```bash
java -XX:+UnlockDiagnosticVMOptions \
     -Djava.library.path=. \
     -XX:+PrintAssembly \
     -XX:CompileCommand=print,*Dfa.searchForward \
     --add-modules jdk.incubator.vector \
     -cp "$CP" \
     io.airlift.slice.re2.DfaAssemblyDump \
     > searchForward-assembly.txt 2>&1
```

### 4. Find the optimized compilation

```bash
# Find all C2 (tier 4) compilations (non-OSR = no '@')
grep -n "Compiled method (c2).*Dfa::searchForward" searchForward-assembly.txt | grep -v "@"

# Extract the last one (most optimized)
sed -n '<START_LINE>,<END_LINE>p' searchForward-assembly.txt > searchForward-c2.txt
```

### 5. Analyze the hot loop

Look for:
- `b.lt loop_top` — the loop back-edge (2x unrolled inner loop)
- 37 instructions between back-edge target and `b.lt` = one unrolled pair (2 bytes)
- `ldrb` for `bytes[p]` — should have NO preceding `cmp`/`b.cs` in inner loop (bounds check hoisted)
- NO `ldr [x28, #48]` + `ldr wzr` pattern in inner loop (safepoint per-chunk only)
- `cmp w29, #0x0` / `b.le` — sentinel/break check on transition result
- `and w10, w29, #0x40000000` — T_MATCH_BIT test
- `ldrb w17, [x10, #17]` — prefetch of `bytes[p+1]` in first iteration of unrolled pair

## Files Referenced

- Search implementation: `src/main/java/io/airlift/slice/re2/prog/Dfa.java`
- Reproduction driver: `src/test/java/io/airlift/slice/re2/DfaAssemblyDump.java`
- Benchmark inputs: `src/test/java/io/airlift/slice/re2/Re2BenchmarkRunner.java`
- Generated output: `searchForward-assembly.txt` (not checked in)
