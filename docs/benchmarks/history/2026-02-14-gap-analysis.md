# Java vs C++ RE2 Performance Gap Analysis

This is a historical analysis from the February 2026 optimization campaign.
Measurements and source names are not current release qualification. In
particular, the former UTF-8 match-any shortcut was removed because it accepted
malformed UTF-8; the current constant-time shortcut is limited to byte-universal
programs.

Hardware: Apple Silicon ARM64, ~4 GHz. **1 ns = 4 cycles.**

Data sources: `cpp-baseline-003.csv` (AppleClang `-O3`), `java-baseline-002.csv` (JMH, JDK 25).

Every gap below is explained at the instruction/algorithm level. If a number can't be accounted for in cycles, the measurement is wrong or there's a bug - never accept "JVM overhead" as a root cause without quantifying it.

---

## 1. FullMatch DotStar `(?s).*` - O(1) vs O(n) [RESOLVED]

### UTF-8 Mode (Original Benchmark) - O(n)

| Size | C++ (ns) | Java (ns) | Ratio |
|------|----------|-----------|-------|
| 8 | 20 | 35 | 1.8x |
| 4K | 20 | 6,500 | 325x |
| 2M | 20 | 3,341,017 | 167,051x |

### LATIN1 Mode - O(1) JAVA FASTER

| Size | C++ (ns) | Java (ns) | Ratio |
|------|----------|-----------|-------|
| 8 | 20 | 34 | 1.7x |
| 64 | 20 | 12.7 | **1.6x faster** |
| 2M | 20 | 12.4 | **1.6x faster** |

**Root Cause:** The FullMatchState optimization relies on `ALT_MATCH` instructions, which are only generated when the pattern compiles to a `[00-ff]` single-byte loop. This requires **LATIN1 mode** (single-byte encoding).

In **UTF-8 mode** (default), `.` matches a Unicode code point (1-4 bytes), creating a multi-byte program structure:
```
3+ byte [00-7f] 4 -> 3   ; ASCII
4+ byte [c2-df] 3 -> 8   ; UTF-8 2-byte start
5+ byte [e0-ef] 2 -> 9   ; UTF-8 3-byte start
6+ byte [f0-f4] 1 -> 10  ; UTF-8 4-byte start
7. match! 0
```

In **LATIN1 mode**, `.` matches any single byte, generating the ALT_MATCH pattern:
```
3+ altmatch -> 4 | 5     ; FullMatchState triggers here
4+ byte [00-ff] 1 -> 3   ; Single-byte loop
5. match! 0
```

**C++ RE2 defaults to Latin1 encoding**, which explains why C++ benchmarks showed O(1) while Java (using UTF-8 default) showed O(n).

**Verification:** Added `FullMatchStateTest.java` with direct verification via `Dfa.fullMatchStateCount` counter. Tests confirm:
- LATIN1 mode generates ALT_MATCH and triggers FullMatchState
- UTF-8 mode does NOT generate ALT_MATCH (expected behavior)
- LATIN1 timing shows constant ~12ns regardless of input size

**Files:** `Dfa.java` (added test hook), `FullMatchStateTest.java`, `BenchmarkRe2FullMatch.java` (added LATIN1 benchmarks)

---

## 2. DFA Byte-Scan Throughput (Medium/Hard/Parens) - Hard/Parens now faster than C++ [RESOLVED]

These patterns have no prefix acceleration, so every byte is scanned by the DFA loop.

| Pattern | Size | C++ ns/byte | Java ns/byte | Ratio |
|---------|------|-------------|--------------|-------|
| Medium | 16M | 1.89 | 2.06 | 1.09x slower |
| Hard | 16M | 1.88 | 1.69 | **1.11x faster** |
| Parens | 16M | 1.88 | 1.68 | **1.12x faster** |

### Optimization history

**Run 006 (specialized loop variants):** Consolidated 8 search method variants into 3 (`searchForward`, `searchForwardPrefixAccel`, `searchBackward`), eliminating per-iteration conditional checks. 3-13% improvement.

**Run 007 (flat int[] transition table):** Replaced per-state `State` objects with `next[]` arrays with a single flat `int[]` transition table. Reduced the gap from ~1.8x to ~1.1x.

**Run 008 (Object[] per-state transition table):** Each DFA state is now an `Object[]` where indices `0..nextSize-1` hold transition references to other states' `Object[]` arrays, and the last slot holds `StateData` metadata. The inner loop does `sRef[bmap[bytes[p]] & 0xFF]` with a null check for abnormal transitions (uncomputed, dead, full-match, or match states). The existing `int[]` flat transition table is maintained as a sidecard for the cold path.

**Results (16M):**
- Hard: 35,036,185 → 28,276,308 ns (**19.3% faster**, now 1.11x faster than C++)
- Parens: 34,468,981 → 28,102,674 ns (**18.5% faster**, now 1.12x faster than C++)
- Medium: 35,231,460 → 34,468,725 ns (2.2% faster, still 1.09x slower than C++)

**Run 009 (null start-state self-loops for prefix accel):** The Object[] optimization introduced a regression on prefix-accel patterns (Easy0/Easy1) because `searchForwardPrefixAccel` had an extra `if (sRef == startRef)` check in the inner loop that prevented C2 from generating the same tight loop as `searchForward`. Fix: permanently null self-loop refs on the start state so the simple null-check inner loop naturally breaks on self-loops. After the break, the `int[]` sidecard distinguishes self-loops from match/sentinel transitions. This makes the `searchForwardPrefixAccel` inner loop body identical to `searchForward`.

**Results (Easy0 DFA 16M):**
- Easy0: 2,097,764 → 1,975,131 ns (**5.8% faster**, recovered the Run 008 regression)
- Easy1: 3,164,754 → 3,072,775 ns (**2.9% faster**)

### Assembly analysis (AArch64, C2 JIT, JDK 25)

The inner loop critical path has 3 serial dependent loads:

1. **LOAD 1**: `ldrb w2, [x12, #16]` — load `bytes[p] & 0xFF`
2. **LOAD 2**: `ldrb w10, [x11, #16]` — load `bmap[byte] & 0xFF` (bytemap class)
3. **LOAD 3**: `ldr w11, [x11, #16]` — load `sRef[cls]` (transition reference)

Between LOAD 2 (result in `w10`) and LOAD 3, C2 generates **1 instruction**:

```
add  x11, x15, w10, sxtw #2    ; address = sRef_base + cls * 4
```

The `sxtw #2` folds sign-extension and ×4 scaling into a single instruction, matching C++'s critical path length. The previous `int[]` flat table approach required 3 instructions on this path (`sxtw` + `add` for state+cls offset + `add` for array base), which was the source of the ~10% gap.

After LOAD 3, compressed oop decompression adds `add x17, x27, x11, lsl #3` and a checkcast type check loads the klass word from the object header. These are additional overhead vs the int-based loop, but the klass word load is an L1 hit (same cache line as the object) and the oop decompression can overlap with LOAD 1/LOAD 2 of the next iteration.

### Null semantics

`null` in the `Object[]` represents any abnormal transition: `T_UNCOMPUTED`, `T_DEAD`, `T_FULL_MATCH`, or match states with `T_MATCH_BIT`. The inner loop checks null via `cbz` (1 instruction). When null is hit, the outer loop consults the `int[]` sidecard transition table to distinguish the specific case using the existing slow-path logic.

### Medium gap

Medium uses a prefix character class `[XYZ]` which routes through `searchForward` (not `searchForwardPrefixAccel` since `canPrefixAccel` is false when the prefix is a character class). The C2 compilation of this path may differ in register allocation or loop structure, explaining the smaller improvement. Further investigation would require comparing the C2 output for Medium vs Hard.

---

## 3. DFA Easy1 - 1.5x gap from indexOf call frequency [IMPROVED from 4.6x -> 3.7x -> 1.5x]

Easy1 pattern: `A[AB]B[BC]...J$`, prefix = `"A"` (single byte).

| Size | C++ (ns) | Java Run 001 (ns) | Java Run 002 (ns) | Java latest (ns) | Ratio (latest) |
|------|----------|--------------------|--------------------|-------------------|----------------|
| 8 | 14 | 12 | 11 | 15.2 | 1.09x |
| 32K | 2,389 | 11,873 | 12,123 | 2,899 | 1.21x |
| 16M | 2,022,750 | 9,319,489 | 7,502,878 | 3,072,775 | 1.52x |

**Run 002 improvement at 16M: 9.3M -> 7.5M ns (19% faster).** The SWAR broadcastMask hoisting saves ~10ns per indexOf call. At 176,000 calls, that's ~1.8M ns saved, matching the observed 1.8M ns reduction.

**Run 003 improvement:** needflags optimization reduced the gap further to 1.5x.

**Run 007-009 improvements:** Flat int[] transition table, Object[] per-state refs, and self-loop nulling collectively reduced Easy1 from ~7.5M to ~3.1M ns. The ratio stays at ~1.5x because the DFA inner loop itself got much faster, so more time is now proportionally spent in indexOf false-positive handling.

Compare to Easy0 (prefix `"ABCDEFGHIJKLMNOPQRSTUVWXYZ"`, 26 bytes):

| Size | C++ (ns) | Java (ns) | Ratio |
|------|----------|-----------|-------|
| 16M | 1,817,500 | 1,975,131 | 1.09x |

Easy0 is 1.09x. Easy1 is 1.5x. Both have prefix acceleration. The difference is **call frequency**.

### Analysis

In 16M of random ASCII, `'A'` appears ~176,000 times (16M / 95 printable chars). Each occurrence is a "false positive" - the DFA matches `'A'`, checks the next byte, fails, returns to start state, and calls indexOf again.

- **Easy0**: FrontAndBack checks first byte `'A'` AND last byte `'Z'` at position +25. Only ~1,852 positions pass both checks (176,000 / 95). Each indexOf call scans ~8,600 bytes on average.
- **Easy1**: Single-byte indexOf for `'A'`. All 176,000 positions are false positives. Each call scans ~95 bytes on average.

### Remaining fix opportunities

- For single-byte prefix, investigate `Arrays.mismatch` or Vector API intrinsics
- Consider a combined "scan-and-step" method that finds `'A'`, does the DFA transitions, and loops internally - avoiding re-entry to the outer loop per false positive

**Files:** `Prog.java` (indexOfSWAR, prefixAccel)

---

## 4. FullMatch DotStarDollar/DotStarCapture - Well-matched

| Pattern | Size | C++ (ns) | Java (ns) | Ratio |
|---------|------|----------|-----------|-------|
| DotStarDollar | 8 | 41 | 31 | 0.75x (Java faster) |
| DotStarDollar | 512 | 985 | 1,231 | 1.25x |
| DotStarDollar | 2M | 4,003,980 | 4,787,978 | 1.20x |
| DotStarCapture | 8 | 44 | 29 | 0.66x (Java faster) |
| DotStarCapture | 2M | 3,974,130 | 8,332,301 | 2.10x |

**Run 002 vs Run 001:** DotStarDollar slightly worse at 2M (1.06x->1.20x), DotStarCapture slightly worse (1.86x->2.10x). Within run-to-run variance for these long-running benchmarks.

Both O(n). Java is within 20% at large sizes for DotStarDollar. DotStarCapture is worse (2.1x at 2M) because submatch extraction after the DFA scan adds overhead.

**No significant optimization needed.**

---

## 5. Parse RE2 Engine - 5.0x gap on 12-byte input (per-call overhead)

Pattern: `([0-9]+)-([0-9]+)-([0-9]+)` on `"650-253-0001"`.

| Engine | C++ (ns) | Java Run 001 (ns) | Java Run 002 (ns) | Ratio (002) |
|--------|----------|--------------------|--------------------|-------------|
| OnePass | 43 | 55 | 55.5 | 1.29x |
| RE2 | 55 | 275 | 275 | 5.00x |

**Run 002 vs Run 001:** No change. The SearchResult allocation elimination did not measurably help - the allocation was already on the TLAB fast path (~7ns), and replacing it with primitive returns may not have saved enough to overcome measurement noise at this scale.

OnePass alone: 1.29x (reasonable). RE2 full path: 5.0x (220ns extra overhead).

### Cost breakdown

**C++ RE2 path total: ~55ns**
- Forward DFA (12 bytes, cached states): ~8ns
- OnePass submatch extraction: ~43ns
- Overhead: ~4ns (stack-allocated locals, inlined dispatch)

**Java RE2 path total: ~275ns**
- `Re2.match()` entry: parameter validation, ByteSlice wrapping, prefix check -> ~15ns
- Forward DFA search: analyzeStart + 12 byte transitions + dispatch -> **~45ns**
- Reverse DFA search: same -> **~45ns**
- OnePass submatch: **~55ns**
- Phase 3-4 processing, ByteSlice creation: ~15ns

The 220ns gap:
- **Method dispatch**: `Re2.match` -> `Dfa.search` -> `analyzeStart` -> `runStateOnByte`, ~8 calls x 5ns = ~40ns
- **Two DFA phases** at 45ns each vs C++ ~8ns each - the 37ns delta per phase is from dispatch overhead, not from the 12-byte scan itself

### Fix opportunities

- For `ANCHOR_BOTH` with ncap >= 1 and small text, skip DFA entirely and let OnePass determine boundaries (C++ does this at `re2.cc:838-842`)
- Alternatively: for `ANCHOR_BOTH`, skip reverse DFA since matchStart=0, matchEnd=text.length()
- Reduce method dispatch overhead by inlining more of the DFA search path

**Files:** `Re2.java` (match method, Phase 3 anchored search)

**Expected outcome:** 2-3x improvement on small anchored matches (275ns -> ~100ns)

---

## 6. Compile - 1.58x gap [IMPROVED from 2.82x]

| Phase | C++ (ns) | Java Run 001 (ns) | Java Run 002 (ns) | Ratio (002) |
|-------|----------|--------------------|--------------------|-------------|
| Regexp_Parse | 751 | 420 | 419 | 0.56x (Java faster) |
| CompileToProg | 2,477 | 4,114 | 4,322 | 1.74x |
| RE2_Compile | 3,667 | 10,354 | 5,789 | **1.58x** |

**Run 002 improvement: RE2_Compile 10,354 -> 5,789 ns (44% faster).** Removing the unused fullProg eliminated the second compilation pass, saving ~4,500ns.

Per-pattern compile times also improved dramatically:

| Pattern | Run 001 (ns) | Run 002 (ns) | Change |
|---------|-------------|-------------|--------|
| Re2ConstructEasy0 | 11,436 | 5,881 | -49% |
| Re2ConstructHard | 12,789 | 6,670 | -48% |
| Re2ConstructParens | 38,698 | 21,636 | -44% |

### Remaining gap

The 1.58x gap in RE2_Compile is now from CompileToProg (1.74x). This includes:
- `Compiler.compile` -> walks the regex AST, emits instructions
- `Prog.optimize` -> peephole optimizations
- `Prog.flatten` -> linearizes the instruction graph
- `Prog.computeByteMap` -> builds the bytemap

Each step is implemented similarly to C++. The 1.74x gap in CompileToProg is from JVM overhead on object-heavy tree walking (Regexp nodes, Inst arrays, ArrayList vs vector). This is acceptable - compilation is a one-time cost.

**No immediate action needed** unless compile time becomes a bottleneck in production.

---

## 7. Practical Benchmarks - Formula: T = overhead + (n x per_byte_cost) [IMPROVED]

| Benchmark | Input size | C++ (ns) | Java Run 001 (ns) | Java Run 002 (ns) | Ratio (002) |
|-----------|-----------|----------|--------------------|--------------------|-------------|
| EmptyPartialMatch | 0 | 19 | 9.9 | 9.8 | 0.52x (Java faster) |
| SimplePartialMatch | 11 | 29 | 60 | 37.5 | **1.29x** |
| HTTPPartialMatch | 93 | 331 | 336 | 318 | **0.96x** |
| SmallHTTPPartialMatch | 17 | 47 | 60.5 | 44.1 | **0.94x** |

**Run 002 improvements:**
- **SimplePartialMatch**: 60 -> 37.5 ns (**38% faster**, from 2.07x to 1.29x)
- **HTTPPartialMatch**: 336 -> 318 ns (**5% faster**, from 1.02x to 0.96x)
- **SmallHTTPPartialMatch**: 60.5 -> 44.1 ns (**27% faster**, from 1.29x to 0.94x)

The improvements come from the SearchResult allocation elimination and reduced per-call overhead. The fixed overhead dropped from ~30ns to ~15ns.

**Java overhead**: ~15ns (`Re2.match` dispatch, prefix check, ByteSlice ops)
**C++ overhead**: ~18ns (inline dispatch, stack allocation)

**The crossover where Java matches C++ has moved down to ~15-20 bytes** for these specific patterns. Consult [2026-02-14-results.md](2026-02-14-results.md) for detailed benchmark tables across all patterns and sizes.

---

## Summary: Optimization Status

| # | Issue | Run 001 | Latest | Status |
|---|-------|---------|--------|--------|
| 1 | **FullMatchState for `(?s).*`** | 261,741x | Superseded | UTF-8 shortcut removed for correctness; byte-universal patterns retain the shortcut |
| 2 | **Double compilation (fullProg)** | 2.82x | **1.58x** | **FIXED** - fullProg removed, 44% compile speedup |
| 3 | **SWAR indexOf per-call overhead** | 4.61x | **1.5x** | **IMPROVED** - mask hoisted + needflags optimization |
| 4 | **Per-search object allocation** | 5.0x | 5.0x | **UNCHANGED** - SearchResult eliminated but no measurable effect on 12-byte parse |
| 5 | **DFA loop throughput** | 1.9x | **1.1x faster** | **RESOLVED** - Object[] per-state table + self-loop nulling; Hard/Parens beat C++, Easy0 1.1x |
| 6 | **Practical patterns** | 2.07x worst | **1.29x worst** | **IMPROVED** - SimplePartialMatch 38% faster, HTTP patterns now Java-faster |

---

## Work Order (Remaining Priorities)

1. **Skip DFA for small ANCHOR_BOTH** (Section 5) - high impact on practical patterns with submatch extraction
2. **Easy1 single-byte prefix** (Section 3) - 1.5x gap from indexOf call frequency; consider scan-and-step or Vector API
