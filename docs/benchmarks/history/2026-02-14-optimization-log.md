# DFA Inner Loop Optimization Log

This is a historical campaign notebook. Commit IDs, method names, paths, and
measurements describe the repository and machine at the time; they are not
current release-qualification results.

Lab notebook tracking every optimization attempt on the DFA search inner loop.
Each entry records what was changed, what was measured, and why it was kept or rejected.

Benchmark target: **Easy1 DFA 16M** (most sensitive to inner loop + prefix-accel interaction).
C++ baseline: **2,022,750 ns** (constant across all measurements).

Hardware: Apple M-series, ~4 GHz. **1 ns = 4 cycles.**

---

## Run 006: Specialized Loop Variants

**Commit:** `fcc4e8f`
**Approach:** Created 8 specialized search methods (searchFFF, searchFFT, etc.) with three boolean flags hardcoded at compile time: `canPrefixAccel`, `wantEarliestMatch`, `runForward`. Mirrors C++ template specialization.

**Inner loop:** Per-state `State` objects with `next[]` arrays. 4 serial dependent loads per byte.

**Results (16M):**
| Pattern | Before | After | Change |
|---------|--------|-------|--------|
| Medium | 60.6M ns | 58.6M ns | -3% |
| Hard | 61.2M ns | 56.3M ns | -8% |
| Parens | 62.6M ns | 54.3M ns | -13% |
| Easy1 | — | 3,039,174 ns | baseline |

**Verdict:** Kept. Eliminated per-byte conditional checks.

---

## Run 007: Flat int[] Transition Table

**Commit:** `a414991`
**Approach:** Replaced per-state `State` objects with flat `int[]` transition table. State = int offset. Sentinels encoded directly: T_UNCOMPUTED=0, T_DEAD=-1, T_FULL_MATCH=-2, T_MATCH_BIT=1<<30.

**Inner loop (searchForward):**
```java
for (; p < textEnd; p++) {
    s = trans[s + (bmap[bytes[p] & 0xFF] & 0xFF)];
    if (s <= 0) { break; }
}
```

**Critical path:** 3 serial dependent loads (byte → bytemap → transition). Down from 4.

**Results (16M):**
| Pattern | Before | After | Change |
|---------|--------|-------|--------|
| Easy1 | 3,039,174 | 2,544,681 | **-16%** |
| Hard | 56,280,407 | 35,036,185 | **-38%** |
| Parens | 54,339,267 | 34,468,981 | **-37%** |

**Assembly:** 3 loads on critical path, but C2 needed 3 instructions between bytemap result and transition load (sxtw + add for state offset + add for array base).

**Verdict:** Kept. Massive improvement. Hard/Parens now 1.1x faster than C++.

---

## Run 008: Object[] Per-State Transition References

**Commit:** `5d53269`
**Approach:** Each DFA state is now an `Object[]` where indices `0..nextSize-1` hold transition references to other states' `Object[]` arrays. Last slot holds `StateData` metadata. Maintains `int[]` sidecard for cold path.

**Inner loop (searchForward):**
```java
for (; p < textEnd; p++) {
    Object nextRef = sRef[bmap[bytes[p] & 0xFF] & 0xFF];
    if (nextRef == null) { break; }
    sRef = (Object[]) nextRef;
}
```

**Inner loop (searchForwardPrefixAccel) — with `sRef == startRef` check:**
```java
boolean reachedStart = false;
for (; p < textEnd; p++) {
    Object nextRef = sRef[bmap[bytes[p] & 0xFF] & 0xFF];
    if (nextRef == null) { break; }
    sRef = (Object[]) nextRef;
    if (sRef == startRef) {
        reachedStart = true;
        p++;
        break;
    }
}
```

**Critical path:** C2 compiles bytemap → transition load to **1 instruction**: `add x, x_sRef, w_cls, sxtw #2`. Matches C++'s critical path exactly. Down from 3 instructions in Run 007.

**Results (16M):**
| Pattern | Before | After | Change |
|---------|--------|-------|--------|
| Hard | 35,036 | 28,276 | **-19%** |
| Parens | 34,469 | 28,103 | **-18%** |
| Easy0 | 1,876 | 2,098 | +12% regression |
| Easy1 | 2,545 | 3,165 | **+24% regression** |

**Assembly (searchForward):** 3-load critical path confirmed. 2x JIT unrolling. 18.5 instructions/byte.

**Assembly (searchForwardPrefixAccel):** The `sRef == startRef` check added `if_acmpne` to every unrolled iteration. C2 still unrolled but the extra branch changed compilation characteristics. The prefixAccel inner loop compiled differently from searchForward despite having the same logical structure plus one extra check.

**Verdict:** Kept for Hard/Parens. Easy0/Easy1 regression addressed in Run 009.

---

## Run 009: Null Start-State Self-Loops

**Commit:** `81c05c0`
**Approach:** Permanently null self-loop refs on start state via `nullStartSelfLoops()`. This makes the `searchForwardPrefixAccel` inner loop body **identical** to `searchForward` — no `sRef == startRef` branch. When the inner loop breaks on a null self-loop, the `int[]` sidecard distinguishes it from match/sentinel transitions.

**Inner loop (searchForwardPrefixAccel) — identical to searchForward:**
```java
for (; p < textEnd; p++) {
    Object nextRef = sRef[bmap[bytes[p] & 0xFF] & 0xFF];
    if (nextRef == null) { break; }
    sRef = (Object[]) nextRef;
}
```

**After inner loop (sidecard handler for nulled self-loops):**
```java
s = ((StateData) sRef[nextSize]).offset();
int ns = trans[s + (bmap[bytes[p] & 0xFF] & 0xFF)];
if (ns > 0 && (ns & T_MATCH_BIT) == 0) {
    s = ns;
    sRef = dfa.stateRefArrays[s / nextSize];
    p++;
    continue; // -> prefix accel
}
```

**Results (16M) vs Run 008:**
| Pattern | Run 008 | Run 009 | Change |
|---------|---------|---------|--------|
| Easy0 | 2,098 | 1,975 | **-6%** (recovered) |
| Easy1 | 3,165 | 3,073 | **-3%** |
| Hard | 28,276 | 28,276 | unchanged |
| Parens | 28,103 | 28,103 | unchanged |

**Assembly:** 3x loop unrolling confirmed (3 ifnonnull, 0 checkcast). Inner loop now identical between searchForward and searchForwardPrefixAccel. Single `add x, x_sRef, w_cls, sxtw #2` on critical path.

**Problem:** Easy1 remained ~3ms, not the ~2.5ms from the flat int[] table (Run 007). The regression from Run 007 → Run 008/009 on Easy1 was only partially recovered.

**Verdict:** Kept. Best available compromise. But Easy1 regression remains open.

---

## Run 010: Split Prefix-Accel by Mode + Fused Single-Byte int[] Path (current HEAD)

**Commit:** `887442a`
**Approach:** Split forward prefix acceleration into two code paths:

1. `searchForwardPrefixAccelSingleByte` for single-byte, case-sensitive prefix mode (`Easy1` shape):
   - Uses fused `int[]` DFA transitions.
   - Keeps start-state re-entry handling in the same hot loop.
   - Uses top-two-bit cold-path gate for sentinel detection:
     `if ((ns & 0xC000_0000) != 0) break;`
2. `searchForwardPrefixAccel` remains the generic Object[] path for multi-byte and foldcase prefixes (`Easy0` shape and others).

`Prog` also gained explicit single-byte helpers:
- `canUseSingleBytePrefixAccelFastPath()`
- `prefixAccelSingleByteNoFoldcase(...)`

**Reasoning:** Easy1 has high false-positive frequency and repeatedly flips between prefix scan and short DFA bursts. In that regime, keeping re-entry + transition handling on an `int[]` path is lower overhead than bouncing through the generic Object[] sidecard handoff each cycle.

**Fresh JMH results (16M, 3 forks, 10x1s warmup, 5x1s measurement):**
| Pattern | Run 009 | Run 010 | Change |
|---------|---------|---------|--------|
| Easy0 | 1,975,000 | 2,086,718 | +5.7% |
| Easy1 | 3,073,000 | 2,365,278 | **-23.0%** |

**C++ comparison at 16M:**
- Easy0: `2,086,718 ns` vs `1,817,500 ns` (1.15x slower)
- Easy1: `2,365,278 ns` vs `2,022,750 ns` (1.17x slower)

**Profiling/assembly state (Easy1):**
- CPU sampling still shows most time in prefix scan (`indexOfSWAR`) with the remainder in the DFA-side fused loop.
- Inner loop is recognized as a counted loop with strip-mining/unroll behavior.
- The dominating open gap to C++ is now largely in prefix-scan cost, not only DFA transition mechanics.

**Verdict:** Kept. This closes most of the Easy1 regression while preserving generic-path behavior.

---

## Current Gap: Easy1 vs C++ (Run 010)

**Target:** Easy1 16M as close as possible to C++ (`2,022,750 ns`)
**Current:** Easy1 16M = `2,365,278 ns` (~17% gap)

### What is now most likely limiting

1. Prefix scan implementation cost (`indexOfSWAR` vs C++ `memchr`-style search)
2. Prefix-scan/DFA handoff frequency in high false-positive random-text workloads

---

## Run 011: NFA No-Submatch Campaign (Accepted)

### Accepted changes

1. **Pass 1: no-submatch specialized execution path**
   - **Commit:** `c0af967`
   - **What changed:** Split `Nfa.search(...)` into dedicated no-submatch and capture-aware paths; removed capture-copy overhead from no-submatch hot loops.
   - **Observed impact (256K):**
     - Hard: `7,585,875 -> 6,027,532 ns` (**-20.5%**)
     - Parens: `10,450,017 -> 9,187,899 ns` (**-12.1%**)
   - **Verdict:** Kept.

2. **Pass 2: internal sparse queue fast path**
   - **Commit:** `ca202f0`
   - **What changed:** Replaced `SparseIntArray` usage in no-submatch path with specialized internal queue representation tuned for NFA enqueue/step hot loops.
   - **Observed impact (256K):**
     - Hard: `6,027,532 -> 6,056,320 ns` (~neutral)
     - Parens: `9,187,899 -> 8,492,286 ns` (**-7.6%**)
   - **Verdict:** Kept (net positive on target workloads).

3. **Pass 3: no-submatch instruction flattening**
   - **Commit:** `d568e57`
   - **What changed:** Flattened no-submatch instruction access to avoid repeated `Prog.Inst` object dereferences/getter calls in hot loops.
   - **Observed impact:** Mixed in isolation; unlocked follow-on wins when combined with later no-submatch cleanups.
   - **Verdict:** Kept as part of the final no-submatch optimization stack.

4. **Follow-on no-submatch cleanups**
   - **Commits:** `824e738`, `3963150`, `678affb`, `8039334`
   - **What changed:** Cached no-submatch tables per program, removed thread-arena overhead in hot path, improved capture-op enqueue, and optimized word-boundary checks.
   - **Observed impact:** Incremental wins on Hard/Parens without violating regression gates; B3 full-gate 256K deltas were:
     - Easy0: **-2.88%**
     - Easy1: **-0.05%**
     - Medium: **-1.43%**
     - Hard: **-1.36%**
     - Parens: **-1.46%**
   - **Verdict:** Kept.

### Rejected changes (strict no-regression policy)

| Candidate | Commit (reverted) | Why rejected |
|-----------|--------------------|--------------|
| A2 closure enqueue path | `ba0249b` (`be63592`) | HardNFA256K regressed beyond threshold (rerun still >2%). |
| B1 hasEmptyWidth split | `b20f599` (`a595bc4`) | Broad NFA regressions after rerun (Hard/Medium/Easy1). |
| B2 empty flags hoist | `2929e4f` (`af93e4a`) | Persistent HardNFA256K regression after rerun. |
| C1 BitState flattening | `d3ef421` (`b14302c`) | Large `searchDigitsBitState` regression (~24% rerun). |
| C2 BitState no-submatch | `08822ec` (`0e1883e`) | `searchDigitsBitState` full-gate regression (~3%). |
| C3 BitState hoists | `578f205` (`fce3008`) | `searchDigitsBitState` regression persisted after rerun. |
| D1 Re2 threshold tuning | `aa24299` (`6ff3fc8`) | Severe practical regression (`smallHttpPartialMatch` ~+29%). |
| E1 DFA single-byte scan alt | `368fe49` (`1c22536`) | Easy1 and broader DFA regressions. |
| E2 DFA branch simplification | `6c14790` (`f643229`) | Broad Easy0/Hard/Parens DFA regressions. |

---

## Assembly Files Reference

| File | Date | Contents |
|------|------|----------|
| `asm-baseline.txt` | Feb 11 12:12 | Baseline searchForward assembly |
| `asm-exp1.txt` | Feb 11 12:15 | Experiment 1 |
| `asm-exp2.txt` | Feb 11 12:18 | Experiment 2 |
| `asm-exp3.txt` | Feb 11 12:21 | Experiment 3 |
| `asm-exp4.txt` | Feb 11 12:31 | Experiment 4: Object[] variant |
| `asm-exp4-forceinline.txt` | Feb 11 12:34 | Experiment 4 with force-inlining |
| `asm-exp4-split.txt` | Feb 11 12:40 | Experiment 4 with split methods |
| `asm-abnormal.txt` | Feb 11 14:36 | Abnormal transition handling |
| `asm-checkfromto.txt` | Feb 11 13:54 | Check-from-to variant |
| `asm-prefixaccel.txt` | Feb 12 00:06 | searchForwardPrefixAccel (Run 009 generic path) |
| `searchForward-assembly.txt` | Feb 11 09:57 | searchForward flat int[] |
| `searchForward-assembly-objref.txt` | Feb 11 17:13 | searchForward Object[] |

---

## Run 012: Engine Campaign v2 (P1-P5, strict no-regression)

Campaign branch: `user/dain/engine-24h-campaign-v2`
Running log: `docs/benchmarks/history/2026-02-13-engine-campaign-v3.md`

### Accepted

1. **P2 BigFixed anchored long-prefix DFA path**
   - **Commit:** `a055220`
   - **What changed:** Added a gated anchored long-prefix `int[]` DFA forward path.
   - **Key impact:**
     - BigFixedDfa 32K: `111,310.7 -> 57,985.9 ns` (mini, -47.9%)
     - BigFixedRe2 32K: `126,068.9 -> 61,062.5 ns` (mini, -51.6%)
   - **Full gate snapshot:** `/private/tmp/engine-v2-P2-full-20260213-142854`
   - **Verdict:** Kept.

2. **P4 OnePass match-every-byte fast path (boolean-only)**
   - **Commit:** `d5cea06`
   - **What changed:** Early return in `OnePass.search` for boolean matches of
     byte-universal programs. The current `matchesAnyByteString` predicate
     excludes UTF-8 dot-all because malformed UTF-8 must be rejected.
   - **Key impact:**
     - AltMatchOnePass 32K: `44,882.6 -> 4.3 ns` (full, ~100x faster)
     - AltMatchOnePass 256K: `324,031.5 -> 4.1 ns` (full, ~100x faster)
   - **Full gate snapshot:** `/private/tmp/engine-v2-P4-full-20260213-151401`
   - **Verdict:** Kept.

3. **P5 Easy1 single-byte DFA hot-loop cleanup**
   - **Commit:** `b1156ca`
   - **What changed:** Kept precomputed `T_MATCH_BIT` transitions in the single-byte fused hot loop; only negative sentinels break.
   - **Key impact (mini):**
     - Easy1Dfa 262K: `23,806.3 -> 22,352.6 ns` (-6.1%)
     - Easy1Dfa 16M: `2,998,763.9 -> 2,476,769.5 ns` (-17.4%)
   - **Full gate snapshot:** `/private/tmp/engine-v2-P5-full-20260213-152829`
   - **Verdict:** Kept.

### Rejected

1. **P1 Easy2 foldcase dispatch change**
   - **Result:** Easy2Dfa 256K regressed heavily on rerun (`+46.5%`).
   - **Verdict:** Reverted.

2. **P3 BitState list-head verification removal**
   - **Result:** produced unstable mixed results and failed strict no-regression gate on protected practical checks.
   - **Verdict:** Reverted.

---

## Run 013: Engine Campaign v3 (24h, exhaustive, strict perf+memory)

Campaign branch: `user/dain/engine-24h-campaign-v3`
Running log: `docs/benchmarks/history/2026-02-13-engine-campaign-v3.md`

### Outcome

- **Retained changes:** none
- **Policy result:** all candidates were rejected/reverted under strict no-regression gate (`>2%` persistent regression after rerun) and memory/alloc guardrails.
- **Net code delta:** none (campaign produced benchmark evidence and revert history only).

### Rejected candidates (summary)

| Candidate | Area | Why rejected |
|-----------|------|--------------|
| `P1.1`, `P1.2`, `P1.3` | DFA Easy2 foldcase path | Some Easy2 wins were observed, but protected-matrix regressions persisted (notably `BigFixedDfa` and misc guards). |
| `P2.1`, `P2.2`, `P2.3` | Re2 BigFixed anchored-prefix routes | Target gains were offset by persistent protected regressions (especially `BigFixedDfa` guard points). |
| `P3.1`, `P3.2`, `P3.3` | BitState no-submatch / flattening variants | Regressed protected BitState guards (`Success1`, `AltMatch`, or `Digits`) after reruns. |
| `P4.1`, `P4.2` | OnePass no-submatch / empty-flag reduction | `AltMatchOnePass` regressed beyond threshold after rerun. |
| `P5.1` | DFA forward micro-variant | Severe regressions on misc guard benchmarks (`asciiMatch`, `dotMatch`) and `BigFixedDfa`. |
| `P6.1` | Re2 anchored boolean shortcut | No target gain and persistent protected regressions (`BigFixedDfa`) after rerun. |

### Stop condition reached

Campaign stopped after repeated failures in the same optimization area with no credible low-risk follow-up under the active host-variance window. Remaining exploratory items were deferred (`P5.2`, `P6.2`, `P7.1`, `P7.2`).

---

## Run 014: Parser/Compiler Campaign v1 (targeted, strict no-regression)

Campaign branch: `user/dain/parser-compiler-campaign-v1`  
Running log: `docs/benchmarks/history/2026-02-13-parser-compiler-campaign-v1.md`

### Accepted

1. **C4.1 Re2 full-range subtext allocation fast path**
   - **Commit:** `1dad0f5`
   - **What changed:** In `Re2.match(...)`, avoid creating a wrapped `ByteSlice` when the call already spans the full input (`start == 0 && end == text.length()`), and reuse the original `text` object.
   - **Why it worked:** This removes a hot-path wrapper allocation/indirection from the common full-range search entry path used by parse/practical workloads.
   - **Evidence (full gate):** `/private/tmp/parser-campaign-v1-C4.1-full-20260213-235608`
     - `CompilePhaseRe2Compile`: `4,788.0 ns`
     - `SearchPhone@8`: `40.1 ns` (no guarded regression)
   - **Verdict:** Kept.

### Rejected

1. **C1.1 Prog flatten pre-sizing/workspace reuse**
   - **Why rejected:** Persistent guarded regression on `searchPhoneRe2@8` after rerun.
   - **Artifacts:** `/private/tmp/parser-campaign-v1-C1.1-tier2-20260213-223821`, `/private/tmp/parser-campaign-v1-C1.1-noisecheck-20260213-224541`

2. **C1.2 Prog optimize fast path**
   - **Why rejected:** Persistent tier-2 regressions (`searchPhone`, `compilePhaseParse`) after rerun.
   - **Artifacts:** `/private/tmp/parser-campaign-v1-C1.2-tier2-20260213-225635`, `/private/tmp/parser-campaign-v1-C1.2-tier2-rerun-20260213-230134`

3. **C1.3 Prog computeByteMap fast path**
   - **Why rejected:** Persistent guarded regressions after focused rerun.
   - **Artifacts:** `/private/tmp/parser-campaign-v1-C1.3-tier2-20260213-231357`, `/private/tmp/parser-campaign-v1-C1.3-noisecheck-20260213-231836`

4. **C2.2 Re2 default-constructor allocation removal**
   - **Why rejected:** `searchPhoneRe2@8` regression persisted beyond threshold after rerun (`+3.46%`).
   - **Artifacts:** `/private/tmp/parser-campaign-v1-C2.2-tier2-20260213-234546`, `/private/tmp/parser-campaign-v1-C2.2-noisecheck-20260213-235018`

5. **C3.1 Parser escape-class caching extension**
   - **Why rejected:** Broader tier-2 regressions persisted after rerun.
   - **Artifacts:** `/private/tmp/parser-campaign-v1-C3.1-tier2-20260213-232535`, `/private/tmp/parser-campaign-v1-C3.1-noisecheck-20260213-233017`

6. **C3.2 Parser UTF-8 validation ASCII fast path**
   - **Why rejected:** Guarded `searchPhoneRe2@8` regression persisted after focused rerun.
   - **Artifacts:** `/private/tmp/parser-campaign-v1-C3.2-tier2-20260213-233514`, `/private/tmp/parser-campaign-v1-C3.2-noisecheck-20260213-234121`

### No-op assessment

- **C2.1 Re2 lazy metadata extension:** Already lazy in current code (`namedCapturingGroups` and `capturingGroupNames` computed on first accessor), so no code change retained.

### Post-campaign results publication refresh (2026-02-14)

- **What was run:** publish-scope full refresh for all sections represented in `RESULTS.md`.
  - Completed in initial sweep: `BenchmarkRe2Search`, `BenchmarkRe2SearchNfa`, `BenchmarkRe2FullMatch`, `BenchmarkRe2Practical`, `BenchmarkRe2Parse`.
  - `SearchExtra` full default-size run was stopped and replaced with a full-config **published-rows tail run** (same JMH config) plus `BenchmarkRe2Misc`, then merged with the completed class outputs.
- **Artifacts:**
  - Initial sweep: `/private/tmp/results-refresh-targeted-20260214-002847`
  - Tail sweep + merged CSV: `/private/tmp/results-refresh-tail-20260214-022120`
  - Published dataset: `/private/tmp/results-refresh-tail-20260214-022120/java-targeted-full.csv`
- **Docs updated from this refresh:**
  - `docs/benchmarks/history/2026-02-14-results.md`
  - `docs/benchmarks/history/2026-02-13-parser-compiler-campaign-v1.md` (post-closeout refresh log block)

---

## Run 015: Focused Compiler/Parser Attribution Benchmarks

Branch: `user/dain/parser-compiler-campaign-v1`

### Scope

- Added benchmark-only compile stage hook in `Compiler`:
  - `CompileStage { RAW, OPTIMIZED, FLATTENED, BYTEMAP }`
  - `compileForBenchmark(Regexp, boolean, long, CompileStage)`
- Added focused benchmark classes:
  - `/Users/dain/work/airlift/slice/src/test/java/io/airlift/slice/re2/BenchmarkRe2CompileFocused.java`
  - `/Users/dain/work/airlift/slice/src/test/java/io/airlift/slice/re2/prog/BenchmarkRe2CompilerPhases.java`

### Validation

- `./mvnw -q -DskipTests test-compile` passed.
- The focused parser/compiler tests passed under their names at the time.

### Artifacts

- Fast profile: `/private/tmp/parser-compile-focused-20260214-091857`
  - `BenchmarkRe2CompileFocused.fast.txt`
  - `BenchmarkRe2CompilerPhases.fast.txt`
- Full profile + guards:
  - `BenchmarkRe2CompileFocused.full.txt`
  - `BenchmarkRe2CompilerPhases.full.txt`
  - `BenchmarkCompileGuards.full.txt`

### Findings

1. Compile phase attribution is clear: **flatten + bytemap dominate `compileToProg` cost** on all tested patterns.
   - `(.*)-(\\d+)-of-(\\d+)`: `67.8%` of BYTEMAP endpoint is `FLATTENED->BYTEMAP` work.
   - `Easy0`: `84.8%`
   - `Hard`: `81.1%`
   - `Parens`: `65.9%`
   - `SplitHard-shape`: `71.1%`
   - `Easy2 foldcase`: `81.1%`
2. Parser-only cost is comparatively small except foldcase-heavy parse (`(?i)ABCDEFGHIJKLMNOPQRSTUVWXYZ$`), which remains parse-heavier than the other patterns but still below full compile endpoint.
3. Guard run versus currently published `RESULTS.md` values was mixed (expected host variance), with compile-to-prog stable and compile-total slightly slower in this isolated rerun:
   - `compilePhaseCompileToProg`: `4256.7 -> 4245.3 ns` (`-0.27%`)
   - `compilePhaseRe2Compile`: `4795.3 -> 4897.4 ns` (`+2.13%`)
   - `smallHttpPartialMatch`: `36.9 -> 37.1 ns` (`+0.43%`)

### Decision

- **Kept** benchmark infrastructure changes (diagnostic only, no production behavior change).
- **Next optimization priority confirmed:** flatten reductions first, then bytemap churn reduction, then optimize-pass traversal micro-costs.

---

## Run 016: Parser/Compiler Campaign v2 (flatten+bytemap focus)

Campaign branch: `user/dain/parser-compiler-campaign-v2`  
Running log: `docs/benchmarks/history/2026-02-14-parser-compiler-campaign-v2.md`

### Kept

1. **B1.1 ByteMapBuilder primitive storage**
   - Replaced `List`-based range/color map bookkeeping with primitive arrays.
   - Result: substantial compile-path wins with no guarded regressions.
2. **B1.2 ByteMap recolor O(1) remap table**
   - Replaced linear recolor-map scan with generation-stamped remap table.
   - Result: retained compile wins; guarded rows stable after noise check.
3. **B1.3 Precomputed word-boundary ranges**
   - Removed repeated scan over `[0..255]` for word/non-word partitioning.
   - Result: additional bytemap build improvement; no guarded regressions.
4. **B2.1 flatten temp allocation removal**
   - Removed hot temporary allocation in predecessor marking path.
   - Result: compile phase gains; runtime guard stable.
5. **B2.2 flatten predecessor primitive table**
   - Replaced `List<IntList>` predecessor storage with primitive bucket table.
   - Result: retained compile gains; initial `searchPhone@8` spike did not persist on rerun.
6. **B4.1 parser UTF-8 ASCII fast path**
   - Added ASCII short-circuit in `RegexpParser.isValidUtf8`.
   - Result: parser-focused benchmark improved; guard remained within threshold.

### Rejected / Reverted

1. **B2.3 flatten loop lookup micro-tightening**
   - `searchPhoneRe2@16M` remained `>2%` slower on rerun; reverted.
2. **B3.1 optimize no-NOP quick skip**
   - Multiple `searchPhoneRe2` sizes regressed beyond threshold on rerun; reverted.
3. **B3.2 optimize ALT/ALT_MATCH loop micro-tightening**
   - No promotable focused gain vs kept state; rejected and reverted.

### Net Outcome

- Compile path moved materially in the right direction under strict no-regression policy.
- Representative guard outcomes for retained state stayed within allowed bounds after required reruns.
- Remaining open work is likely in larger structural compile costs rather than micro-optimizing `optimize()`.

### Promotion full gate (retained state)

- Full gate artifact:
  - `/private/tmp/parser-campaign-v2-fullgate-20260214-140933`
    - `jmh-focused-full.txt`
    - `jmh-phases-full.txt`
    - `jmh-guard-full.txt`
- Key full-gate outcomes vs campaign baseline:
  - `compilePhaseCompileToProg`: `4200.053 -> 3722.168 ns` (`-11.38%`)
  - `compilePhaseRe2Compile`: `4896.754 -> 4350.807 ns` (`-11.15%`)
  - `smallHttpPartialMatch`: `37.531 -> 35.073 ns` (`-6.55%`)
  - `searchPhoneRe2` guard sizes: all neutral-to-better (`8 -9.18%` to `16M -0.21%`)
- Decision: retained changes pass promotion gate under strict no-regression policy.

### Artifacts

- Baseline:
  - `/private/tmp/parser-campaign-v2-baseline-20260214-122353`
  - `/private/tmp/parser-campaign-v2-baseline-phases-20260214-123248`
- Kept-candidate runs:
  - `/private/tmp/parser-campaign-v2-B1.1-20260214-123809`
  - `/private/tmp/parser-campaign-v2-B1.2-20260214-124751`
  - `/private/tmp/parser-campaign-v2-B1.3-20260214-125704`
  - `/private/tmp/parser-campaign-v2-B2.1-20260214-130610`
  - `/private/tmp/parser-campaign-v2-B2.2-20260214-131640`
  - `/private/tmp/parser-campaign-v2-B4.1-focused-20260214-140012`
  - `/private/tmp/parser-campaign-v2-B4.1-guard-20260214-140637`
- Reverted/rejected checks:
  - `/private/tmp/parser-campaign-v2-B2.3-20260214-132753`
  - `/private/tmp/parser-campaign-v2-B2.3-noise-20260214-133729`
  - `/private/tmp/parser-campaign-v2-B3.1-20260214-134119`
  - `/private/tmp/parser-campaign-v2-B3.1-noise-20260214-134939`
  - `/private/tmp/parser-campaign-v2-B3.2-focused-20260214-135253`
