# Parser/Compiler Campaign v2

> Historical record. This document is evidence, not current project status.
> See the repository-root `RE2_TASKS.md` for open work.

Start: 2026-02-14 12:22 PST
Branch: `user/dain/parser-compiler-campaign-v2`
Policy: strict no-regression (`>2%` persistent regression rejected after one rerun), memory cap `+64KB/prog`.

## Benchmark Config
- Focused fast: `-f 2 -wi 5 -i 4 -w 400ms -r 400ms`
- Runtime guard mini: `-f 2 -wi 6 -i 4 -w 500ms -r 500ms`
- Promotion: `-f 3 -wi 10 -i 5 -w 1s -r 1s`

## CPU Contention Guard
Before and after each benchmark run:
1. Capture top CPU process snapshot.
2. If top non-benchmark process is high, wait 60s and recheck once.
3. Record pre/post snapshots in campaign artifacts and decisions log.

## Process Guard
- Keep at most one active long-running benchmark process.
- Reuse/poll one session for long benchmark runs.

## Now
- BASELINE setup: compile gate, correctness suite, focused baseline, runtime guard baseline.

## Queue
- BASELINE: running
- B1.1 ByteMapBuilder primitive ranges/map storage: running
- B1.2 ByteMap recolor O(1) remap table: pending
- B1.3 ByteMap word-boundary precomputed ranges: pending
- B2.1 flatten temporary allocation removal: pending
- B2.2 flatten predecessor primitive storage: pending
- B2.3 flatten loop lookup/bounds tightening: pending
- B3.1 optimize no-NOP quick skip: pending
- B3.2 optimize queue/dispatch micro-tightening: pending
- B4.1 parser UTF-8 ASCII quick scan: pending

## Decisions
- 2026-02-14 12:22 | INIT | campaign initialized on `user/dain/parser-compiler-campaign-v2` | running
- 2026-02-14 12:23 | BASELINE-TEST | compile + targeted parser/compiler suite (+ProgCompileEquivalenceTest) passed | pass
- 2026-02-14 12:31 | BASELINE-FOCUSED+GUARD | cpu-pre/post captured, jmh=/private/tmp/parser-campaign-v2-baseline-20260214-122353/{jmh-focused.txt,jmh-guard.txt} | captured
- 2026-02-14 12:34 | BASELINE-PHASES | cpu-pre/post captured, jmh=/private/tmp/parser-campaign-v2-baseline-phases-20260214-123248/jmh-phases.txt | captured
- 2026-02-14 12:36 | BASELINE-METRICS | compileToProg=4200.053ns re2Compile=4896.754ns smallHttp=37.531ns searchPhone@8=44.457ns | locked

## Next
- Run baseline compile/test and baseline focused + guard benchmark captures.

## Baseline Snapshot (locked)
- Artifacts:
  - `/private/tmp/parser-campaign-v2-baseline-20260214-122353`
  - `/private/tmp/parser-campaign-v2-baseline-phases-20260214-123248`
- Focused medians:
  - `BenchmarkRe2CompileFocused.compileToProgFromParsed`: `5576.643 ns`
  - `BenchmarkRe2CompileFocused.re2CompileTotal`: `6147.388 ns`
  - `BenchmarkRe2CompilerPhases.compileToRawProg`: `858.761 ns`
  - `BenchmarkRe2CompilerPhases.compileToOptimizedProg`: `1163.117 ns`
  - `BenchmarkRe2CompilerPhases.compileToFlattenedProg`: `3945.181 ns`
  - `BenchmarkRe2CompilerPhases.compileToByteMapProg`: `5464.961 ns`
- Runtime guard baseline:
  - `compilePhaseCompileToProg`: `4200.053 ns`
  - `compilePhaseRe2Compile`: `4896.754 ns`
  - `smallHttpPartialMatch`: `37.531 ns`
  - `searchPhoneRe2@8`: `44.457 ns`
  - `searchPhoneRe2@64`: `160.569 ns`
  - `searchPhoneRe2@512`: `1160.712 ns`
  - `searchPhoneRe2@4096`: `9162.297 ns`
  - `searchPhoneRe2@32768`: `73930.526 ns`
  - `searchPhoneRe2@262144`: `578076.259 ns`
  - `searchPhoneRe2@2097152`: `4661255.769 ns`
  - `searchPhoneRe2@16777216`: `36773781.982 ns`

## B1.1 Attempt (ByteMapBuilder primitive arrays)
- 2026-02-14 12:37 | B1.1-TEST | compile + targeted parser/compiler suites passed | pass
- 2026-02-14 12:46 | B1.1-BENCH | cpu-pre/post captured, jmh=/private/tmp/parser-campaign-v2-B1.1-20260214-123809/{jmh-focused.txt,jmh-phases.txt,jmh-guard.txt} | complete
- 2026-02-14 12:47 | B1.1-RESULT | compileToByteMap median -13.97%, compilePhaseCompileToProg -9.53%, compilePhaseRe2Compile -12.71%, no guarded regressions | keep

## Queue (updated)
- BASELINE: completed
- B1.1 ByteMapBuilder primitive ranges/map storage: kept
- B1.2 ByteMap recolor O(1) remap table: kept
- B1.3 ByteMap word-boundary precomputed ranges: pending
- B2.1 flatten temporary allocation removal: pending
- B2.2 flatten predecessor primitive storage: pending
- B2.3 flatten loop lookup/bounds tightening: pending
- B3.1 optimize no-NOP quick skip: pending
- B3.2 optimize queue/dispatch micro-tightening: pending
- B4.1 parser UTF-8 ASCII quick scan: pending

## B1.2 Attempt (ByteMap recolor O(1) remap)
- 2026-02-14 12:47 | B1.2-TEST | compile + targeted parser/compiler suites passed | pass
- 2026-02-14 13:00 | B1.2-BENCH | cpu-pre/post captured, jmh=/private/tmp/parser-campaign-v2-B1.2-20260214-124751/{jmh-focused.txt,jmh-phases.txt,jmh-guard.txt} | complete
- 2026-02-14 13:00 | B1.2-RESULT | compileToByteMap median -12.77%, compilePhaseCompileToProg -10.71%, compilePhaseRe2Compile -9.56%, no guarded regressions | keep

## Queue (updated)
- BASELINE: completed
- B1.1 ByteMapBuilder primitive ranges/map storage: kept
- B1.2 ByteMap recolor O(1) remap table: kept
- B1.3 ByteMap word-boundary precomputed ranges: kept
- B2.1 flatten temporary allocation removal: pending
- B2.2 flatten predecessor primitive storage: pending
- B2.3 flatten loop lookup/bounds tightening: pending
- B3.1 optimize no-NOP quick skip: pending
- B3.2 optimize queue/dispatch micro-tightening: pending
- B4.1 parser UTF-8 ASCII quick scan: pending

## B1.3 Attempt (ByteMap word-boundary precomputed ranges)
- 2026-02-14 13:01 | B1.3-TEST | compile + targeted parser/compiler suites passed | pass
- 2026-02-14 13:14 | B1.3-BENCH | cpu-pre/post captured, jmh=/private/tmp/parser-campaign-v2-B1.3-20260214-125704/{jmh-focused.txt,jmh-phases.txt,jmh-guard.txt} | complete
- 2026-02-14 13:14 | B1.3-RESULT | compileToByteMap median -14.57%, compilePhaseCompileToProg -8.56%, compilePhaseRe2Compile -9.40%, no guarded regressions | keep

## Queue (updated)
- BASELINE: completed
- B1.1 ByteMapBuilder primitive ranges/map storage: kept
- B1.2 ByteMap recolor O(1) remap table: kept
- B1.3 ByteMap word-boundary precomputed ranges: kept
- B2.1 flatten temporary allocation removal: running
- B2.2 flatten predecessor primitive storage: pending
- B2.3 flatten loop lookup/bounds tightening: pending
- B3.1 optimize no-NOP quick skip: pending
- B3.2 optimize queue/dispatch micro-tightening: pending
- B4.1 parser UTF-8 ASCII quick scan: pending

## B2.1 Attempt (flatten temp allocation removal)
- 2026-02-14 13:15 | B2.1-TEST | compile + targeted parser/compiler suites passed | pass
- 2026-02-14 13:27 | B2.1-BENCH | cpu-pre/post captured, jmh=/private/tmp/parser-campaign-v2-B2.1-20260214-130610/{jmh-focused.txt,jmh-phases.txt,jmh-guard.txt} | complete
- 2026-02-14 13:27 | B2.1-RESULT | compilePhaseCompileToProg -12.28%, compilePhaseRe2Compile -11.51%, no guarded regressions | keep

## B2.2 Attempt (flatten predecessor primitive table)
- 2026-02-14 13:28 | B2.2-TEST | compile + targeted parser/compiler suites passed | pass
- 2026-02-14 13:39 | B2.2-BENCH | jmh=/private/tmp/parser-campaign-v2-B2.2-20260214-131640/{jmh-focused.txt,jmh-phases.txt,jmh-guard.txt} | complete
- 2026-02-14 13:42 | B2.2-NOISE | rerun guard jmh=/private/tmp/parser-campaign-v2-B2.2-noise-20260214-132504/jmh-guard.txt | searchPhone@8 regression did not persist
- 2026-02-14 13:42 | B2.2-RESULT | compilePhaseCompileToProg -7.61%..-10.86% range across runs, no persistent guarded regressions | keep

## B2.3 Attempt (flatten loop lookup micro-tightening)
- 2026-02-14 13:43 | B2.3-TEST | compile + targeted parser/compiler suites passed | pass
- 2026-02-14 13:55 | B2.3-BENCH | jmh=/private/tmp/parser-campaign-v2-B2.3-20260214-132753/{jmh-focused.txt,jmh-phases.txt,jmh-guard.txt} | guarded regression on searchPhone@262144/+3.66% and @16M/+2.22%
- 2026-02-14 13:58 | B2.3-NOISE | rerun guard jmh=/private/tmp/parser-campaign-v2-B2.3-noise-20260214-133729/jmh-guard.txt | searchPhone@16M +2.47% persisted
- 2026-02-14 13:59 | B2.3-DECISION | strict guard violated after rerun; reverted B2.3 code changes | revert

## Queue (updated)
- BASELINE: completed
- B1.1 ByteMapBuilder primitive ranges/map storage: kept
- B1.2 ByteMap recolor O(1) remap table: kept
- B1.3 ByteMap word-boundary precomputed ranges: kept
- B2.1 flatten temporary allocation removal: kept
- B2.2 flatten predecessor primitive storage: kept
- B2.3 flatten loop lookup/bounds tightening: reverted
- B3.1 optimize no-NOP quick skip: running
- B3.2 optimize queue/dispatch micro-tightening: pending
- B4.1 parser UTF-8 ASCII quick scan: pending

## B3.1 Attempt (optimize no-NOP quick skip)
- 2026-02-14 13:59 | B3.1-TEST | compile + targeted parser/compiler suites passed | pass
- 2026-02-14 14:11 | B3.1-BENCH | jmh=/private/tmp/parser-campaign-v2-B3.1-20260214-134119/{jmh-focused.txt,jmh-phases.txt,jmh-guard.txt} | multiple searchPhone regressions (+2% to +5.6%)
- 2026-02-14 14:13 | B3.1-NOISE | rerun guard jmh=/private/tmp/parser-campaign-v2-B3.1-noise-20260214-134939/jmh-guard.txt | regressions persisted (searchPhone@64 +3.30%, @16M +3.12%)
- 2026-02-14 14:14 | B3.1-DECISION | strict guard violated after rerun; reverted code changes | revert

## B3.2 Attempt (optimize ALT/ALT_MATCH loop micro-tightening)
- 2026-02-14 14:15 | B3.2-TEST | compile + targeted parser/compiler suites passed | pass
- 2026-02-14 14:23 | B3.2-FOCUSED | jmh=/private/tmp/parser-campaign-v2-B3.2-focused-20260214-135253/{jmh-focused.txt,jmh-phases.txt} | no incremental win vs kept state; rejected before guard promotion
- 2026-02-14 14:24 | B3.2-DECISION | rejected (non-promotable focused result); reverted code changes | revert

## B4.1 Attempt (RegexpParser UTF-8 ASCII fast path)
- 2026-02-14 14:24 | B4.1-TEST | compile + targeted parser/compiler suites passed | pass
- 2026-02-14 14:31 | B4.1-FOCUSED | jmh=/private/tmp/parser-campaign-v2-B4.1-focused-20260214-140012/{jmh-focused.txt,jmh-phases.txt} | parseOnly -13.75%
- 2026-02-14 14:34 | B4.1-GUARD | jmh=/private/tmp/parser-campaign-v2-B4.1-guard-20260214-140637/jmh-guard.txt | no guarded regressions >2%
- 2026-02-14 14:34 | B4.1-DECISION | keep

## Queue (final)
- BASELINE: completed
- B1.1 ByteMapBuilder primitive ranges/map storage: kept
- B1.2 ByteMap recolor O(1) remap table: kept
- B1.3 ByteMap word-boundary precomputed ranges: kept
- B2.1 flatten temporary allocation removal: kept
- B2.2 flatten predecessor primitive storage: kept
- B2.3 flatten loop lookup/bounds tightening: reverted
- B3.1 optimize no-NOP quick skip: reverted
- B3.2 optimize queue/dispatch micro-tightening: reverted
- B4.1 parser UTF-8 ASCII quick scan: kept

## Final Summary
- Kept: B1.1, B1.2, B1.3, B2.1, B2.2, B4.1
- Reverted/rejected: B2.3, B3.1, B3.2
- Key retained impact (mini gates vs campaign baseline):
  - `compilePhaseCompileToProg`: consistently ~`-8%` to `-12%`
  - `compilePhaseRe2Compile`: consistently ~`-9%` to `-12%`
  - parser `parseOnly`: improved (B4.1 focused `-13.75%`)
- Guard status for retained state: no persistent `>2%` regressions after rerun policy.

## Promotion Full Gate (retained state)
- 2026-02-14 14:09 | FULLGATE-START | cpu-pre snapshots captured, jmh=/private/tmp/parser-campaign-v2-fullgate-20260214-140933/{jmh-focused-full.txt,jmh-phases-full.txt,jmh-guard-full.txt} | running
- 2026-02-14 14:54 | FULLGATE-END | completed successfully (focused + phases + guard) | pass
- 2026-02-14 14:55 | FULLGATE-RESULT | compilePhaseCompileToProg `3722.168 ns` (`-11.38%` vs baseline `4200.053`), compilePhaseRe2Compile `4350.807 ns` (`-11.15%` vs baseline `4896.754`) | keep
- 2026-02-14 14:55 | FULLGATE-GUARD | smallHttpPartialMatch `35.073 ns` (`-6.55%`), searchPhoneRe2 deltas: `8 -9.18%`, `64 -1.92%`, `512 -2.54%`, `4K -1.91%`, `32K -1.66%`, `256K -2.02%`, `2M -0.81%`, `16M -0.21%` | pass

## Post-Campaign Exploratory (2026-02-14)
- 2026-02-14 15:53 | X1-RE2-PREFIX-DEDUP-TEST | removed duplicate `partial.configurePrefixAccel(...)` in `Re2.build`; compile + targeted tests passed | pass
- 2026-02-14 15:59 | X1-RE2-PREFIX-DEDUP-FOCUSED | jmh=/private/tmp/parser-campaign-v2-re2-prefixdedup-20260214-155350/focused.txt | complete
- 2026-02-14 16:02 | X1-RE2-PREFIX-DEDUP-GUARD | jmh=/private/tmp/parser-campaign-v2-re2-prefixdedup-20260214-155350/guard.txt vs baseline /private/tmp/parser-campaign-v2-baseline-now-guard-20260214-154758/jmh-guard.txt | regressions across all guarded rows
- 2026-02-14 16:03 | X1-RE2-PREFIX-DEDUP-DECISION | reverted; key deltas: compileToProg +9.07%, re2Compile +5.65%, smallHttp +5.67%, searchPhone +2.27%..+5.58% | revert

## B2.4 Attempt (flatten computeHints scratch reuse)
- 2026-02-14 16:11 | B2.4-TEST | compile + `ProgCompileEquivalenceTest` passed | pass
- 2026-02-14 16:18 | B2.4-MINI | jmh=/private/tmp/parser-campaign-v2-B2.4-hintscratch-20260214-160605/{jmh-focused.txt,jmh-guard.txt,jmh-guard-rerun.txt} | compile improved but runtime guard unstable/noisy
- 2026-02-14 16:27 | B2.4-AB | jmh=/private/tmp/parser-campaign-v2-B2.4-hintscratch-ab-20260214-162342/{jmh-candidate.txt,jmh-baseline.txt} | mixed signal under high variance
- 2026-02-14 16:37 | B2.4-FULL | jmh=/private/tmp/parser-campaign-v2-B2.4-hintscratch-fullguard-20260214-162923/jmh-fullguard.txt | compileToProg -3.14%, re2Compile -6.42%, but smallHttp +7.94% and searchPhone 256K/2M/16M +3.50%/+4.12%/+5.22%
- 2026-02-14 16:37 | B2.4-DECISION | strict no-regression policy violated on full guard; reverted code changes | revert

## Additional Exploratory Attempts (2026-02-14)
- 2026-02-14 16:57 | B3.3-RUNE-CACHE-PRIMITIVE | replaced `HashMap<Long,Integer>` rune suffix cache with primitive open-address cache in `CompilerImpl` | test pass
- 2026-02-14 17:03 | B3.3-GATE | jmh=/private/tmp/parser-campaign-v2-B3.3-rune-cache-20260214-164606/{jmh-focused.txt,jmh-guard.txt} | regressions: compileToProg +7.93%, searchPhone +1.39%..+1.40% on larger guarded sizes
- 2026-02-14 17:03 | B3.3-DECISION | reverted (strict no-regression)

- 2026-02-14 17:18 | B3.4-INLINE-LITERAL | added inline byte-literal-string path in `Compiler.literalString` | test pass
- 2026-02-14 17:18 | B3.4-GATE | jmh=/private/tmp/parser-campaign-v2-B3.4-inline-literal-20260214-165611/{jmh-focused.txt,jmh-guard.txt} | broad regressions: compileToProg +10.78%, re2Compile +5.79%, searchPhone +2.49%..+5.23%, smallHttp +3.78%
- 2026-02-14 17:18 | B3.4-DECISION | reverted (strict no-regression)

- 2026-02-14 17:22 | B3.5-CHARCLASS-SINGLE | added single-range non-folded charClass fast path | test pass
- 2026-02-14 17:22 | B3.5-GATE | jmh=/private/tmp/parser-campaign-v2-B3.5-charclass-single-20260214-170627/{jmh-focused.txt,jmh-guard.txt} | regressions: compileToProg +5.00%, re2Compile +3.40%, searchPhone +2.56% @256K, +2.78% @2M
- 2026-02-14 17:22 | B3.5-DECISION | reverted (strict no-regression)

- 2026-02-14 17:28 | B2.4b-HINT-SCRATCH-RESET | retried `computeHints` scratch reuse with explicit color reset to preserve semantics | test pass
- 2026-02-14 17:33 | B2.4b-GATE | jmh=/private/tmp/parser-campaign-v2-B2.4b-hintscratch-20260214-171732/{jmh-focused.txt,jmh-guard.txt,jmh-guard-rerun.txt} | compile mixed, runtime regressions persisted on searchPhone (e.g., +4.38%..+4.73% large sizes) and smallHttp (+3.94%)
- 2026-02-14 17:33 | B2.4b-DECISION | reverted (strict no-regression)

## C5 Attempt (Simplifier Coalescer single-pass merge/filter)
- 2026-02-14 17:35 | C5-TEST | compile + targeted parser/compiler suites (`RegexpParserTest`, `UpstreamParseEscapeTest`, `UpstreamParseDumpTest`, `UpstreamCompileByteMapTest`, `UpstreamCompileMemoryBudgetTest`, `Re2AccessorsTest`, `ProgCompileEquivalenceTest`) passed | pass
- 2026-02-14 17:44 | C5-AB-GATE | cpu-pre/post captured, jmh=/private/tmp/parser-campaign-v2-C5-coalescer-20260214-173503/{baseline,candidate} with focused+guard profile | complete
- 2026-02-14 17:45 | C5-RESULT | `compilePhaseCompileToProg -3.99%` (3923.492 -> 3766.803 ns), `compilePhaseRe2Compile -0.45%`, focused `compileToProgFromParsed` improved across all 6 patterns (`-2.95%` to `-6.73%`), searchPhone guard improved at all sizes (`-2.21%` to `-8.91%`), `smallHttpPartialMatch +2.44%` (below rerun threshold) | keep
