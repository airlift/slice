# Parser/Compiler Campaign v1

> Historical record. This document is evidence, not current project status.
> See the repository-root `RE2_TASKS.md` for open work.

Start: 2026-02-13 22:18 PST  
Branch: `user/dain/parser-compiler-campaign-v1`  
Policy: strict no-regression (`>2%` persistent regression rejected after one rerun), utilities deferred.

## Benchmark Config
- Mini: `-f 2 -wi 6 -i 4 -w 500ms -r 500ms`
- Full: `-f 3 -wi 10 -i 5 -w 1s -r 1s`

## CPU Contention Guard
Before and after each benchmark run:
1. Capture top CPU processes snapshot.
2. If top non-benchmark process is high, wait 60s and recheck once.
3. Record pre/post snapshots in campaign artifacts and decisions log.

## Now
- BASELINE setup: compile gate, parser/compiler correctness suite, mini+full baseline captures.

## Queue
- BASELINE: running
- C1.1 Prog flatten pre-sizing/workspace reuse: pending
- C1.2 Prog optimize fast path: pending
- C1.3 Prog computeByteMap fast path: pending
- C2.1 Re2 lazy metadata extension: pending
- C2.2 Re2 compile duplicate-work elimination: pending
- C3.1 Parser escape-class caching extension: pending
- C3.2 Parser UTF-8 validation fast path: pending
- C4.1 Parse-extra SplitBig/SearchPhone shape path: pending

## Decisions
- 2026-02-13 22:18 | INIT | campaign initialized on `user/dain/parser-compiler-campaign-v1` | running

## Next
- Run baseline compile/test gates and baseline mini matrix.
- 2026-02-13 22:24 | BASELINE-CORRECTNESS | compile + parser/compiler suites passed | pass
- 2026-02-13 22:24 | BASELINE-MINI | cpu-pre=/private/tmp/parser-campaign-v1-baseline-mini-20260213-221901/cpu-pre-1.txt cpu-pre2=/private/tmp/parser-campaign-v1-baseline-mini-20260213-221901/cpu-pre-2.txt cpu-post=/private/tmp/parser-campaign-v1-baseline-mini-20260213-221901/cpu-post.txt jmh=/private/tmp/parser-campaign-v1-baseline-mini-20260213-221901/jmh-mini.txt csv=/private/tmp/parser-campaign-v1-baseline-mini-20260213-221901/mini.csv | captured
- 2026-02-13 22:31 | BASELINE-FULL | aborted at ~29% by request to shorten iteration cycle; switching to targeted mini gates and full-only-on-promotion | adjusted

## Fast-Gate Update (By Request)
- 2026-02-14 10:41 | PLAN-UPDATE | switched to targeted benchmark gates for iteration speed; full runs only for promoted candidates | active

### Tiered Benchmark Gates
- Tier 1 (per-candidate, default):
  - `BenchmarkRe2Practical.(compilePhaseParse|compilePhaseSimplify|compilePhaseCompileToProg|compilePhaseRe2Compile)`
  - `BenchmarkRe2Parse.(parse3DigitsRe2|parse3DigitsOnePass|parse3DigitsBitState|parse3DigitsNfa|parse3DigitsBacktrack)`
  - Config: `-f 2 -wi 5 -i 4 -w 400ms -r 400ms`
- Tier 2 (candidate promotion gate):
  - Tier 1 + `BenchmarkRe2Parse.(parseSplitBig1Re2|parseSplitBig2Re2|searchPhoneRe2)`
  - + `BenchmarkRe2Practical.(emptyPartialMatch|simplePartialMatch|httpPartialMatch|smallHttpPartialMatch)`
  - Config: `-f 2 -wi 6 -i 4 -w 500ms -r 500ms`
- Tier 3 (publish-quality, promoted-only):
  - Full parser/practical suite with standard config `-f 3 -wi 10 -i 5 -w 1s -r 1s`

### CPU Guard Note
- If top non-benchmark process is high before/after run, wait 60s and re-check once before accepting results.
- 2026-02-14 10:34 | BASELINE-TIER1 | cpu-pre=/private/tmp/parser-campaign-v1-tier1-baseline-20260213-223309/cpu-pre-1.txt cpu-pre2=/private/tmp/parser-campaign-v1-tier1-baseline-20260213-223309/cpu-pre-2.txt cpu-post=/private/tmp/parser-campaign-v1-tier1-baseline-20260213-223309/cpu-post.txt jmh=/private/tmp/parser-campaign-v1-tier1-baseline-20260213-223309/jmh-tier1.txt csv=/private/tmp/parser-campaign-v1-tier1-baseline-20260213-223309/tier1.csv | captured

## Tier-1 Baseline Snapshot (ns/op)
- CompilePhaseParse: 394.3
- CompilePhaseSimplify: 955.9
- CompilePhaseCompileToProg: 4,631.3
- CompilePhaseRe2Compile: 5,152.7
- Parse3Digits RE2: 300.5
- Parse3Digits OnePass: 58.6
- Parse3Digits BitState: 304.0
- Parse3Digits NFA: 929.8
- Parse3Digits Backtrack: 228.3

## C1.1 Attempt (Prog flatten allocation/workspace)
- 2026-02-14 10:46 | C1.1-TEST | compile + targeted parser/compiler suites passed | pass
- 2026-02-14 10:47 | C1.1-TIER1 | jmh=/private/tmp/parser-campaign-v1-C1.1-tier1-20260213-223629/jmh-tier1.txt csv=/private/tmp/parser-campaign-v1-C1.1-tier1-20260213-223629/tier1.csv | compilePhaseCompileToProg -15.5%, compilePhaseRe2Compile -16.5%
- 2026-02-14 10:52 | C1.1-TIER2 | jmh=/private/tmp/parser-campaign-v1-C1.1-tier2-20260213-223821/jmh-tier2.txt csv=/private/tmp/parser-campaign-v1-C1.1-tier2-20260213-223821/tier2.csv | searchPhone@8 +7.4%, compilePhaseCompileToProg -13.1%
- 2026-02-14 10:56 | C1.1-NOISE | jmh=/private/tmp/parser-campaign-v1-C1.1-noisecheck-20260213-224541/jmh-noise.txt csv=/private/tmp/parser-campaign-v1-C1.1-noisecheck-20260213-224541/noise.csv | searchPhone@8 +5.9% persisted
- 2026-02-14 10:58 | C1.1-DECISION | strict guard violated (`searchPhoneRe2 textSize=8` >2% slower after rerun); reverted working-tree changes | revert

## Queue (updated)
- BASELINE: completed
- C1.1 Prog flatten pre-sizing/workspace reuse: reverted
- C1.2 Prog optimize fast path: pending
- C1.3 Prog computeByteMap fast path: pending
- C2.1 Re2 lazy metadata extension: pending
- C2.2 Re2 compile duplicate-work elimination: pending
- C3.1 Parser escape-class caching extension: pending
- C3.2 Parser UTF-8 validation fast path: pending
- C4.1 Parse-extra SplitBig/SearchPhone shape path: pending

## C1.2 Attempt (Prog optimize fast-path)
- 2026-02-14 11:00 | C1.2-TEST | compile + targeted parser/compiler suites passed | pass
- 2026-02-14 11:03 | C1.2-TIER1 | jmh=/private/tmp/parser-campaign-v1-C1.2-tier1-20260213-225449/jmh-tier1.txt csv=/private/tmp/parser-campaign-v1-C1.2-tier1-20260213-225449/tier1.csv | compilePhaseCompileToProg -8.4%, compilePhaseRe2Compile -3.7%
- 2026-02-14 11:08 | C1.2-TIER2 | jmh=/private/tmp/parser-campaign-v1-C1.2-tier2-20260213-225635/jmh-tier2.txt csv=/private/tmp/parser-campaign-v1-C1.2-tier2-20260213-225635/tier2.csv | regressions on searchPhone@8/+8.4%, searchPhone@256K/+9.2%, compilePhaseParse/+5.9%
- 2026-02-14 11:13 | C1.2-TIER2-RERUN | jmh=/private/tmp/parser-campaign-v1-C1.2-tier2-rerun-20260213-230134/jmh-tier2.txt csv=/private/tmp/parser-campaign-v1-C1.2-tier2-rerun-20260213-230134/tier2.csv | regressions persisted (searchPhone@8 +5.9%, compilePhaseParse +7.1%, parse3DigitsRe2 +3.4%)
- 2026-02-14 11:14 | C1.2-DECISION | strict guard violated after rerun; reverted working-tree changes | revert

## Queue (updated)
- BASELINE: completed
- C1.1 Prog flatten pre-sizing/workspace reuse: reverted
- C1.2 Prog optimize fast path: reverted
- C1.3 Prog computeByteMap fast path: running
- C2.1 Re2 lazy metadata extension: pending
- C2.2 Re2 compile duplicate-work elimination: pending
- C3.1 Parser escape-class caching extension: pending
- C3.2 Parser UTF-8 validation fast path: pending
- C4.1 Parse-extra SplitBig/SearchPhone shape path: pending

## C1.3 Attempt (Prog computeByteMap fast path)
- 2026-02-14 11:16 | C1.3-TEST | compile + targeted parser/compiler suites passed | pass
- 2026-02-14 11:20 | C1.3-TIER1 | jmh=/private/tmp/parser-campaign-v1-C1.3-tier1-20260213-230853/jmh-tier1.txt csv=/private/tmp/parser-campaign-v1-C1.3-tier1-20260213-230853/tier1.csv | broad gains (compilePhaseParse -14.2%, compileToProg -4.0%)
- 2026-02-14 11:24 | C1.3-TIER2 | jmh=/private/tmp/parser-campaign-v1-C1.3-tier2-20260213-231357/jmh-tier2.txt csv=/private/tmp/parser-campaign-v1-C1.3-tier2-20260213-231357/tier2.csv | regressions vs baseline on compilePhaseParse +11.8%, compilePhaseRe2Compile +4.2%, searchPhone@8 +5.2%
- 2026-02-14 11:27 | C1.3-NOISE | jmh=/private/tmp/parser-campaign-v1-C1.3-noisecheck-20260213-231836/jmh.txt csv=/private/tmp/parser-campaign-v1-C1.3-noisecheck-20260213-231836/out.csv | regressions persisted (compilePhaseParse +3.3%, searchPhone@8 +4.0%)
- 2026-02-14 11:28 | C1.3-DECISION | strict guard violated after rerun; reverted working-tree changes | revert

## Queue (updated)
- BASELINE: completed
- C1.1 Prog flatten pre-sizing/workspace reuse: reverted
- C1.2 Prog optimize fast path: reverted
- C1.3 Prog computeByteMap fast path: reverted
- C2.1 Re2 lazy metadata extension: running
- C2.2 Re2 compile duplicate-work elimination: pending
- C3.1 Parser escape-class caching extension: pending
- C3.2 Parser UTF-8 validation fast path: pending
- C4.1 Parse-extra SplitBig/SearchPhone shape path: pending
- 2026-02-14 11:31 | C1.3-DECISION | strict guard violated after focused rerun; reverted working-tree changes | revert

## Queue (updated)
- BASELINE: completed
- C1.1 Prog flatten pre-sizing/workspace reuse: reverted
- C1.2 Prog optimize fast path: reverted
- C1.3 Prog computeByteMap fast path: reverted
- C2.1 Re2 lazy metadata extension: no-op (already lazy)
- C2.2 Re2 compile duplicate-work elimination: pending
- C3.1 Parser escape-class caching extension: running
- C3.2 Parser UTF-8 validation fast path: pending
- C4.1 Parse-extra SplitBig/SearchPhone shape path: pending

## C3.1 Attempt (Parser escape-class caching)
- 2026-02-14 11:36 | C3.1-TEST | parser/compiler correctness suites passed after foldcase fix | pass
- 2026-02-14 11:40 | C3.1-TIER1 | jmh=/private/tmp/parser-campaign-v1-C3.1-tier1-20260213-232353/jmh-tier1.txt csv=/private/tmp/parser-campaign-v1-C3.1-tier1-20260213-232353/tier1.csv | broad gains
- 2026-02-14 11:45 | C3.1-TIER2 | jmh=/private/tmp/parser-campaign-v1-C3.1-tier2-20260213-232535/jmh-tier2.txt csv=/private/tmp/parser-campaign-v1-C3.1-tier2-20260213-232535/tier2.csv | regressions on compilePhaseParse +6.7%, emptyPartialMatch +16.2%
- 2026-02-14 11:47 | C3.1-NOISE | jmh=/private/tmp/parser-campaign-v1-C3.1-noisecheck-20260213-233017/jmh.txt csv=/private/tmp/parser-campaign-v1-C3.1-noisecheck-20260213-233017/out.csv | regressions persisted (compilePhaseParse +4.3%, searchPhone@8 +3.0%)
- 2026-02-14 11:48 | C3.1-DECISION | strict guard violated after rerun; reverted working-tree changes | revert

## Queue (updated)
- BASELINE: completed
- C1.1 Prog flatten pre-sizing/workspace reuse: reverted
- C1.2 Prog optimize fast path: reverted
- C1.3 Prog computeByteMap fast path: reverted
- C2.1 Re2 lazy metadata extension: no-op (already lazy)
- C2.2 Re2 compile duplicate-work elimination: pending
- C3.1 Parser escape-class caching extension: reverted
- C3.2 Parser UTF-8 validation fast path: running
- C4.1 Parse-extra SplitBig/SearchPhone shape path: pending

## C3.2 Attempt (Parser UTF-8 validation fast path)
- 2026-02-13 23:35 | C3.2-TIER1 | jmh=/private/tmp/parser-campaign-v1-C3.2-tier1-20260213-233331/jmh-tier1.txt csv=/private/tmp/parser-campaign-v1-C3.2-tier1-20260213-233331/tier1.csv | broad compile/parse gains
- 2026-02-13 23:40 | C3.2-TIER2 | cpu-pre=/private/tmp/parser-campaign-v1-C3.2-tier2-20260213-233514/cpu-pre-1.txt cpu-pre2=/private/tmp/parser-campaign-v1-C3.2-tier2-20260213-233514/cpu-pre-2.txt jmh=/private/tmp/parser-campaign-v1-C3.2-tier2-20260213-233514/jmh-tier2.txt csv=/private/tmp/parser-campaign-v1-C3.2-tier2-20260213-233514/tier2.csv | only protected regression searchPhone@8 +2.47%
- 2026-02-13 23:41 | C3.2-NOISE | cpu-pre=/private/tmp/parser-campaign-v1-C3.2-noisecheck-20260213-234121/cpu-pre-1.txt cpu-pre2=/private/tmp/parser-campaign-v1-C3.2-noisecheck-20260213-234121/cpu-pre-2.txt cpu-post=/private/tmp/parser-campaign-v1-C3.2-noisecheck-20260213-234121/cpu-post.txt jmh=/private/tmp/parser-campaign-v1-C3.2-noisecheck-20260213-234121/jmh-noise.txt csv=/private/tmp/parser-campaign-v1-C3.2-noisecheck-20260213-234121/noise.csv | searchPhone@8 +2.22% persisted
- 2026-02-13 23:41 | C3.2-DECISION | strict guard violated (>2% persistent regression at searchPhone@8); reverted working-tree changes | revert

## Queue (updated)
- BASELINE: completed
- C1.1 Prog flatten pre-sizing/workspace reuse: reverted
- C1.2 Prog optimize fast path: reverted
- C1.3 Prog computeByteMap fast path: reverted
- C2.1 Re2 lazy metadata extension: no-op (already lazy)
- C2.2 Re2 compile duplicate-work elimination: running
- C3.1 Parser escape-class caching extension: reverted
- C3.2 Parser UTF-8 validation fast path: reverted
- C4.1 Parse-extra SplitBig/SearchPhone shape path: pending

## C2.2 Attempt (Re2 default-constructor allocation removal)
- 2026-02-13 23:44 | C2.2-TEST | compile + parser/compiler/re2 suites passed | pass
- 2026-02-13 23:45 | C2.2-TIER1 | cpu-pre=/private/tmp/parser-campaign-v1-C2.2-tier1-20260213-234413/cpu-pre-1.txt cpu-pre2=/private/tmp/parser-campaign-v1-C2.2-tier1-20260213-234413/cpu-pre-2.txt cpu-post=/private/tmp/parser-campaign-v1-C2.2-tier1-20260213-234413/cpu-post.txt jmh=/private/tmp/parser-campaign-v1-C2.2-tier1-20260213-234413/jmh-tier1.txt csv=/private/tmp/parser-campaign-v1-C2.2-tier1-20260213-234413/tier1.csv | promoted
- 2026-02-13 23:50 | C2.2-TIER2 | cpu-pre=/private/tmp/parser-campaign-v1-C2.2-tier2-20260213-234546/cpu-pre-1.txt cpu-pre2=/private/tmp/parser-campaign-v1-C2.2-tier2-20260213-234546/cpu-pre-2.txt cpu-post=/private/tmp/parser-campaign-v1-C2.2-tier2-20260213-234546/cpu-post.txt jmh=/private/tmp/parser-campaign-v1-C2.2-tier2-20260213-234546/jmh-tier2.txt csv=/private/tmp/parser-campaign-v1-C2.2-tier2-20260213-234546/tier2.csv | protected regression searchPhone@8 +2.96%
- 2026-02-13 23:50 | C2.2-NOISE | cpu-pre=/private/tmp/parser-campaign-v1-C2.2-noisecheck-20260213-235018/cpu-pre-1.txt cpu-pre2=/private/tmp/parser-campaign-v1-C2.2-noisecheck-20260213-235018/cpu-pre-2.txt cpu-post=/private/tmp/parser-campaign-v1-C2.2-noisecheck-20260213-235018/cpu-post.txt jmh=/private/tmp/parser-campaign-v1-C2.2-noisecheck-20260213-235018/jmh-noise.txt csv=/private/tmp/parser-campaign-v1-C2.2-noisecheck-20260213-235018/noise.csv | searchPhone@8 +3.46% persisted
- 2026-02-13 23:51 | C2.2-DECISION | strict guard violated (>2% persistent regression); reverted working-tree changes | revert

## C4.1 Attempt (Re2 full-range subtext allocation fast path)
- 2026-02-13 23:51 | C4.1-TEST | compile + parser/compiler/re2 + search suites passed | pass
- 2026-02-13 23:58 | C4.1-TIER2 | cpu-pre=/private/tmp/parser-campaign-v1-C4.1-tier2-20260213-235133/cpu-pre-1.txt cpu-pre2=/private/tmp/parser-campaign-v1-C4.1-tier2-20260213-235133/cpu-pre-2.txt cpu-post=/private/tmp/parser-campaign-v1-C4.1-tier2-20260213-235133/cpu-post.txt jmh=/private/tmp/parser-campaign-v1-C4.1-tier2-20260213-235133/jmh-tier2.txt csv=/private/tmp/parser-campaign-v1-C4.1-tier2-20260213-235133/tier2.csv | no protected >2% regressions (searchPhone@8 -0.49%)
- 2026-02-14 00:15 | C4.1-FULL | cpu-pre=/private/tmp/parser-campaign-v1-C4.1-full-20260213-235608/cpu-pre-1.txt cpu-pre2=/private/tmp/parser-campaign-v1-C4.1-full-20260213-235608/cpu-pre-2.txt cpu-post=/private/tmp/parser-campaign-v1-C4.1-full-20260213-235608/cpu-post.txt jmh=/private/tmp/parser-campaign-v1-C4.1-full-20260213-235608/jmh-full.txt csv=/private/tmp/parser-campaign-v1-C4.1-full-20260213-235608/full.csv | completed
- 2026-02-14 00:15 | C4.1-DECISION | kept (passes mini strict gate; full gate run completed with stable practical/parse/search rows) | keep

## Queue (final)
- BASELINE: completed
- C1.1 Prog flatten pre-sizing/workspace reuse: reverted
- C1.2 Prog optimize fast path: reverted
- C1.3 Prog computeByteMap fast path: reverted
- C2.1 Re2 lazy metadata extension: no-op (already lazy)
- C2.2 Re2 compile duplicate-work elimination: reverted
- C3.1 Parser escape-class caching extension: reverted
- C3.2 Parser UTF-8 validation fast path: reverted
- C4.1 Parse-extra SplitBig/SearchPhone shape path: kept

## Final Summary (Current Pass)
- Kept: C4.1 (`Re2.match` full-range subtext allocation fast path).
- Reverted: C1.1, C1.2, C1.3, C2.2, C3.1, C3.2.
- No-op: C2.1 (already lazy in current code).

## Campaign Closeout

- End: 2026-02-14 00:15 PST
- Stop condition: queue exhausted with one retained candidate and all remaining candidates either reverted by strict guard or identified as no-op.
- Retained code change:
  - `1dad0f5` (`/Users/dain/work/airlift/slice/src/main/java/io/airlift/slice/re2/Re2.java`)
- Campaign outcomes/docs:
  - `docs/benchmarks/history/2026-02-14-optimization-log.md` (Run 014 section)
  - `docs/benchmarks/history/2026-02-14-results.md` (parser/practical refresh in this pass)
- Benchmark artifacts (campaign run):
  - `/private/tmp/parser-campaign-v1-C4.1-full-20260213-235608`
  - `/private/tmp/parser-campaign-v1-C4.1-tier2-20260213-235133`
  - `/private/tmp/parser-campaign-v1-C2.2-tier2-20260213-234546`
  - `/private/tmp/parser-campaign-v1-C3.2-tier2-20260213-233514`

## Post-Closeout Results Refresh

- 2026-02-14 00:28 | RESULTS-REFRESH-START | started full-config results sweep for all sections represented in `RESULTS.md`.
- 2026-02-14 02:12 | RESULTS-REFRESH-PARTIAL | completed `Search`, `SearchNfa`, `FullMatch`, `Practical`, `Parse`; `SearchExtra` full default-size run was projected to run mostly non-published rows.
- 2026-02-14 02:21 | RESULTS-REFRESH-TAIL | restarted tail with full config on published `SearchExtra`/`Misc` rows only, then merged with already-complete class outputs.
- 2026-02-14 02:43 | RESULTS-REFRESH-DONE | produced merged CSV and updated the historical results document.

- Artifacts:
  - partial sweep dir: `/private/tmp/results-refresh-targeted-20260214-002847`
  - tail sweep dir: `/private/tmp/results-refresh-tail-20260214-022120`
  - merged CSV used for docs update: `/private/tmp/results-refresh-tail-20260214-022120/java-targeted-full.csv`
