# Code Cleanup Campaign

> Historical record. This document is evidence, not current project status.
> See the repository-root `RE2_TASKS.md` for open work.

Start: 2026-02-14 17:55 PST  
Branch: `user/dain/code-cleanup-phase1`

## Goal
Improve readability and maintainability while preserving behavior and performance.

## Guardrails
1. Phase 1 is naming-only and simple data-shape cleanup (records where safe).
2. No semantic changes in cleanup commits unless explicitly marked.
3. Every cleanup step must pass:
   - `./mvnw -q -DskipTests test-compile`
   - targeted tests for touched subsystem
4. For hot-path engine structural cleanup (Phase 3+), run targeted JMH guard before keep.

## Phases
1. Naming cleanup:
   - expand terse variable names (`re`, `sub`, `cc`, `rr`, etc.) to descriptive names.
   - prioritize parser/compiler and AST paths first.
2. Data-shape cleanup:
   - convert immutable data carriers to records where it improves clarity.
3. Structural cleanup:
   - split oversized methods into named helpers without changing algorithm behavior.
   - keep hot loops intact unless benchmark-protected.
4. API polish:
   - align public/internal naming with Java style conventions.

## Queue
- C0: Baseline cleanup scaffolding (this document) - `done`
- C1: Simplifier/Coalescer naming pass - `done`
- C2: Safe record conversion (`Dfa.SearchParams`) - `done`
- C3: Expand naming pass across parser/compiler (`RegexpParser`, `Compiler`, `Prog` non-hot helpers) - `done`
- C3b: Engine-wide naming normalization (`endMatch/submatchCount`, hot-loop variable clarity) - `running`
- C4: Structural decomposition for long methods in parser/compiler - `pending`
- C5: Engine structural decomposition with JMH guardrails - `pending`

## Decisions
- 2026-02-14 17:45 | C1 | Expanded coalescer variable names for clarity; preserved logic; compile/tests pending for this cleanup batch.
- 2026-02-14 17:46 | C2 | Converted `Dfa.SearchParams` from class to record; no behavior change expected; compile/tests pending for this cleanup batch.
- 2026-02-14 17:57 | C1+C2 | compile + targeted parser/compiler + DFA suites passed; kept.
- 2026-02-14 18:04 | C3.1 | `RegexpParser` naming cleanup: clarified top-level `parse(...)` local names, `ParseError` field names, and UTF-8 validator locals; compile + targeted suites passed.
- 2026-02-14 18:11 | C3.2 | `Compiler` naming cleanup in compile entry flow and anchor-strip start helper (`re/c/sre/all/sub` -> descriptive names); compile + targeted suites passed.
- 2026-02-14 18:38 | C3b.1 | Bulk identifier normalization across engine core: `endmatch` -> `endMatch`, `nsubmatch` -> `submatchCount` (`Dfa`, `Nfa`, `BitState`, `Backtrack`, `Tester`) plus deep DFA hot-loop local renames (`p/s/trans/bmap` -> `position/stateOffset/transitions/byteMap`) in forward/prefix/backward paths.
- 2026-02-14 18:44 | C3b.2 | Additional naming sweep in `OnePass`, `Backtrack`, `BitState`, `Nfa`, `Re2`, `Re2Set`, `Prog`, and `RegexpGenerator` for remaining short locals in active paths.
- 2026-02-14 18:46 | C3b-VALIDATION | `./mvnw -q -DskipTests test-compile` and targeted engine/parser suite passed: `UpstreamDfaTest, UpstreamPrefixAccelTest, DfaAnchoredSearchTest, DfaReverseSearchTest, UpstreamBitStateSearchTest, UpstreamSearchDfaAgreementTest, UpstreamNfaSearchTest, NfaNoSubmatchParityTest, RegexpParserTest, UpstreamCompileByteMapTest, UpstreamCompileMemoryBudgetTest`.
- 2026-02-14 18:49 | C3b-SMOKE | quick JMH smoke gate passed (`BenchmarkRe2Search.searchEasy0Dfa|searchEasy1Dfa|searchHardDfa`, `BenchmarkRe2SearchNfa.searchHardNfa|searchParensNfa`, 1 fork/3x200ms warmup/3x200ms measure) artifacts: `/private/tmp/code-cleanup-sweep1-search-smoke.txt`, `/private/tmp/code-cleanup-sweep1-nfa-smoke.txt`.

## Next
Continue C3b outlier cleanup (`Re2Set.step/emptyFlags`, `Compiler` and `Prog` residual terse locals), then start C4 structural decomposition (non-hot methods first, benchmark-guard hot paths).
