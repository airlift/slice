# Engine 24h Campaign Log

> Historical record. This document is evidence, not current project status.
> See the repository-root `RE2_TASKS.md` for open work.

Start: 2026-02-13 09:38:24 PST
Branch: `user/dain/engine-24h-campaign`
Policy: strict protect (>2% retained regression disallowed after rerun)

## Now
2026-02-13 09:39 | INIT | establishing baseline compile/tests + mini/full benchmark snapshots

## Queue
- A1 closure table scaffolding + fallback: pending
- A2 closure enqueue (no-capture): pending
- A3 closure enqueue (capture-op path): pending
- B1 hasEmptyWidthOps flag: pending
- B2 compute empty flags once per p: pending
- B3 word-boundary fast path: pending
- C1 BitState flattening: pending
- C2 BitState no-submatch fast path: pending
- C3 BitState prefix/empty-width hoisting: pending
- D1 Re2 threshold tuning: pending
- D2 Re2 guardrail assertions/tests: pending
- E1 DFA single-byte scan alternative: pending
- E2 DFA scan/dispatch simplification: pending

## Decisions
- (none yet)

## Next
- Run compile + targeted correctness baseline
- Run mini benchmark baseline
- Run full benchmark baseline (campaign scope)
2026-02-13 09:46 | BASELINE-MINI | search=/tmp/jmh-mini-baseline-search-20260213-093930.txt nfa=/tmp/jmh-mini-baseline-nfa-20260213-094328.txt integration=/tmp/jmh-mini-baseline-integration-20260213-094429.txt | captured

## Decisions
- BASELINE-MINI | captured protected matrix with short config | keep as promotion reference

## Next
- Run full baseline snapshot (standard JMH config) for protected matrix
2026-02-13 10:13 | BASELINE-FULL | search=/private/tmp/jmh-full-baseline-search-20260213-094628.txt nfa=/private/tmp/jmh-full-baseline-nfa-20260213-100218.txt integration=/private/tmp/jmh-full-baseline-integration-20260213-100614.txt csv=/private/tmp/engine-campaign-full-baseline.csv | captured
2026-02-13 10:13 | BASELINE-REF | Easy1DFA16M=2,410,719ns HardDFA16M=27,872,212ns ParensDFA16M=28,655,902ns HardNFA256K=4,446,413ns ParensNFA256K=6,845,196ns ParseSplitHardRe2=236.5ns PracticalHttp=209.1ns | locked

## Decisions
- BASELINE-FULL | captured standard-profile protected matrix | keep as final acceptance reference

## Next
- A1 implementation: add no-submatch closure table scaffolding + 64KB cap + fallback flag (no behavior switch)
2026-02-13 10:24 | A1 | mini search=/private/tmp/jmh-mini-A1-search-20260213-101616.txt nfa=/private/tmp/jmh-mini-A1-nfa-20260213-102020.txt integration=/private/tmp/jmh-mini-A1-integration-20260213-102121.txt csv=/private/tmp/engine-campaign-mini-A1.csv | gate pass (0 points >2% regression)

## Decisions
- A1 | closure scaffolding + 64KB cap + fallback metadata added, no runtime switch; mini gate: no protected regressions | keep

## Next
- A2 implementation: closure-driven enqueue for no-capture no-submatch path
2026-02-13 10:42 | A2 | attempts=/private/tmp/jmh-mini-A2-search-20260213-102535.txt,/private/tmp/jmh-mini-A2c-nfa-20260213-103833.txt,/private/tmp/jmh-mini-A2c-integration-20260213-103935.txt rerun=/private/tmp/jmh-mini-A2c-hard-rerun-20260213-104148.txt | fail (HardNFA256K +2.7% rerun) -> reverted

## Decisions
- A2 | closure-driven no-capture enqueue regressed HardNFA256K beyond strict threshold after rerun; commit `ba0249b` reverted by `be63592` | revert

## Next
- A3 skipped (depends on A2 path behavior); move to Bundle B
- B1 implementation: add `hasEmptyWidthOps` and skip empty-width logic when absent
2026-02-13 10:50 | B1 | search=/private/tmp/jmh-mini-B1-search-20260213-104354.txt nfa=/private/tmp/jmh-mini-B1-nfa-20260213-104759.txt rerun=/private/tmp/jmh-mini-B1-nfa-rerun-20260213-104909.txt | fail (Hard/Medium/Easy1 NFA >2% after rerun) -> reverted

## Decisions
- B1 | empty-width split increased NFA cost (likely code-size/inlining side effect); commit `b20f599` reverted by `a595bc4` | revert

## Next
- B2 implementation: compute empty boundary flags once per `p` in no-submatch search loop and thread through enqueue paths
2026-02-13 11:00 | B2 | nfa=/private/tmp/jmh-mini-B2-nfa-20260213-105439.txt rerun=/private/tmp/jmh-mini-B2-nfa-rerun-hard-medium-20260213-110022.txt | fail (HardNFA256K +4.17%, rerun +2.33%; Medium rerun +0.58%) -> reverted

## Decisions
- B2 | empty-flags-once-per-enqueue regressed HardNFA256K beyond strict threshold after rerun; commit `2929e4f` reverted by `af93e4a` | revert

## Next
- B3 implementation: word-boundary fast path helpers for no-submatch NFA empty-width checks
2026-02-13 11:31 | B3 | mini=/private/tmp/jmh-mini-B3-nfa-20260213-110215.txt rerun=/private/tmp/jmh-mini-B3-nfa-rerun-easy0-262144-20260213-110744.txt full=/private/tmp/jmh-full-B3-nfa-20260213-110811.txt | pass (rerun Easy0NFA256K -0.76%; full 256K deltas: Easy0 -2.88%, Easy1 -0.05%, Hard -1.36%, Medium -1.43%, Parens -1.46%)

## Decisions
- B3 | no-submatch word-boundary fast path accepted; clears strict mini/full gates with no protected regressions and improves Easy0NFA256K on full gate; commit `8039334` kept | keep

## Next
- C1 implementation: BitState flattened instruction access in hot loop
2026-02-13 11:42 | C1 | baseline=/private/tmp/jmh-mini-C1-baseline-*-20260213-113353.txt current=/private/tmp/jmh-mini-C1-current-*-20260213-113728.txt rerun=/private/tmp/jmh-mini-C1-current-digits-rerun-20260213-114113.txt | fail (searchDigitsBitState +27.4%, rerun +23.7%) -> reverted

## Decisions
- C1 | flattened BitState inst tables improved Success1 but caused large persistent regression on searchDigitsBitState; commit `d3ef421` reverted by `b14302c` | revert

## Next
- C2 implementation: BitState no-submatch fast path (`submatch == null`) while preserving capture path semantics
2026-02-13 11:55 | C2 | mini current=/private/tmp/jmh-mini-C2-current-*-20260213-114329.txt rerun=/private/tmp/jmh-mini-C2-rerun-simplePartial-20260213-114722.txt full-ab=/private/tmp/jmh-full-C2-{baseline,current}-bitstate-20260213-114814.txt full-rerun=/private/tmp/jmh-full-C2-rerun-alt-digits-20260213-115322.txt | fail (searchDigitsBitState full +2.87%, rerun +2.94%) -> reverted

## Decisions
- C2 | no-submatch BitState path improved searchSuccess1 but produced persistent full-gate regression on searchDigitsBitState; commit `08822ec` reverted by `0e1883e` | revert

## Next
- C3 implementation: BitState prefix-accel and empty-width check hoists in hot loop (conservative, semantic no-op)
2026-02-13 11:58 | C3 | mini bitscan=/private/tmp/jmh-mini-C3-current-bitscan-20260213-115534.txt digits=/private/tmp/jmh-mini-C3-current-digits-20260213-115534.txt rerun=/private/tmp/jmh-mini-C3-rerun-digits-20260213-115807.txt | fail (searchDigitsBitState +2.48%, rerun +2.75%) -> reverted

## Decisions
- C3 | hoisting prefix/empty-width checks in BitState improved alt-match but regressed searchDigitsBitState beyond strict threshold on rerun; commit `578f205` reverted by `fce3008` | revert

## Next
- Move to Bundle D (Re2 engine-cascade threshold tuning) with conservative threshold sweep and DFA/BitState correctness tests
2026-02-13 12:02 | D1 | integration=/private/tmp/jmh-mini-D1-current-integration-20260213-115954.txt rerun=/private/tmp/jmh-mini-D1-rerun-http-smallhttp-20260213-120144.txt | fail (smallHttpPartialMatch +28.4%, rerun +29.5%) -> reverted

## Decisions
- D1 | threshold sweep (unanchored multi-cap 64->96, anchored tiny 16->32) regressed practical small-http path severely; commit `aa24299` reverted by `6ff3fc8` | revert

## Next
- E1 implementation: alternative DFA single-byte prefix scan kernel for high false-positive workloads (Easy1)
2026-02-13 12:08 | E1 | search-mini=/private/tmp/jmh-mini-E1-current-search-20260213-120358.txt | fail (Easy1Dfa 256K +9.2%, 16M +27.0%; broad regressions) -> reverted

## Decisions
- E1 | inline restart/prefix rescan in single-byte DFA loop regressed Easy1 and several other DFA points; commit `368fe49` reverted by `1c22536` | revert

## Next
- E2 implementation: simplify single-byte scan-to-DFA handoff branches without changing loop structure
2026-02-13 12:12 | E2 | current=/private/tmp/jmh-mini-E2-current-dfa-20260213-120907.txt baseline-ab=/private/tmp/jmh-mini-E2-baseline-dfa-20260213-121059.txt | fail (A/B: Easy0 +4-6%, Hard +4-6%, Parens +4-5%) -> reverted

## Decisions
- E2 | branch-condition simplification looked benign but A/B showed broad DFA regressions; commit `6c14790` reverted by `f643229` | revert

## Next
- Campaign pause point: all queued bundles attempted; keep only prior accepted `A1` and `B3`; no additional wins from C/D/E passes
