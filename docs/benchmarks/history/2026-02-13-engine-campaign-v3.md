# Engine Autonomous Campaign v3 Log

> Historical record. This document is evidence, not current project status.
> See the repository-root `RE2_TASKS.md` for open work.

Start: 2026-02-13 17:05:01 PST
Branch: `user/dain/engine-24h-campaign-v3`
Policy: strict protect (>2% retained regression disallowed after rerun)
Memory policy: max `+64KB` retained incremental memory per compiled `Prog`
Duration budget: 24h

## Benchmark Config
- Mini gate: `-f 2 -wi 6 -i 4 -w 500ms -r 500ms`
- Full gate: `-f 3 -wi 10 -i 5 -w 1s -r 1s`

## CPU Contention Guard
- Before each benchmark command, check system CPU (`ps -Ao pid,pcpu,command | sort -k2 -nr | head`).
- If non-benchmark background load is high (single process >200% CPU or sustained UI/system contention), wait 60s and re-check.
- After each benchmark command, run the same CPU check.
- If post-run contention is high, wait 60s and re-check before launching the next benchmark.
- Record waits and notable offenders in `Decisions` for traceability.

## Now
2026-02-13 21:00 | CLOSEOUT | Campaign v3 closed (strict gate + stop condition reached)

## Queue
- P1.1 Easy2 foldcase prefix scan: failed/reverted
- P1.2 Easy2 foldcase single-byte handoff: failed/reverted
- P1.3 Easy2 foldcase DFA transition overhead: failed/reverted
- P2.1 BigFixed anchored boolean fast route: failed/reverted
- P2.2 BigFixed skip redundant cascade: failed/reverted
- P2.3 BigFixed narrowed cascade guard: failed/reverted
- P3.1 BitState no-submatch split: failed/reverted
- P3.2 BitState empty-width hoist when absent: failed/reverted
- P3.3 BitState guarded flattening for no-submatch capture-free: failed/reverted
- P4.1 OnePass nmatch==0 no-allocation path: failed/reverted
- P4.2 OnePass satisfy/empty flags reduction: failed/reverted
- P5.1 DFA forward hot loop branch/unroll micro-variants: pending
- P5.1 DFA forward hot loop branch/unroll micro-variants: failed/reverted
- P5.2 DFA reverse micro-variants: pending
- P6.1 Re2 anchored boolean-only cascade fast checks: failed/reverted
- P6.2 Re2 safeguard-preserving cascade tuning: skipped (no credible follow-up after repeated Re2-area failures)
- P7.1 Nfa capture path empty-width/stack micro-opt: skipped (deferred after stop condition)
- P7.2 Nfa capture thread arena micro-opt: skipped (deferred after stop condition)

## Decisions
- 2026-02-13 17:05 | V3-START | Branch created and v3 campaign log initialized | locked
- 2026-02-13 17:16 | BASELINE-COMPILE-TEST | `test-compile` + targeted correctness suite passed | locked
- 2026-02-13 17:47 | BASELINE-MINI | search=`/private/tmp/engine-v3-baseline-mini-search-20260213-170559.txt` nfa=`/private/tmp/engine-v3-baseline-mini-nfa-20260213-170953.txt` extra=`/private/tmp/engine-v3-baseline-mini-extra-sized-20260213-171057.txt` digits=`/private/tmp/engine-v3-baseline-mini-extra-digits-20260213-171057.txt` practical=`/private/tmp/engine-v3-baseline-mini-practical-20260213-171338.txt` parse=`/private/tmp/engine-v3-baseline-mini-parse-20260213-171338.txt` misc=`/private/tmp/engine-v3-baseline-mini-misc-20260213-171338.txt` csv=`/private/tmp/engine-v3-baseline-mini.csv` | locked
- 2026-02-13 17:48 | P1.1-IMPLEMENT | simplified ShiftDFA final-state position checks in `Prog.prefixAccelShiftDfa`; est mem delta/prog=`+0B` | running
- 2026-02-13 18:00 | P1.1-MINI | search=`/private/tmp/engine-v3-P1.1-mini-search-20260213-174851.txt` nfa=`/private/tmp/engine-v3-P1.1-mini-nfa-20260213-174851.txt` extra=`/private/tmp/engine-v3-P1.1-mini-extra-sized-20260213-174851.txt` digits=`/private/tmp/engine-v3-P1.1-mini-extra-digits-20260213-174851.txt` practical=`/private/tmp/engine-v3-P1.1-mini-practical-20260213-174851.txt` parse=`/private/tmp/engine-v3-P1.1-mini-parse-20260213-174851.txt` misc=`/private/tmp/engine-v3-P1.1-mini-misc-20260213-174851.txt` gc=`/private/tmp/engine-v3-P1.1-mini-easy2-gc-20260213-174851.txt` csv=`/private/tmp/engine-v3-P1.1-mini.csv` | evaluated
- 2026-02-13 18:01 | P1.1-DELTA | Easy2Dfa32K `-39.2%`, Easy2Dfa256K `-18.5%`, but regressions: AsciiMatch `+22.1%`, BigFixedDfa32K `+12.9%`, ParensDfa16M `+3.0%`, Easy1Re2 16M `+2.4%` | rerun-required
- 2026-02-13 18:03 | P1.1-RERUN | ascii=`/private/tmp/engine-v3-P1.1-rerun-misc-ascii-20260213-175937.txt` bigfixed=`/private/tmp/engine-v3-P1.1-rerun-bigfixeddfa-20260213-175937.txt` parens=`/private/tmp/engine-v3-P1.1-rerun-parensdfa-20260213-175937.txt` easy1re2=`/private/tmp/engine-v3-P1.1-rerun-easy1re2-20260213-175937.txt` csv=`/private/tmp/engine-v3-P1.1-rerun.csv` | evaluated
- 2026-02-13 18:05 | P1.1-DECISION | persistent regressions after rerun (AsciiMatch `+23.0%`, BigFixedDfa32K `+10.1%`, ParensDfa16M `+2.5%`) under strict gate; alloc snapshot Easy2Dfa `~24-26 B/op` | revert
- 2026-02-13 18:06 | P1.2-IMPLEMENT | added single-byte foldcase prefix-accel handoff route in `Dfa` with `Prog` single-byte foldcase scanner; est mem delta/prog=`+0B` | running
- 2026-02-13 18:14 | P1.2-MINI | search=`/private/tmp/engine-v3-P1.2-mini-search-20260213-180346.txt` nfa=`/private/tmp/engine-v3-P1.2-mini-nfa-20260213-180346.txt` extra=`/private/tmp/engine-v3-P1.2-mini-extra-sized-20260213-180346.txt` digits=`/private/tmp/engine-v3-P1.2-mini-extra-digits-20260213-180346.txt` practical=`/private/tmp/engine-v3-P1.2-mini-practical-20260213-180346.txt` parse=`/private/tmp/engine-v3-P1.2-mini-parse-20260213-180346.txt` misc=`/private/tmp/engine-v3-P1.2-mini-misc-20260213-180346.txt` gc=`/private/tmp/engine-v3-P1.2-mini-easy2-gc-20260213-180346.txt` csv=`/private/tmp/engine-v3-P1.2-mini.csv` | evaluated
- 2026-02-13 18:16 | P1.2-DECISION | strict gate failed (`11` points >2% regression, `5` >3%; notable: AsciiMatch `+23.1%`, BigFixedDfa32K `+14.4%`, Easy0Dfa16M `+5.1%`), despite Easy2 gains; alloc snapshot unchanged (Easy2Dfa `~24-26 B/op`, Easy2Re2 `48 B/op`) | revert
- 2026-02-13 18:35 | P2.1-IMPLEMENT | added anchored-prefix no-submatch fast route in `Re2.match` for `^literal$` / `^literal.*$` suffix shapes; tests added in `Re2MatchTest`; est mem delta/prog=`+0B` | running
- 2026-02-13 18:43 | P2.1-CORRECTNESS | `test-compile` + `UpstreamPrefixAccelTest,UpstreamDfaTest,DfaAnchoredSearchTest,DfaReverseSearchTest,Re2MatchTest` passed | pass
- 2026-02-13 18:56 | P2.1-MINI | search=`/private/tmp/engine-v3-P2.1-mini-20260213-184712/search_core.txt` nfa=`/private/tmp/engine-v3-P2.1-mini-20260213-184712/nfa_core.txt` extra=`/private/tmp/engine-v3-P2.1-mini-20260213-184712/extra_sized.txt` bigfixed=`/private/tmp/engine-v3-P2.1-mini-20260213-184712/extra_bigfixed.txt` digits=`/private/tmp/engine-v3-P2.1-mini-20260213-184712/extra_digits.txt` practical=`/private/tmp/engine-v3-P2.1-mini-20260213-184712/practical.txt` parse=`/private/tmp/engine-v3-P2.1-mini-20260213-184712/parse.txt` misc=`/private/tmp/engine-v3-P2.1-mini-20260213-184712/misc.txt` gc=`/private/tmp/engine-v3-P2.1-mini-20260213-184712/bigfixed_gc.txt` csv=`/private/tmp/engine-v3-P2.1-mini-20260213-184712/candidate.csv` | evaluated
- 2026-02-13 18:57 | P2.1-RERUN | regressions >3% rerun: ascii=`/private/tmp/engine-v3-P2.1-rerun-20260213-185701/ascii.txt` bigfixedDfa=`/private/tmp/engine-v3-P2.1-rerun-20260213-185701/bigfixeddfa.txt` csv=`/private/tmp/engine-v3-P2.1-rerun-20260213-185701/rerun.csv`; cpu contention noted (`contactsd`, `AddressBookManager`, `WindowServer`) | evaluated
- 2026-02-13 18:57 | P2.1-DECISION | target win observed (`BigFixedRe2` `-7.8%`, alloc unchanged), but protected point `BigFixedDfa32K` remained >2% slower after rerun (`+6.3%`); strict policy requires reject | revert
- 2026-02-13 18:58 | P2.2-IMPLEMENT | deferred ANCHOR_BOTH temp `int[2]` allocation in `Re2.match` until submatch engine fallback is required; est mem delta/prog=`+0B` | running
- 2026-02-13 18:59 | P2.2-CORRECTNESS | `test-compile` + `UpstreamPrefixAccelTest,UpstreamDfaTest,DfaAnchoredSearchTest,DfaReverseSearchTest,Re2MatchTest` passed | pass
- 2026-02-13 19:08 | P2.2-MINI | search=`/private/tmp/engine-v3-P2.2-mini-20260213-185912/search_core.txt` nfa=`/private/tmp/engine-v3-P2.2-mini-20260213-185912/nfa_core.txt` extra=`/private/tmp/engine-v3-P2.2-mini-20260213-185912/extra_sized.txt` bigfixed=`/private/tmp/engine-v3-P2.2-mini-20260213-185912/extra_bigfixed.txt` digits=`/private/tmp/engine-v3-P2.2-mini-20260213-185912/extra_digits.txt` practical=`/private/tmp/engine-v3-P2.2-mini-20260213-185912/practical.txt` parse=`/private/tmp/engine-v3-P2.2-mini-20260213-185912/parse.txt` misc=`/private/tmp/engine-v3-P2.2-mini-20260213-185912/misc.txt` gc=`/private/tmp/engine-v3-P2.2-mini-20260213-185912/bigfixed_gc.txt` csv=`/private/tmp/engine-v3-P2.2-mini-20260213-185912/candidate.csv` | evaluated
- 2026-02-13 19:14 | P2.2-RERUN | regressions >3% rerun under sustained OS contention (waited 5m for `mediaanalysisd`/`mds_stores`): ascii=`/private/tmp/engine-v3-P2.2-rerun-20260213-190855/ascii.txt` bigfixedDfa=`/private/tmp/engine-v3-P2.2-rerun-20260213-190855/bigfixeddfa.txt` csv=`/private/tmp/engine-v3-P2.2-rerun-20260213-190855/rerun.csv` | evaluated
- 2026-02-13 19:14 | P2.2-DECISION | target win observed (`BigFixedRe2` `-6.9%`, alloc unchanged), but protected point `BigFixedDfa32K` remained >2% slower after rerun (`+4.2%`); strict policy requires reject | revert
- 2026-02-13 19:24 | BASELINE-REFRESH | full mini control snapshot under current host load: search=`/private/tmp/engine-v3-baseline-refresh-mini-20260213-191503/search_core.txt` nfa=`/private/tmp/engine-v3-baseline-refresh-mini-20260213-191503/nfa_core.txt` extra=`/private/tmp/engine-v3-baseline-refresh-mini-20260213-191503/extra_sized.txt` bigfixed=`/private/tmp/engine-v3-baseline-refresh-mini-20260213-191503/extra_bigfixed.txt` digits=`/private/tmp/engine-v3-baseline-refresh-mini-20260213-191503/extra_digits.txt` practical=`/private/tmp/engine-v3-baseline-refresh-mini-20260213-191503/practical.txt` parse=`/private/tmp/engine-v3-baseline-refresh-mini-20260213-191503/parse.txt` misc=`/private/tmp/engine-v3-baseline-refresh-mini-20260213-191503/misc.txt` csv=`/private/tmp/engine-v3-baseline-refresh-mini-20260213-191503/candidate.csv` | locked-current-window
- 2026-02-13 19:25 | P3.1-IMPLEMENT | BitState no-submatch capture bookkeeping skip (`cap=null` when `nsubmatch==0`, CAPTURE undo bypass); est mem delta/prog=`+0B` | running
- 2026-02-13 19:25 | P3.1-CORRECTNESS | `test-compile` + `UpstreamBitStateSearchTest,UpstreamSearchDfaAgreementTest` passed | pass
- 2026-02-13 19:26 | P3.1-MINI | focused matrix: extra-bitstate=`/private/tmp/engine-v3-P3.1-mini-20260213-192559/extra_bitstate.txt` digits=`/private/tmp/engine-v3-P3.1-mini-20260213-192559/extra_digits.txt` parse=`/private/tmp/engine-v3-P3.1-mini-20260213-192559/parse_bitstate.txt` practical=`/private/tmp/engine-v3-P3.1-mini-20260213-192559/practical_re2.txt` gc=`/private/tmp/engine-v3-P3.1-mini-20260213-192559/success1_gc.txt` csv=`/private/tmp/engine-v3-P3.1-mini-20260213-192559/candidate.csv` | evaluated
- 2026-02-13 19:28 | P3.1-RERUN | rerun of key BitState targets: `Success1BitState`, `AltMatchBitState`, `DigitsBitState` via `/private/tmp/engine-v3-P3.1-rerun-20260213-192822/candidate.csv` | evaluated
- 2026-02-13 19:29 | P3.1-DECISION | persistent regressions after rerun vs refreshed baseline (`Success1BitState32K +4.8%`, `Success1BitState256K +6.2%`, `AltMatchBitState256K +6.6%`, `DigitsBitState +14.6%`); strict policy requires reject | revert
- 2026-02-13 19:31 | P4.1-IMPLEMENT | OnePass `nmatch==0` split to a dedicated `searchNoSubmatch(...)` path (removed cap/matchcap allocation+copy from boolean-only path); est mem delta/prog=`+0B` | running
- 2026-02-13 19:31 | P4.1-CORRECTNESS | `test-compile` + `UpstreamOnePassSearchTest,UpstreamSearchDfaAgreementTest` passed | pass
- 2026-02-13 19:35 | P4.1-MINI | focused matrix: onepass=`/private/tmp/engine-v3-P4.1-mini-20260213-193354/extra_altmatch_onepass.txt` digits=`/private/tmp/engine-v3-P4.1-mini-20260213-193354/extra_digits.txt` practical=`/private/tmp/engine-v3-P4.1-mini-20260213-193354/practical.txt` parse=`/private/tmp/engine-v3-P4.1-mini-20260213-193354/parse_phone.txt` gc=`/private/tmp/engine-v3-P4.1-mini-20260213-193354/altmatch_onepass_gc.txt` csv=`/private/tmp/engine-v3-P4.1-mini-20260213-193354/candidate.csv` | evaluated
- 2026-02-13 19:36 | P4.1-RERUN | rerun target under CPU guard (`max non-benchmark CPU pre-wait: 509.9%, post-wait: 395.6%`): onepass=`/private/tmp/engine-v3-P4.1-rerun-20260213-193600/altmatch_onepass.txt` csv=`/private/tmp/engine-v3-P4.1-rerun-20260213-193600/candidate.csv` | evaluated
- 2026-02-13 19:38 | P4.1-DECISION | persistent protected regression vs refreshed baseline after rerun (`AltMatchOnePass32K +14.6%`, `AltMatchOnePass256K +11.9%`); strict policy requires reject | revert
- 2026-02-13 19:41 | P3.2-IMPLEMENT | BitState split into `trySearchWithEmptyWidth(...)` / `trySearchNoEmptyWidth(...)`, selected by `prog.getInstCount(EMPTY_WIDTH)`; est mem delta/prog=`+0B` | running
- 2026-02-13 19:42 | P3.2-CORRECTNESS | `test-compile` + `UpstreamBitStateSearchTest,UpstreamSearchDfaAgreementTest` passed | pass
- 2026-02-13 19:43 | P3.2-MINI | focused matrix: extra-bitstate=`/private/tmp/engine-v3-P3.2-mini-20260213-193951/extra_bitstate.txt` digits=`/private/tmp/engine-v3-P3.2-mini-20260213-193951/extra_digits.txt` parse=`/private/tmp/engine-v3-P3.2-mini-20260213-193951/parse_bitstate.txt` practical=`/private/tmp/engine-v3-P3.2-mini-20260213-193951/practical_re2.txt` gc=`/private/tmp/engine-v3-P3.2-mini-20260213-193951/success1_gc.txt` csv=`/private/tmp/engine-v3-P3.2-mini-20260213-193951/candidate.csv`; cpu wait observed (`extra_digits-post`) | evaluated
- 2026-02-13 19:44 | P3.2-RERUN | rerun key protected points: `Success1BitState`, `AltMatchBitState`, `DigitsBitState` via `/private/tmp/engine-v3-P3.2-rerun-20260213-194327/candidate.csv` | evaluated
- 2026-02-13 19:45 | P3.2-DECISION | persistent regressions after rerun vs refreshed baseline (`AltMatchBitState32K +17.3%`, `AltMatchBitState256K +25.0%`, `Success1BitState32K +7.7%`, `Success1BitState256K +4.0%`); strict policy requires reject | revert
- 2026-02-13 19:49 | P3.3-IMPLEMENT | added guarded BitState no-submatch/capture-free instruction-array fast path (`Prog` optional `Inst[]` cache, 64KB cap) with dedicated `trySearchNoSubmatchCaptureFree(...)`; est mem delta/prog=`<=64KB` (guarded) | running
- 2026-02-13 19:49 | P3.3-CORRECTNESS | `test-compile` + `UpstreamBitStateSearchTest,UpstreamSearchDfaAgreementTest` passed | pass
- 2026-02-13 19:53 | P3.3-MINI-A | focused matrix: extra-bitstate=`/private/tmp/engine-v3-P3.3-mini-20260213-194747/extra_bitstate.txt` digits=`/private/tmp/engine-v3-P3.3-mini-20260213-194747/extra_digits.txt` parse=`/private/tmp/engine-v3-P3.3-mini-20260213-194747/parse_bitstate.txt` practical=`/private/tmp/engine-v3-P3.3-mini-20260213-194747/practical_re2.txt` gc=`/private/tmp/engine-v3-P3.3-mini-20260213-194747/success1_gc.txt` csv=`/private/tmp/engine-v3-P3.3-mini-20260213-194747/candidate.csv` | mixed (Success1 win, AltMatch regression)
- 2026-02-13 19:54 | P3.3-TUNE-B | narrowed guard: disable fast path when `ALT_MATCH` present; revalidated compile/tests | running
- 2026-02-13 19:58 | P3.3-MINI-B | focused matrix rerun with narrowed guard: `/private/tmp/engine-v3-P3.3b-mini-20260213-195045/candidate.csv` | mixed (AltMatch still >2% slower)
- 2026-02-13 19:59 | P3.3-TUNE-C | narrowed guard further: disable fast path when `matchesAnyString`; revalidated compile/tests | running
- 2026-02-13 19:59 | P3.3-MINI-C | focused matrix rerun with second guard: `/private/tmp/engine-v3-P3.3c-mini-20260213-195355/candidate.csv` | mixed (AltMatch still >2% slower)
- 2026-02-13 20:00 | P3.3-RERUN | required rerun of key protected points: `/private/tmp/engine-v3-P3.3c-rerun-20260213-195651/candidate.csv` | evaluated
- 2026-02-13 20:00 | P3.3-DECISION | persistent protected regressions after rerun (`AltMatchBitState32K +7.0%`, `AltMatchBitState256K +7.9%`) despite strong `Success1` gains; strict policy requires reject | revert
- 2026-02-13 20:02 | P4.2-IMPLEMENT | OnePass empty-flag caching in hot loop (single `Prog.emptyFlags` eval per byte when needed) and simplified end-of-input boundary check; est mem delta/prog=`+0B` | running
- 2026-02-13 20:02 | P4.2-CORRECTNESS | `test-compile` + `UpstreamOnePassSearchTest,UpstreamSearchDfaAgreementTest` passed | pass
- 2026-02-13 20:03 | P4.2-MINI | focused matrix: onepass=`/private/tmp/engine-v3-P4.2-mini-20260213-195953/extra_altmatch_onepass.txt` digits=`/private/tmp/engine-v3-P4.2-mini-20260213-195953/extra_digits.txt` practical=`/private/tmp/engine-v3-P4.2-mini-20260213-195953/practical.txt` parse=`/private/tmp/engine-v3-P4.2-mini-20260213-195953/parse_phone.txt` gc=`/private/tmp/engine-v3-P4.2-mini-20260213-195953/altmatch_onepass_gc.txt` csv=`/private/tmp/engine-v3-P4.2-mini-20260213-195953/candidate.csv` | evaluated
- 2026-02-13 20:04 | P4.2-RERUN | required rerun of `AltMatchOnePass` under CPU guard: `/private/tmp/engine-v3-P4.2-rerun-20260213-200155/candidate.csv` | evaluated
- 2026-02-13 20:04 | P4.2-DECISION | persistent protected regression after rerun (`AltMatchOnePass32K +9.8%`, `AltMatchOnePass256K +4.8%`); strict policy requires reject | revert
- 2026-02-13 20:05 | P1.3-IMPLEMENT | foldcase ShiftDFA transition-overhead tweak in `Prog.prefixAccelShiftDfa` (replace rare-path diff checks with direct final-state checks; precompute final state constant); est mem delta/prog=`+0B` | running
- 2026-02-13 20:06 | P1.3-CORRECTNESS | `test-compile` + `UpstreamPrefixAccelTest,UpstreamDfaTest,DfaAnchoredSearchTest,DfaReverseSearchTest` passed | pass
- 2026-02-13 20:09 | P1.3-MINI | focused DFA matrix: search=`/private/tmp/engine-v3-P1.3-mini-20260213-200400/search_core.txt` extra=`/private/tmp/engine-v3-P1.3-mini-20260213-200400/extra_sized.txt` misc=`/private/tmp/engine-v3-P1.3-mini-20260213-200400/misc_ascii.txt` practical=`/private/tmp/engine-v3-P1.3-mini-20260213-200400/practical_http.txt` csv=`/private/tmp/engine-v3-P1.3-mini-20260213-200400/candidate.csv`; cpu wait observed (`search_core-post`) | evaluated
- 2026-02-13 20:11 | P1.3-RERUN | required rerun subset: search=`/private/tmp/engine-v3-P1.3-rerun-20260213-201103/search_subset.txt` extra=`/private/tmp/engine-v3-P1.3-rerun-20260213-201103/extra_subset.txt` misc=`/private/tmp/engine-v3-P1.3-rerun-20260213-201103/misc_ascii.txt` csv=`/private/tmp/engine-v3-P1.3-rerun-20260213-201103/candidate.csv` | evaluated
- 2026-02-13 20:14 | P1.3-DECISION | no target Easy2 gain after rerun (`Easy2Dfa32K -0.2%`, `Easy2Dfa256K -0.1%`) and unstable non-target regressions in guard subset (`BigFixedDfa32K +4.1%`, `Easy0Dfa256K +5.2%`); strict policy requires reject | revert
- 2026-02-13 20:25 | BASELINE-REFRESH-2 | full mini control snapshot under current host load: search=`/private/tmp/engine-v3-baseline-refresh2-mini-20260213-201503/search_core.txt` nfa=`/private/tmp/engine-v3-baseline-refresh2-mini-20260213-201503/nfa_core.txt` extra=`/private/tmp/engine-v3-baseline-refresh2-mini-20260213-201503/extra_sized.txt` digits=`/private/tmp/engine-v3-baseline-refresh2-mini-20260213-201503/extra_digits.txt` practical=`/private/tmp/engine-v3-baseline-refresh2-mini-20260213-201503/practical.txt` parse=`/private/tmp/engine-v3-baseline-refresh2-mini-20260213-201503/parse.txt` misc=`/private/tmp/engine-v3-baseline-refresh2-mini-20260213-201503/misc.txt` csv=`/private/tmp/engine-v3-baseline-refresh2-mini-20260213-201503/candidate.csv` | locked-current-window
- 2026-02-13 20:26 | P2.3-IMPLEMENT | removed eager `ANCHOR_BOTH` scratch array allocation when `submatch == null` (preserve undersized-array fallback only); est mem delta/prog=`+0B` | running
- 2026-02-13 20:26 | P2.3-CORRECTNESS | `test-compile` + `UpstreamPrefixAccelTest,UpstreamDfaTest,DfaAnchoredSearchTest,DfaReverseSearchTest,Re2MatchTest` passed | pass
- 2026-02-13 20:33 | P2.3-MINI | focused matrix: search=`/private/tmp/engine-v3-P2.3-mini-20260213-202450/search_core.txt` bigfixed=`/private/tmp/engine-v3-P2.3-mini-20260213-202450/extra_bigfixed.txt` practical=`/private/tmp/engine-v3-P2.3-mini-20260213-202450/practical.txt` parse=`/private/tmp/engine-v3-P2.3-mini-20260213-202450/parse_phone.txt` misc=`/private/tmp/engine-v3-P2.3-mini-20260213-202450/misc_ascii.txt` csv=`/private/tmp/engine-v3-P2.3-mini-20260213-202450/candidate.csv` | evaluated
- 2026-02-13 20:34 | P2.3-RERUN | required rerun of large regressions: `/private/tmp/engine-v3-P2.3-rerun-20260213-203040/candidate.csv` | evaluated
- 2026-02-13 20:35 | P2.3-DECISION | no target gain (`BigFixedRe2 32K -0.7%` vs refresh-2 baseline) and persistent guard instability (`BigFixedDfa32K +5.4%` rerun); strict policy requires reject | revert
- 2026-02-13 20:36 | P5.1-IMPLEMENT | DFA Object-loop micro-variant: carry byte-class (`cls`) across inner-loop break to remove redundant class-map reload on sidecar transition lookup in `searchForward`/`searchForwardPrefixAccel`; est mem delta/prog=`+0B` | running
- 2026-02-13 20:36 | P5.1-CORRECTNESS | `test-compile` + `UpstreamPrefixAccelTest,UpstreamDfaTest,DfaAnchoredSearchTest,DfaReverseSearchTest` passed | pass
- 2026-02-13 20:41 | P5.1-MINI | broad guard matrix run: `/private/tmp/engine-v3-P5.1-mini-20260213-203242/candidate.csv` | fail (large regressions in `Misc` + BigFixedDfa)
- 2026-02-13 20:42 | P5.1-RERUN | required rerun of key regressions: `/private/tmp/engine-v3-P5.1-rerun-20260213-204059/candidate.csv` | fail confirmed
- 2026-02-13 20:42 | P5.1-DECISION | persistent severe regressions (`AsciiMatch +127.7%`, `DotMatch +154.8%`, `BigFixedDfa32K +4.1%`); strict policy requires reject | revert
- 2026-02-13 20:53 | P6.1-IMPLEMENT | added anchored-boolean (`ANCHOR_BOTH`, `submatch == null`) DFA definitive-answer fast check in `Re2.match`; est mem delta/prog=`+0B` | running
- 2026-02-13 20:54 | P6.1-CORRECTNESS | `test-compile` + targeted DFA/Re2 agreement suite passed (one transient generated-test-sources issue resolved by serial rerun) | pass
- 2026-02-13 20:58 | P6.1-MINI | focused matrix: search=`/private/tmp/engine-v3-P6.1-mini-20260213-205304/search_core.txt` bigfixed=`/private/tmp/engine-v3-P6.1-mini-20260213-205304/extra_bigfixed.txt` practical=`/private/tmp/engine-v3-P6.1-mini-20260213-205304/practical.txt` parse=`/private/tmp/engine-v3-P6.1-mini-20260213-205304/parse_phone.txt` misc=`/private/tmp/engine-v3-P6.1-mini-20260213-205304/misc_ascii.txt` csv=`/private/tmp/engine-v3-P6.1-mini-20260213-205304/candidate.csv` | fail (broad >2% regressions)
- 2026-02-13 20:59 | P6.1-RERUN | required rerun subset: `/private/tmp/engine-v3-P6.1-rerun-20260213-205843/candidate.csv` | fail confirmed (`BigFixedDfa32K +10.8%`)
- 2026-02-13 21:00 | P6.1-DECISION | no measurable target gain and persistent protected regression after rerun; strict policy requires reject | revert
- 2026-02-13 21:00 | CAMPAIGN-STOP | stop condition met: repeated failures in Re2-cascade area (`P2.3`, `P6.1`) and no credible low-risk follow-up in remaining queue under current host variance | closed

## Memory Accounting Baseline
- Pending baseline table (to be filled after compile/test baseline):
  - DFA per-instance retained structures
  - Prog prefix/onepass/nosubmatch retained structures
  - BitState/Nfa/OnePass per-search transient allocations

## Next
- Campaign closed. No further candidates scheduled in v3.

## Final Summary
- Kept candidates: none
- Reverted candidates: `P1.1`, `P1.2`, `P1.3`, `P2.1`, `P2.2`, `P2.3`, `P3.1`, `P3.2`, `P3.3`, `P4.1`, `P4.2`, `P5.1`, `P6.1`
- Skipped/deferred after stop condition: `P5.2`, `P6.2`, `P7.1`, `P7.2`
- Net code delta in branch after v3 run: none (only campaign log updates retained)
