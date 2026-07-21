# Extended OnePass Capture Campaign

**Status:** Closed. The extended OnePass capture sidecar is retained.

**Baseline source commit:** `a59127f5b74443239d10201b0eed1cb068b55f3a`

This is internal engineering evidence, not publication qualification. The
campaign addressed the remaining Unicode capture-grep deficit without changing
the accepted DFA or BitState layouts.

## Question

The current native census placed
`curated/07-unicode-character-data/parse-line` at `1.459x/1.053x` native time on
Intel/Graviton. Line-pipeline decomposition established that line setup was less
than 0.7 ms while capture dominated the operation.

The exact Java/native route diagnostic found identical 78-instruction programs:

- both programs are structurally one-pass;
- the workload requests 16 groups including group zero;
- native RE2's packed transition format supports only five groups;
- both implementations therefore skipped the DFA and selected BitState before
  this change;
- both processed 34,924 successful capture calls over 1,878,780 bytes and
  produced result 558,784.

## Design

The original OnePass action remains a 32-bit upstream-compatible word. Programs
with capture slots beyond that word's capacity additionally compile parallel
64-bit capture masks for state matches and byte-class transitions. A separate
extended loop iterates only set capture bits. It supports up to 32 groups,
including group zero, and is charged to the existing OnePass memory budget.

Programs and requests within native RE2's limit retain the existing fields and
hot loop. Requests beyond the 64-slot sidecar retain the BitState/NFA cascade.

## Result

Elapsed-time ratios use lower-is-better convention. Public time is the mean of
the before/after medians.

| Architecture | Baseline Java | Extended Java | Change | Native | Java/native |
|---|---:|---:|---:|---:|---:|
| Intel | 30.582 ms | 11.028 ms | -63.9% | 19.044 ms | 0.579x |
| Graviton | 29.661 ms | 13.773 ms | -53.6% | 27.759 ms | 0.496x |

The retained Java implementation is therefore 42.1% faster than native RE2 on
Intel and 50.4% faster on Graviton for this complete Rebar operation. The direct
extended capture stage takes 10.845 ms on Intel and 13.219 ms in the Graviton
confirmation.

The unchanged reusable BitState control takes 30.404 ms on Intel and 29.573 ms
on the paired Graviton candidate host, consistent with the pre-change route and
showing that the improvement comes from selecting OnePass rather than an
unrelated integration change.

## Stability And Correctness

- Baseline route session: `20260719T193619Z-35711`.
- Paired candidate session: `20260719T195845Z-53093`.
- Graviton confirmation session: `20260719T200502Z-56646`.
- Intel candidate public drift was 0.046%.
- The initial Graviton candidate drifted 2.73%, so it was not used as final
  qualification evidence.
- The longer Graviton confirmation reduced public drift to 0.037%.
- Native public drift was 0.24% on Intel and 0.60% in the Graviton confirmation.
- Native diagnostics were compiled with `-O3 -DNDEBUG` and `-march=native` or
  `-mcpu=native`.
- Every route manifest agreed on program shape, work volume, and result. The
  only permitted candidate differences were Java OnePass eligibility and Java
  `ONE_PASS` versus native `BIT_STATE` selection.
- The complete RE2 selector passed after adding direct high-capture,
  partial-capture-buffer, and route tests.
- Every AWS host exited successfully and all temporary instances were removed.

## Decision

Retain the extended OnePass sidecar and route. The campaign used one paired
candidate and one Graviton-only confirmation, so no further candidate is opened
under this goal. Ruff logical regions and repeated group-zero boundary searches
remain separate questions.
