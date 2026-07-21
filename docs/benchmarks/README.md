# Benchmark Documentation

[`ENGINEERING_CAMPAIGN.md`](ENGINEERING_CAMPAIGN.md) records the completed
iterative benchmarking and optimization campaign. Its results build internal
confidence but are not release claims.
[`ENGINEERING_RESULTS.md`](ENGINEERING_RESULTS.md) is the final engineering
ledger for that campaign, including retained and rejected experiments and the
remaining formal-qualification concerns.
[`ENGINE_RESEARCH.md`](ENGINE_RESEARCH.md) ranks transferable techniques from
other regex engines and records prototype acceptance constraints.
[`DFA_ABSOLUTE_POINTER_PLAN.md`](DFA_ABSOLUTE_POINTER_PLAN.md) records the
completed bounded qualification and acceptance of the forward compact-DFA
absolute-pointer representation.
[`REBAR_NATIVE_COMPARISON_PLAN.md`](REBAR_NATIVE_COMPARISON_PLAN.md) defines
the bounded current-engine comparison with official and pinned native RE2. Its
executed results are in
[`history/2026-07-16-rebar-native-comparison.md`](history/2026-07-16-rebar-native-comparison.md).
[`VECTOR_EXPLORATION.md`](VECTOR_EXPLORATION.md) records the bounded Vector API
exploration and its accepted fused literal scanner. The initial measurements
are in
[`history/2026-07-20-vector-literal-scanning.md`](history/2026-07-20-vector-literal-scanning.md),
and the rejected ranked-byte campaign is in
[`history/2026-07-21-packed-pair-selection.md`](history/2026-07-21-packed-pair-selection.md).

[`METHODOLOGY.md`](METHODOLOGY.md) is the current guide for designing, running,
and interpreting benchmarks.

[`QUALIFICATION_PLAN.md`](QUALIFICATION_PLAN.md) defines the formal Intel and
Graviton release campaign. It is an execution plan, not a results document.

The [`history/`](history/) directory contains dated results, gap analyses,
assembly notes, and campaign notebooks. Those files remain useful for
understanding earlier decisions and avoiding rejected experiments, but their
measurements are not current release qualification.

Formal qualification remains tracked in
[`RE2_TASKS.md`](../../RE2_TASKS.md). It will be run only after correctness and
public API work, using dedicated Intel and Arm machines and the comparator set
defined in [`TRINO.md`](../integrations/TRINO.md).
