# Historical Benchmark Evidence

These documents record experiments and measurements from the dates in their
names. They describe the code, machines, branches, and names that existed at
the time.

Use them to understand retained optimizations, rejected approaches, and old
performance gaps. Do not use their queues as current work lists or their numbers
as release claims. Current work is in
[`RE2_TASKS.md`](../../../RE2_TASKS.md), and current methodology is in
[`METHODOLOGY.md`](../METHODOLOGY.md).

| Document | Historical purpose |
|---|---|
| [`2026-07-21-packed-pair-selection.md`](2026-07-21-packed-pair-selection.md) | Rejected ranked UTF-8 literal integration and the Intel generated-code stability gate |
| [`2026-07-20-vector-literal-scanning.md`](2026-07-20-vector-literal-scanning.md) | Accepted cross-platform fused vector literal scanner and cutoff qualification |
| [`2026-07-20-vector-literal-native-comparison.csv`](2026-07-20-vector-literal-native-comparison.csv) | Final Intel and Graviton rows against pinned host-tuned native RE2 |
| [`2026-07-20-joni-final-acceptance.md`](2026-07-20-joni-final-acceptance.md) | Final three-session, complete-matrix Intel and Graviton acceptance against Joni |
| [`2026-07-20-joni-final-qualified.csv`](2026-07-20-joni-final-qualified.csv) | Qualified row-level final Joni evidence |
| [`2026-07-20-joni-final-underqualified.csv`](2026-07-20-joni-final-underqualified.csv) | Rows excluded from final Joni aggregates by the strict bracket gate |
| [`2026-07-20-joni-sparse-boundary-campaign.md`](2026-07-20-joni-sparse-boundary-campaign.md) | Qualified bulk candidate scan that closes the long sparse Joni regression |
| [`2026-07-20-joni-sparse-boundary-qualified.csv`](2026-07-20-joni-sparse-boundary-qualified.csv) | Complete 64-row Intel and Graviton focused comparison against Joni |
| [`2026-07-20-candidate-qualification.md`](2026-07-20-candidate-qualification.md) | Candidate-scoped Intel and Graviton qualification, residual gaps, and release-gate disposition |
| [`2026-07-20-date-candidate-start-cursor.md`](2026-07-20-date-candidate-start-cursor.md) | Accepted bounded candidate-start cursor and remaining Intel Date gap |
| [`2026-07-20-logical-region-matcher.md`](2026-07-20-logical-region-matcher.md) | Accepted allocation-free matcher regions and remaining Ruff `tweaked` gap |
| [`2026-07-19-line-capture-integration.md`](2026-07-19-line-capture-integration.md) | Rejected offset-aware SWAR line discovery and classification of the remaining Ruff and Unicode gaps |
| [`2026-07-19-line-capture-integration.csv`](2026-07-19-line-capture-integration.csv) | Complete Intel and Graviton stage comparison for the line integration candidate |
| [`2026-07-19-group-zero-pipeline.md`](2026-07-19-group-zero-pipeline.md) | Complete span-pipeline decomposition isolating Date to repeated forward/reverse DFA boundary searches |
| [`2026-07-19-group-zero-pipeline.csv`](2026-07-19-group-zero-pipeline.csv) | Complete Intel and Graviton group-zero stage measurements |
| [`2026-07-19-current-native-census.md`](2026-07-19-current-native-census.md) | Current-source Intel and Graviton census with normal cases separated from extreme outliers |
| [`2026-07-19-current-traditional-native-comparison.csv`](2026-07-19-current-traditional-native-comparison.csv) | Complete 596-row traditional census with row-level confirmation and strict drift status |
| [`2026-07-19-current-rebar-native-comparison.csv`](2026-07-19-current-rebar-native-comparison.csv) | Complete 82-row Rebar census with row-level confirmation and strict drift status |
| [`2026-07-18-dfa-large-pointer-sidecar.md`](2026-07-18-dfa-large-pointer-sidecar.md) | Accepted budget-derived absolute-pointer sidecar without a size-based object handoff |
| [`2026-07-18-joni-memory-qualification.md`](2026-07-18-joni-memory-qualification.md) | Trino-shaped retained-memory and operation-throughput qualification against Joni |
| [`2026-07-18-dfa-cache-capacity-policy.md`](2026-07-18-dfa-cache-capacity-policy.md) | Accepted runtime-aware DFA accounting and 96 MiB default policy |
| [`2026-07-18-capture-engine-campaign.md`](2026-07-18-capture-engine-campaign.md) | Bounded NFA, OnePass, BitState, and public capture campaign with retained and rejected mechanisms |
| [`2026-07-18-capture-pipeline-campaign.md`](2026-07-18-capture-pipeline-campaign.md) | Capture/count route decomposition, reusable matcher workspaces, and bounded direct BitState integration |
| [`2026-07-18-capture-pipeline-native-comparison.csv`](2026-07-18-capture-pipeline-native-comparison.csv) | Exact affected-model Intel/Graviton native comparison after capture-pipeline integration |
| [`2026-07-18-capture-pipeline-audit-confirmation.csv`](2026-07-18-capture-pipeline-audit-confirmation.csv) | Post-audit exact-source Graviton qualification and Intel matrix rejected by strict native drift |
| [`2026-07-18-capture-engine-native-comparison.csv`](2026-07-18-capture-engine-native-comparison.csv) | Complete final 82-row Intel/Graviton Rebar comparison for the capture-engine campaign |
| [`2026-07-18-traditional-native-comparison.md`](2026-07-18-traditional-native-comparison.md) | Corrected 298-pair traditional Java/native benchmark after the capture-engine campaign |
| [`2026-07-18-traditional-native-comparison.csv`](2026-07-18-traditional-native-comparison.csv) | Complete final 596-row Intel/Graviton traditional comparison |
| [`2026-07-17-capture-count-campaign.md`](2026-07-17-capture-count-campaign.md) | Three-round capture/count campaign with retained count optimizations and rejected sparse capture histories |
| [`2026-07-17-capture-count-native-comparison.csv`](2026-07-17-capture-count-native-comparison.csv) | Complete final 82-row Intel/Graviton comparison against host-tuned native RE2 |
| [`2026-07-17-traditional-native-comparison.md`](2026-07-17-traditional-native-comparison.md) | Audited 298-pair traditional Java/native benchmark baseline on Intel and Graviton |
| [`2026-07-17-capture-optimization-probes.md`](2026-07-17-capture-optimization-probes.md) | Superseded pre-campaign capture probes and rejected allocation-only candidates |
| [`2026-07-16-rebar-native-comparison.md`](2026-07-16-rebar-native-comparison.md) | Pre-capture-engine bounded comparison against bundled, pinned portable, and host-tuned native RE2 |
| [`2026-07-16-dfa-absolute-pointer-integrated.md`](2026-07-16-dfa-absolute-pointer-integrated.md) | Accepted Intel and Graviton production integration result |
| [`2026-07-16-dfa-absolute-pointer-cutoff.md`](2026-07-16-dfa-absolute-pointer-cutoff.md) | Original strict absolute-pointer cutoff result and Graviton root cause |
| [`2026-02-14-results.md`](2026-02-14-results.md) | Consolidated Java and native RE2 measurements |
| [`2026-02-14-gap-analysis.md`](2026-02-14-gap-analysis.md) | Root-cause analysis of measured gaps |
| [`2026-02-14-optimization-log.md`](2026-02-14-optimization-log.md) | Optimization experiment ledger |
| [`2026-02-14-cpp-optimization-report.md`](2026-02-14-cpp-optimization-report.md) | Native `-O0` versus `-O3` comparison |
| [`2026-02-09-dfa-assembly-analysis.md`](2026-02-09-dfa-assembly-analysis.md) | DFA hot-loop assembly analysis |
| [`2026-02-13-engine-campaign-v1.md`](2026-02-13-engine-campaign-v1.md) | Initial engine campaign notebook |
| [`2026-02-13-engine-campaign-v3.md`](2026-02-13-engine-campaign-v3.md) | Closed strict engine campaign |
| [`2026-02-13-parser-compiler-campaign-v1.md`](2026-02-13-parser-compiler-campaign-v1.md) | Parser/compiler campaign v1 notebook |
| [`2026-02-14-parser-compiler-campaign-v2.md`](2026-02-14-parser-compiler-campaign-v2.md) | Parser/compiler campaign v2 notebook |
