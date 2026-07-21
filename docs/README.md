# RE2 Port Documentation

This index separates current project guidance from historical evidence. Do not
infer current status from a dated audit, campaign log, or benchmark result.

## Resume Here

When returning to the project after time away:

1. Read [`AGENTS.md`](../AGENTS.md) for project rules and verification commands.
2. Read [`RE2_TASKS.md`](../RE2_TASKS.md) for the authoritative status, current
   phase, next work, and release criteria.
3. Read [`RE2_DECISIONS.md`](../RE2_DECISIONS.md) for intentional differences
   from pinned upstream RE2.
4. When assessing compatibility or future adoption, read
   [`integrations/TRINO.md`](integrations/TRINO.md).
5. Consult the test map, workflow, or historical evidence only as required by
   the task.

## Current Snapshot

- The parser, compiler, execution engines, Slice API, reusable matcher, set API,
  filtered API, and Trino operation adapter are implemented.
- The confirmed blockers from the July 2026 readiness review, applicable
  upstream case-table ports, and JVM-sourced Unicode migration are complete.
- The Trino syntax/function corpora pass. Actual Trino module wiring is a
  separate adoption project.
- The candidate qualification campaign covers native RE2 and Trino-shaped Joni
  operations on Intel and Graviton. The final three-session Joni acceptance
  closes the known regression across the complete operation matrix. Native
  outliers and incomplete native full-matrix coverage keep release
  qualification open. See the dated
  [`Joni acceptance report`](benchmarks/history/2026-07-20-joni-final-acceptance.md)
  and
  [`candidate qualification report`](benchmarks/history/2026-07-20-candidate-qualification.md).
- The broader official Rebar intersection and final three-session full matrix
  remain release work. See [`RE2_TASKS.md`](../RE2_TASKS.md) for the exact
  completion state.

## Document Authority

| Kind | Authority | Rule |
|---|---|---|
| Current status and work | [`RE2_TASKS.md`](../RE2_TASKS.md) | The only authoritative progress tracker |
| Current design decisions | [`RE2_DECISIONS.md`](../RE2_DECISIONS.md) | Records implemented, intentional deviations from upstream |
| Project instructions | [`AGENTS.md`](../AGENTS.md) | Governs development and verification |
| Current process and reference docs | Non-`history/` files below | Use for ongoing work |
| Dated audits, campaigns, and results | Files under `history/` | Evidence only; status and paths may be superseded |

When documents disagree, use the first applicable source in this table. Update
`RE2_TASKS.md` when work changes project status; do not update an old campaign
queue.

## Current Documentation

### Porting And Correctness

| Document | Purpose |
|---|---|
| [`porting/WORKFLOW.md`](porting/WORKFLOW.md) | Upstream-grounded development and verification workflow |
| [`porting/TEST_MAP.md`](porting/TEST_MAP.md) | Exact upstream C++ test coverage and remaining gaps |
| [`audits/PROCEDURES.md`](audits/PROCEDURES.md) | Audit and optimization-verification procedures |
| [`audits/templates/`](audits/templates/) | Reusable audit templates |

### API And Integration

| Document | Purpose |
|---|---|
| [`integrations/TRINO.md`](integrations/TRINO.md) | Trino semantics, historical RE2J research, Slice boundary, API requirements, and qualification plan |
| [`integrations/JVM_UNICODE.md`](integrations/JVM_UNICODE.md) | Implemented JVM-sourced Unicode policy, design, and validation requirements |

### Performance

| Document | Purpose |
|---|---|
| [`benchmarks/METHODOLOGY.md`](benchmarks/METHODOLOGY.md) | Current benchmarking and performance-analysis method |
| [`benchmarks/ENGINEERING_RESULTS.md`](benchmarks/ENGINEERING_RESULTS.md) | Active Intel, Graviton, native, and Trino engineering evidence |
| [`benchmarks/DFA_LAYOUT_EXPLORATION.md`](benchmarks/DFA_LAYOUT_EXPLORATION.md) | Compact DFA representation results and retained stable-self-loop design |
| [`benchmarks/DFA_ABSOLUTE_POINTER_PLAN.md`](benchmarks/DFA_ABSOLUTE_POINTER_PLAN.md) | Completed bounded plan and accepted absolute-pointer sidecar result |
| [`benchmarks/QUALIFICATION_PLAN.md`](benchmarks/QUALIFICATION_PLAN.md) | Formal Intel, Graviton, native RE2, Joni, and historical RE2J campaign plan |
| [`benchmarks/ENGINE_RESEARCH.md`](benchmarks/ENGINE_RESEARCH.md) | Transferable algorithms from other regex engines and prototype evidence |
| [`reference/OPTIMIZATION_REGISTRY.md`](reference/OPTIMIZATION_REGISTRY.md) | Upstream optimization inventory and trigger conditions |
| [`reference/REVERSE_DFA.md`](reference/REVERSE_DFA.md) | Reverse-DFA design reference |

### Cleanup And Development Standards

| Document | Purpose |
|---|---|
| [`cleanup/GUIDE.md`](cleanup/GUIDE.md) | Cleanup risk tiers, validation gates, and autonomous scope |
| [`cleanup/DECISIONS.md`](cleanup/DECISIONS.md) | Cleanup-specific style and architecture decisions |
| [`development-philosophy.md`](development-philosophy.md) | Readability and review principles |
| [`coding-standards.md`](coding-standards.md) | General coding standards |
| [`naming.md`](naming.md) | Naming conventions |
| [`comments.md`](comments.md) | Comment conventions |
| [`testing.md`](testing.md) | Test conventions |
| [`git.md`](git.md) | Git and commit conventions |

## Historical Evidence

Historical documents are retained because they explain why optimizations were
kept or rejected, provide old measurements, and record earlier audit findings.
They are not active plans and do not establish current correctness or
performance.

| Archive | Contents |
|---|---|
| [`audits/history/`](audits/history/) | Dated readiness, fidelity, encoding, and remediation reports with detail files |
| [`benchmarks/history/`](benchmarks/history/) | February 2026 benchmark results, analyses, and campaign notebooks |
| [`cleanup/history/`](cleanup/history/) | Completed cleanup campaign notebook |

The
[`2026-07-13 port readiness review`](audits/history/2026-07-13-port-readiness-review.md)
is the baseline assessment for the current correctness campaign. Its findings
have been transferred to `RE2_TASKS.md`, where their current disposition is
recorded.

## Maintenance Rules

- Put current project state and open work only in `RE2_TASKS.md`.
- Put implemented deviations from upstream only in `RE2_DECISIONS.md`.
- Put reusable procedures and technical references outside `history/`.
- Put dated reports, measurements, experiment logs, and closed campaign queues
  under the appropriate `history/` directory.
- Add a status warning when a historical document could be mistaken for a
  current result or plan.
- Prefer links relative to the repository. Do not add machine-specific absolute
  paths to current documentation.

## Upstream Source

- Repository: <https://github.com/google/re2>
- Pinned commit: `972a15cedd008d846f1a39b2e88ce48d7f166cbd`
- Reproducible fetch/build: `../tools/re2-golden/build.sh`
- Fetched source: `../target/re2-golden-dependencies/re2/`
