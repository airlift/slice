# Capture Pipeline And Workspace Campaign

**Status:** Closed. Implementation and focused Intel/Graviton campaigns passed.
The retained two-host native baseline passed its contemporary acceptance
review. The single post-audit exact-source confirmation passed on Graviton and
stopped at the strict Intel native-stability gate. This is internal engineering
evidence, not publication qualification.

This campaign answered whether the remaining capture losses came from the
matching algorithms or from how the public operation composed and provisioned
them. Lower elapsed-time ratios are better.

## Fixed Scope

The campaign decomposed four representative public operations:

| Workload | Public model | Selected route |
|---|---|---|
| Veryl lexer | `count-captures` | Forward and reverse DFA, then anchored NFA capture |
| Unicode parse line | `grep-captures` | BitState capture |
| Date | `count-spans` | Forward and reverse DFA only |
| `SplitBig2` | `count-captures` | Forward and reverse DFA, then BitState capture |

Compilation was out of scope. The campaign allowed one routing round, one
workspace round per selected capture engine, and a separate group-zero
composition assessment. A hard correctness, lifecycle, memory, or protected
performance failure stopped the corresponding idea.

## Baseline Decomposition

Session `20260718T060813Z-capture-pipeline` measured public-before, public-after,
control, setup, forward DFA, reverse DFA, capture, composition, and result stages.
The stages accounted for at least 92% of public time in every representative
case.

- Veryl was capture dominated: the anchored NFA capture stage alone was
  approximately 98%-101% of public time.
- Unicode was capture dominated: BitState was approximately 97% of public time.
- `SplitBig2` spent approximately 56%-63% in capture and 38%-45% in its two DFA
  boundary searches.
- Date requested only group zero. Forward and reverse DFA consumed essentially
  the complete operation; no capture engine ran.

This established that Veryl and Unicode were not principally slowed by public
composition. They were slowed by capture-engine scratch allocation and
lifecycle within the selected algorithms. `SplitBig2` had both composition and
capture costs. Date remains a separate group-zero DFA problem.

## Routing Decisions

Session `20260718T063031Z-capture-pipeline` tested direct full-window routes.

- Direct unanchored NFA made Veryl approximately 49%-50% slower. NFA benefits
  from the narrow exact range supplied by the DFA boundary searches, so this
  route was rejected.
- Direct BitState completed the `SplitBig2` operation in approximately 0.9 ms,
  compared with a 5.0-6.3 ms public baseline. It avoids two DFA searches and an
  additional anchored capture pass while preserving the same leftmost-first
  result.

The retained direct BitState route is deliberately narrow: reusable matcher,
unanchored leftmost-first search, subgroup capture, at least 4 KiB of input, and
a complete visited bitmap no larger than 256 KiB. The low-level caller-buffer
API remains stateless and allocation-free.

## Matcher-Owned Workspaces

Reusable `Re2Matcher` instances now own BitState and NFA scratch storage.

- BitState reuses the visited bitmap, capture scratch, and initial traversal
  jobs.
- NFA reuses both sparse queues, traversal stacks, best-match storage, and its
  primitive thread arena and capture rows.
- An exceptional NFA exit invalidates the workspace before propagating the
  failure, so partially live thread state cannot be reused by a later search.
- Neither workspace retains an input `Slice`, its backing byte array, nor a
  caller-provided group array.
- The stateless low-level API continues to allocate no capture buffer or hidden
  workspace.

Allocation tests measure no more than 128 bytes per matcher invocation after
warmup: 64 bytes for the BitState invocation wrapper and 104 bytes for the NFA
invocation wrapper in the measured JVM. The prior Veryl NFA path allocated about
26 KiB per search, or roughly 1.64 GiB across one benchmark operation's 62,400
capture searches.

## Final Focused Result

Session `20260718T065915Z-capture-pipeline` measured the retained routing and
workspace implementation. Public time is the mean of the bracketed
public-before and public-after medians.

| Workload | Intel baseline | Intel current | Ratio | Graviton baseline | Graviton current | Ratio |
|---|---:|---:|---:|---:|---:|---:|
| Veryl count captures | 272.224 ms | 163.251 ms | 0.600x | 363.552 ms | 218.013 ms | 0.600x |
| Unicode grep captures | 36.939 ms | 31.310 ms | 0.848x | 49.076 ms | 29.768 ms | 0.607x |
| Date group-zero spans | 6.396 ms | 6.310 ms | 0.986x | 7.272 ms | 7.205 ms | 0.991x |
| `SplitBig2` count captures | 5.038 ms | 0.864 ms | 0.171x | 6.297 ms | 0.983 ms | 0.156x |

The retained NFA workspace reduces Veryl by 40% on both architectures. The
BitState workspace reduces Unicode by 15% on Intel and 39% on Graviton. Direct
BitState reduces `SplitBig2` by 83%-84%. The protected date path remains within
1.4% of its baseline. The exact focused rows are retained in the
[`Intel CSV`](2026-07-18-capture-pipeline-intel.csv) and
[`Graviton CSV`](2026-07-18-capture-pipeline-graviton.csv).

## Final Native Confirmation

Session `20260718T074110Z-72844` measured the affected `count-captures` and
`grep-captures` models on the exact retained source. Native RE2 was pinned to
`972a15cedd008d846f1a39b2e88ce48d7f166cbd` and compiled with `-O3 -DNDEBUG`
plus `-march=native` on Intel and `-mcpu=native` on Graviton. Ratios are Java
time divided by the bracketed host-tuned native time, so lower is better.

| Model | Rows | Intel | Graviton |
|---|---:|---:|---:|
| Count with captures | 1 | 1.033x | 0.960x |
| Grep with captures | 5 | 1.180x | 0.968x |

The count result is now at parity on both architectures. The grep aggregate is
mixed rather than uniformly at parity: the AWS-keys row takes 54.0% of native
time on Intel and 44.7% on Graviton, which offsets slower rows in the geometric
mean.

| Residual loss | Intel ratio | Intel deficit | Graviton ratio | Graviton deficit | Route |
|---|---:|---:|---:|---:|---|
| Ruff `real` | 1.423x | 30.92 ms | 1.161x | 15.88 ms | Per-line forward DFA; reverse DFA plus OnePass on matches |
| Ruff `tweaked` | 1.262x | 11.02 ms | 1.188x | 9.91 ms | Prefix-accelerated per-line forward DFA; reverse DFA plus OnePass on matches |
| Unicode parse line | 1.495x | 10.23 ms | 1.013x | 0.38 ms | BitState capture |
| Unstructured extract | 1.576x | 0.13 ms | 1.362x | 0.12 ms | BitState capture |

Veryl improved from `1.922x/1.644x` to `1.033x/0.960x` on
Intel/Graviton. Unicode improved from `1.606x/1.617x` to `1.495x/1.013x`.
The Ruff rows did not improve materially and are the principal remaining public
capture losses. Each processes 890,906 lines and makes 890,926 forward DFA
attempts, but only 20 reverse searches and 20 OnePass capture extractions. The
remaining Ruff deficit is therefore in repeated line-oriented search and
no-match rejection, not the capture engine. The AWS-keys row also processes
890,906 no-match lines but performs only forward DFA searches; it is
substantially faster than native. Unstructured extraction has a large ratio but
only about 0.1 ms absolute deficit per complete benchmark operation.

Intel Veryl's native-before/native-after medians drifted by 6.96%, above the
ordinary 2% stability gate. The Java median was 1.068x the native-before value
and 0.999x the native-after value, so both ends of the bracket independently
place the row inside the 0.90x-1.10x parity band. The parity conclusion does not
depend on averaging through that drift.

The complete 12-row result is
[`2026-07-18-capture-pipeline-native-comparison.csv`](2026-07-18-capture-pipeline-native-comparison.csv).

## Post-Audit Exact-Source Confirmation

Session `20260718T083119Z-79902` reran the six affected workloads after the
workspace-lifecycle and benchmark-provenance audit. It used source hash
`cf2cd73e1d6e1682c51987f402461f9fc164d22a119b69bd6212fe444cdf4111`
and made the 2% native before/after drift gate mandatory.

| Model | Rows | Intel diagnostic | Graviton qualified |
|---|---:|---:|---:|
| Count with captures | 1 | 1.060x | 0.971x |
| Grep with captures | 5 | 1.172x | 0.949x |

Graviton passed the complete campaign. Intel completed all correctness,
semantic-verification, and measurement work, but native Unicode moved from
21.57 ms to 20.84 ms (3.50%) and native unstructured extraction moved from
232.88 us to 243.60 us (4.60%). The strict reducer rejected the complete Intel
host result. Its values are retained only to show that no new Java regression
is apparent; they are not a current-source Intel qualification. The plan's
single optional confirmation was consumed, so no stability rerun was opened.

The current-source rows and explicit qualification labels are in
[`2026-07-18-capture-pipeline-audit-confirmation.csv`](2026-07-18-capture-pipeline-audit-confirmation.csv).

## Group-Zero Assessment

The date expression is variable width, compiles to 3,248 instructions, and has
35 source captures that are intentionally not requested by the group-zero
operation. It is neither OnePass nor eligible for BitState. Each operation
performs 42,916 required reverse searches after forward boundary discovery.

Eliminating that second traversal would require a new tagged-state DFA capable
of preserving the winning start boundary during forward execution. This is a
new matching algorithm, not a composition cleanup, and did not clear the fixed
5% plausibility gate for this bounded campaign. It was not implemented.

## Rejected Ideas

Do not repeat these unchanged:

| Candidate | Rejection |
|---|---|
| Direct unanchored NFA for Veryl | 49%-50% slower than the existing narrowed capture route |
| Treat group-zero spans as a capture-engine problem | No capture engine runs on the measured path |
| Fuse date boundaries in the existing DFA | Requires tagged-state semantics and a new algorithm |
| Give the public low-level API hidden reusable scratch | Violates its caller-managed zero-allocation contract |

## Verification

- Focused route, workspace, allocation, input-lifecycle, and pipeline tests pass.
- The complete local RE2 selector passes 906 tests with zero failures or errors
  and one intentional skip.
- The full Maven install passes 2,696 tests with zero failures or errors and one
  intentional skip.
- On both Intel and Graviton, the post-audit archived source passes the complete
  906-test RE2 selector both with and without native access.
- Both post-audit hosts passed semantic verification for every selected
  engine/workload pair. Graviton exited successfully; Intel exited at the
  mandatory native-drift gate after producing the complete matrix.
- Both EC2 instances terminated, and the temporary S3 bucket, IAM role, and
  instance profile were removed.
- The archived post-audit main/test RE2 source hash is
  `cf2cd73e1d6e1682c51987f402461f9fc164d22a119b69bd6212fe444cdf4111`,
  exactly matching the retained worktree source.
- The filtered-campaign validator and normalizer now use the exact selected
  engine manifest while separately requiring the complete 41-workload
  provenance manifest. Unit tests cover both full and filtered campaigns.

### Post-Audit Provenance

| Item | Intel | Graviton |
|---|---|---|
| Instance | `c8i.8xlarge`, Intel Xeon 6975P-C | `c8g.4xlarge`, AWS Graviton4 |
| JVM | Temurin 25.0.3+9 | Temurin 25.0.3+9 |
| Native flags | `-O3 -DNDEBUG -march=native` | `-O3 -DNDEBUG -mcpu=native` |
| Portable native binary | `56dddb55506b6cd66308432f68b61644580e5ad68fbd7b4904f93a34fb3944a8` | `0d6831bd8ab6a29bf64628a8b11d2d0d2e7d939141876928857871058142da04` |
| Host-tuned native binary | `6024fa71445c3c4937418446d0b0c271791ea2caf62f643365972f4d56fbe3be` | `bbb475fbff3b0e3f3302c7a435b22365b8df592abad46b3c229f83ee21523dea` |
| Result archive | `5be01184ba5cdd2b3b4a52f44351c2c642a5f69c94f55de6ef11909360842164` | `627709a2f5cb78805147e668babed9af155d637eda1ff5f65ef4522a7f048e9d` |

The source archive is
`e884674f8f6806cbb90c796d5846bbdee38047267f3bd6c8d438d7002c46dd31`.
The pinned Rebar archive is
`aa2831bc4c80e2a718661698c78c5e5dac314748b541b24c347eba116bd152b6`.
Both hosts used workload manifest
`3061aba3fa80b896f9e54a7fd2038dea4f59948eaa038b18e92ed87d43473c14`
and engine manifest
`13589cdd4a9cec9590430047548ffcb1118e4cd7a127653df26fb7fb5a8a7c2e`.
Both instances terminated, the IAM role and profile were removed, and the
private transfer bucket was deleted after both result archives were recovered.
