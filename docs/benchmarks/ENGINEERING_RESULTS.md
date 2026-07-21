# RE2 Performance Engineering Results

**Status:** Active engineering baseline. Bounded paired transitions and the
adaptive stable-self-loop scanner are complete on Intel and Graviton. Eligible
productive graphs outperform the compact Java loop and native RE2 without a
protected regression. The absolute-pointer sidecar closes the measured
state-changing compact DFA gap and is accepted as the one-byte DFA design. The
bounded capture-engine campaign now retains structural NFA, OnePass, and
BitState improvements with no protected regression. Direct NFA and OnePass are
at Intel parity, while direct Graviton capture and BitState on both
architectures retain explicit residual gaps. The final Rebar comparison shows
that those changes materially reduce ordinary capture extraction, while
matcher-owned NFA and BitState workspaces remove the dominant repeated-search
allocation cost. A bounded direct BitState route closes the long `SplitBig2`
outlier. A bounded candidate-start cursor subsequently removes most of the Date
forward-plus-reverse replay cost; Date is faster than native on Graviton but
remains 21.8% slower on Intel. The retained two-host affected-model baseline
places capture counting at parity. A post-audit exact-source confirmation
requalified Graviton but was rejected on Intel because two native before/after
brackets exceeded the 2% stability gate; this is not evidence of a Java
regression. Repeated short Ruff searches now use the absolute-pointer sidecar
after bounded warmup, closing the original `real` row. Allocation-free logical
matcher regions then reduce Ruff `tweaked` Java time by 3.0%/6.2% on
Intel/Graviton, although it remains slower than same-session native. Ruff
`tweaked` and Intel Date remain formal-qualification concerns. The
corrected final traditional comparison places equivalent stateless public
capture near parity overall. The current-source census now supersedes those
earlier whole-engine headlines: public failed search, successful search, full
match, and capture take `0.507x/0.456x`, `0.655x/0.589x`, `0.389x/0.326x`, and
`0.984x/1.012x` host-tuned native time on Intel/Graviton.

## Current Candidate Qualification

Candidate `c212684` supersedes the earlier current-source census for release
assessment. Three-session traditional and bounded Rebar-native campaigns place
normal public execution at `0.730x/0.638x` native time, public capture at
`1.028x/1.021x`, and normal Rebar execution at `0.991x/0.970x` on
Intel/Graviton. Trino-shaped operations have a `0.481x` Intel and current
`0.435x` Graviton median against Joni at that snapshot.

The aggregate wins do not close the project. Native outliers remain for Intel
Unicode literal scanning and `SplitBig2` capture. A focused follow-up closes the
long Cloudflare span regression on both architectures and reduces
case-insensitive English scanning to a 1.157x Intel residual while making it
0.855x native time on Graviton. The native full matrix failed to produce the
required three complete sessions per architecture, and the broader official
Rebar intersection remains open. See the dated
[`candidate qualification report`](history/2026-07-20-candidate-qualification.md)
for the current conclusions, outliers, and release-gate disposition.

The final search-outlier campaign retains a primitive matching-self-loop scan
and a fused folded-prefix candidate scan. Cloudflare simplified-long now takes
`0.950x/0.942x` host-tuned native time on Intel/Graviton, down from
`5.177x/4.136x`. Case-insensitive English takes `1.157x/0.855x`, down from
`2.163x/1.628x`. See the
[`final search-outlier closure`](history/2026-07-20-final-outlier-closure.md).

The completed follow-up corrected the initial route diagnosis: the five Joni rows
use the candidate-start cursor and never compute the reverse program. A bulk
candidate scan removes per-byte mutable budget accounting. Target session
`20260720T182018Z-72726` wins all 32 focused rows on each architecture, with
`0.546x/0.463x` median Slice/Joni time on Intel/Graviton. The original 32 KiB
`captureSparse` rows now take `0.335x`-`0.552x` and `0.395x`-`0.469x` Joni
time. See the
[`Joni sparse boundary campaign`](history/2026-07-20-joni-sparse-boundary-campaign.md).

The final Joni acceptance runs the complete 80-row operation matrix in three
independent sessions on each architecture. At the strict 2% Slice-bracket
gate, 78 rows qualify per architecture and all 78 beat Joni. Intel takes
geometrically `0.384x` Joni time with a `0.453x` median and `0.991x` worst row;
Graviton takes `0.337x`, `0.434x`, and `0.963x`. Every raw session wins all
80 rows. Four architecture-specific rows are excluded for bracket drift while
showing directional ratios of `0.041x`-`0.312x`, so no reproducible Joni loss
remains. See the
[`final Joni acceptance`](history/2026-07-20-joni-final-acceptance.md).

## Prior Current-Source Native Census

Session `20260719T071144Z-current-census` measures engine checkpoint
`23d21694c0cf94d1d9e3dc06ab4f0bc61ceadfde` across the complete 298-pair
traditional intersection and 41-workload Rebar corpus. Aggregate ratios are
geometric means over rows whose native-before/native-after bracket drift is at
most 2%.

The traditional census qualifies 593 of 596 rows. Public boolean matching is
substantially faster than native, and stable public capture is `0.984x/1.012x`
on Intel/Graviton. Rebar count is `0.827x/0.569x`, count captures is
approximately `1.058x/0.964x`, grep captures is `1.089x/0.920x`, and count
spans is `1.997x/1.704x`. The Intel capture-count ratio is diagnostic because
native drift remained 2.89% after the one allowed confirmation; its primary
and confirmation ratios both indicate parity.

The normal-case summary, material deficits, extreme relative outliers,
row-level confirmation policy, and complete CSV evidence are in the dated
[`current-source native census`](history/2026-07-19-current-native-census.md).

The complete group-zero span decomposition established that Date's composed
forward/reverse replay accounted for 98.6%/95.7% of public time on
Intel/Graviton. The retained candidate-start cursor now combines candidate
discovery and anchored verification under the existing DFA cache protocol. It
reduces Date from `1.630x/1.385x` to `1.218x/0.888x` native time. Both English
controls remain native wins, while the three Cloudflare rows remain tiny
fixed-cost ratio outliers. Intel Date remains a material 0.84 ms deficit and
must be revisited during formal qualification. See the dated
[`candidate-start campaign`](history/2026-07-20-date-candidate-start-cursor.md).

Reverse absolute pointers do not close the group-zero date gap. Focused session
`20260719T053547Z-15241` measures `1.000x/1.013x` object reverse-stage time and
`1.000x/1.005x` object public time on Intel/Graviton. The candidate was active,
but 42,916 reverse calls averaging 6.3 bytes cannot amortize FFM setup. No
production change is retained. See the dated
[`reverse pointer experiment`](history/2026-07-18-dfa-reverse-pointer.md).

The focused Ruff campaign closes the material short-DFA outlier. Confirmation
session `20260719T061153Z-33390` measures Ruff `real` at `0.998x/0.983x`
native time on Intel/Graviton, down from `1.423x/1.161x`. Repeated short
eligible searches promote to the absolute-pointer sidecar after 16 calls;
isolated short calls retain later paired-table eligibility. Matcher reset and
failure also stop redundantly clearing inaccessible capture slots. Ruff
`tweaked` remains an integration outlier because its forward stage is already
faster than native's whole operation while timed line-window setup remains.
See the dated [`Ruff short-search campaign`](history/2026-07-18-ruff-short-dfa.md).

The bounded line-integration follow-up rejects offset-aware SWAR newline
discovery. Ruff `tweaked` public time changed by `+2.13%/+4.68%` on
Intel/Graviton, while Unicode changed by `-0.18%/-1.06%`. Unicode setup fell to
0.48/0.64 ms but remained immaterial beside its 35.6/45.3 ms BitState capture
stage. Ruff's stable Graviton regression fails the 2% retention gate, so no
production change or native confirmation is retained. Its logical-region and
Unicode follow-ups are recorded below. See the dated
[`line capture integration campaign`](history/2026-07-19-line-capture-integration.md).

The retained logical-region matcher carries context bounds through DFA setup
without constructing a Slice view for every failed line. Ruff `tweaked` Java
time improves from 51.476/57.277 ms to 49.912/53.749 ms on Intel/Graviton, or
3.0%/6.2%, while setup improves 18.3%/16.1%. Protected Ruff `real` changes by
+1.3%/-2.8%, and the Unicode control by +1.7%/+1.7%. The DFA transition loops
are unchanged. Same-session Ruff `tweaked` remains `1.487x/1.135x` native time;
native controls shifted materially from the prior campaign, so the candidate
effect is assessed from Java time rather than cross-session ratio changes. See
the dated [`logical region matcher campaign`](history/2026-07-20-logical-region-matcher.md).

The Unicode follow-up routes its structurally one-pass 16-group program through
a 64-bit capture-mask sidecar instead of BitState. It reduces public Java time
by 63.9%/53.6% to `0.579x/0.496x` native time on Intel/Graviton. See the dated
[`extended OnePass capture campaign`](history/2026-07-19-extended-onepass-captures.md).

The focused large-DFA follow-up closes the reset-free bounded-context object-row
gap. Session `20260719T003857Z-7477` retains all 75,850 states and measures the
demand-sized absolute-pointer route at `0.903x/0.964x` native RE2 time on
Intel/Graviton, versus `0.640x/0.608x` of bracketed Java object-row time. See the
dated [`large pointer sidecar`](history/2026-07-18-dfa-large-pointer-sidecar.md)
report.

Intel follow-up `20260719T042441Z-48534` removes the sidecar size cutoff. A
subsequent memory census rejected eagerly reserving the maximum table for tiny
graphs. The accepted sidecar grows geometrically under exclusive cache mutation
and never hands an eligible instance back to object traversal. Two-host memory
session `20260719T051215Z-5387` records a 144-byte sidecar for warmed `x*` and a
7,959,120-byte sidecar for the 75,850-state graph, with zero resets. Final
two-host session `20260719T052408Z-11066` measures that graph at
`0.908x/0.946x` native and `0.648x/0.619x` bracketed object-row time on
Intel/Graviton.

Formal publication qualification remains open. These measurements are retained
engineering evidence and are superseded for current assessment by the candidate
qualification report above.

The method and retention gates are defined in
[`ENGINEERING_CAMPAIGN.md`](ENGINEERING_CAMPAIGN.md). Formal multi-session
qualification remains a separate future activity in
[`QUALIFICATION_PLAN.md`](QUALIFICATION_PLAN.md).

## Capture Engine Baseline

The 2026-07-18 campaign isolated NFA, OnePass, and BitState capture execution
before public integration. Lower ratios are better.

| Engine | Intel Java/native | Graviton Java/native |
|---|---:|---:|
| NFA | 1.013x | 1.302x |
| OnePass | 0.935x | 1.216x |
| BitState | 1.180x | 1.565x |

The retained implementation uses compiled opcode counts and a primitive sparse
queue in NFA, direct/rebase/copy finalization in OnePass, bounded initial object
jobs in BitState, and compact immutable instructions shared by NFA and
BitState. Primitive BitState traversal stacks and combined public offset
adjustment were rejected by protected regressions. See the dated
[`capture engine campaign`](history/2026-07-18-capture-engine-campaign.md) for
rounds, exact sessions, guards, and rejected candidates.

## Capture Pipeline And Reusable Matcher Baseline

The direct-engine ratios above describe stateless calls. Session
`20260718T065915Z-capture-pipeline` decomposed and measured the reusable public
matcher. Lower ratios are current/baseline elapsed time.

| Workload | Intel | Graviton | Root cause and retained change |
|---|---:|---:|---|
| Veryl count captures | 0.600x | 0.600x | NFA scratch allocation; matcher-owned NFA workspace |
| Unicode grep captures | 0.848x | 0.607x | BitState scratch allocation; matcher-owned BitState workspace |
| `SplitBig2` count captures | 0.171x | 0.156x | Three-pass composition; direct bounded BitState |
| Date group-zero spans | 0.986x | 0.991x | Protected control; no capture engine runs |

This explains why stateless direct NFA remains `1.013x/1.302x` native while the
Veryl public operation improves by 40%: the algorithm is unchanged, but a
reusable matcher no longer allocates its complete thread arena and capture rows
for each of 62,400 searches. The exact stage results and rejected routes are in
the dated
[`capture pipeline campaign`](history/2026-07-18-capture-pipeline-campaign.md).

## Retained Affected Capture Baseline

Session `20260718T074110Z-72844` measured the exact retained source against
pinned, host-tuned native RE2. Lower Java/native ratios are better.

| Model | Rows | Intel | Graviton |
|---|---:|---:|---:|
| Count with captures | 1 | 1.033x | 0.960x |
| Grep with captures | 5 | 1.180x | 0.968x |

Veryl capture counting is now at parity on both architectures. Capture grep is
mixed: AWS-keys is `0.540x/0.447x`, but Ruff `real` remains
`1.423x/1.161x` with 30.9/15.9 ms deficits and Ruff `tweaked` remains
`1.262x/1.188x` with 11.0/9.9 ms deficits. Unicode parse-line is
`1.495x/1.013x`, a 10.2 ms Intel deficit and Graviton parity. Unstructured
extraction is `1.576x/1.362x`, but its absolute deficit is only 0.13/0.12 ms.
Each Ruff operation makes about 890,000 per-line forward DFA attempts but only
20 reverse searches and OnePass capture extractions. Its residual is a repeated
line-search/no-match problem rather than a capture-engine deficit.

The later short-search campaign supersedes the Ruff `real` row with
`0.998x/0.983x` native time. The historical table remains here because the
complete affected-model matrix has not yet been rerun on the retained source.

The complete current subset is
[`2026-07-18-capture-pipeline-native-comparison.csv`](history/2026-07-18-capture-pipeline-native-comparison.csv).
The earlier 41-workload result below remains the complete pre-workspace census;
unaffected model values were not rerun in this bounded confirmation.

## Post-Audit Confirmation

Session `20260718T083119Z-79902` measured source
`cf2cd73e1d6e1682c51987f402461f9fc164d22a119b69bd6212fe444cdf4111`
after the final workspace-lifecycle and provenance-audit fixes. The runner used
strict 2% native before/after drift rejection.

| Model | Rows | Intel diagnostic | Graviton qualified |
|---|---:|---:|---:|
| Count with captures | 1 | 1.060x | 0.971x |
| Grep with captures | 5 | 1.172x | 0.949x |

Graviton passed every gate. Intel completed both correctness modes, semantic
verification, and all measurements, but the Unicode native bracket drifted
3.50% and unstructured extraction drifted 4.60%. The entire Intel result is
therefore diagnostic rather than qualification evidence. Its row shapes remain
consistent with the retained baseline: Ruff is `1.411x/1.254x`, Unicode is
`1.443x`, AWS keys is `0.532x`, and unstructured extraction is `1.630x` with a
0.15 ms absolute deficit. No additional round was opened because this was the
campaign's single optional confirmation.

The exact rows and their qualification status are in
[`2026-07-18-capture-pipeline-audit-confirmation.csv`](history/2026-07-18-capture-pipeline-audit-confirmation.csv).

## Pre-Pipeline Full Rebar Count And Capture Result

Session `20260718T023744Z-76514` reran the 41-workload Rebar comparison on the
settled capture-engine source. Every ratio uses exact pinned, host-tuned native
RE2 as the denominator, so lower is better.

| Model | Intel before | Intel current | Graviton before | Graviton current |
|---|---:|---:|---:|---:|
| Capture-free count | 0.949x | 0.951x | 0.651x | 0.650x |
| Count with captures | 2.291x | 1.922x | 2.150x | 1.644x |
| Group-zero spans | 1.997x | 1.986x | 1.726x | 1.729x |
| Boolean line match | 1.152x | 1.022x | 0.870x | 0.878x |
| Grep with captures | 1.482x | 1.182x | 1.241x | 1.051x |

The capture engines close most of the grep gap and reduce the dominant Veryl
count-with-captures row by 16% on Intel and 24% on Graviton. They do not alter
the group-zero-only traversal used by count-spans, whose aggregate remains
effectively unchanged. Capture-free count also remains stable, preserving the
earlier accepted DFA counting work.

At that snapshot, the remaining material capture losses were the Veryl lexer at
`1.922x/1.644x` and Unicode parse-line grep at `1.606x/1.617x` on
Intel/Graviton. The Cloudflare
group-zero rows reach large ratios but only about 0.1 ms absolute deficit; the
date row is the meaningful group-zero deficit at `1.612x/1.422x`, or about
2.4/2.2 ms. Literal Russian and Chinese count remain extreme Intel ratio
outliers at `9.02x` and `21.90x`, but only 2.4 ms and 0.7 ms absolute deficit,
and are not capture-engine paths.

Both hosts passed the complete RE2 selector with and without native access,
uploaded complete artifacts, exited successfully, and were removed with their
temporary AWS resources. The complete 82-row result is
[`2026-07-18-capture-engine-native-comparison.csv`](history/2026-07-18-capture-engine-native-comparison.csv).

## Pre-Workspace Traditional Native Comparison

Session `20260718T023744Z-76513` reran the audited 298-pair traditional census
with corrected capture equivalence. Stable public results are:

| Family | Intel | Graviton |
|---|---:|---:|
| Failed search | 0.499x | 0.452x |
| Successful search | 0.670x | 0.587x |
| Full match | 0.387x | 0.343x |
| Capture | 1.035x | 1.144x |

The former `2.60x/2.69x` capture headline is not a valid before/after metric:
four Java registrations used unanchored first-match while native measured
anchored full-match. The corrected rows are now semantically equivalent and
covered by an input test. At that snapshot, the long `SplitBig2` public row was
the material capture outlier at `1.973x/1.984x`, or about 2.5/3.1 ms absolute
deficit. The later focused exact-source pipeline campaign reduces that operation
by 83%-84%; the complete traditional census was not rerun in this bounded round.

The direct capture results corroborate the focused campaign: NFA is
`1.005x/1.295x`, OnePass is `0.932x/1.229x`, and BitState is
`1.153x/1.458x` on Intel/Graviton. See the dated
[`traditional comparison report`](history/2026-07-18-traditional-native-comparison.md)
for provenance, stability exclusions, and separated outliers.

## Pre-Capture-Engine Rebar Count And Capture Baseline

Final sessions `20260717T202823Z-82766` and `20260717T204410Z-85223`
measure the 41-workload Rebar suite against exact pinned, host-tuned native RE2
on Intel and Graviton. Lower ratios are better.

| Model | Intel | Graviton |
|---|---:|---:|
| Capture-free count | 0.949x | 0.651x |
| Count with captures | 2.291x | 2.150x |
| Group-zero spans | 1.997x | 1.726x |
| Grep with captures | 1.482x | 1.241x |

One DFA reader lease per complete count and a bounded character-class scan
move capture-free count from `1.751x/1.203x` to `0.949x/0.651x` native time.
The principal remaining count loss is the complex bounded-context expression
at `1.480x/1.534x`. The Veryl lexer remains the dominant capture deficit at
`2.291x/2.150x`, or 199/257 ms per operation. See the dated
[`capture and count campaign`](history/2026-07-17-capture-count-campaign.md)
for focused rounds, rejected capture work, outliers, and the complete CSV.

A later bounded cache-capacity campaign replaces the conservative state charge
with runtime-aware retained-memory accounting and raises the demand-driven
default to 96 MiB. This eliminates bounded-context resets and improves the
operation by 6.7x/5.8x relative to 8 MiB on Intel/Graviton, while both controls
remain within 0.2%. The reset-free Java loop still takes `1.386x/1.590x` native
time, isolating a separate ordinary object-row transition-loop deficit. See the
[`DFA cache capacity policy`](history/2026-07-18-dfa-cache-capacity-policy.md).

The follow-up Joni memory qualification measures the practical Trino
comparator. Across the complete 80-row adapter matrix, Slice takes 0.412x Joni
time at the median on Intel and 0.401x on Graviton; the slowest rows are
0.995x and 1.003x. Ordinary warm-pattern memory is 0.474x Joni at the median,
with no ordinary megabyte-scale outlier. Current two-host census
`20260719T051215Z-5387` places ordinary warm-pattern memory at 0.473x Joni at
the median, 0.996x at P90, and 1.003x at the maximum. The 75,850-state bounded
graph retains a 7.96 MiB sidecar and 29.8 MiB warmed total, resets zero times,
and runs at 0.070x/0.065x Joni time on Intel/Graviton. See the
[`Joni memory qualification`](history/2026-07-18-joni-memory-qualification.md).

## Evidence Boundaries

The corrected full AWS session `20260714T120851Z` contains the thread-safe DFA
reader registry, sparse-capture boundaries, allocation removal, caller-buffer
OnePass path, eager OnePass preparation, required-prefix suffix compilation,
DFA state-budget accounting, stack-safe simplification, forward-only counting,
and the zero-boundary shortcut. It does not contain fixed-width boundary
recovery or the single-byte Trino matcher.

The targeted session `20260714T135741Z` adds those two candidates. It reruns the
complete RE2 selector, the full 80-point Slice Trino operation matrix, and
focused compile benchmarks on both architectures. Comparator numbers in the
targeted analysis come from the corrected full session because the pattern,
source, comparator code, host types, JDK, and JMH settings are unchanged.

The DFA diagnostic session `20260714T145055Z` isolates the remaining 16 MiB
Hard and Parens gap. Each architecture ran ten default-reference forks, five
uncompressed-reference forks, direct native RE2, hardware counters, and
annotated C2 assembly. Both hosts passed the complete 585-test RE2 selector and
exited successfully.

The retained paired-transition source is captured by session
`20260715T032009Z`, with `Dfa.java` SHA-256
`e36da18a27da127990b4cd5094d96a66e2aa137acdb0f1f92db5f686df43e456`.
Both c8i.xlarge and c8g.xlarge hosts passed their focused correctness preflight,
completed three-fork candidate-before, disabled-control, and candidate-after
measurements, uploaded complete artifacts, and exited successfully.

The final compact-layout session `20260716T051735Z-81238` captures the retained
adaptive stable-self-loop scanner, with `Dfa.java` SHA-256
`ff880c679faeb79e73699d9f23fc4a05dd1c5b0cde2fd209dc718fa10634740c`.
Both hosts ran candidate-before, an exact source-disabled control,
candidate-after, and optimized native RE2. They passed the focused correctness
preflight, uploaded complete artifacts, exited with status zero, and were
terminated with all temporary AWS resources removed.

An earlier session and a failed follow-up remain useful failure evidence but are
superseded as performance baselines. In particular, the failed follow-up exposed
recursive simplification of the 10,000-capture parser case before any benchmark
ran; the corrected full session includes the hybrid stack-safe fix.

## Pre-Capture-Engine Traditional Baseline

Session `20260717T093143Z-96711` measures the audited 298-row intersection of
the Java ports of `regexp_benchmark.cc` and exact pinned, host-tuned native RE2.
It uses `c8i.2xlarge` and `c8g.2xlarge` hosts, two JMH forks, native-before and
native-after brackets, and an exact operation/parameter manifest. The full
863-test RE2 selector passed both with and without native access on each host.

Across stable rows, Slice took geometrically `0.676x` native time on Intel and
`0.617x` on Graviton. This all-row index is informational because it mixes
internal routes and input sizes. The useful public and gap views are:

| Family | Intel | Graviton |
|---|---:|---:|
| Public failed search | 0.493x | 0.447x |
| Public successful search | 0.666x | 0.594x |
| Public full match | 0.390x | 0.336x |
| Public capture | 2.601x | 2.693x |
| Compilation | 1.239x | 1.798x |

The traditional baseline is a code-path census, not a replacement for Rebar's
application-shaped count, boundary, and capture evidence. Both campaigns agree
that capture extraction and compilation are material remaining gaps. See the
dated
[`traditional comparison report`](history/2026-07-17-traditional-native-comparison.md)
and its complete CSV.

Three bounded follow-ups tested a tiny OnePass boundary probe, a primitive
BitState job stack, and reuse of the caller capture buffer as OnePass scratch.
Each improved one narrow metric but regressed a protected throughput case, so
all three were removed. That was the retained state at the conclusion of the
2026-07-17 probe, before the newer capture-engine campaign; see the
[`capture optimization probes`](history/2026-07-17-capture-optimization-probes.md).

## Cross-Architecture Session

| Property | Intel | Arm |
|---|---|---|
| Instance | `c8i.8xlarge` | `c8g.4xlarge` |
| Processor | Intel Xeon 6975P-C | AWS Graviton4 |
| Physical cores used | 16 | 16 |
| Memory | 61 GiB | 30 GiB |
| JVM | Temurin 25.0.3+9 | Temurin 25.0.3+9 |
| Native compiler | GCC 11.5.0 | GCC 11.5.0 |
| Operating system | Amazon Linux 2023 | Amazon Linux 2023 |

Both corrected-baseline hosts built pinned native RE2 directly, ran the Slice
and Trino semantic preflights, and exited with status zero. Each host passed 578
Slice RE2 tests and the two Trino comparator tests. The session produced 64
Java/native DFA comparison rows, 64 public-API rows, 160 Trino comparison rows,
and 28 cache rows.

Each targeted host passed 583 snapshot tests and exited with status zero. The
current local tree adds byte-complete differential, start-byte acceleration,
paired fallback, adaptive rejection, and memory-release guards and passes 618
RE2 tests. All campaign instances terminated and all temporary transfer buckets
were removed.

Raw artifacts are stored under
`benchmark-results/re2-engineering/<session>/`, outside the Maven `target/`
tree. The directory is ignored by Git so a clean build does not erase evidence.

The reusable runner snapshots dirty worktrees, records source checksums and
environment manifests, uses private temporary transfer storage, and performs
cloud cleanup.

## Direct DFA Comparison

The Java and native benchmarks use the same fixed xorshift corpus and expected
results. Native RE2 runs directly as C++, not through JNI. Long 16 MiB results
from the AWS snapshot were:

| Architecture | Workload | Java | Native | Java/native |
|---|---|---:|---:|---:|
| Intel | Easy0 | 2.722 ms | 1.872 ms | 1.454x |
| Intel | Easy1 | 3.287 ms | 2.253 ms | 1.459x |
| Intel | Hard | 30.406 ms | 22.300 ms | 1.363x |
| Intel | Parens | 30.359 ms | 22.005 ms | 1.380x |
| Graviton | Easy0 | 2.905 ms | 2.246 ms | 1.294x |
| Graviton | Easy1 | 3.562 ms | 2.568 ms | 1.387x |
| Graviton | Hard | 42.378 ms | 30.192 ms | 1.404x |
| Graviton | Parens | 42.334 ms | 30.172 ms | 1.403x |

The gap is stable across the corrected snapshots, so the high-level capture and
boundary work did not regress the transition loop. It remains material.

The pre-restack implementation at
`archive/user-dain/re2-port/pre-restack` was replayed locally with the current
JDK and byte-identical corpus. Current 16 MiB Easy0, Easy1, Hard, and Parens
results were within -1.6% to +1.4% of that historical implementation. The
current native gap is therefore not explained by recent correctness, API, or
cleanup refactoring.

Local JVM-layout and loop-shape ablations initially provided a more specific
direction:

- Ten default compressed-reference forks averaged 25.03 ms for 16 MiB Hard;
  most forks clustered near 25.0 ms, while earlier runs intermittently produced
  a distinct 29.2 ms compilation.
- Ten uncompressed-reference forks averaged 23.14 ms, 7.5% faster, without the
  29 ms shape. Easy0 and Easy1 were unchanged in the paired four-workload run.
- Disabling unrolling measured 31.18 ms. Constraining the maximum unroll factor
  did not eliminate the slow compilation, so a JVM-wide unroll flag is not a
  valid mitigation.
- A manually unrolled two-byte source loop left Easy0 and Easy1 unchanged,
  failed to improve Hard, and trended 5.8% slower on Parens. It was rejected.

The dedicated cross-architecture diagnostic produced stable current/native and
uncompressed/native controls:

| Architecture | Workload | Current | Uncompressed | Native | Current/native | Uncompressed/native |
|---|---|---:|---:|---:|---:|---:|
| Intel | Hard | 30.447 ms | 23.928 ms | 22.606 ms | 1.347x | 1.058x |
| Intel | Parens | 30.479 ms | - | 22.595 ms | 1.349x | - |
| Graviton | Hard | 42.511 ms | 36.476 ms | 30.194 ms | 1.408x | 1.208x |
| Graviton | Parens | 42.325 ms | - | 30.249 ms | 1.399x | - |

The default-reference Hard result was stable across all ten forks on both
architectures. Removing compressed references improved Intel by 21.4% and
Graviton by 14.2%. This is a causal control, not a deployment recommendation:
it doubles every heap reference and would materially increase large DFA caches
and retained heap.

The generated code explains the remaining shape:

- On Intel, the loop sustained 2.98 instructions per cycle with negligible
  branch misses. C2 `Dfa.searchForward` contained 90.65% of sampled cycles, and
  27.19% of all samples landed on one recursive-array load and compressed-
  reference decode sequence immediately before the surviving array checkcast.
- On Graviton, the loop sustained 3.50 instructions per cycle with a 0.01% L1
  data-cache miss rate. C2 `Dfa.searchForward` contained 84.49% of sampled
  cycles, and two recursive-array checkcast instructions alone contained 17.13%
  of all samples.
- Neither hot region contains synchronization, an acquire operation, a call, or
  a material branch-miss problem. The per-search reader registration therefore
  does not explain this single long traversal.

The direct gap is now explained as the generated cost of representing a
recursive native state-pointer table with covariant Java object arrays and
compressed references. Two primitive-table controls failed to preserve the
object row's favorable address dependency: a flat `int[]` loop and a byte-
offset `byte[]`/`VarHandle` loop were both slower. The current object-state loop
remains the best measured representation despite its visible JVM tax.

## DFA Concurrency And Cache

The retained design keeps compiled `Re2` instances thread-safe and preserves a
shared warm DFA cache without a read lock or acquire load in the per-byte
transition loop:

- searches register in padded thread-local reader slots
- cold construction and reset block new readers and wait for active readers
- a cache generation prevents queued builders from rebuilding published work
- stale weak reader registrations are drained without a linear cleanup pass
- cache reset and prefix self-loop rewriting use the same exclusive invariant

Correctness coverage starts cold workers together, mixes searches with reset,
checks cache-generation behavior, and exercises short-lived platform and virtual
threads. The AWS cold-wave benchmark is diagnostic rather than a release claim;
the 16-worker case also oversubscribes a 16-core host with its coordinator.

Retained reader-registry memory across many low-frequency patterns and many
short-lived threads remains to be quantified.

A focused audit demonstrated the concrete retention mechanism: after 5,000
virtual-thread searches and collection, the global registry still contained
5,000 cleared weak-reference keys. They are drained by the next thread
registration or exclusive cache mutation, but a final short-lived thread wave
can remain represented indefinitely on an otherwise warm, read-only process.
This needs a bounded cleanup design and retained-memory test. A per-search
reference-queue poll is not acceptable without proving that it does not regress
dense repeated matching.

A controlled single-thread ablation removed reader registration and publication
while retaining the same DFA transitions. Dense 32 KiB count improved from
122.039 us to 109.502 us, or 10.3%, because it performs thousands of separate
searches. This quantifies a real per-search cost but cannot explain the
single-search 16 MiB DFA gap. Removing the lifetime protocol is not viable:
plain shared cache access previously produced reproducible incorrect results.

### Retained-memory closure

Fresh-JVM JOL graph measurements on JDK 25 quantified the retained structures
that were previously inferred only from logical counters:

| Graph | Stage | Retained bytes | Objects |
|---|---|---:|---:|
| Representative DFA | cold | 9,152 | 84 |
| Representative DFA | warm with paired rows | 16,544 | 99 |
| Representative DFA | reset | 9,256 | 86 |
| Representative DFA | after 100 warm/reset cycles | 9,256 | 86 |
| `[a-z]+` compiled pattern | compiled | 1,872 | 48 |
| `[a-z]+` compiled pattern | boolean match, no reverse | 4,384 | 101 |
| `[a-z]+` compiled pattern | boundary match with lazy reverse | 9,888 | 219 |
| 1,000 varied compiled patterns | compiled | 2,069,712 | 37,918 |

Reset clears the state map, metadata roots, reference rows, start states, and
paired rows while intentionally retaining reusable primitive/reference array
capacity. The identical 9,256-byte graph after one and 101 resets demonstrates
a plateau rather than reset-cycle growth. Lazy reverse construction is visible
and bounded instead of being hidden in the initial compile graph.

The reader registry did have a real high-water retention defect. With the old
`ConcurrentHashMap`, a 10,000-virtual-thread wave retained 705,704 bytes while
the weak keys awaited draining and still retained a 65,640-byte empty table
after all 10,000 keys were removed. Reader registration is a once-per-thread
cold path, so the registry now uses a locked weak-reference set and replaces
the set after stale-heavy drains. The same measurement returns to a 64-byte
empty graph. Search-loop publication is unchanged. Direct tests additionally
verify primitive-only slots, one slot per persistent worker, root compaction,
cache reset, and concurrent search/reset behavior.

## Sparse Captures And Trino

The baseline performed a full forward and reverse DFA search for every requested
capture in a sparse repeated-match workload. The retained candidate finds the
exact full-match boundaries once and lets the capture engine operate only on
that bounded region. Pinned native cases protect greedy, reluctant, anchored,
and unmatched capture boundaries.

At 32 KiB, representative AWS improvements were:

| Architecture | Operation | Baseline | Candidate | Speedup |
|---|---|---:|---:|---:|
| Intel | extract | 263.9 us | 15.0 us | 17.6x |
| Intel | count | 621.3 us | 60.2 us | 10.3x |
| Intel | position third | 601.1 us | 45.5 us | 13.2x |
| Intel | replace | 625.3 us | 62.8 us | 10.0x |
| Graviton | extract | 291.7 us | 20.9 us | 14.0x |
| Graviton | count | 742.0 us | 83.3 us | 8.9x |
| Graviton | position third | 727.7 us | 85.7 us | 8.5x |
| Graviton | replace | 744.8 us | 85.0 us | 8.8x |

At the corrected full-session checkpoint, before the later start-byte,
single-byte, capture-demand, and nullable-repeat work, the geometric Slice/Joni
ratio across all 80 Trino operation points was 1.223x on Intel and 1.182x on
Graviton. The
Slice/historical-RE2J ratio was 1.259x and 1.334x. A ratio above one means this
port is slower. Slice won 22/80 and 22/80 points against Joni and 24/80 and
23/80 against historical RE2J. These historical values explain the work that
followed; they are superseded by the current matrix later in this section.

At that checkpoint, the sparse-capture workload remained 2.52x slower than Joni
on Intel and 2.31x slower on Graviton at 32 KiB, and 6.08x and 5.67x slower than
historical RE2J. Operation-level profiles attributed the residual to repeated
search rather than allocation. Later start-byte acceleration and
operation-specific capture demand eliminate this deficit; current evidence is
reported below.

Local operation-level profiles showed that `count` allocated only about 70-87
bytes for the complete 32 KiB operation; `split` allocation was dominated by its
required result list and Slice views. Samples instead stayed in repeated forward
and reverse DFA searches. Counting never reads match starts, but the reusable
matcher requested group zero and therefore ran the reverse DFA for every match.

The retained count path uses compiler nullability to separate two safe cases:

- non-nullable patterns count successive forward-DFA match ends without creating
  a matcher or compiling the reverse program
- when a nullable pattern's forward DFA reports boundary zero, group zero is
  known to start and end at the current search offset, so boundary recovery also
  skips the reverse DFA

Nullable patterns with a non-empty match, DFA cache failure, and operations that
need match starts retain the existing matcher and engine cascade. Direct tests
compare the fast count with matcher iteration across anchors, alternation,
greediness, captures, case folding, boundaries, and UTF-8. They also assert that
nullable patterns select the fallback and that an empty boundary does not compile
the reverse program.

Local same-method 32 KiB results were:

| Workload | Baseline | Candidate | Speedup |
|---|---:|---:|---:|
| Dense `[,;]` count | 285.8 us | 122.9 us | 2.33x |
| Empty `x*` count | 1.013 ms | 597.1 us | 1.70x |

The dense fast path reduced normalized allocation from about 70 bytes to
measurement noise. The corrected cross-architecture session confirms the
algorithmic improvement, but dense 32 KiB count still took 181.2 us on Intel and
232.2 us on Graviton, about 1.79x Joni on both. Empty count remained about
1.83x and 1.80x Joni.

The later single-byte matcher resolves the dense count deficit. The remaining
empty-count loss was not allocation or capture extraction: `x*` returned an
empty match at every input boundary, causing one DFA search protocol per code
point. A local specialization now recognizes only greedy zero-or-more repetition
of a proven one-byte language and counts matching runs and unmatched code points
in one pass.

At 32 KiB, the complete local Trino count benchmark improved from 125.15 us to
17.50 us, or 7.15x. A same-host Joni run over the byte-identical workload took
358.38 us. Controlled inputs containing all misses, one matching run,
alternating bytes, 64-byte runs, and four-byte UTF-8 misses improved by 3.5x to
33x over regular matcher iteration. The analyzer rejects reluctant repetition,
assertions, multi-byte languages, and structurally complex nullable patterns.

Target session `20260715T164403Z` validates the specialized counter against
ordinary matcher iteration. At 32 KiB, candidate/control ratios were:

| Input shape | Intel | Graviton |
|---|---:|---:|
| All misses | 0.12x | 0.14x |
| One matching run | 0.02x | 0.03x |
| Alternating match and miss | 0.02x | 0.03x |
| 64-byte runs | 0.08x | 0.10x |
| Four-byte UTF-8 misses | 0.26x | 0.30x |

The complete 32 KiB Trino count takes 18.78 us on Intel and 26.59 us on
Graviton. The implementation is retained. The replacement operation matrix
reported below includes this path and supersedes the stale comparison.

Exact-current session `20260715T195116Z` extends that control to both counting
and reusable match-boundary iteration and runs the complete nullable operation
A/B. At 32 KiB, direct counting takes 0.022-0.258x ordinary matcher time on
Intel and 0.027-0.300x on Graviton. Boundary iteration takes 0.014-0.387x and
0.021-0.473x. Empty-match extract-all, lambda replacement, replacement, and
split take 0.676-0.793x disabled time on Intel and 0.571-0.841x on Graviton.
Third-position lookup takes 0.032x and 0.001x after the independent code-point
upper-bound fix.

The corresponding exact-one extract-all, replacement, and split controls stay
within 1.1% on both architectures. One Intel candidate-before lambda fork took
109.9 us instead of the 57.8-57.9 us cluster seen in its other four forks and
all five candidate-after forks. The disabled control was 59.0 us. This is the
previously observed alternate JVM compilation, not a source-induced regression;
the stable candidate-after/control ratio is 0.981x. Both hosts passed the full
RE2 selector and uploaded exit status zero. The nullable matcher is accepted.

The operation gate is capture-demand based rather than capture-presence based.
Captured nullable patterns use the same boundary cursor for position, split,
group-zero extraction, and replacements that reference only group zero;
subgroup extraction and lambda replacement retain the ordinary matcher. Direct
route tests protect both decisions. Adapter-level differential coverage compares
the specialized operations with general matcher boundaries over malformed
UTF-8, Latin1, non-default position starts, and nonzero Slice backing offsets.

## Fixed-Width Boundary Recovery

An unanchored DFA search normally reports the match end and runs the reverse DFA
to recover the match start. That second search is unnecessary when every match
of the expression has the same encoded byte length: the start is the end minus
that length. Dense one-byte delimiter and fixed-width Unicode operations were
therefore paying for two searches when one was sufficient.

The retained implementation analyzes the simplified suffix AST once during
`Re2` construction and stores an exact byte length only when all successful
paths agree. The normal analyzer is allocation-free and uses bounded recursion;
deep expressions switch to the existing iterative AST walker. Unknown and
variable-width expressions conservatively retain reverse-DFA boundary recovery.
Direct tests cover captures, alternation, repetition, UTF-8, Latin1, variable
width, reverse-program selection, and a 10,000-capture expression on a 256 KiB
thread stack.

Two earlier designs were rejected:

- Propagating length through compiler fragments avoided a separate AST walk but
  increased representative total compile costs by up to 2.9%.
- Computing and caching the length lazily removed that compile cost, but its
  volatile load remained on every match and made the dense split path about 3.4%
  slower than eager immutable metadata.

The final design was compared with the exact corrected AWS source snapshot on
the same Apple Arm host. Three-fork 32 KiB results were:

| Workload | Snapshot | Candidate | Change |
|---|---:|---:|---:|
| Literal sparse split | 2.578 us | 2.517 us | 2.4% faster |
| Variable-width captured split | 21.610 us | 21.567 us | unchanged |
| Dense delimiter split | 315.295 us | 182.840 us | 42.0% faster |
| Empty-match split | 874.069 us | 899.899 us | unchanged within overlapping error intervals |
| Fixed-width Unicode split | 51.933 us | 36.438 us | 29.8% faster |

All six complete `Re2.compile` rows remained within -1.6% to +1.2% of the exact
snapshot. A compiler-only row moved by 2.4%, but the compared method is
source-identical and its confidence intervals overlap, so there is no causal
compile regression.

The targeted cross-architecture session confirms the boundary result. At
32 KiB, fixed-width Unicode extract and split improved by 25.5% and 26.0% on
Intel and by 29.8% and 29.2% on Graviton. Count and contains do not request a
match start and therefore remain unchanged, as expected.

## Single-Byte Trino Matching

Dense one-byte languages such as `[,;]` remained expensive after forward-only
counting because the generic engine still started a separate DFA search for
every match. For capture-free Trino operations, the complete language can
instead be represented as a 256-entry byte membership table and scanned once.

The retained implementation analyzes the simplified AST only for context-free
expressions with exact encoded length one and no stripped required prefix. It
accepts byte literals, safe case folds, byte character classes, alternatives,
capture wrappers used by count, exact-one repetition, `AnyByte`, and Latin1
`AnyChar`. Anchors, boundaries, variable repetition, required prefixes, and
multibyte UTF-8 conservatively use the normal engine. Count scans the source
once; capture-free repeated operations use the same table to find boundaries.
Patterns requiring capture extraction always use the existing matcher.

The table is initialized lazily in `TrinoRegexp`. Eager construction increased
the representative `[;x]` compile row from 856 ns to 1,058 ns, or 23.5%, and was
rejected. The lazy design measured 840 ns with overlapping intervals and does
not change `Re2.compile` work.

Correctness tests compare every optimized boundary and count with the regular
matcher over all 256 byte values in a non-zero Slice window. They also cover
malformed UTF-8, Latin1, case folding, unsupported contexts, required prefixes,
capturing wrappers, and 10,000 nested captures on a 256 KiB thread stack.

Representative targeted 32 KiB results were:

| Architecture | Operation | Corrected baseline | Candidate | Change |
|---|---|---:|---:|---:|
| Intel | count | 181.236 us | 8.432 us | 95.3% faster |
| Intel | extract-all | 413.716 us | 47.762 us | 88.5% faster |
| Intel | split | 433.424 us | 48.836 us | 88.7% faster |
| Graviton | count | 232.229 us | 25.520 us | 89.0% faster |
| Graviton | extract-all | 543.936 us | 77.175 us | 85.8% faster |
| Graviton | split | 528.043 us | 76.931 us | 85.4% faster |

Across the complete 80-point matrix, Slice now takes geometrically 0.810x Joni
time on Intel and 0.819x on Graviton, winning 42/80 points on both. Against
historical RE2J it takes 0.834x and 0.924x, winning 40/80 and 37/80 points.

The subsequent current Graviton full session `20260715T170407Z` completed the
same-host Slice, Joni, and historical RE2J matrix after start-byte acceleration
and nullable repetition counting. Slice took geometrically 0.478x Joni time and
won 74/80 points. All six losses had concrete bounded causes:

- Unicode group-zero extraction was 1.19-1.21x Joni because the matcher still
  requested the pattern's otherwise unused capture.
- Third Unicode position was 1.001-1.020x Joni for the same capture-demand path.
- Dense one-byte containment was 1.05-1.07x Joni because `TrinoRegexp.contains`
  bypassed its existing single-byte matcher.

Replacement Slice session `20260715T193619Z` and comparator session
`20260715T185531Z` cover the current source snapshot on both architectures. On
Intel, Slice took geometrically 0.355x Joni time and won 79/80 points. The only
nominal loss was 1 KiB Unicode extraction at 1.013x; the Slice and Joni 99.9%
confidence intervals overlap. On Graviton, Slice took 0.294x Joni time and won
all 80/80 points. Against historical RE2J, Slice took 0.365x and 0.332x time and
won 62/80 and 70/80 points. These are engineering comparisons from matching
instance families but separate Slice and comparator runs, not the later formal
qualification campaign. A focused same-host Intel comparison was therefore
required before calling the 1.3% point a regression.

At that snapshot, focused same-host session `20260715T211625Z` resolved that
specific point. On Intel,
interleaved Slice-before, Joni, and Slice-after measurements were 62.046,
62.734, and 62.551 ns/op. The geometric Slice/Joni ratio is 0.993x with
overlapping uncertainty. Slice forks repeat the previously observed bimodal JVM
compilation at approximately 56.9 and 63.3 ns/op; even the common slower cluster
was only about 1% behind Joni. On Graviton, the corresponding scores were 75.937,
90.592, and 76.725 ns/op, for a 0.843x ratio. Both hosts passed the full RE2
selector and exited successfully. That row had no reproducible loss; the later
candidate qualification supersedes any broader interpretation of this
historical result.

The source-disabled group-zero campaign `20260715T184445Z` accepts
operation-specific capture demand on both architectures. Across the 15 protected
32 KiB operation rows, the candidate/control geomean was 0.851x on both Intel
and Graviton. Unicode extraction, third position, replacement, and split took
0.703-0.789x control on Intel and 0.684-0.755x on Graviton. The three 1 KiB
Unicode rows took geometrically 0.736x and 0.718x. Capture and delimiter controls
were neutral; the largest candidate/control value was 1.005x. The implementation
is retained.

A follow-up initialization audit removed two capture-proportional costs from
these routes. Group-zero matchers now use an empty named-group map instead of
walking and retaining the pattern's named captures, while full-capture matchers
do not analyze or allocate a one-byte table that they cannot use. Direct
sentinels prove that each cache remains uninitialized on the ineligible route.

The initial single-byte containment campaign `20260715T184449Z` provisionally
accepted the optimization. Direct
delimiter containment took 0.180x control time at both 1 KiB and 32 KiB on
Intel, and 0.148x and 0.144x on Graviton. The eight protected capture, empty,
literal, and Unicode rows stayed within 1.1% of control on both architectures;
candidate-before and candidate-after stayed within 1.4%. Later exact-current
controls below supersede that provisional decision.

The targeted sessions showed a few point regressions on unsupported workloads,
but the exact same-host A/B guard did not reproduce them: literal contains was
+0.12%, empty count +0.21%, empty third-position -0.32%, and empty replacement
+1.84%, all with overlapping confidence intervals. No protected regression
persisted above the campaign's 2% gate in that source snapshot.

Exact-current follow-up session `20260715T231720Z` added explicit no-match and
late-match controls. Those four rows passed: candidate/control was
0.943-1.003x on Intel and 0.933-1.014x on Graviton. The refreshed protected
matrix nevertheless reopened the acceptance decision. Intel capture-sparse
containment took 1.036x disabled time at 1 KiB and 1.029x at 32 KiB;
candidate-before and candidate-after differed by at most 1.4%, and the 1 KiB
confidence intervals do not overlap the control. Graviton capture-sparse rows
were neutral. This is a fixed dispatch or code-shape cost on an ineligible
route, not an edge-search failure.

Follow-up session `20260716T062041Z-20691` moved direct containment into a
specialized subtype. This removed the capture-sparse cost: all four 32 KiB
capture rows were within 0.3% of the disabled control across Intel and
Graviton. Direct one-byte no-match and late-match rows took 0.94-1.00x control.
However, the broad fixed-width eligibility predicate also selected unsupported
anchor patterns, which then paid a volatile sentinel load before falling back.
The worst Intel row was 1.022x control and Graviton end-anchor rows were
1.058-1.062x, so this version fails the 2% gate. Candidate-before/after drift
stayed within 1.3%, and both host runs exited successfully.

Exact-current session `20260716T114214Z-51237` tested the narrower AST-based
eligibility design. Its main operation matrix remained within 0.6% of the
source-disabled control, and dense direct containment improved to
0.139-0.143x control. Graviton generic routes nevertheless reached 1.033x for
single-byte no-match and 1.030x for an unsupported boundary pattern. Those
stable regressions fail the 2% no-regression gate, so the narrow dense win does
not justify a separate dispatch route. Compile control session
`20260716T114420Z-52459` passed on Intel and Graviton. The specialized subtype
and eligibility metadata are removed; containment again uses the generic
partial-match route. The one-byte matcher remains retained for count and
repeated-operation paths that already qualify without this shared cost.

Literal-sparse operations show the opposite result: at 32 KiB, several
contains, count, extract, extract-all, split, and replacement points are about
3-7x faster than Joni. This confirms that the high-level harness can expose both
wins and losses rather than systematically favoring one implementation.

Current Trino-shaped results call equivalent scalar operations directly. They
measure the library and its Slice operation adapter without including Trino
page projections, block construction, SQL registration, or distributed query
execution; those belong to the separate Trino adoption project.

## Sparse Start-Byte Acceleration

The remaining capture-sparse deficit was already present in `contains`, which
requests neither captures nor a match-start boundary. Operation-level profiles
kept essentially all runnable samples in `Dfa.searchForward`: the pattern
`([a-z]+)-([0-9]+)` executed every preceding dot through the transition loop.
Historical Trino RE2J instead scanned the input to the next byte that could
leave the unanchored start state before resuming DFA execution.

The retained Java extension derives a conservative first-byte set from the
flattened program when no literal-prefix acceleration applies. It is limited to
forward, unanchored, non-nullable programs with at most 64 candidate bytes.
Leading empty-width assertions and denser sets use the normal DFA because
skipping would respectively lose previous-byte context or create excessive
scan/DFA handoffs. The immutable 256-byte table is constructed with the lazy
`DfaInstance`, charged to that cache's memory budget, and safely published with
the instance. It does not add work to `Re2.compile`.

The direct path test uses a pinned native-golden window and covers optional
leading bytes, UTF-8 leading-byte ranges, ASCII case folding, anchors,
nullability, dense classes, malformed surrounding bytes, and empty-width
assertions. The upstream DFA/NFA agreement corpus and Trino function corpus
caught an early word-boundary bug before benchmarking; rejecting
`EMPTY_WIDTH` paths corrected both failures.

An eager program-level design was rejected even though it produced the target
runtime win: compiler-only and complete compile rows regressed by 11.2% and
10.5%. Moving analysis to lazy DFA construction restored those rows to -0.1%
and -0.5% of the exact disabled control. Reusing the DFA traversal stack and
marking byte ranges directly reduced compile-plus-cold-DFA construction from an
initial noisy 2.1% mean regression to 0.14% with overlapping intervals.

Five-fork same-source local A/B results at 32 KiB were:

| Operation | Disabled control | Candidate | Change |
|---|---:|---:|---:|
| Contains | 5.368 us | 2.503 us | 53.4% faster |
| Count | 21.177 us | 10.073 us | 52.4% faster |
| Extract | 5.455 us | 2.611 us | 52.1% faster |
| Replace | 23.115 us | 11.752 us | 49.2% faster |

The exact protected gate changed 16 MiB Easy0, Easy1, Hard, and Parens and
32 KiB Easy2 and BigFixed by -0.34% to +1.17%; every result stayed below the
2% rejection threshold with overlapping uncertainty. Target session
`20260715T165107Z` then confirmed capture-sparse candidate/control ratios of
0.35-0.45x on Intel and 0.39-0.43x on Graviton, with neutral protected search
controls. The one-time first-DFA construction row was 1.027x control on Intel
and 1.001x on Graviton; complete `Re2` compilation was 1.007x and 1.017x. The
runtime gap is closed without moving analysis onto every compile.

## Public API Allocations

The corrected full session includes removal of temporary DFA search parameters,
the public match search-result object, and OnePass capture scratch storage.
`TestRe2Allocations` verifies exact zero allocation after warmup for boolean
matching and caller-owned capture buffers. Intel and Graviton JMH allocation
profiles round these paths to approximately zero; the remaining values are
measurement noise. Result-returning and new-matcher convenience APIs still
allocate by contract.

Local dense and empty `count` profiles show no material per-match allocation;
their cost was repeated two-phase boundary recovery. Split and extraction APIs
still allocate their required result containers and Slice views. Further work on
those operations should target measured engine dispatch or output construction,
not assume capture-engine allocation.

## Eager OnePass Preparation

Forward compilation now determines OnePass eligibility once, applies upstream's
quarter-DFA-budget gate, deducts retained OnePass storage from the DFA budget,
and avoids repeated synchronized analysis during matching. Reverse programs do
not prepare OnePass because they do not use it.

Local A/B results show the expected tradeoff:

- compile-only program construction increased by about 2.8-14.7%
- complete `Re2` compile plus first full match stayed within -1.4% to +1.0%
- steady tiny caller-buffer matching improved by 5.2%
- large matching and no-match paths remained within 0.4%

The change is retained because it moves required preparation to object
construction without changing normal compile-and-first-use cost, improves a
small steady-state path, and gives memory accounting one deterministic owner.

## Required-Prefix Architecture

Pinned C++ RE2 extracts an anchored required prefix, compiles only the suffix,
checks and removes the prefix before matching, and restores group zero afterward.
The Java port extracted prefix metadata but still compiled and executed the full
expression. It now follows the upstream architecture and has native-golden tests
for captures, case folding, mismatches, search windows, and group-zero offsets.

The BigFixed benchmark was also corrected to use upstream's 2 GiB cached-program
budget and 1 MiB maximum parameter. On the same Apple Arm host and JDK, the
public `Re2` A/B was:

| Input | Full program | Scalar suffix | Intrinsic suffix | Speedup vs full |
|---:|---:|---:|---:|---:|
| 8 B | 22.9 ns | 12.6 ns | 12.3 ns | 1.9x |
| 64 B | 144.4 ns | 43.6 ns | 35.0 ns | 4.1x |
| 512 B | 1.008 us | 219.6 ns | 164.0 ns | 6.1x |
| 4 KiB | 19.74 us | 1.685 us | 1.238 us | 15.9x |
| 32 KiB | 45.41 us | 13.19 us | 9.435 us | 4.8x |
| 256 KiB | 3.314 ms | 106.6 us | 73.47 us | 45.1x |
| 1 MiB | 13.38 ms | 402.0 us | 302.4 us | 44.2x |

`Arrays.mismatch` is neutral at 8 bytes and improves the suffix path by 20-31%
from 64 bytes through 1 MiB. It is retained for the case-sensitive comparison;
the folded-prefix path remains scalar because it must normalize ASCII case.

The pinned direct C++ `RE2` benchmark measured 21.7 ns through 965.9 us across
the same sizes and corpus. The intrinsic suffix-program Java path was 0.30-0.57x
native time locally. Apple results are directional, but the scaling and root
cause are clear: the old path executed a literal program proportional to the
required prefix, while both the retained Java path and C++ execute a prefix
check plus a small suffix program.

The corrected full session confirms the architecture and removes the prior
algorithmic failure. Public `Re2` was faster than native at 8 bytes and settled
near the general DFA gap for larger inputs: Java/native was 1.402x, 1.406x, and
1.408x on Intel at 4 KiB, 32 KiB, and 1 MiB, and 1.410x, 1.401x, and 1.413x on
Graviton. Required-prefix handling is no longer a separate scaling problem.

## DFA State-Budget Accounting

Correcting BigFixed to use upstream's 2 GiB cached-program budget exposed a real
Java DFA failure at 256 KiB. Java estimated every cached state as though it held
the program's maximum instruction list, limited the longest-match cache to 1,019
states, reset once, and returned `SEARCH_FAILED`. Pinned C++ uses that maximum
only to require 20 states of initialization room; each allocated state is then
charged for its actual instruction count.

Java now follows that policy. The 256 KiB literal builds 131,074 small states
without resetting, while constrained De Bruijn tests still exhaust and reset the
cache. The benchmark smoke test asserts the direct DFA result rather than
allowing the failure sentinel to become a fast benchmark result.

Aligned local direct-DFA results after the correction were:

| Input | Java | Native | Java/native |
|---:|---:|---:|---:|
| 8 B | 18.7 ns | 17.4 ns | 1.076x |
| 64 B | 130.8 ns | 107.7 ns | 1.215x |
| 512 B | 981.8 ns | 895.6 ns | 1.096x |
| 4 KiB | 5.518 us | 8.374 us | 0.659x |
| 32 KiB | 59.28 us | 61.87 us | 0.958x |
| 256 KiB | 234.6 us | 495.7 us | 0.473x |
| 1 MiB | 912.2 us | 2.090 ms | 0.437x |

The large-input advantage comes from Java's anchored long-prefix DFA
specialization; both implementations still traverse the full literal program in
this direct-engine benchmark. The corrected Intel and Graviton session confirms
that all sizes complete without the failure sentinel. At 1 MiB, direct Java DFA
was 0.723x native time on Intel and 0.808x on Graviton.

## Correctness Gate

After all retained campaign changes, the complete RE2 selector passes 658 tests
with zero failures or errors. This includes exhaustive and randomized agreement,
native golden cases, shared-cache concurrency, allocation assertions, Trino
corpora, fixed-width and single-byte differential guards, and benchmark smoke
tests. A Maven install on 2026-07-15 passed all 2,448 project tests and build
checkers with no skipped tests.

The cross-architecture preflight found that the maximum-capture test depended on
the larger local stack. Simplification now retains recursive processing for
shallow ASTs and switches to the existing explicit post-order walker at depth
256. The regression test runs the 10,000-capture parse on a thread requesting a
256 KiB stack, and a standalone `-Xss256k` JVM confirms the same path.

An always-iterative prototype was rejected because local same-process A/B runs
slowed ordinary parse and simplify rows by roughly 30-160%. The retained hybrid
reduced the observed differences to mostly within 3% of the recursive control;
one noisy row reached about 6%. The corrected full and targeted
cross-architecture sessions include the retained hybrid rather than the rejected
always-iterative prototype.

## Rejected And Superseded Experiments

- A persistent sparse OnePass capture history regressed the Intel target
  aggregate by 3.6% and digit captures by about 5.2%.
- Packed `long[]` and flat `int[]` BitState traversal-job stacks reduced
  allocation and improved selected Graviton capture rows, but failed protected
  capture-free or Intel capture gates by 4.6%-12.9%. The retained compact
  instruction sidecar is a separate representation and keeps object traversal
  jobs.
- Combining public capture-offset adjustment passes was neutral on large inputs
  and Graviton but regressed Intel tiny matched `find()` and `matchInto()` by
  13.6% and 7.5%.
- Plain loads without a cache-lifetime protocol were rejected after shared
  searches produced reproducible incorrect results.
- A read lock around every search was correct but collapsed tiny shared-search
  throughput and left acquire loads in the transition loop.
- Linear weak-reader cleanup was rejected because short-lived threads could make
  a later registration or reset unbounded.
- A generic flat `int[]` transition path regressed local 16 MiB Hard and Parens
  by about 30% relative to the normal object-state compilation shape. It added
  state-and-class address dependencies that the object row avoids.
- A native-order byte transition table carrying pre-scaled byte offsets passed
  43 focused DFA tests but regressed Hard by 9.9% and Parens by 14.2% relative
  to the outlier-excluded object-state means. Its `VarHandle` load and offset
  arithmetic did not recover the native pointer-table shape, so it was rejected
  before an AWS run.
- Early cold-cache benchmark results were discarded after finding that the
  benchmark reset a different DFA kind than it executed.
- Historical numbers using a different random corpus are not used for current
  Java/native ratios.
- Always-iterative simplification was rejected because its per-node traversal
  objects materially regressed ordinary parser and compiler workloads. Only
  deep ASTs use the explicit traversal.
- Eager fixed-width propagation and lazy volatile fixed-width metadata were
  rejected for compile and repeated-match regressions respectively.
- Eager single-byte table construction was rejected for a 23.5% compile
  regression; the retained table is lazy.
- Eager sparse start-byte analysis was rejected for 10-11% compiler and total
  compile regressions; the retained table is owned by the lazy DFA instance.
- Disabling compressed references improved the local object-state loop but was
  not retained because reference width and large-cache memory would double.
- JVM unroll limits and manual two-byte unrolling did not stabilize or improve
  the DFA loop and were rejected.
- Fully expanding compact byte classes into direct 256-entry transition rows did
  not improve the uninterrupted loop. Intel Hard measured 30.526 ms for direct
  rows and 30.256 ms for compact rows; Graviton measured 42.250 ms and 42.415 ms
  respectively. Avoiding the byte-map lookup did not offset the larger cache
  footprint.

## Bounded Multi-Byte Transition Control

A diagnostic depth-two transition composition reduced one dependent state load
per two input bytes on the actual Hard and Parens graph. That graph had only five
states and 29 byte classes: compact transition references occupied about 580
bytes and the composed references about 16.8 KiB. In isolated five-fork AWS
measurements, compact/composed Hard was 26.777/15.171 ms on Intel and
42.176/18.672 ms on Graviton. The corresponding native Hard results were 22.479
ms and 30.186 ms.

This is not evidence for a fully expanded DFA. It demonstrates potential for a
strictly bounded small-DFA specialization when the composed table remains in
cache. Before integration, test larger state and byte-class counts, determine
the cache-footprint crossover on both architectures, enforce an absolute memory
cap charged to the DFA budget, and retain the compact path as fallback.

Session `20260714T205326Z` measured the crossover with 4-256 states and 16, 29,
or 64 byte classes. The compact control remained near 26.8 ms on Intel and 42.1
ms on Graviton until the largest graph. Intel was the limiting architecture:

| Paired reference payload | Representative graph | Intel pair/compact | Graviton pair/compact |
|---:|---:|---:|---:|
| 64 KiB | 64 states, 16 classes | 0.856x | 0.465x |
| 105 KiB | 32 states, 29 classes | 1.079x | 0.627x |
| 128 KiB | 128 states, 16 classes | 1.151x | 0.676x |
| 256 KiB | 256 states, 16 classes | 1.305x | 0.893x |
| 512 KiB | 32 states, 64 classes | 1.566x | 1.059x |

This reproduces the cache-footprint failure of broader expansion and establishes
an intentionally conservative candidate gate: estimate references at eight
bytes and allow at most 64 KiB, including row overhead. On the normal
compressed-oop JVM this keeps payload near 32 KiB, while the five-state,
29-class Hard/Parens graph still qualifies. The specialization must remain
discardable when the core DFA cache needs its budget.

The integrated candidate in session `20260714T214530Z` constructs that table
only after a generic forward search of at least 256 bytes requests it, discards
it before denying memory to normal DFA states, and falls back to compact rows
before either byte of an abnormal pair is consumed. Both 2 GiB benchmark JVMs
reported compressed oops. Five-fork 16 MiB direct-DFA results were:

| Architecture | Java Hard | Native Hard | Java Parens | Native Parens |
|---|---:|---:|---:|---:|
| Intel | 13.816 ms | 22.593 ms | 13.814 ms | 22.070 ms |
| Graviton | 16.179 ms | 30.186 ms | 16.172 ms | 30.246 ms |

Java is 37-39% faster than native on Intel and about 46% faster on Graviton for
these uninterrupted cached-DFA workloads. This resolves the representation
feasibility question for the dominant small-DFA path; broader public and Trino
qualification remains required and global transition expansion remains
rejected.

### Partial Paired Rows

The complete-table cap leaves a narrow gap where a large DFA graph spends nearly
all of its time in one or two hot rows. A Rebar census at commit
`463d00f31887e84c38467805b9e3122c314b9521` expanded and executed all 285
definitions with Rebar's exact patterns, flags, and inputs. The initial partial
candidate selected cached entry rows followed by states with observed
self-loops. It produced 12 partial routes:

- six Hard/ReallyHard definitions retained two rows, eight composed
  transitions, and 13.4-14.4 KiB
- Leipzig retained six rows and about 21 KiB
- both keyword definitions retained three rows and about 27 KiB
- no-quadratic retained three rows and about 8 KiB
- URL retained two rows and about 54 KiB while the DFA cache reset five times

Exact local corpus measurements rejected the broad policy. Leipzig and keywords
were unchanged, no-quadratic was too small to benefit, and URL regressed from
182.9 ms to 687.1 ms because the partial table displaced core DFA states. The
Hard tail improved consistently. The retained policy therefore allows a partial
table only when exactly two rows fit within 16 KiB; complete tables continue to
use the separate 64 KiB cap.

The first narrowed AWS candidate exposed a separate eligibility-cost bug even
though it retained no table for URL. Every exclusive cache mutation scanned all
states and byte classes before discovering that two rows exceeded 16 KiB. URL
therefore took 566-569 ms instead of the 189 ms Intel control and 820-838 ms
instead of the 238 ms Graviton control. The final gate computes the minimum
two-row size first and rejects oversized rows before row selection. A direct
path test asserts that large-row rejection performs zero state/class scans.

With the retained policy, the 285-definition route census contains 29 complete
paired DFAs and only six partial DFAs, exactly the Hard/ReallyHard definitions.
The remaining previously selected definitions stay on the compact path. Direct
tests cover both halves of paired fallback, uncomputed, match, and dead
transitions, exact memory charging, reset release, concurrent reset, multi-row
rejection, and large-row rejection.

Exploratory session `20260715T001654Z` compared the retained Hard-tail shape with
an all-partial-disabled source control and both generic and host-tuned pinned
native RE2. Both native binaries were built with `-O3 -DNDEBUG`; host tuning
added `-march=native` on Intel and `-mcpu=native` on Graviton. At 16 MiB:

| Architecture | Paired Java | Compact Java | Host-tuned native | Paired/compact | Paired/native |
|---|---:|---:|---:|---:|---:|
| Intel | 13.712 ms | 23.853 ms | 22.672 ms | 0.575x | 0.605x |
| Graviton | 16.213 ms | 36.851 ms | 30.145 ms | 0.440x | 0.538x |

The candidate-before and candidate-after results agree, and Easy0, Easy1, Hard,
and Parens protected controls showed no material regression. These are
engineering results from three Java forks and five native repetitions, not the
later formal qualification result.

Full current session `20260715T170407Z` independently retained the same coverage
split. At 16 MiB, paired Hard and Parens took 0.694-0.695x native time on Intel
and 0.624-0.626x on Graviton. Unpaired Easy0 and Easy1 remained at 1.48-1.50x
native on Intel and 1.31-1.39x on Graviton. Pairing therefore closes and exceeds
the native target where it applies; the compact fallback remains the material
native gap.

### Representative Rebar Completion

Complete paired tables were then tested against six representative Rebar
workloads that distinguish long productive scans from repeated short matches.
The first broad run, `20260715T010516Z`, confirmed large wins for
`AROUND_HOLMES`, `BOUNDED_ENDING`, `LINE_BOUNDARY`, and `WORD_ENDING`, but
regressed `ANY_CODE_POINT` and `WORD_BOUNDARY`. Those two workloads each issue
100,000 DFA searches per benchmark operation and usually match within a few
bytes, so paired entry and match-terminal handling cannot amortize.

Several narrower controls were rejected rather than hidden behind a tradeoff:

- `20260715T014728Z` continued through paired rows only after a match candidate;
  short routes remained 14-25% slower on Intel and 11-18% slower on Graviton.
- `20260715T020259Z` decoded computed normal and terminal transitions directly
  after a candidate. This improved continuation but left 11-13% Intel
  regressions and a 3% Graviton `WORD_BOUNDARY` regression.
- `20260715T022448Z` and `20260715T023610Z` tried compact prologues before paired
  entry. Extra metadata and loop handoff did not remove the short-route deficit.
- `20260715T025410Z` sampled one paired search after every 15 compact searches.
  It retained the long wins, but the per-call countdown alone left Intel 6-8%
  slower and Graviton 2-5% slower on the 100,000-call workloads.

The retained policy observes results only while paired execution is active. If
16 paired searches return within 16 bytes, the next retry obtains the existing
exclusive cache mutation protocol, releases the complete paired-table charge,
and permanently selects compact rows for that cache generation. This removes
all steady-state sampling work. A cache reset clears the classification so a
new input distribution can be evaluated again. Concurrent readers update the
observation count on a best-effort basis; the exclusive publication and memory
release remain ordered, and either route implements identical DFA semantics.

Final session `20260715T032009Z` measured the retained source against a build
with pairing disabled. Ratios below are candidate-after/control; lower is
faster:

| Workload | Behavior | Intel | Graviton |
|---|---|---:|---:|
| `ANY_CODE_POINT` | Repeated short match, pairing rejected | 0.981x | 0.997x |
| `WORD_BOUNDARY` | Repeated short match, pairing rejected | 0.998x | 0.996x |
| `AROUND_HOLMES` | Productive complete table | 0.933x | 0.427x |
| `BOUNDED_ENDING` | Productive complete table | 0.508x | 0.436x |
| `LINE_BOUNDARY` | Productive complete table | 0.607x | 0.449x |
| `WORD_ENDING` | Productive complete table | 0.605x | 0.442x |

Candidate-before and candidate-after agree within 1.1% for all six rows. Compact
routes also agree with control except for one non-causal Intel Leipzig sample:
the identical before, control, and after sources each produced the previously
observed 20.7/24.8 ms bimodal JIT shapes, with two slow forks landing in the
after group and one in each other group. Graviton compact routes were within
1.0%; Intel keyword, no-quadratic, and URL routes were within 1.6%.

Direct tests cover match priority, end-of-text and context boundaries, partial
paired-to-compact continuation, cache exhaustion after a candidate, concurrent
reset, exact adaptive rejection, cache-reset re-evaluation, and exact release
of the paired memory charge. The complete 618-test RE2 selector passes. This
closes the bounded paired-transition engineering phase without claiming that
every larger compact DFA now matches native throughput.

## Fixed-Distance Selective-Byte Scanner

Research across .NET, Rust regex-automata, VectorScan, PCRE2, and Joni identified
fixed-distance selective-byte scanning as the strongest next bounded candidate.
The prototype analyzes at most 16 consumed bytes and accepts only a byte set
that occurs at one offset on every path and is strictly more selective than the
existing start-byte set. Assertions, nullable expressions, variable byte
distances, anchored searches, and short inputs retain their existing routes.

For `[a-z]{8}-[0-9]{4}`, the scanner searches for `-` at offset eight and rewinds
to the possible match start. It permanently switches to the existing start-byte
scanner for the remainder of a search when a candidate skips fewer than 16
bytes. Analysis is lazy, occurs before DFA reader registration, charges a
304-byte retained plan plus instance bookkeeping, and preserves that charge
across cache reset. A temporary lack of cache budget is retried after reset
without evicting a warm DFA.

A fresh one-fork local A/B at 32 KiB measured candidate/control ratios of
0.16x for sparse matches, 0.57-0.58x for no-match input, 0.96x for dense
false-positive boundary matching, and 1.00x for dense false-positive boolean
matching. Earlier three-fork local runs showed the same direction. Construction
and total compile controls remained neutral after moving analysis out of DFA
construction.

Correctness coverage directly selects the route and compares DFA with NFA over
first and longest semantics, boundary-free matching, deterministic malformed
byte corpora, nonzero windows and Slice backing offsets, dense false positives,
UTF-8, Latin1, the 4 KiB activation boundary, offsets one and 16, and concurrent
cache reset. The current RE2 selector passes all 658 tests when the selected
Rebar corpus is installed.

The official Rebar corpus at
`463d00f31887e84c38467805b9e3122c314b9521` provides broader shape
coverage. Its 285 analyzable RE2-compatible rows contain 26 eligible rows and
15 distinct eligible pattern/flag combinations; 22 rows execute searches and
four are compile-only. All 198 distinct analyzed combinations compile. The
earlier selected paired-transition corpus contained no fixed-distance candidates
because it was selected around a different eligibility boundary.

The source-disabled target session `20260715T164348Z` accepts the scanner on
both architectures. Candidate-before and candidate-after measurements were
averaged below; the disabled source is otherwise identical.

| API and input | Intel candidate/control | Graviton candidate/control |
|---|---:|---:|
| Match boundary, sparse match | 0.21x | 0.18x |
| Match boundary, dense false positives | 0.98x | 0.89x |
| Match boundary, no match | 0.16x | 0.15x |
| Boolean match, sparse match | 0.22x | 0.19x |
| Boolean match, dense false positives | 1.00x | 0.74x |
| Boolean match, no match | 0.16x | 0.14x |

The sparse and no-match gains reproduce strongly, and the protected dense
shape is neutral on Intel and faster on Graviton. The scanner is retained.
Deterministic cache-budget coverage also forces candidate analysis to defer
when less than its 304-byte charge remains, resets the DFA cache, and proves
that analysis is retried and charged exactly once afterward.

## Minimum Encoded Byte Width

The former exact fixed-length analysis now computes minimum and exact encoded
byte lengths in one stack-safe AST walk. Short search windows can therefore
return before reverse-program construction. The analysis handles UTF-8 and
Latin1 widths, Unicode case-fold cycles, character classes, impossible
languages, repetition overflow, required-prefix stripping, and 10,000 nested
captures.

The match path consults this metadata only below 4 KiB. An unconditional load
added about 0.7% to an approximately 11.5 ns anchored rejection on the local
development host; the bounded form reduced that to about 0.4% with overlapping
confidence intervals. At eight bytes, five representative short-rejection paths
improved by about 4.7x. A three-fork case-fold `Re2.compile` control measured
9,562 ns against 9,506 ns at the checkpoint, or 1.006x with overlapping
intervals.

This candidate is locally accepted because it adds no match-path allocation,
preserves the existing fixed-width shortcut, and has direct reverse-compilation
coverage. The numbers remain directional local evidence rather than target-host
qualification.

## Rejected Dominant Start Self-Loop Prototype

Rust regex-automata and VectorScan use scanners for DFA states where common
bytes are exact self-loops. The local Java prototype recognizes only an
unanchored entry state whose dominant transition reaches a state that self-loops
for the same bytes. It requires one to three raw exit bytes, no more than eight
byte classes, no more than 16 program instructions, at least 4 KiB of input,
and a productive skip of at least 16 bytes. Dense exits fall through to the
existing paired path. The ordinary compact and paired loops are unchanged.

The byte-class limit is semantic isolation as well as a cost gate. The first
version characterized every entry row and changed partial-pair topology for a
large suffix graph even though no plan was retained. Rejecting large byte maps
before transition analysis restored the protected paired tests. The retained
plan costs 40 bytes, is charged to the DFA budget, and is cleared with the
states it references on cache reset.

The program-size gate came from exact Rebar evidence. The multi-stage
`(.*?,){13}z` workload also has a sparse start self-loop, but an initial-only
scanner measured 34.7 ms against 18.2 ms for its complete paired DFA, a 1.91x
regression. The scanner therefore remains a small-graph specialization rather
than displacing recurring internal-state acceleration.

For `\C*END`, a one-fork public-path A/B over 32 KiB measured approximately
0.25-0.26x checkpoint for sparse-match and no-match inputs. At 2 MiB the ratios
were 0.24-0.26x. A same-tree three-fork dense control measured 1.005x for
boundary matching and 0.972x for boolean matching, with overlapping confidence
intervals. Randomized DFA/NFA comparison, nonzero Slice backing offsets,
anchored exclusion, unsupported large maps, cache rebuild, and concurrent reset
all pass locally.

Corrected target session `20260715T170120Z` rejected the prototype. On Intel,
the scanner reduced sparse and no-match searches to 0.16x control, but made the
protected dense 32 KiB boundary and boolean searches 1.080x and 1.073x control.
The dense 2 MiB rows were neutral, proving that the regression was a fixed
per-search cost rather than a worse scanning slope. Only one of 246 successful
Rebar search rows selected this route, and raising the input threshold would
remove that representative use. The implementation and campaign mode were
therefore removed rather than retaining a workload tradeoff.

## Rejected Required-Suffix Boolean Search

A required case-sensitive ASCII suffix can reject absent literals with one
intrinsic Slice search. A boolean-only prototype also reused the first suffix
location for one reverse-DFA existence proof, then delegated to the ordinary
engine if that proof failed. Group-zero and capture callers bypassed the route
because the first successful suffix does not necessarily belong to the
leftmost match.

Exact source-disabled Intel and Graviton session `20260715T211620Z` compared
candidate-before, disabled, and candidate-after sources over synthetic,
representative Rebar, protected direct-DFA, and compilation controls. Both
hosts passed the complete RE2 selector and exited successfully. The boolean
candidate/control ratios were:

| Input shape | Intel 32 KiB / 2 MiB | Graviton 32 KiB / 2 MiB |
|---|---:|---:|
| No suffix | 0.040x / 0.040x | 0.031x / 0.042x |
| Sparse late match | 0.295x / 0.267x | 0.302x / 0.284x |
| Late false suffix | 1.046x / 1.043x | 1.051x / 1.077x |
| Late false suffix, then match | 1.032x / 1.031x | 1.037x / 1.068x |
| Long spanning match | 5.667x / 5.563x | 4.441x / 4.457x |

Absent and sparse suffixes produce the expected algorithmic win. Once a suffix
is present, however, the candidate pays for literal and reverse scans before
falling back to the ordinary engine. Late false candidates regress by 3-8%, and
a long-spanning match regresses by 4.4-5.7x. Intel Rebar boolean controls also
include 2.4-6.9% regressions. Protected 16 MiB DFA rows took 1.014x disabled
time on Intel and 1.007-1.017x on Graviton; compilation took 1.006x and 1.017x.
The candidate therefore fails the 2% acceptance gate and has been removed with
its tests and campaign mode.

## Compact DFA Layout And Stable Self-Loops

The original compact one-byte DFA exploration tested object-reference rows,
exact-stride and power-of-two primitive heap tables, pre-shifted indexes,
bounded FFM and relative-offset buffer layouts, and diagnostic raw pointers. No
replacement in that campaign improved both Intel and Graviton. Its supported
off-heap access did not reproduce native pointer throughput and added ownership
complexity. Raw `Unsafe` pointers were fast enough to identify relative address
formation and JVM reference handling as costs, but they are diagnostic only and
are not shippable.

A later supported FFM diagnostic used a `static final` everything segment to
dereference 64-bit absolute transition-row addresses. It improved on object rows
at bounded table footprints. Useful isolated Graviton footprints took
1.231-1.264x minimal direct C++ loop time, while Intel remained within 1.019x.
A follow-up Graviton perfasm experiment found that JDK 25 C2 emits a
separate dependent address `add` where GCC uses AArch64's scaled indexed `ldr`.
Forcing the separate-address shape in C++ reproduced the Java time within 1%,
while removing Java's FFM alignment guard recovered only 0.4%. See
[`history/2026-07-16-dfa-absolute-pointer-cutoff.md`](history/2026-07-16-dfa-absolute-pointer-cutoff.md).

The project accepted that known compiler limitation and ran the bounded
production integration. Session `20260716T233209Z-7064` uses a 48 KiB
row-rounded allocation and measured the state-changing route at 0.709x object
and 0.996x native time on Intel, and 0.728x object and 1.006x native time on
Graviton. Paired protected controls stayed within 2%. Native access is optional;
denied access retains object rows without restricted initialization. See
[`history/2026-07-16-dfa-absolute-pointer-integrated.md`](history/2026-07-16-dfa-absolute-pointer-integrated.md).

The retained result keeps the existing `Object[]` rows and specializes code
shape. A long end-constrained search that has not entered paired execution
samples 64 normal transitions. If at least 58 are exact self-loops, execution
enters an isolated identity-first loop; otherwise it resumes the original
compact continuation. Selection allocates no memory and adds no branch to the
established paired or compact loops.

The production-path benchmark uses a 520-byte suffix and a warmed graph with
more than 500 states. Complete and partial paired tables are ineligible, as is
fixed-distance scanning. Final source-disabled session
`20260716T051735Z-81238` measured:

| Architecture | Java candidate | Java control | Candidate/control | Native RE2 | Candidate/native |
|---|---:|---:|---:|---:|---:|
| Intel | 13.222 ms | 30.643 ms | 0.432x | 22.056 ms | 0.599x |
| Graviton | 15.550 ms | 42.544 ms | 0.366x | 30.188 ms | 0.515x |

Native RE2 used the same deterministic 16 MiB printable corpus and equivalent
expression, built with `-O3 -DNDEBUG -march=native` on Intel and
`-O3 -DNDEBUG -mcpu=native` on Graviton. Candidate-before and candidate-after
are combined geometrically to control for ordering drift.

Physical method separation is required. A predicate that excluded paired
execution still left the sampling branch in shared generated code and regressed
Graviton `HARD` by 5.6%. Direct dispatch around the sampling method restored the
protected paths:

| Protected route | Intel candidate/control | Graviton candidate/control |
|---|---:|---:|
| Paired `HARD` | 1.009x | 0.988x |
| Paired `PARENS` | 1.010x | 0.994x |
| State-changing compact | 0.999x | 1.001x |

This is not a universal Java/native claim. Stable-self-loop graphs accepted by
the guard remain faster than native, while eligible state-changing compact
graphs use the bounded absolute-pointer route described above.
The complete current RE2 selector passes 917 tests, and the latest full Maven
install passes 2,709 tests. Detailed
representation and rejected-policy evidence is in
[`DFA_LAYOUT_EXPLORATION.md`](DFA_LAYOUT_EXPLORATION.md).

## Remaining Confidence Work

1. Investigate Intel Unicode literal scanning and decide whether the remaining
   1.157x Intel case-insensitive result warrants another bounded campaign.
2. Investigate the remaining `SplitBig2` and Veryl capture lifecycle gaps.
3. Run the broader official Rebar intersection and a final three-session full
   matrix after retained production changes are complete.

Formal publication qualification remains open. Current gate disposition is in
the dated
[`candidate qualification report`](history/2026-07-20-candidate-qualification.md).
