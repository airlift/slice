# AWS Engineering Run

`run-campaign.sh` performs one internal-confidence benchmark session on one
Intel and Graviton host pair. Bounded investigations run independent shards as
concurrent invocations, so four shards use four host pairs. It is not the
three-session formal qualification campaign.

The default host pair in `us-west-2` is:

- `c8i.8xlarge`: 16 Intel physical cores, 32 vCPUs, 64 GiB
- `c8g.4xlarge`: 16 Graviton physical cores, 16 vCPUs, 32 GiB

The driver snapshots the current dirty Slice and isolated Trino worktrees,
uploads encrypted private archives to a temporary S3 bucket, and launches both
instances without inbound access. A per-campaign EC2 role can read only the
bucket's `input/*` objects and write only `results/*`, so long runs do not depend
on the lifetime of the operator's temporary SSO credentials. Each instance is
configured to terminate when the host script shuts it down. The local driver
also retries and verifies instance termination and deletion of the role and
bucket after a successful run. A successful benchmark exits nonzero if cleanup
cannot be verified. Failed campaigns preserve the private bucket, whose
lifecycle expires artifacts after one day.

Run it from the Slice worktree:

```bash
AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

The default `CAMPAIGN_MODE=full` runs the complete matrix below. Formal Java
measurements use five forks, ten one-second warmup iterations, and ten
one-second measurement iterations. Each host records a randomized major-suite
order. Direct native controls use five one-second repetitions. Use the targeted
mode to rerun the Trino operation and compile gates after a focused change:

```bash
CAMPAIGN_MODE=targeted AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the DFA diagnostic mode when a direct Java/native gap requires generated-code
evidence rather than the unrelated public and Trino matrices:

```bash
CAMPAIGN_MODE=dfa-diagnostic AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the large-pointer mode to bracket the exact 75,850-state bounded-context
workload between native-access-disabled object controls and compare the enabled
absolute-pointer route with host-tuned native RE2:

```bash
CAMPAIGN_MODE=dfa-large-pointer \
  COUNT_PIPELINE_MEMORY_MEGABYTES=96 \
  INTEL_INSTANCE_TYPE=c8i.2xlarge ARM_INSTANCE_TYPE=c8g.2xlarge \
  AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the compact DFA layout screen to compare selected synthetic transition-table
layouts. Every `DFA_LAYOUT_FILTER` must include `objectReferences` so each split
retains the current object-row control:

```bash
CAMPAIGN_MODE=dfa-layout-screen \
  DFA_LAYOUT_FILTER='BenchmarkDfaCompactTransitionLayout\.(objectReferences|exactStrideHeapIntegers|powerOfTwoStateIds|preShiftedHeapRowIndexes)$' \
  AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the real-layout screen to compare the candidate layouts over DFA graphs
warmed from the Hard and Parens programs. It runs only the focused real-layout
correctness test before measuring the selected object control and candidate
layouts:

```bash
CAMPAIGN_MODE=dfa-real-layout-screen \
  DFA_REAL_LAYOUT_PATTERNS=HARD,PARENS \
  DFA_REAL_LAYOUT_FORKS=5 \
  DFA_REAL_LAYOUT_RUN_NATIVE=true \
  AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the layout perfasm mode to inspect one synthetic or real-layout method at
one parameter point. The mode also measures the matching object-layout control
unless that control is itself selected:

```bash
CAMPAIGN_MODE=dfa-layout-perfasm \
  DFA_LAYOUT_PERFASM_BENCHMARK=BenchmarkDfaCompactTransitionLayout.exactStrideHeapIntegers \
  DFA_LAYOUT_PERFASM_STATE_COUNT=512 \
  DFA_LAYOUT_PERFASM_CLASS_COUNT=29 \
  AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh

CAMPAIGN_MODE=dfa-layout-perfasm \
  DFA_LAYOUT_PERFASM_BENCHMARK=BenchmarkDfaRealTransitionLayout.exactStrideHeapRowIndexes \
  DFA_LAYOUT_PERFASM_PATTERN=HARD \
  AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the final self-loop mode to bracket the production adaptive scanner around
an exact source-disabled control. It protects paired and state-changing compact
routes and runs the equivalent host-tuned native RE2 workload:

```bash
CAMPAIGN_MODE=dfa-self-loop-final \
  AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the start-byte mode to validate the sparse start-byte accelerator against an
otherwise identical disabled control on both architectures:

```bash
CAMPAIGN_MODE=start-byte AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the byte-scan fallback mode to compare adaptive rejection of unproductive
start-byte and fixed-distance scans against the previous always-scan behavior.
It measures exact Rebar literal and alternation rows together with sparse,
fixed-distance, paired, and compact controls:

```bash
CAMPAIGN_MODE=byte-scan-fallback AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the fixed-distance mode for the selective-byte scanner and its source-level
disabled control:

```bash
CAMPAIGN_MODE=fixed-distance AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the nullable-repeat mode for a same-build comparison of regular matcher
iteration and the specialized single-byte repetition matcher. The mode also
runs a source-disabled A/B for empty-match position, extract-all, split, and
replacement operations, with exact-one delimiter controls:

```bash
CAMPAIGN_MODE=nullable-repeat AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the group-zero mode to compare operation-specific whole-match bounds against
the full capture buffer. It covers Unicode and capture-sparse operations, exact
one-byte controls, the shorter Unicode points that previously lost to Joni, and
no-match and late-match extraction and replacement controls:

```bash
CAMPAIGN_MODE=group-zero AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the capture-count mode for bounded capture and count optimization rounds. It
compares batched and repeated DFA counting, bounded character-class counting,
and focused traditional capture methods on Intel and Graviton:

```bash
CAMPAIGN_MODE=capture-count AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use the capture-engine mode for bounded direct NFA, OnePass, or BitState
capture rounds. Select the concrete engine and source control explicitly. Run
direct, scaling, and guard shards as separate campaigns so they use independent
Intel and Graviton pairs concurrently. These are single-thread benchmarks, so
the smaller example hosts do not need equal total core counts. The scaling
shard decomposes metadata lookup and complete NFA capture searches at 1, 4, 16,
and 64 groups:

```bash
CAMPAIGN_MODE=capture-engine CAPTURE_ENGINE=nfa CAPTURE_SHARD=direct \
  CAPTURE_CONTROL=instruction-scan \
  INTEL_INSTANCE_TYPE=c8i.2xlarge ARM_INSTANCE_TYPE=c8g.2xlarge \
  AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh

CAMPAIGN_MODE=capture-engine CAPTURE_ENGINE=nfa \
  CAPTURE_CONTROL=capture-queue \
  INTEL_INSTANCE_TYPE=c8i.2xlarge ARM_INSTANCE_TYPE=c8g.2xlarge \
  AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh

CAMPAIGN_MODE=capture-engine CAPTURE_ENGINE=onepass \
  CAPTURE_CONTROL=onepass-finalization \
  INTEL_INSTANCE_TYPE=c8i.2xlarge ARM_INSTANCE_TYPE=c8g.2xlarge \
  AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh

CAMPAIGN_MODE=capture-engine CAPTURE_ENGINE=bitstate \
  CAPTURE_CONTROL=bitstate-job-capacity \
  INTEL_INSTANCE_TYPE=c8i.2xlarge ARM_INSTANCE_TYPE=c8g.2xlarge \
  AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh

CAMPAIGN_MODE=capture-engine CAPTURE_ENGINE=bitstate CAPTURE_SHARD=guard \
  CAPTURE_CONTROL=bitstate-instruction-words \
  CAPTURE_GUARD_SIZES=512,4096 \
  INTEL_INSTANCE_TYPE=c8i.2xlarge ARM_INSTANCE_TYPE=c8g.2xlarge \
  AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh

CAMPAIGN_MODE=capture-engine CAPTURE_ENGINE=nfa CAPTURE_SHARD=scaling \
  CAPTURE_CONTROL=instruction-scan \
  INTEL_INSTANCE_TYPE=c8i.2xlarge ARM_INSTANCE_TYPE=c8g.2xlarge \
  AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Each direct or guard run measures candidate-before, exact source control, and
candidate-after on the same host. Direct runs additionally bracket Java with
native-before and native-after. The host tests the control semantically and
verifies exact source-hash restoration before the second candidate run.
Candidate/control and Java/native are elapsed-time ratios, so lower is better.
An important stable regression above 2% rejects the candidate regardless of its
aggregate result.

Use at most three NFA rounds, two OnePass rounds, three BitState rounds, and two
public integration rounds. Stop earlier for parity, a correctness or lifecycle
failure, a protected regression, or the absence of a measured hypothesis likely
to recover at least 5%.

Use the official Rebar mode to run the pinned curated native-RE2 intersection
through Rebar's own KLV protocol and timer on Intel and Graviton:

```bash
CAMPAIGN_MODE=rebar-official AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

The mode records verification failures separately and retains every measurement
row, including errors, so unsupported syntax or semantic differences can be
classified rather than silently omitted.

Use the bounded native-comparison mode to compare the current Slice checkpoint
with Rebar's bundled RE2 and exact pinned portable and host-tuned native builds.
The host-tuned native runner brackets Slice on every row, while a second Slice
runner verifies the ordinary object-row path without native access:

```bash
CAMPAIGN_MODE=rebar-native-comparison \
  INTEL_INSTANCE_TYPE=c8i.2xlarge \
  ARM_INSTANCE_TYPE=c8g.2xlarge \
  AWS_PROFILE=dev \
  tools/re2-benchmark/aws/run-campaign.sh
```

With the default model filter, this mode also generates exact workload and
engine manifests, verifies all 41 rows, confirms Java tier-4 compilation, and
retains native compiler commands, binary hashes, symbols, and disassembly.

Set `REBAR_MODEL_FILTER` to shard this comparison by Rebar model. For example,
`^count-captures$` runs only capture-counting workloads while retaining the
same pinned native build, correctness checks, and comparator ordering.
Native before/after drift above 2% fails the campaign by default. Set
`REBAR_EXCLUDE_NATIVE_DRIFT=true` for formal multi-session reduction. This
retains stable, independently bracketed workloads and records excluded rows in
`native-drift.csv`. Set `REBAR_ALLOW_NATIVE_DRIFT=true` only for a diagnostic
run whose results will be reported as drift-affected; it is not valid for final
qualification.

Use the traditional native-comparison mode for the audited intersection of the
Java ports of `regexp_benchmark.cc` and the pinned upstream executable:

```bash
CAMPAIGN_MODE=traditional-native-comparison \
  INTEL_INSTANCE_TYPE=c8i.2xlarge \
  ARM_INSTANCE_TYPE=c8g.2xlarge \
  AWS_PROFILE=dev \
  tools/re2-benchmark/aws/run-campaign.sh
```

The mode runs 298 one-to-one operation and parameter pairs. It brackets the
Java JMH measurements with direct host-tuned native runs and uses the geometric
mean of the two native medians as the reference. Lower Slice/native ratios are
better. The mode rejects missing, duplicate, unexpected, non-finite, or
wrong-unit rows and retains the pair manifest, raw JSON, native compile
commands, source and binary identities, and normalized CSV and JSON summaries.
Java-only UTF-8 FullMatch rows, uncached native rows, PCRE rows, thread-scaling
registrations, and duplicate Java implementations are not mixed into the
paired aggregate.

Run the seven Java benchmark classes on independent Intel/Graviton pairs when
the complete census is needed without the latency of one serial host pair:

```bash
AWS_PROFILE=dev tools/re2-benchmark/aws/run-traditional-native-shards.sh
```

Every shard retains its own native-before/Java/native-after bracket. The
launcher merges the raw shard outputs and reruns the complete 298-pair
validator for each architecture; it does not combine separately normalized
ratios. Each Java row uses five forks with ten one-second warmup and measurement
iterations; each native bracket uses five one-second repetitions. The launcher
defaults to three concurrent shard pairs on `c8i.2xlarge` and `c8g.2xlarge`
hosts so the campaign stays within ordinary regional vCPU quotas.

When the campaign's drift or variation gate requires the single allowed
confirmation, reverse the complete comparator order on both hosts:

```bash
CAMPAIGN_MODE=rebar-native-comparison \
  REBAR_COMPARATOR_ORDER=reverse \
  INTEL_INSTANCE_TYPE=c8i.2xlarge \
  ARM_INSTANCE_TYPE=c8g.2xlarge \
  AWS_PROFILE=dev \
  tools/re2-benchmark/aws/run-campaign.sh
```

Use joni-focused mode for an interleaved same-host comparison of the sole
nominal current Slice/Joni loss. It runs Slice before and after Joni for 1 KiB
Unicode extraction with five independent forks per phase:

```bash
CAMPAIGN_MODE=joni-focused AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use comparator-only mode to refresh Joni and historical RE2J without rerunning
the Slice and native matrices. This mode still runs the comparator's semantic
tests and skips Trino's unrelated frontend packaging:

```bash
CAMPAIGN_MODE=trino-comparator AWS_PROFILE=dev tools/re2-benchmark/aws/run-campaign.sh
```

Use `joni-memory` for the bounded Slice/Joni qualification. It runs the current
Slice operation matrix immediately before and after Joni on each host, records
normalized allocation, and runs the retained-memory census with the selected
96 MiB DFA budget and native access enabled. The exact bounded-context pattern
and input are packaged from the accepted cache-capacity campaign. Each phase
uses one fork; the independent Slice phases bracket Joni and expose host drift
without approaching the campaign timeout:

```bash
CAMPAIGN_MODE=joni-memory \
  INTEL_INSTANCE_TYPE=c8i.2xlarge \
  ARM_INSTANCE_TYPE=c8g.2xlarge \
  AWS_PROFILE=dev \
  tools/re2-benchmark/aws/run-campaign.sh
```

Override `JONI_MEMORY_CONTEXT_DIR` only to repeat the qualification with a
different preserved `pattern.txt` and `haystack.bin` pair.

If a completed operation phase needs only its retained-memory companion
recovered, `CAMPAIGN_MODE=joni-memory-census` runs the same semantic tests and
96 MiB census without repeating the operation matrix.

The Slice-only focused modes do not build Trino's Joni comparator. Pair their
results with `trino-comparator`, or use `full` to refresh every matrix.

If the local SSO session expires while hosts are still running, the controller
stops without classifying them as terminated. The hosts retain transfer access,
upload results, and terminate normally. After authenticating again, recover the
two archives from the bucket recorded in the session's `session.txt`.

Useful overrides are `AWS_REGION`, `INTEL_INSTANCE_TYPE`,
`ARM_INSTANCE_TYPE`, `INSTANCE_MARKET_TYPE`, `CAMPAIGN_MODE`, `TRINO_DIR`,
`RESULT_ROOT`, and `TIMEOUT_SECONDS`. An exact JDK archive can be selected with
`BENCHMARK_JAVA_ARCHIVE_URL` and `BENCHMARK_JAVA_ARCHIVE_SHA256`; both must be
set, and the archive must match the selected host architecture. Without these
overrides the hosts use the latest Temurin 25 GA build. The layout screen
additionally accepts `DFA_LAYOUT_FILTER`,
`DFA_LAYOUT_STATE_COUNTS`, `DFA_LAYOUT_CLASS_COUNTS`, `DFA_LAYOUT_FORKS`, and
`DFA_LAYOUT_RUN_NATIVE`. The real-layout screen accepts
`DFA_REAL_LAYOUT_PATTERNS` (`HARD`, `PARENS`, or both),
`DFA_REAL_LAYOUT_FORKS`, and `DFA_REAL_LAYOUT_RUN_NATIVE`. The perfasm mode
accepts `DFA_LAYOUT_PERFASM_BENCHMARK`, `DFA_LAYOUT_PERFASM_STATE_COUNT`,
`DFA_LAYOUT_PERFASM_CLASS_COUNT`, and `DFA_LAYOUT_PERFASM_PATTERN`. The
traditional native comparison accepts `TRADITIONAL_JMH_FORKS`,
`TRADITIONAL_NATIVE_REPETITIONS`, and `TRADITIONAL_NATIVE_MINIMUM_TIME`. Raw
JSON, environment metadata, build logs, and test logs are downloaded under
`benchmark-results/re2-engineering/<session>/` by default before cloud resources
are removed. This directory is ignored by Git but is outside Maven's `target/`
tree, so `clean` does not erase benchmark evidence.

`INSTANCE_MARKET_TYPE=spot` uses the independent standard Spot vCPU quota. A
Spot interruption invalidates that host session rather than producing partial
qualification evidence.

The host driver runs:

1. The complete RE2 test selector.
2. The hermetic pinned native RE2 build.
3. Protected single-thread Java and native DFA cases.
4. Paired direct-DFA and public-RE2 BigFixed scaling through 1 MiB.
5. Shared warm and cold-wave DFA cases over physical cores.
6. Public API timing and allocation cases.
7. Slice Trino-adapter operations.
8. Trino Joni and historical RE2J operations after semantic equivalence tests.

Targeted mode always runs the complete RE2 correctness selector, then runs the
full Slice Trino-adapter benchmark and focused compile benchmarks. It skips the
native build, direct engine and cache matrices, public allocation benchmarks,
and external Trino comparator. Use it only when the existing full-session native
and comparator results remain the appropriate baseline.

DFA diagnostic mode runs the complete RE2 correctness selector and pinned native
build, then records ten-fork Hard and Parens timings, hardware counters, raw
HotSpot and `perf` data, a compressed-reference control, and direct native
baselines. It is intentionally narrow so generated-code investigations can be
repeated on Intel and Graviton without paying for the full Trino campaign.

DFA layout screen mode runs the compact-layout benchmark test together with the
existing DFA layout and pairing benchmark tests. It then runs the methods named
by `DFA_LAYOUT_FILTER` with ten warmup iterations, seven measurement iterations,
500 ms iterations, an 8 GiB pre-touched heap, and `DFA_LAYOUT_FORKS` forks. The
default state grid is `32,128,512,2048`; the default byte-class grid is
`8,29,64`. Each filter must include `objectReferences`, the current object-row
control.

DFA real-layout screen mode runs `TestBenchmarkDfaRealTransitionLayout`, then
measures the methods selected by `DFA_REAL_LAYOUT_FILTER` for the requested
patterns. Available controls include `compactObjectReferences`,
`selfLoopObjectReferences`, `selfLoopFirstObjectReferences`, and
`pairedObjectReferences`, together with the retained heap-integer controls.
When `DFA_REAL_LAYOUT_RUN_NATIVE=true`, the mode also builds host-tuned native
RE2 and runs the selected direct cached-DFA Hard and Parens controls.

DFA layout perfasm mode runs the focused correctness test for the selected
benchmark class, then records hardware counters and JMH perfasm output for the
selected method and its object-layout control. Synthetic methods use exactly
one power-of-two `stateCount` and one `classCount`; real-layout methods use
exactly one `pattern`.

DFA self-loop final mode runs the focused layout and route tests, then measures
candidate-before, a source-disabled control, and candidate-after for the
integrated unpaired stable-self-loop route, paired Hard and Parens routes, and a
state-changing compact route. It finishes by building pinned native RE2 with
`-O3 -DNDEBUG` and host-specific architecture tuning and running the equivalent
16 MiB self-loop workload. The result bundle contains the candidate source,
control patch, source hashes, native build log, and all raw repetitions.

Start-byte mode runs the complete RE2 correctness selector, then measures the
32 KiB capture-sparse Trino operations, focused compilation and first-DFA
construction, and the protected search workloads. Protected coverage includes
tiny direct-DFA calls and dense and nullable repeated-search Trino operations
where a once-per-search dispatch check could accumulate. Each host runs the
retained candidate, a control produced by disabling candidate-table construction
and dispatch, and a second candidate measurement to expose time-dependent host
drift. The result bundle includes the exact control patch.

Capture-pipeline mode runs the complete RE2 selector, then measures one exact
capture workload as public-before, trace control, setup, forward DFA, reverse
DFA, capture engine, composed pipeline, result consumption, and public-after.
Use `run-capture-pipeline-shards.sh` to run the Veryl, Unicode, date, and
`SplitBig2` shards concurrently on separate Intel and Graviton pairs. The
default `2xlarge` hosts provide enough memory for the qualified 8 GiB
pre-touched heap.

Single-thread processes are pinned to one logical CPU. Shared runs derive one
logical CPU per physical core from `lscpu`, avoiding Intel sibling threads.
