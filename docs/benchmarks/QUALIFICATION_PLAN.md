# RE2 Performance Qualification Plan

**Status:** Executed for candidate `c212684`; strict release qualification is
incomplete.

The traditional native, bounded Rebar-native, and Trino-shaped Joni layers have
multi-session Intel and Graviton results. The Joni layer now has three complete
80-row sessions per architecture and passes its acceptance gate. The native
full matrix did not produce three complete sessions per architecture because
long Spot runs were interrupted and the original controller timeout was too
short. The broader official Rebar intersection also remains open. See the dated
[`Joni acceptance report`](history/2026-07-20-joni-final-acceptance.md) and
[`candidate qualification report`](history/2026-07-20-candidate-qualification.md)
for results and the exact gate disposition.

The iterative benchmark-and-optimization work that precedes qualification is
defined in [`ENGINEERING_CAMPAIGN.md`](ENGINEERING_CAMPAIGN.md). Results from that
campaign identify and fix gaps but do not become release claims.

This plan defines the formal performance campaign required before the RE2 port
is released as a general Slice API. Trino-shaped operations remain an important
comparison workload, but integrating this library into Trino is a separate
project. Historical benchmark results are workload-selection evidence only.
They are not release results.

## Objectives

The campaign must answer five separate questions:

1. Does the Java engine preserve native RE2's performance shape for every
   supported algorithm and input size?
2. What overhead is added by the Slice facade and by Trino-compatible repeated
   operations?
3. Is the proposed backend faster than Joni and historical Trino RE2J for the
   common syntax and operations Trino actually performs?
4. Where does the engine fall in Rebar's curated cross-engine search and compile
   intersections?
5. Are throughput, allocation, retained memory, and concurrency behavior stable
   on both modern Intel and Arm Graviton systems?

Ratios alone are not answers. Every material difference must be attributed to
an algorithm, memory access, allocation, dispatch path, or generated-code
difference.

## Entry Gate

Freeze one candidate commit only after all of the following are true:

- the complete Maven build passes
- the RE2 differential, exhaustive, randomized, native-golden, and Trino
  compatibility suites pass
- the public API and benchmark corpus will not change during a comparison run
- pinned native RE2 and every Java comparator can be built reproducibly
- benchmark methods consume their results and have smoke tests for every
  parameter value
- unsupported syntax is classified before timing; it is never recorded as a
  performance failure

Any code change made while investigating a result creates a new candidate
commit and invalidates all cross-system numbers collected for the old one.

## Comparators

Build and record exact revisions for:

1. pinned native RE2 at `972a15cedd008d846f1a39b2e88ce48d7f166cbd`
2. this port through direct engine entry points used only for diagnosis
3. this port through the public Slice API
4. this port through `TrinoRegexp`
5. the Joni version used by the selected Trino revision
6. historical `io.trino:trino-re2j:1.7`

Native and Java benchmarks must use identical pattern bytes, input bytes,
search windows, anchors, expected results, and deterministic random seeds. The
native comparator runs as a direct C++ executable; JNI measurements are not a
substitute for native engine measurements.

## Host Matrix

Use dedicated, non-burstable AWS instances with local execution on:

- one current-generation Intel server instance
- one current-generation Arm Graviton instance

Select exact instance types immediately before the campaign so the choice
reflects then-current AWS hardware. Prefer equivalent vCPU and memory sizes,
avoid shared or burstable families, and use one NUMA node. Record:

- instance type, region, availability zone, AMI, kernel, and CPU model
- architecture, sockets, cores, threads, cache sizes, NUMA topology, and
  microcode
- JDK vendor and build, JVM flags, heap size, and compressed-oops state
- C++ compiler and version, CMake version, optimization flags, and linked C++
  library versions
- benchmark commit, comparator commits, and corpus checksum

Pin single-threaded runs to one logical CPU and verify that no second benchmark
uses its sibling thread. Disable unrelated agents and scheduled work. Use a
fixed CPU power policy where the platform permits it. Reboot or replace the
instance between independent host sessions.

Run at least three independent host sessions per architecture. A session is a
fresh instance or reboot followed by the complete randomized benchmark order.

## Workload Manifest

Store the final manifest as data, not duplicated Java and C++ constants. Every
case has a stable identifier and records pattern bytes, input-generation seed,
input size, operation, anchor, encoding, capture count, expected match ranges,
and expected output checksum.

Required dimensions are:

| Dimension | Cases |
|---|---|
| Input size | Empty, 8 B, 64 B, 512 B, 4 KiB, 32 KiB, 256 KiB, 2 MiB, and 16 MiB where practical |
| Match location | No match, beginning, middle, end, and complete input |
| Match density | Zero, one, sparse, dense, and empty matches |
| Captures | None, group zero, one capture, many captures, and unmatched alternatives |
| Pattern family | Literal, character class, alternation, repetition, counted repetition, anchors, dot-star, Unicode, case folding, required prefix, and high DFA-state patterns |
| Input bytes | ASCII UTF-8, multi-byte UTF-8, Latin-1, NUL, and malformed UTF-8 where defined |
| Cache state | Cold DFA, warm DFA, reset/thrash fallback, and shared concurrent cache |
| Compilation | Cold compile, repeated compile, and precompiled matching |

Include every historical gap named in `RE2_TASKS.md`, but do not tune the
manifest around the current implementation. Add representative Trino workload
cases from the shared function corpus and, if available, anonymized production
pattern families and value-size distributions.

Pin a Rebar revision separately and retain its curated definitions, inputs, and
expected results unchanged. Classify unsupported syntax and semantic differences
before measurement. Report common-engine intersections instead of assigning a
penalty to engines that intentionally implement different languages or match
reporting contracts.

## Benchmark Layers

### Engine Layer

Measure parser, simplifier, compiler, DFA, NFA, OnePass, BitState, prefix
scanning, reverse search, capture extraction, DFA cache reset, and set matching.
These measurements diagnose why an end-to-end result differs; they are not the
release score by themselves.

### Public Slice Layer

Measure boolean partial/full matching, caller-owned capture buffers,
`MatchResult`, matcher construction, matcher reset, and repeated `find`. Verify
that logical non-zero Slice offsets do not cause copies or slower fallback
paths.

### Trino Operation Layer

Measure complete behavior for contains, count, first and Nth position, extract,
extract-all, split, template replacement, and lambda-replacement match
iteration. Isolate regex work from an arbitrary SQL lambda body. Include no
match, one match, dense matches, empty matches, null captures, and multi-byte
input.

This layer qualifies the library against representative database operations. It
does not include Trino module wiring, SQL registration, block integration, or
deployment work.

### Rebar Cross-Engine Layer

Add a long-lived JVM runner for Rebar's KLV protocol and run the pinned curated
search and compile suites through the public Slice API. Report the intersection
with native RE2 and the broader intersection with every participating engine.
Keep these official measurements separate from the Rebar-derived JMH routes used
to diagnose DFA eligibility and code shape.

### Resource And Concurrency Layer

Measure:

- allocation per operation for boolean, capture, repeated-match, and result APIs
- retained size of compiled patterns before and after lazy reverse compilation
- cold and warm DFA cache size and reset behavior
- throughput scaling with one compiled pattern shared by 1, 2, 4, 8, and all
  physical cores
- latency distribution while cache resets and fallback occur

## Measurement Protocol

### Java

Use JMH with result consumption through `Blackhole` or returned values. Begin
with a one-fork smoke run over every parameter. For qualification use at least:

- 5 forks
- 10 one-second warmup iterations, increased when calibration shows late C2
- 10 one-second measurement iterations
- randomized major-suite order per host session, with the executed order
  retained alongside the raw results
- fixed heap settings with no GC during the timed region for allocation-free
  throughput cases when feasible

Capture JMH JSON, GC-profiler allocation data, JVM compilation logs for a
calibration fork, and JFR or async-profiler recordings for material gaps.
Confirm tier-4 compilation of hot methods. Do not combine cold compilation and
steady-state matching in one score.

### Native

Use Google Benchmark with the same data manifest, result checksums, thread
counts, minimum duration, and at least five repetitions. Record JSON output and
the exact release build flags. Prevent whole-operation elimination by consuming
match ranges or output checksums.

### Statistics

Report each system's median, confidence interval, coefficient of variation,
throughput or time, bytes per second where meaningful, and allocation per
operation. Compare slopes across sizes as well as point ratios. Treat a result
as unresolved when host-to-host variation overlaps the claimed difference.

Investigate any of these before accepting a number:

- either implementation appears more than 5x faster
- expected constant-time behavior scales with input size
- expected linear behavior appears constant
- a ratio changes materially with input size
- coefficient of variation exceeds 5%
- allocation appears in a documented allocation-free path

## Acceptance Criteria

- No non-capturing boolean operation allocates per match.
- Caller-owned capture and reusable matcher paths allocate no per-match result.
- Slice and Trino paths do not copy input or convert it to `String`.
- Java and native RE2 have the same scaling class for every common case.
- Every supported native RE2 case is at least as fast through the public Slice
  API, unless an exception is explicitly approved with a quantified cause.
- The Trino adapter is faster than Joni and historical RE2J for the agreed
  common workload. Syntax unavailable to RE2 is reported separately.
- Rebar search and compile results are reported for explicit semantic
  intersections, with individual workload results retained alongside any
  geometric summary.
- No accepted optimization regresses either Intel or Graviton.
- Compile budgets, retained memory, and concurrent cache behavior remain within
  their documented contracts.

## Gap Investigation

For each failed criterion:

1. reproduce the gap on both architectures and verify input/result equivalence
2. identify whether the difference is algorithmic, API overhead, allocation,
   memory locality, synchronization, or generated code
3. compare scaling and engine selection with pinned native RE2
4. profile the complete operation, then isolate the responsible component
5. inspect C2 and native assembly when profiles do not explain the cycles
6. add a direct code-path test before implementing an optimization
7. rerun the affected microbenchmark and its end-to-end Trino operation
8. rerun the complete matrix on both architectures before accepting the change

Rejected experiments belong in the historical benchmark archive with enough
evidence to avoid repeating them.

## Required Artifacts

Keep one immutable directory per candidate commit containing:

- environment manifests for every host session
- comparator revisions and build logs
- workload manifest and checksum
- raw JMH and Google Benchmark JSON
- allocation, JFR, profiler, and assembly artifacts used in conclusions
- normalized comparison tables generated from raw data
- a gap ledger with status, root cause, evidence, and disposition
- a final report separating engine, Slice API, and Trino-operation conclusions

Publish scripts that create the hosts, build all comparators, run the randomized
matrix, validate result checksums, and generate tables. Manual shell history is
not a reproducible campaign.

## Execution Sequence

1. Review and freeze this plan, acceptance thresholds, comparator revisions,
   and workload manifest.
2. Add or update benchmark harnesses and run local smoke tests only.
3. Provision Intel and Graviton hosts and capture environment manifests.
4. Run correctness preflight on both architectures.
5. Run three independent randomized qualification sessions per architecture.
6. Normalize results and open a gap ledger before changing code.
7. Investigate and fix gaps one at a time with both correctness and benchmark
   gates.
8. Rerun the complete matrix for the final candidate commit.
9. Publish the raw artifacts and decision-focused final report.
