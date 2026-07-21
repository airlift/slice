# Logical Region Matcher Campaign

**Status:** Closed. The allocation-free logical-region matcher is retained.

**Baseline source commit:** `b8f4133c`

**Candidate source hash:**
`599635564c193a29272069961f25aeb2aff960d9049791844951a52bb83b4919`

This is internal engineering evidence, not publication qualification. The
campaign addressed Ruff's per-line Slice-view allocation without changing the
protected DFA transition loops.

## Question

Ruff performs approximately 890,000 logical-line searches but reaches reverse
search and capture extraction only 20 times. The accepted short-search campaign
made Ruff `real` native-competitive, but Ruff `tweaked` continued to spend a
material part of the complete operation constructing a Slice view for every
line.

The candidate adds `Re2Matcher.reset(input, start, end)`. The matcher carries
the region bounds through DFA start-state analysis and reports offsets relative
to the logical region. It constructs an actual Slice view only after a
search reaches a capture engine or an immutable `MatchResult` is requested;
lines rejected by DFA construct no view.
Ordinary full-Slice matching continues through the existing wrappers, and the
DFA transition loops are unchanged.

## Correctness

The logical region is required to behave exactly like an actual Slice view.
Focused tests cover anchors, multiline anchors, word boundaries, empty matches,
empty regions, CRLF endings, malformed UTF-8, nonzero backing offsets, captures,
the single-byte route, and region validation. An exhaustive test compares all
start/end regions of a UTF-8 input, including boundaries inside encoded code
points, across eleven patterns. A direct DFA test verifies that context flags
are computed from the logical region rather than the backing Slice.

The complete RE2 selector passes 923 tests with and without native access. The
benchmark results are also unchanged: Ruff `tweaked` returns 44, Ruff `real`
returns 84, and the Unicode control returns 558,784.

## Local Paired Result

The exact Ruff `tweaked` KLV was measured in alternating baseline/candidate
runs on the development Apple host. This result is directional only, not target
qualification.

| Source | Median of run medians |
|---|---:|
| Baseline | 40.445 ms |
| Candidate | 37.355 ms |

The candidate is 7.6% faster locally.

## Target-Host Result

Elapsed-time ratios use the lower-is-better convention. Public time is the mean
of the before/after medians.

### Ruff `tweaked`

| Architecture | Prior Java | Candidate Java | Change | Candidate setup | Native | Java/native |
|---|---:|---:|---:|---:|---:|---:|
| Intel | 51.476 ms | 49.912 ms | -3.0% | 17.374 ms | 33.577 ms | 1.487x |
| Graviton | 57.277 ms | 53.749 ms | -6.2% | 14.743 ms | 47.345 ms | 1.135x |

Setup improves from 21.254 to 17.374 ms on Intel and from 17.568 to
14.743 ms on Graviton, reductions of 18.3% and 16.1%. The candidate therefore
removes the intended allocation cost, but Ruff `tweaked` remains slower than
native RE2.

### Protected Ruff `real` Control

| Architecture | Prior Java | Candidate Java | Change | Forward stage | Native | Java/native |
|---|---:|---:|---:|---:|---:|---:|
| Intel | 72.960 ms | 73.875 ms | +1.3% | 61.185 ms | 59.284 ms | 1.246x |
| Graviton | 96.830 ms | 94.091 ms | -2.8% | 77.459 ms | 92.027 ms | 1.022x |

The Intel result is within the 2% protected-control gate. Its 61.185 ms forward
stage is effectively identical to the retained 61.18 ms baseline, supporting
the intended invariant that this change does not alter DFA transition
throughput.

### Unicode Control

| Architecture | Extended OnePass baseline | Candidate | Change | Native | Java/native |
|---|---:|---:|---:|---:|---:|
| Intel | 11.028 ms | 11.216 ms | +1.7% | 20.326 ms | 0.552x |
| Graviton | 13.773 ms | 14.005 ms | +1.7% | 27.896 ms | 0.502x |

Both changes remain within the 2% control gate.

## Evidence Limits

- Ruff sessions: `20260720T001609Z-3715` and `20260720T001609Z-3716`.
- Unicode control session: `20260720T000944Z-98916`.
- Java before/after drift is at most 1.36% for Ruff and 0.16% for Unicode.
- Native controls were materially faster than the earlier retained Ruff
  sessions. Candidate-over-baseline conclusions therefore use Java time, not
  cross-session Java/native ratio changes.
- The first Ruff run exposed an invalid harness assumption that Java and native
  must perform the same number of capture calls. Java can reject a line with
  DFA before extraction, while native performs extraction inside one match
  call. Capture counts remain diagnostic rather than equality gates.
- The confirmation reducer also requested a native BitState diagnostic for a
  OnePass route. The unsupported diagnostic correctly returned zero after all
  measured stages had completed. The harness now requests that stage only for
  BitState routes; the retained summary excludes it.
- Temporary AWS resources were removed after both campaigns.

## Decision

Retain logical matcher regions. They remove a measurable repeated-line setup
cost while preserving actual Slice-view semantics and protected transition
throughput. This campaign does not establish native parity for Ruff `tweaked`;
formal qualification must measure it again with the final source.

The next performance question is the independent Date group-zero span gap. It
requires a bounded repeated-boundary design rather than additional line-region
or matcher cleanup.
