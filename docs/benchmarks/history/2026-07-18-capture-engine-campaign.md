# Capture Engine Campaign

**Status:** Bounded engine and public-integration rounds complete. Retained
source passed the complete local RE2 selector. Final whole-engine native
comparison is recorded below.

This is internal engineering evidence, not publication qualification. The
campaign isolated capture execution in NFA, OnePass, and BitState before
checking the public `Re2` path. It did not modify the accepted DFA layouts.

## Ratio Convention

All timing ratios are elapsed-time ratios, so lower is better:

- `candidate/control < 1.0` means the candidate is faster than the exact source
  control;
- `Java/native < 1.0` means Java is faster than host-tuned native RE2;
- `1.0` is parity.

Allocation is reported separately. Lower allocation does not excuse a timing
regression.

## Protocol

The campaign fixed these limits before implementation:

- NFA: at most three rounds
- OnePass: at most two rounds
- BitState: at most three rounds
- public integration: at most two rounds

Each retained-source campaign ran candidate-before, exact source control, and
candidate-after on the same host. Direct engine comparisons additionally ran
native-before and native-after. Controls passed semantic tests, and reversing a
control had to restore the candidate source hash exactly.

An important stable regression above 2% rejected a candidate regardless of its
aggregate result. Intel and Graviton determined retention; local measurements
were diagnostic only. Independent direct, scaling, public, and guard shards ran
on separate host pairs concurrently.

## Retained Implementation

The production changes retained by this campaign are:

- NFA capture-stack sizing from compiled opcode counts;
- a primitive sparse NFA capture-thread queue;
- one compact immutable capture-instruction sidecar shared by NFA and BitState;
- OnePass direct, rebase, and scratch-copy capture finalization modes;
- BitState object traversal jobs with initial capacity bounded by program size.

The compact instruction sidecar stores one `long` per flattened instruction. It
is built lazily once, safely published, and reused by capture engines. BitState
continues to use object traversal jobs; instruction representation and traversal
stack representation are independent decisions.

## NFA

NFA used all three rounds.

| Round | Retained mechanism | Session | Intel candidate/control | Graviton candidate/control |
|---|---|---|---:|---:|
| 1 | Primitive capture-thread queue | `20260717T232739Z-84768` | 0.870x | 0.961x |
| 2 | Opcode-count capture-stack sizing | `20260717T235033Z-17036` | 0.954x | 0.982x |
| 3 | Compact capture-instruction words | `20260718T001127Z-42992` | 0.869x | 0.851x |

The final direct capture benchmark is at Intel parity but retains a Graviton
gap:

| Architecture | Java/native geometric ratio |
|---|---:|
| Intel | 1.013x |
| Graviton | 1.302x |

The Intel rows range from `1.005x` to `1.017x` native time. Graviton ranges from
`1.244x` to `1.355x`. The round ceiling stopped further NFA work; the residual
is explicit rather than hidden in an aggregate public result.

## OnePass

OnePass retains capture finalization selected before the hot loop:

- zero-origin full matches with several captures can write directly to the
  caller buffer without a final copy;
- non-zero origins use direct storage with rebasing;
- other match kinds retain scratch state and copy the winning captures.

Session `20260717T234219Z-6019` measured the complete 19-row guard at
`0.993x/0.994x` control time on Intel/Graviton. The targeted three-digit capture
rows improved to about `0.927x` control on Intel and `0.954x` on Graviton, while
the split and capture-free guards remained at parity.

| Architecture | Java/native geometric ratio |
|---|---:|
| Intel | 0.935x |
| Graviton | 1.216x |

Intel is faster than native in aggregate, although the split row is `1.108x`.
Graviton remains `1.158x` to `1.324x` native time across the three direct capture
rows.

A persistent sparse capture-history representation was rejected in session
`20260718T003039Z-61361`: it regressed the Intel target aggregate by 3.6% and
the digit rows by about 5.2%.

## BitState

### Initial Object-Stack Capacity

BitState previously allocated 64 traversal `Job` objects for every search. The
retained capacity is `max(16, min(64, programSize))`, with ordinary growth when
needed. Session `20260717T234806Z-12820` reduced fixed capture-row time to about
`0.70x-0.77x` control on Intel and `0.60x-0.66x` on Graviton while removing
roughly 1 KiB of initial allocation. Large capture-free controls remained at
parity.

### Compact Instructions

Session `20260718T004624Z-88740` replaced instruction-object loads in the
BitState loop with the shared compact instruction sidecar. Fixed capture rows
took `0.643x` control time on Intel and `0.774x` on Graviton.

The final guard split the range across four concurrent Intel/Graviton pairs:

| Input sizes | Session |
|---|---|
| 8, 64 | `20260718T021629Z-69946` |
| 512, 4,096 | `20260718T021629Z-69947` |
| 32,768, 262,144 | `20260718T021629Z-69948` |
| 2,097,152, 16,777,216 | `20260718T021629Z-69949` |

Across all sizes, Intel `AltMatch` ratios were `0.917x-1.010x` and Graviton
ratios were `0.991x-1.016x`. The productive `Success1` rows improved to
`0.653x-0.812x` on Intel and `0.486x-0.727x` on Graviton. The worst protected
regression was `1.016x`, below the `1.02x` gate, so the sidecar is retained.

The final direct capture ratios remain materially behind native:

| Architecture | Java/native geometric ratio |
|---|---:|
| Intel | 1.180x |
| Graviton | 1.565x |

### Rejected Primitive Traversal Stacks

Two primitive traversal-job representations were rejected:

- A packed `long[]` reduced allocation and improved several Graviton capture
  rows, but regressed Intel split capture by 4.6% and Graviton capture-free
  `Success1` by 4.6% at 512 bytes and 7.2% at 4,096 bytes.
- A flat three-`int` representation improved direct Graviton capture rows by
  20%-33%, but regressed Intel digit captures by 10.9%-12.9% and Graviton
  capture-free `Success1` at 512 bytes by 5.9%.

The second candidate is evidence for a possible future capture-specialized
BitState implementation, but it fails the current shared-loop retention gate.
The campaign stopped at the third-round ceiling rather than duplicating the hot
loop speculatively.

## Public Integration

The public-path candidate combined capture-offset adjustment passes. Large
inputs and Graviton were neutral, but session `20260718T013608Z-36017` measured
stable Intel regressions of 13.6% for reused-matcher `find()` and 7.5% for
caller-buffer `matchInto()` on the tiny matched workload. The change was
rejected after one round. No second public round was opened because the
candidate was a loop cleanup rather than a mechanism likely to recover a
material residual.

## Correctness And Lifecycle Evidence

- Every mechanism has a direct code-path test.
- Exact controls ran upstream NFA, OnePass, BitState, public-engine agreement,
  and `matchInto` semantic tests.
- The compact instruction sidecar requires a flattened program, validates its
  packed output range, and uses safe one-time publication.
- Reversing every retained control restored the exact source hash.
- The settled source passed 889 RE2 tests locally with zero failures or errors
  and one intentional skip.
- The full Maven install passed 2,679 tests with zero failures or errors and one
  intentional skip.

## Rejected Candidates

Do not repeat these unchanged:

| Candidate | Useful result | Rejection reason |
|---|---|---|
| OnePass sparse capture histories | Avoided dense copies | 3.6%-5.2% Intel regression |
| BitState packed `long[]` traversal jobs | Lower allocation; faster Arm captures | 4.6%-7.2% protected regressions |
| BitState flat `int[]` traversal jobs | 20%-33% faster Arm captures | 10.9%-12.9% Intel capture loss and 5.9% Arm guard loss |
| Combined public offset adjustment | Removed one possible offset pass | 7.5%-13.6% Intel tiny-match regression |

## Residual Direct-Engine Gaps

The bounded rounds do not establish direct-engine parity everywhere:

| Engine | Intel Java/native | Graviton Java/native |
|---|---:|---:|
| NFA | 1.013x | 1.302x |
| OnePass | 0.935x | 1.216x |
| BitState | 1.180x | 1.565x |

These ratios isolate capture execution. They do not by themselves predict the
public API, where boundary discovery, engine selection, and reusable matcher
costs change the total. The final traditional and Rebar comparisons below are
the integration authority.

## Final Rebar Integration

Session `20260718T023744Z-76514` ran the settled source through the 41-workload
Rebar comparison against exact pinned, host-tuned native RE2. Lower ratios are
better.

| Model | Intel before | Intel current | Graviton before | Graviton current |
|---|---:|---:|---:|---:|
| Capture-free count | 0.949x | 0.951x | 0.651x | 0.650x |
| Count with captures | 2.291x | 1.922x | 2.150x | 1.644x |
| Group-zero spans | 1.997x | 1.986x | 1.726x | 1.729x |
| Boolean line match | 1.152x | 1.022x | 0.870x | 0.878x |
| Grep with captures | 1.482x | 1.182x | 1.241x | 1.051x |

The direct-engine gains survive ordinary capture extraction. The Veryl
count-with-captures row improves by 16% on Intel and 24% on Graviton. The
capture-grep aggregate improves by 20% and 15%; the Unicode parse-line row
improves by 40% and 27%. Capture-free count is unchanged, satisfying its
protected gate.

Group-zero span counting is also unchanged. It does not request subgroup
captures and does not benefit from the retained capture-engine mechanisms. Its
meaningful residual is the date workload at `1.612x/1.422x`, or about 2.4/2.2
ms absolute deficit. The Cloudflare rows reach `3.29x-5.19x` on Intel and
`2.73x-4.14x` on Graviton but add at most about 0.1 ms per operation.

The remaining material capture rows are:

| Workload | Model | Intel | Graviton | Absolute deficit Intel/Graviton |
|---|---|---:|---:|---:|
| Veryl lexer | Count with captures | 1.922x | 1.644x | 134/143 ms |
| Unicode parse-line | Grep with captures | 1.606x | 1.617x | 14/18 ms |
| Ruff noqa real | Grep with captures | 1.402x | 1.088x | 29/9 ms |
| Date ASCII | Group-zero spans | 1.612x | 1.422x | 2.4/2.2 ms |

Literal Russian and Chinese count remain extreme Intel ratio outliers at
`9.02x` and `21.90x`, but their absolute deficits are only 2.4 ms and 0.7 ms.
Graviton does not reproduce those losses. They are listed separately because
they are capture-free and do not measure this campaign's mechanisms.

Both hosts passed the complete RE2 selector with and without native access,
uploaded complete normalized artifacts, exited with status zero, and were
terminated. The transfer bucket and temporary IAM resources were removed. The
complete 82-row result is
[`2026-07-18-capture-engine-native-comparison.csv`](2026-07-18-capture-engine-native-comparison.csv).

## Final Traditional Integration

Session `20260718T023744Z-76513` measures the corrected 298-pair traditional
census. Equivalent public capture takes `1.035x` native time on Intel and
`1.144x` on Graviton across stable rows. Direct NFA is `1.005x/1.295x`,
OnePass is `0.932x/1.229x`, and BitState is `1.153x/1.458x`.

The old public capture aggregate is not a valid before/after comparator because
four fixed-size Java registrations used unanchored first-match while native
measured anchored full-match. This campaign corrects the Java rows and adds a
semantic input test. The independent Rebar comparison above remains the valid
evidence that application-level capture improved.

The remaining serious public capture row is the long `SplitBig2` workload at
`1.973x/1.984x`, or about 2.5/3.1 ms absolute deficit. Public failed search,
successful search, and full match remain faster than native at
`0.499x/0.452x`, `0.670x/0.587x`, and `0.387x/0.343x` on Intel/Graviton.

See the
[`traditional comparison report`](2026-07-18-traditional-native-comparison.md)
and its complete CSV for provenance, stability exclusions, and separated
outliers.
