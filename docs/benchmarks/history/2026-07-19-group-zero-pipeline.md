# Group-Zero Span Pipeline Census

**Status:** Closed. The remaining material gap is inside repeated forward and
reverse DFA boundary searches. No production change is retained.

**Source commit:** `2f2d6a753138d593ce59ef003ca383d924953370`

**Source content hash:**
`f7b34a29f15d91505cfb4f6818bbde94e9edd6e1012eae51a95b0080c1d06518`

**Campaign root:** `20260719T173556Z-group-zero-pipeline-baseline`

## Question

The current native census places Rebar group-zero span counting at
`1.997x/1.704x` native time on Intel/Graviton. Removing the three tiny
Cloudflare rows reduces that aggregate to `1.041x/0.926x`; the material
remaining row is Date at `1.630x/1.385x`, or 2.43/2.00 ms slower per complete
operation.

This campaign measured every count-span workload to determine whether that gap
comes from matcher setup, result enumeration, public composition, or the two
DFA boundary searches themselves. It did not reopen generic transition-layout
experiments.

## Result

Lower time is better. Public time is the mean of the public-before and
public-after medians. Forward, reverse, and composed stages independently replay
the exact attempts produced by the public matcher and verify every result before
timing.

| Architecture | Workload | Public | Attempts | ns/attempt | Forward | Reverse | Composed/public | Drift |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Graviton | `curated/03-date/ascii` | 7.158 ms | 42,917 | 166.8 | 58.8% | 38.3% | 95.7% | 0.65% |
| Intel | `curated/03-date/ascii` | 6.207 ms | 42,917 | 144.6 | 64.6% | 37.8% | 98.6% | 1.11% |
| Graviton | `curated/06-cloud-flare-redos/original` | 0.025 ms | 2 | 12,274.4 | 82.3% | 20.7% | 161.6% | 1.93% |
| Intel | `curated/06-cloud-flare-redos/original` | 0.028 ms | 2 | 14,069.0 | 78.3% | 18.0% | 143.6% | 2.33% |
| Graviton | `curated/06-cloud-flare-redos/simplified-long` | 0.221 ms | 2 | 110,678.8 | 58.1% | 12.3% | 106.1% | 0.30% |
| Intel | `curated/06-cloud-flare-redos/simplified-long` | 0.170 ms | 2 | 84,829.4 | 72.1% | 11.7% | 103.3% | 0.89% |
| Graviton | `curated/06-cloud-flare-redos/simplified-short` | 0.021 ms | 2 | 10,479.9 | 80.8% | 23.4% | 114.0% | 3.40% |
| Intel | `curated/06-cloud-flare-redos/simplified-short` | 0.025 ms | 2 | 12,730.1 | 76.5% | 17.8% | 112.6% | 1.73% |
| Graviton | `curated/08-words/all-english` | 1.615 ms | 15,009 | 107.6 | 54.1% | 44.5% | 90.9% | 1.78% |
| Intel | `curated/08-words/all-english` | 1.258 ms | 15,009 | 83.8 | 51.8% | 49.9% | 91.9% | 0.89% |
| Graviton | `curated/08-words/long-english` | 0.142 ms | 65 | 2,185.7 | 83.9% | 19.8% | 106.6% | 2.95% |
| Intel | `curated/08-words/long-english` | 0.106 ms | 65 | 1,625.2 | 87.4% | 17.3% | 108.4% | 1.73% |

Ratios above 100% on the very short operations are expected measurement noise
from independently timed stages; they are not additive samples. The complete
108-row stage dataset is retained in
[`2026-07-19-group-zero-pipeline.csv`](2026-07-19-group-zero-pipeline.csv).

## Date Diagnosis

Date performs 42,916 matches and one final failed attempt. Each match requires
one forward search for the end boundary and one reverse search for the start
boundary. The reverse windows total 268,796 bytes, only 6.26 bytes per call on
average. No capture engine runs.

On Intel, direct forward and reverse stages take 4.009 ms and 2.348 ms; their
composed replay takes 6.123 ms against 6.207 ms for the public operation. On
Graviton they take 4.206 ms and 2.744 ms; composed replay takes 6.847 ms against
7.158 ms public. Public matcher and result work therefore account for at most a
few percent, while closing the current native gap requires roughly 32%-38% of
the complete operation.

The gap is a dense repeated-boundary-search problem. It is not caused by:

- capture extraction or capture-engine workspace;
- allocating or clearing the two-entry group buffer;
- matcher reset;
- `start()` and `end()` result access;
- general public composition around the two DFA searches.

## Other Workloads

The three Cloudflare workloads execute only two attempts. Their large native
ratios represent at most 0.11 ms absolute deficit in the current census, and
their independently timed stage ratios are dominated by fixed harness and call
costs. A workload-specific shortcut would add production complexity without
addressing Date.

The English workloads are controls. The current native census already places
all-English at `0.955x/0.899x` and long-English at `0.725x/0.639x` native time
on Intel/Graviton. Their decomposition confirms the same forward-plus-reverse
route, so any future algorithm must preserve those wins.

## Decision

No bounded integration candidate is retained. The measured non-DFA remainder
is too small to close the Date gap, and the prior reverse-pointer experiment
already showed that paying FFM setup independently for 42,916 six-byte reverse
windows does not help.

The next plausible design is a repeated-boundary DFA cursor that amortizes
search setup across the complete iteration, or a tagged-state DFA that carries
the winning start boundary through forward execution. Either changes DFA
lifecycle or matching semantics and needs its own correctness-first campaign;
it must not be introduced as a small `Re2Matcher` cleanup. That campaign should
first prove a deterministic route, preserve shared-DFA mutation safety, and
beat the current Date path without regressing the two English controls.

## Qualification

- Six independent workload campaigns ran concurrently on `c8i.2xlarge` and
  `c8g.2xlarge` hosts.
- Every host used the same source commit and content hash.
- Stage replay validated every result against the public matcher before timing.
- Date public-before/public-after drift was 1.11% on Intel and 0.65% on
  Graviton.
- Three non-material control rows exceeded the ordinary 2% drift preference;
  their values are retained as diagnostic rather than qualification evidence.
- All 12 benchmark hosts completed successfully and all temporary AWS resources
  were removed.

This is internal engineering evidence, not formal publication qualification.
