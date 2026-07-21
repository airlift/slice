# Date Candidate-Start Cursor Campaign

**Status:** Closed. The bounded candidate-start cursor is retained.

**Baseline source commit:** `9b6ac746`

**Candidate source hash:**
`6d02ef2595ecdc62bc1d042887abccff74af6dba9a920ffc7d1c9533c43aa15c`

This is internal engineering evidence, not publication qualification. Lower
Java/native ratios are better.

## Question

The Date count-span workload reports 42,917 short variable-length matches.
Each ordinary group-zero `find()` performs a forward DFA search and then a
reverse DFA search to recover the start boundary. The earlier decomposition
measured that composition at 98.6%/95.7% of public time on Intel/Graviton and
placed the complete operation at `1.630x/1.385x` native time.

The candidate adds a private cursor for narrowly eligible reusable matchers. It
uses the forward DFA's start-byte accelerator to find a possible start, then
runs an anchored first-match traversal from that byte. A reset-scoped linear
work budget and a 128-transition limit per candidate bound the additional work.
Any unsupported transition, cache mutation failure, or exhausted bound disables
the cursor until the next matcher reset and falls back to the existing
forward-plus-reverse implementation.

The route excludes anchors, empty-width instructions, required prefixes,
nullable or fixed-length expressions, longest-match mode, matchers requesting
explicit capture groups, and stateless callers. It adds no public API.

## Correctness

Focused tests cover exact Date boundaries, alternation priority, greedy and
reluctant repetition, logical regions, UTF-8 and malformed input, candidate
starts inside encoded code points, work-limit fallback, reset re-enablement,
ineligible programs, and concurrent DFA cache reset/search.

The campaign found a real warm-cache defect before retention. Cached object
transitions advanced the row reference without updating the numeric state
offset, so the end-of-text transition could be read from a stale state. The
regression test warms `(a+|b+)` on `xxxxb` and then searches a longer input
ending in `b`. Recovering the authoritative offset from the current row before
the end-of-text transition fixes the failure.

The complete RE2 selector passes 913 tests with and without native access. The
full Maven install passes 2,703 tests with zero failures or errors and one
intentional skip.

## Target-Host Result

Session `20260720T012825Z-60724` ran the pinned `count-spans` intersection on
one `c8i.2xlarge` and one `c8g.2xlarge`. Native RE2 was built with host tuning.
Both hosts completed successfully and temporary AWS resources were removed.

### Material Rows

| Workload | Intel Java | Intel native | Intel ratio | Graviton Java | Graviton native | Graviton ratio |
|---|---:|---:|---:|---:|---:|---:|
| Date | 4.690 ms | 3.850 ms | 1.218x | 4.590 ms | 5.170 ms | 0.888x |
| all-English | 1.280 ms | 1.335 ms | 0.959x | 1.560 ms | 1.780 ms | 0.876x |
| long-English | 76.11 us | 104.91 us | 0.725x | 93.11 us | 145.64 us | 0.639x |

Date improves by approximately 25% relative to the prior normalized
`1.630x/1.385x` result. It is 11.2% faster than native on Graviton but remains
21.8%, or 0.84 ms, slower than native on Intel. Local route diagnostics report
42,916 cursor routes and zero fallbacks for the exact Date input.

The English controls contain word boundaries and are therefore ineligible for
the cursor. Their ratios remain consistent with the pre-candidate baseline,
which protects unrelated group-zero traversal.

### Extreme Relative Outliers

| Workload | Intel Java/native | Intel absolute deficit | Graviton Java/native | Graviton absolute deficit |
|---|---:|---:|---:|---:|
| Cloudflare original | 2.910x | 0.729 us | 2.262x | 0.698 us |
| Cloudflare simplified-long | 5.177x | 107.55 us | 4.125x | 112.74 us |
| Cloudflare simplified-short | 3.320x | 0.888 us | 2.697x | 0.906 us |

These rows produce alarming ratios because native completes in fractions of a
microsecond to tens of microseconds. They are recorded separately and do not
describe the normal count-span cost. Their largest absolute deficit is about
0.11 ms.

## Evidence Limits

- The pinned Rebar checkout is commit
  `463d00f31887e84c38467805b9e3122c314b9521`.
- The first attempted AWS run used the capture-pipeline harness, which supports
  `grep-captures` but not `count-spans`. It failed before any samples and is not
  performance evidence.
- This bounded campaign used one host per architecture and one integrated
  campaign. Formal qualification must independently repeat the final-source
  comparison with its stronger stability protocol.
- The six-row geometric mean is `1.867x/1.524x`, but it is dominated by the
  three tiny Cloudflare ratios and is not a useful normal-case summary.

## Decision

Retain the candidate-start cursor. It materially reduces the only substantial
group-zero span deficit, preserves the protected controls, and makes Date
faster than native on Graviton. Rejecting it would restore the larger
`1.630x/1.385x` gap.

Do not claim complete parity. Intel Date remains a material 21.8% deficit and
must be listed explicitly in formal qualification. Further work should begin
from that per-match fixed-cost question only if final-source qualification
reproduces it; this campaign does not authorize an open-ended tagged-DFA or
benchmark-specific batch API.
