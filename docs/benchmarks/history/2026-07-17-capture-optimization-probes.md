# Capture Optimization Probes

**Status:** Three bounded candidates rejected; engine remains at the traditional
native-comparison baseline.

This document records the state at the conclusion of the 2026-07-17 probes. It
is superseded for current implementation status by the
[`2026-07-18 capture-engine campaign`](2026-07-18-capture-engine-campaign.md).

**Baseline:** `2c50455` (`Record traditional RE2 baseline`)

The traditional native comparison isolated capture extraction as the largest
public-path deficit. Three deliberately small changes tested whether existing
engines could close that gap without changing the accepted DFA layouts. None
passed the performance retention gate, so no implementation change is retained.

## Tiny OnePass Boundary Probe

The first candidate used an allocation-free OnePass search to find group-zero
boundaries for small eligible expressions, then replayed OnePass over that
bounded window to fill captures. This avoids the ordinary forward-DFA and
reverse-DFA boundary search when a match starts immediately.

The candidate was semantically sound: the complete RE2 selector passed 864
tests with zero failures or errors and one intentional skip. It also produced a
large win for an immediate match, but realistic position changes exposed the
cost of probing with OnePass before falling back:

| Protected local case | Candidate | Control | Change |
|---|---:|---:|---:|
| Match at start | 91.570 ns | 224.418 ns | 59.2% faster |
| Late match | 233.059 ns | 200.567 ns | 16.2% slower |
| No match | 24.005 ns | 19.543 ns | 22.8% slower |
| Above eligibility limit | 221.965 ns | 206.496 ns | 7.5% slower |

An existing-DFA start-byte gate reduced the late-match loss to about 8.9% and
the no-match loss to about 10.4%. A caller-buffer history hint restored the
no-match case but still left late matching about 5.3% slower and the
above-limit control about 5.4% slower. These are workload tradeoffs rather than
a general capture improvement, so the candidate and both refinements were
removed.

A full traditional AWS campaign (`20260717T125005Z-11601`) had already started
from the initial ungated candidate snapshot. It was stopped without publishing
results after the protected local controls rejected that source; completing an
additional 298-pair campaign could not change the failed retention decision.
Both instances terminated and the temporary transfer bucket and IAM resources
were removed.

## Primitive BitState Traversal-Job Stack

The second candidate replaced BitState's preallocated traversal `Job` objects
with packed primitive arrays. This removed roughly 1 KiB per operation but
increased time on both measured shapes. This is distinct from the later retained
packed capture-instruction metadata, which removes instruction-object loads
without changing traversal-job storage:

| Local case | Object stack | Primitive stack | Allocation before | Allocation after |
|---|---:|---:|---:|---:|
| Three digit captures | 275.398 ns | 344.410 ns | 1,960 B/op | 960 B/op |
| Split-hard capture | 246.507 ns | 287.102 ns | 1,944 B/op | 944 B/op |

The allocation is not the dominant cost at these sizes. Packing and unpacking
the traversal state makes the direct BitState engine slower, so this candidate
was removed.

## Reuse OnePass Caller Captures

The third candidate reused the caller's capture array as OnePass's evolving
capture state during unanchored first-match searches. It halved measured scratch
allocation from 64 to 32 B/op for split and from 96 to 48 B/op for three digit
captures.

A same-process, five-fork throughput control showed that the allocation win did
not translate into a general execution win:

| Local case | Candidate | Control | Change |
|---|---:|---:|---:|
| Split capture | 55.699 ns | 52.886 ns | 5.3% slower |
| Three `\d` captures | 51.404 ns | 52.236 ns | 1.6% faster |
| Three digit-class captures | 51.447 ns | 52.179 ns | 1.4% faster |

The split regression is larger than either gain and has non-overlapping
confidence intervals. This candidate was also removed.

## Conclusion

At the conclusion of this probe, the retained engine was exactly the committed
traditional-comparison baseline and the experiments changed no production
source. Capture remained the leading performance gap, but these measurements
rejected three tempting local changes:

- do not probe with OnePass before the established boundary engine unless a
  future design avoids both late-match and no-match duplicate work;
- do not replace BitState's object jobs with packed primitives merely to reduce
  allocation;
- do not alias OnePass's evolving captures with the caller buffer for all
  unanchored first-match searches.

The prescribed next investigation was completed by the 2026-07-18
algorithm-by-algorithm campaign. Its retained NFA, OnePass, and BitState changes
establish the newer baseline. The accepted one-byte DFA layouts remain frozen.
