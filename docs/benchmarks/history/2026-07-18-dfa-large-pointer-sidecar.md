# Large DFA Absolute-Pointer Sidecar

**Status:** Accepted and integrated.

This focused experiment tests whether the existing absolute-pointer FFM
transition sidecar remains effective at a multi-megabyte scale. The target is
the reset-free bounded-context count workload whose 75,850-state object-row DFA
previously took `1.386x/1.590x` native RE2 time on Intel/Graviton.

## Protocol

AWS session `20260719T003857Z-7477` ran on a `c8i.2xlarge` Intel Xeon 6975P-C
and a `c8g.2xlarge` Graviton4. Both hosts used Temurin 25.0.3, an 8 GiB
pre-touched heap, five untimed warmups, seven measured iterations, and one
pinned physical CPU.

Native RE2 was built from the pinned source revision with `-O3 -DNDEBUG` and
`-march=native` on Intel or `-mcpu=native` on Graviton. Java used the 96 MiB
planning budget established by the cache-capacity campaign. Native access was
disabled for object controls and enabled only for the pointer candidate. The
object control was bracketed around the candidate.

Every Java route retained exactly 75,850 states, returned 53 matches, and reset
zero times. The pointer route allocated 8,388,560 row-rounded bytes, populated
122,474 pointer transitions, and retained 3,352,802 bytes of unused state
budget. Native also returned 53 matches without resets.

Lower ratios are better.

| Architecture | Object before | Pointer | Object after | Native | Pointer/object bracket | Pointer/native |
|---|---:|---:|---:|---:|---:|---:|
| Intel | 19.188 ms | 12.268 ms | 19.159 ms | 13.590 ms | 0.640x | 0.903x |
| Graviton | 26.180 ms | 15.526 ms | 24.852 ms | 16.113 ms | 0.608x | 0.964x |

The multi-megabyte sidecar therefore removes the large object-row throughput
gap on both production architectures. It is 36% faster than the bracketed Java
object control on Intel and 39% faster on Graviton. The known extra AArch64
address operation remains present, but it does not prevent the integrated
route from slightly outperforming native RE2 on this workload.

## Allocation Policy

The original implementation selected a geometric tier between 48 KiB and
8 MiB from the graph built by the first exclusive search. Later graph growth
could exceed that fixed capacity and switch the active instance to object rows.
That policy made allocation demand-driven, but it made one DFA instance change
its transition representation over its lifetime.

Intel follow-up session `20260719T042441Z-48534` first replaced the size cutoff
with a maximum budget-derived allocation. It preserved throughput but made even
a two-state `x*` graph reserve about 5 MiB. Memory-census session
`20260719T045521Z-78073` rejected that lifecycle shape.

The accepted policy instead uses demand-driven geometric growth:

- the initial allocation is at most twice the current state rows;
- every expansion preserves budget for the minimum Java state behind each
  additional native row;
- only the current allocation remains permanently charged to the `DfaInstance`;
- growth copies and rebases the table under exclusive cache mutation;
- geometric growth bounds all unreachable prior arenas below twice the current
  allocation until automatic-arena reclamation;
- failure to fund the next expansion is DFA budget exhaustion, not an
  object-layout switch;
- without native access, no native allocation or initialization occurs;
- explicitly unlimited DFAs remain object-based because they have no finite
  maximum table to allocate.

The focused regression probe allocates against a tiny initial graph, then grows
beyond the former 48 KiB tier while remaining exclusively on FFM. It verifies
that growth increases the allocation, budget accounting reconciles exactly,
and reset reuses the final segment and address.

Two-host memory session `20260719T051215Z-5387` confirms the lifecycle shape.
The tiny warmed `x*` containment graph retains a 144-byte sidecar and 4.3 KiB
total, versus 10.4 KiB for Joni. The exact 75,850-state workload selects
7,959,120 bytes and resets zero times. Its warmed compiled pattern retains
29,801,528 bytes in total.

Lower ratios are better.

| Intel policy | Object before | Pointer | Object after | Native | Pointer/native |
|---|---:|---:|---:|---:|---:|
| Budget-derived stable table | 18.920 ms | 12.334 ms | 18.969 ms | 13.651 ms | 0.904x |

The current policy removes both the table-size handoff and maximum reservation
for small graphs. The hot raw-pointer loop is unchanged; only cold allocation
and state-capacity accounting changed.

Final two-host session `20260719T052408Z-11066` runs only the documented 96 MiB
reset-free target. Both hosts preserve 53 matches, 75,850 states, zero resets,
7,959,120 sidecar bytes, and 122,474 populated pointer transitions.

| Architecture | Object before | Pointer | Object after | Native | Pointer/object bracket | Pointer/native |
|---|---:|---:|---:|---:|---:|---:|
| Intel | 19.177 ms | 12.446 ms | 19.226 ms | 13.709 ms | 0.648x | 0.908x |
| Graviton | 24.362 ms | 15.266 ms | 24.946 ms | 16.138 ms | 0.619x | 0.946x |

The first confirmation attempt accidentally inherited the exploratory 8-128
MiB capacity sweep and failed its zero-reset gate at the intentionally small
budgets. Its valid 96 MiB rows agreed with the final result. The campaign mode
now fixes its input to 96 MiB so that capacity exploration and final layout
qualification cannot be mixed again.

## Decision

Retain one geometrically grown absolute-pointer sidecar for eligible forward
compact one-byte DFA searches. Once selected, that `DfaInstance` remains on
FFM until ordinary memory exhaustion invokes the existing reset or matcher
fallback cascade. Reverse-loop integration remains outside this decision.
