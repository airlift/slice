# Integrated Absolute-Pointer DFA Result

> Engineering evidence from 2026-07-16. This is the accepted production design,
> not formal release qualification.

## Decision

Use a bounded 64-bit absolute-pointer FFM sidecar for eligible forward one-byte
DFA searches. The sidecar is optional, capped at 48 KiB of total row-rounded
native allocation, and selected only after the existing paired DFA has had its
opportunity to build. All other searches retain the object-row DFA.

The earlier isolated cutoff campaign rejected this representation because its
pure pointer walker took about 1.25x equivalent C++ loop time on Graviton. The
project subsequently accepted that known JDK C2/AArch64 code-generation cost
and tested the complete production route. In the complete DFA comparison, Java
reached native parity on both target architectures while remaining materially
faster than the object-row control.

## Campaign

- Session: `20260716T233209Z-7064`
- Intel: `c8i.2xlarge`, Intel Xeon 6975P-C
- Graviton: `c8g.2xlarge`, AWS Graviton4
- JVM: Temurin 25.0.3+9, 8 GiB heap, zero-based compressed ordinary object pointers
- Input: deterministic 16 MiB state-changing ASCII sequence
- Expression: anchored 26-state case-insensitive sequence with a failing end alternative
- Java: three JMH forks for candidate-before, object control, and candidate-after
- Native: five direct Google Benchmark repetitions
- Native flags: `-O3 -DNDEBUG -march=native` on Intel and
  `-O3 -DNDEBUG -mcpu=native` on Graviton

The object control used the same Java source with native access denied. The
candidate used `--enable-native-access=ALL-UNNAMED` and
`--illegal-native-access=deny`. Candidate time is the geometric mean of the
before and after measurements.

## Results

| Architecture | Pointer Java | Object Java | Pointer/object | Native RE2 | Pointer/native |
|---|---:|---:|---:|---:|---:|
| Intel | 21.678 ms | 30.554 ms | 0.709x | 21.763 ms | 0.996x |
| Graviton | 31.229 ms | 42.889 ms | 0.728x | 31.042 ms | 1.006x |

Protected paired routes remained within the existing 2% source-disabled gate:

| Architecture | `HARD` candidate/control | `PARENS` candidate/control |
|---|---:|---:|
| Intel | 1.000x | 1.000x |
| Graviton | 1.014x | 0.992x |

The integrated candidate therefore satisfies the production-path object and
native gates on both targets. No confirmation campaign was needed.

## Generated Code

Perfasm places 89.3% of Intel samples and 74.3% of Graviton samples in the
physically separate `searchForwardAbsolutePointers` method. The loop contains
no layout dispatch, allocation, ownership operation, or memory-budget branch.

The known Graviton limitation remains: C2 materializes the scaled transition
offset and adds it to the current row before the dependent load, rather than
using AArch64's scaled-index `ldr`. C2 also retains a range comparison against
the everything segment's `Long.MAX_VALUE` extent. The earlier forced-native
experiment established that the extra dependent address operation accounts for
the isolated loop gap. The integrated result establishes that these generated
instructions do not leave the complete Java DFA materially behind native RE2
for the measured state-changing route.

## Lifecycle And Fallback

- The sidecar is allocated lazily under the existing exclusive cache mutation epoch.
- Restricted FFM initialization is not reached when native access is disabled.
- Existing normal transitions are backfilled before the holder is published.
- Later native entries are written only after canonical integer and object transitions.
- Zero entries delegate to the unchanged object continuation for uncomputed,
  dead, match, full-match, end-of-text, and unavailable transitions.
- The automatic-arena owner remains reachable through every raw-address traversal.
- The complete row-rounded allocation is permanently charged to the DFA budget.
- Reset clears and reuses the same segment and stable addresses without returning its charge.
- Capacity overflow disables the native route for that cache generation and
  continues with object rows; it does not trigger matcher fallback.
- Once a DFA instance selects this one-byte sidecar, paired transitions remain
  disabled across its later resets. Pairing was evaluated first and did not
  produce a usable table for that instance.

Focused tests passed on both target hosts. The local complete RE2 selector also
passes with native access denied and enabled. Formal publication qualification
remains a separate phase.
