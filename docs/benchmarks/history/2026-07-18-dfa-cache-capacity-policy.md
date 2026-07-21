# DFA Cache Capacity Policy

**Status:** Accepted and integrated.

This bounded campaign replaces the conservative DFA state estimate with
runtime-aware retained-memory accounting and raises the default planning budget
from 8 MiB to 96 MiB. It changes neither the transition representation nor the
hot search loop.

## Ratio Convention

All ratios are elapsed-time ratios, so lower is better. A Java/native ratio
above `1.0x` means Java is slower than native RE2. A Java/8-MiB ratio below
`1.0x` means the selected policy is faster than the 8 MiB control.

## Protocol

Cutoff session `20260718T191726Z-61590` ran concurrently on an AWS
`c8i.8xlarge` Intel Xeon 6975P-C and `c8g.4xlarge` Graviton4. Confirmation
session `20260718T192636Z-67046` repeated the selected 96 MiB policy and the
8 MiB control on the same instance families. Both sessions used Temurin 25.0.3,
an 8 GiB pre-touched heap, five untimed warmups, seven measured iterations, and
one pinned physical CPU.

Native RE2 was built from the pinned project revision with `-O3 -DNDEBUG` and
`-march=native` on Intel or `-mcpu=native` on Graviton. Both native-access
modes passed the focused Java tests on both hosts. Every target operation
returned 53 matches.

## Memory Accounting

The old estimate charged fixed conservative constants per state and returned
expanded backing arrays to the budget after reset even though those arrays
remained reachable. The replacement uses `SizeOf` to charge:

- transition, state-data, and state-reference backing arrays;
- per-state reference rows and instruction arrays;
- state metadata, keys, and ordinary `HashMap` entries;
- exact backing-array and hash-table capacity growth.

Reset now returns only memory for objects that become unreachable. Expanded
arrays and the retained hash table remain permanently charged.

A temporary JOL probe measured the exact 75,850-state target graph in fresh
JVMs on both architectures. The table compares lifecycle deltas so unrelated
program and work-queue objects in JOL's complete graph are excluded.

| Reference mode | Component | JOL | `SizeOf` | Difference |
|---|---|---:|---:|---:|
| Compressed | Retained backing growth | 5,885,720 B | 5,885,424 B | -296 B (-0.005%) |
| Compressed | Live per-state objects | 15,891,016 B | 15,891,016 B | 0 B |
| Uncompressed | Retained backing growth | 7,303,552 B | 7,303,240 B | -312 B (-0.004%) |
| Uncompressed | Live per-state objects | 22,246,488 B | 22,565,816 B | +319,328 B (+1.436%) |

The custom model is exact for the production compressed-reference shape and
slightly conservative without compressed references. JOL was removed after the
comparison because it depends on restricted JVM diagnostic access.

The complete compressed target cache occupies approximately 20.8 MiB. At a
64 MiB total planning budget, its first-match state allowance has only 971,023
bytes remaining. At 96 MiB, it has 11,741,362 bytes remaining. The 96 MiB
policy also retains the target when ordinary object references are
uncompressed; 64 MiB does not.

## Cutoff Results

The following medians are from the first campaign. `Resets` is Java cache
resets during one complete count.

### Intel

| Total budget | Java | Native | Java/native | Resets | Retained states | Available state bytes |
|---:|---:|---:|---:|---:|---:|---:|
| 32 MiB | 101.760 ms | 71.288 ms | 1.427x | 3 | 25,222 | 3,264,596 |
| 48 MiB | 89.429 ms | 17.290 ms | 5.172x | 1 | 34,662 | 5,200,850 |
| 64 MiB | 24.708 ms | 16.359 ms | 1.510x | 0 | 75,850 | 971,023 |
| 96 MiB | 24.260 ms | 16.150 ms | 1.502x | 0 | 75,850 | 11,741,362 |

### Graviton

| Total budget | Java | Native | Java/native | Resets | Retained states | Available state bytes |
|---:|---:|---:|---:|---:|---:|---:|
| 32 MiB | 118.173 ms | 91.108 ms | 1.297x | 3 | 25,222 | 3,264,596 |
| 48 MiB | 105.803 ms | 17.875 ms | 5.919x | 1 | 34,662 | 5,200,850 |
| 64 MiB | 28.223 ms | 17.888 ms | 1.578x | 0 | 75,850 | 971,023 |
| 96 MiB | 27.697 ms | 17.717 ms | 1.563x | 0 | 75,850 | 11,741,362 |

The 64 MiB and 96 MiB timings are equivalent within ordinary run variance.
The larger policy is selected for headroom and uncompressed-reference support,
not because it makes the reset-free loop faster.

## Confirmation

| Workload | Architecture | Java 8 MiB | Java 96 MiB | Java 96/8 MiB | Java/native at 96 MiB |
|---|---|---:|---:|---:|---:|
| Bounded context | Intel | 127.742 ms | 18.993 ms | 0.149x | 1.386x |
| Bounded context | Graviton | 153.623 ms | 26.687 ms | 0.174x | 1.590x |
| `capitals` control | Intel | 7.373 ms | 7.390 ms | 1.002x | 0.739x |
| `capitals` control | Graviton | 8.761 ms | 8.775 ms | 1.002x | 0.660x |
| `letters-en` control | Intel | 0.508 ms | 0.508 ms | 0.999x | 1.439x |
| `letters-en` control | Graviton | 0.426 ms | 0.426 ms | 1.000x | 0.903x |

The selected policy eliminates all target resets and improves the target by
6.7x on Intel and 5.8x on Graviton relative to the same implementation at
8 MiB. Both controls remain within 0.2%.

## Decision

The policy is accepted:

1. Use runtime-aware retained-memory accounting for DFA states.
2. Keep retained backing storage charged across cache resets.
3. Set the public default planning budget to 96 MiB.
4. Keep allocation demand-driven; the larger ceiling does not reserve or
   allocate 96 MiB when a pattern compiles.
5. Treat the remaining `1.386x/1.590x` reset-free target gap as ordinary
   object-row transition throughput, outside this memory-policy campaign.

The bounded campaign used one cutoff run and one confirmation. No further
candidate or confirmation was opened.
