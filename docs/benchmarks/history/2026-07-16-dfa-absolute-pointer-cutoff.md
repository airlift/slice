# Absolute-Pointer DFA Cutoff Result

> Historical engineering evidence from 2026-07-16. This document records the
> original strict cutoff decision. It was later superseded by the accepted
> production integration recorded in
> [`2026-07-16-dfa-absolute-pointer-integrated.md`](2026-07-16-dfa-absolute-pointer-integrated.md).

## Decision

Reject the bounded 64-bit absolute-pointer FFM sidecar. Do not integrate it into
the production DFA.

The representation materially beats Java object rows for small and medium
tables on Intel and Graviton, but it does not reproduce direct C++ pointer
throughput on Graviton. The bounded plan required both properties on both target
architectures and ended at the first hard gate failure.

## Campaign

- Session: `20260716T194347Z-47380`
- Intel: `c8i.4xlarge`, Intel Xeon 6975P-C, eight physical cores
- Graviton: `c8g.2xlarge`, AWS Graviton4, eight physical cores
- JVM: Temurin 25.0.3+9, 8 GiB heap, compressed ordinary object pointers
- Input: deterministic 16 MiB xorshift byte corpus
- Byte classes: 29
- Java: three JMH forks per point
- Native: direct Google Benchmark traversal with median repetitions
- Native flags: `-O3 -DNDEBUG -march=native` on Intel and
  `-O3 -DNDEBUG -mcpu=native` on Graviton

The Java benchmark stores absolute target-row addresses in a native table and
loads them through a `static final` everything segment. The C++ control stores
the same absolute row pointers in a contiguous `uintptr_t` table. Both use the
same state recurrence, reserved zero row, byte map, and input bytes.

## Results

`Pointer/object` below 1.0 favors the candidate over the current Java layout.
`Pointer/native` below 1.0 favors Java over the direct C++ traversal.

| Table bytes | States | Intel pointer/object | Intel pointer/native | Graviton pointer/object | Graviton pointer/native |
|---:|---:|---:|---:|---:|---:|
| 7,656 | 32 | 0.716x | 1.008x | 0.716x | 1.256x |
| 15,080 | 64 | 0.716x | 1.007x | 0.720x | 1.264x |
| 22,504 | 96 | 0.717x | 1.009x | 0.716x | 1.256x |
| 29,928 | 128 | 0.721x | 1.008x | 0.719x | 1.261x |
| 37,352 | 160 | 0.735x | 1.011x | 0.726x | 1.231x |
| 44,776 | 192 | 0.776x | 1.019x | 0.724x | 1.237x |
| 52,200 | 224 | 0.901x | 1.011x | 0.727x | 1.240x |
| 59,624 | 256 | 1.051x | 1.014x | 0.737x | 1.251x |
| 74,472 | 320 | 1.247x | 1.011x | 0.834x | 1.177x |
| 89,320 | 384 | 1.230x | 1.012x | 0.950x | 1.124x |
| 104,168 | 448 | 1.143x | 1.009x | 1.034x | 1.099x |
| 119,016 | 512 | 1.090x | 1.009x | 1.032x | 1.077x |

The candidate meets the isolated `0.80x` object-row gate through 44,776 bytes
on both architectures. Intel also meets the `1.03x` native gate. Graviton is
1.231-1.264x native time in that useful range, above both the `1.03x`
acceptance gate and the `1.05x` hard-failure boundary.

This is a directional architecture difference, not benchmark noise. The
Graviton failure is more than 17 percentage points beyond the hard boundary at
every tested footprint. The optional confirmation campaign was therefore not
permitted.

## Graviton Root Cause

A separate diagnostic investigation after the bounded campaign explained the
small-table Graviton gap. It did not reopen the rejected integration.

- Initial capture: `20260716T202247Z-10410`
- Confirmation capture: `20260716T203349Z-34271`
- Host: `c8g.2xlarge`, AWS Graviton4
- Shape: 32 states, 29 byte classes, 7,656-byte transition table, 16 MiB input

| Loop | Time | Relative to native |
|---|---:|---:|
| Native C++ scaled-index load | 24.185 ms | 1.000x |
| Native C++ forced separate address | 30.128 ms | 1.246x |
| Java FFM aligned load | 30.512 ms | 1.262x |
| Java FFM unaligned load | 30.381 ms | 1.256x |

GCC folds the byte-class scale and current-row address into one AArch64 load:

```text
ldrb byteClass, [byteMap, inputByte]
ldr  row, [row, byteClass, lsl #3]
```

JDK 25 C2 instead materializes the pointer-table address before loading it:

```text
ldrb byteClass, [byteMap, inputByte]
lsl  offset, byteClass, #3
add  address, row, offset
ldr  row, [address]
```

That extra address operation is on the loop-carried row-to-row dependency
chain. Forcing GCC to use the same separate-address shape slowed native by
24.6% and put it within 0.8% of the Java unaligned loop. Conversely, switching
Java to an unaligned FFM layout removed the alignment guard but improved
throughput by only 0.4%. Hardware counters reported effectively no L1 data
cache misses in either native shape.

The measured Java/native delta is 0.377 ns per input byte, while the forced
native-address delta is 0.354 ns per byte. The evidence therefore attributes
nearly the entire small-table gap to C2's failure to select AArch64's scaled
indexed `ldr` for the FFM absolute-address expression. FFM range checks remain
in the generated code, but this experiment does not identify them as the
throughput limiter.

This is a JDK 25 C2/AArch64 code-generation limitation, not a fundamental cost
of native memory or Java bounds checking. It also explains why the gap narrows
as transition tables grow: memory latency increasingly hides the fixed
dependent-address cost. A future attempt should begin with a supported Java
source shape or JVM improvement that produces the scaled indexed load. Merely
repeating the current FFM representation cannot meet the Graviton native-parity
gate.

### Supported Source Rewrites

Two bounded follow-ups tested whether a different public FFM expression could
make C2 select the scaled indexed load:

- `20260716T205424Z-72760`: pre-scaled offsets and `getAtIndex`
- `20260716T210330Z-88939`: row segments and targeted address layouts

| Java source shape | Time | Relative to absolute-address baseline |
|---|---:|---:|
| Absolute address with unaligned `get` | 30.277 ms | 1.000x |
| Pre-scaled byte offset | 36.210 ms | 1.196x |
| Absolute word index with `getAtIndex` | 48.687 ms | 1.608x |
| Row `MemorySegment` with targeted `AddressLayout` | 478.789 ms | 15.813x |

Pre-scaling did not remove the dependent address operation. `getAtIndex`
multiplies its index by the layout size before the var-handle access, adding
work rather than preserving separate base and scaled-index operands. Carrying
the row as a targeted `MemorySegment` exposed the desired API-level operands,
but C2 did not scalar-replace the segment flow and its per-transition machinery
made the loop unusable.

No tested public-FFM source rewrite eliminates the extra dependent address
operation. This closes the source-level workaround search for this
representation. The remaining direct remedies are a C2/AArch64 optimization
that preserves the scaled-index address mode, or moving the complete loop into
native code. Algorithmic routes that consume more than one byte per dependent
transition, such as the existing paired DFA, remain separate ways to amortize
the cost rather than eliminate the instruction.

### Newer JDK Check

The same Graviton perfasm capture was repeated with official OpenJDK builds.
Both newer builds retained the separate `lsl`, dependent `add`, and plain
`ldr` sequence.

| Runtime | Java time | Native time | Java/native |
|---|---:|---:|---:|
| Temurin 25.0.3+9 | 30.381 ms | 24.185 ms | 1.256x |
| OpenJDK 26+35 | 31.167 ms | 24.185 ms | 1.289x |
| OpenJDK 27-ea+30 | 31.197 ms | 24.189 ms | 1.290x |

- JDK 26 session: `20260716T212429Z-31402`
- JDK 27 EA session: `20260716T213246Z-48404`

The relevant AArch64 `mem2address` helper is byte-for-byte identical in the
JDK 25 tag, JDK 26 build 35 tag, and current OpenJDK mainline. The
`MemorySegment.getAtIndex` implementation also retains the same
`index * layout.byteSize()` expression in all three sources. The measured
behavior is therefore not a fixed optimization that is merely absent from the
project's JDK 25 runtime.

The closest Java Bug System entry is
[`JDK-8345306`](https://bugs.openjdk.org/browse/JDK-8345306), an open AArch64
C2 matcher investigation covering address computations that are expected to be
subsumed into memory instructions. It has no fix version and does not contain
this FFM reproducer. Searches for AArch64, `MemorySegment`, scaled indexes,
indirect memory inputs, and base-plus-index expressions found no issue that
reports this exact generated-code and throughput failure. Treat
`JDK-8345306` as a likely related umbrella, not confirmation that this specific
case is already tracked.

## Coverage

Coverage was informational. Applying a hypothetical 48 KiB cap to the existing
Rebar structural census covers 66 of 67 routes whose recorded route begins with
`COMPACT`. The largest covered pointer table is 46,400 bytes. The sole excluded
route is `wild/url/search` at 842,016 bytes.

Coverage is therefore not unexpectedly insignificant. It does not override the
failed cross-architecture native-parity gate.

## Consequence

No production code was changed. The object-row DFA and its existing paired,
fixed-distance, start-byte, and adaptive self-loop specializations remain the
supported implementation.

The campaign does not justify another pointer representation, reverse-loop
integration, or a broader transition-layout campaign. Any future attempt must
start from a new decision with a materially different hypothesis rather than
repeating this bounded sidecar.

Raw local artifacts are under
`benchmark-results/re2-engineering/dfa-absolute-pointer-cutoff/20260716T194347Z-47380/`.
They include host manifests, Java JMH output, native output, source snapshots,
and the generated cutoff summary.
