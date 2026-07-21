# Forward Compact DFA Absolute-Pointer Plan

**Status:** Accepted for production engineering on 2026-07-16. Results from
this work establish internal confidence but are not formal release qualification.

## Objective

Integrate and qualify one representation: a bounded sidecar containing 64-bit
absolute pointers between transition rows for eligible forward compact DFA
searches.

The goal is complete when this representation is either accepted in production
or rejected with reproducible evidence. A failed gate ends the goal; it does not
open another transition-layout exploration.

## Scope

This goal includes:

- the ordinary forward compact DFA path
- first-match and longest-match DFA caches where the ordinary compact path is
  otherwise selected
- one supported FFM absolute-pointer representation
- Intel and AWS Graviton cutoff characterization
- optional native access with an unchanged object-row fallback
- correctness, concurrency, lifecycle, memory, assembly, and performance gates

This goal excludes:

- reverse DFA integration
- paired-transition redesign
- self-loop, prefix, fixed-distance, capture, or other matcher specialization
  work
- generic transition-layout abstractions
- alternative heap, buffer, relative-offset, Unsafe, JNI, or native-binding
  representations
- Trino integration and formal release qualification

## Representation

The existing transition structures remain authoritative:

- `int[] transitions` stores offsets, match bits, and transition sentinels
- `Object[][] stateRefArrays` remains the complete ordinary compact-DFA path
- a native sidecar stores only 64-bit absolute addresses of normal target rows

A zero sidecar entry means that the hot loop must return to the existing compact
transition handling. The integer transition table determines whether the entry
is uncomputed, dead, a match, a full match, or otherwise unavailable.

The implementation must not introduce a transition-layout interface, strategy,
or generic table abstraction. Cold allocation, population, reset, and accounting
remain private to `DfaInstance`. The native hot loop reads concrete fields
directly and is physically separate from the object-row loop.

## Eligibility And Cutoff

Eligibility and the cutoff are expressed only in total transition-table bytes.
No state-count threshold is part of the production policy.

The charged size is the complete automatic-arena allocation, including every
reserved row and any alignment or padding. Conceptually:

```text
pointerTableBytes = allocatedRowCapacity * nextSize * Long.BYTES
```

The cutoff campaign determines the largest byte footprint that retains the
required performance on both Intel and Graviton. Only the 64-bit
absolute-pointer representation is characterized. The campaign does not reopen
relative offsets, primitive heap tables, buffers, or other layouts.

Corpus analysis reports how often representative searches fit below the chosen
byte cutoff. Coverage is informational, not an acceptance percentage. Stop for
discussion only if coverage is unexpectedly insignificant, meaning the route is
effectively absent from representative state-changing compact-DFA work.

## Fallback Semantics

Exceeding the pointer-table byte cap disables the native sidecar route for that
cache generation and continues through the existing object-row DFA. It does not
exhaust the DFA cache, reset the cache, or enter the matcher fallback cascade.

Only the existing ordinary DFA state-budget exhaustion and repeated cache
thrashing behavior may reach the established matcher fallback cascade. The
sidecar must not alter those decisions.

## Native Memory Lifecycle

Native access is optional. When the containing module does not have native
access enabled:

- restricted FFM initialization is not attempted
- no warning or initialization failure is produced
- no native sidecar is allocated
- the object-row DFA operates normally

The everything segment is initialized lazily in a `static final` field only
after native access is known to be enabled. The owning transition allocation
uses an automatic arena so it is accessible from every search thread and is
reclaimed after its `DfaInstance` becomes unreachable.

The complete allocation is charged permanently to the owning `DfaInstance`
budget. Cache reset clears and reuses the allocation but does not return its
budget. Budget is not returned while the native memory remains reachable.

The allocation has a stable address and is never resized. Owner reachability is
preserved for every raw-address access. Native entries are populated and reset
only under the existing exclusive cache-mutation and reader-quiescence protocol;
the byte loop receives no per-transition lifecycle or synchronization check.

## Execution Phases

### Phase 1: Cutoff Qualification

1. Freeze the current object-row and absolute-pointer benchmark sources.
2. Verify that the direct C++ control uses the pinned source and release
   optimization for each host.
3. Sweep total pointer-table bytes around the observed crossover on Intel and
   Graviton.
4. Select one conservative byte cutoff that passes on both architectures.
5. Report representative corpus coverage without using it as a percentage gate.

Stop if no useful byte range materially beats object rows and remains within
the native comparison gate on both architectures.

### Phase 2: Test-First Storage Integration

1. Add deterministic tests that prove native-access capability detection,
   allocation, activation, sentinel handling, byte-cap overflow, object-row
   fallback, budget accounting, reset reuse, and permanent budget charging.
2. Add child-JVM coverage showing that disabled native access produces neither a
   warning nor an initialization failure.
3. Add concurrent search, cache build, reset, and forced-GC coverage before
   changing the hot loop.
4. Implement private fixed-capacity automatic-arena management in `DfaInstance`.
5. Populate existing and newly computed normal transitions into both object rows
   and the absolute-pointer sidecar.

Stop on any unexplained correctness, publication, lifecycle, reclamation, or
memory-accounting failure.

### Phase 3: Forward Hot-Loop Integration

1. Add a physically separate forward absolute-pointer compact loop.
2. Select the object or native loop before byte traversal begins.
3. Keep the existing object-row loop unchanged for disabled native access,
   over-cap graphs, unavailable entries, and all ineligible routes.
4. Do not add an interface call, polymorphic access, per-byte mode branch,
   allocation, wrapper construction, or ownership check to the hot loop.
5. Run production-path source-disabled controls for eligible and protected
   workloads on Intel and Graviton.

Stop if integration loses the isolated advantage or regresses a protected path.

### Phase 4: Verification And Finalization

1. Run the complete RE2 selector with native access disabled.
2. Run the complete RE2 selector with native access enabled.
3. Run the full Maven build in the normal configuration.
4. Run retained-memory and native-memory reclamation diagnostics.
5. Capture target-host assembly for the integrated loop.
6. Update `RE2_TASKS.md`, `RE2_DECISIONS.md`, the layout evidence, and the
   engineering-results ledger with the accepted or rejected outcome.
7. Remove temporary representation experiments and leave one concrete
   implementation or the unchanged object-row production path.

## Acceptance Gates

### Correctness And Safety

- The full RE2 selector passes with and without native access.
- Object and native routes produce identical boundaries, sentinels, and fallback
  behavior.
- Byte-cap overflow selects object rows without invoking matcher fallback.
- Concurrent search, transition construction, and reset remain correct.
- No VM crash, memory corruption, use-after-free symptom, warning in disabled
  mode, or unexpected initialization failure is acceptable.

### Performance

- The isolated candidate is at most `0.80x` object-row time for useful table
  footprints on both target architectures.
- The isolated candidate remains within `1.03x` of the equivalent direct C++
  traversal on Intel. The known Graviton C2/AArch64 dependent-address cost is
  accepted only if the integrated DFA remains within `1.03x` of native RE2.
- Integrated eligible cases are at most `0.85x` source-disabled object control.
- Integrated eligible cases have a per-architecture geometric mean within
  `1.03x` of native, with no important case above `1.05x`.
- Every ineligible and protected route remains within the existing `1.02x`
  regression gate.

A result between `1.03x` and `1.05x` may use the single optional confirmation
run. A result above `1.05x` fails the goal.

### Generated Code

- The hot loop contains no temporal-scope check. C2 may retain the everything
  segment's `Long.MAX_VALUE` range comparison; acceptance is based on the
  target-host production benchmark and recorded generated code.
- The hot loop contains no call, allocation, wrapper construction, generic
  dispatch, per-byte mode check, or memory-budget branch.
- The serial transition chain remains the input-byte load, byte-map load, and
  absolute-pointer load.

### Memory And Lifecycle

- The complete automatic-arena allocation is charged once and remains charged
  while reachable.
- Reset reuses the same allocation and stable addresses.
- Allocation failure or insufficient budget leaves the object-row DFA intact.
- Repeated create, warm, reset, discard, and forced-GC diagnostics show no
  unbounded native-memory growth.
- The implementation requires no global arena, Cleaner, public close operation,
  Unsafe, or native library.

## Execution Bound

The goal may run only:

1. one Intel-and-Graviton cutoff campaign
2. one optional cutoff confirmation when the result is inconclusive
3. one Intel-and-Graviton integrated source-disabled campaign
4. one optional integrated confirmation when the result is inconclusive
5. one target-host perfasm capture

Intel and Graviton runs should execute in parallel. A hard correctness,
lifecycle, memory, generated-code, or performance gate failure ends the goal and
returns the evidence. It does not authorize another representation, another
cutoff family, reverse-loop work, or a broader optimization campaign.

## Completion

The accepted outcome contains the bounded forward sidecar, object-row fallback,
complete verification in both native-access modes, target-host evidence, and
updated durable documentation.

The rejected outcome leaves production on the object-row DFA, removes temporary
integration code, and records the failed gate and exact evidence. Either outcome
completes this goal. Formal release qualification remains separate.

## Outcome

The single permitted cutoff campaign, session `20260716T194347Z-47380`, found
that the representation materially beat object rows but failed the original
isolated Graviton/native gate. A separate assembly investigation attributed the
gap to C2 emitting an extra dependent address operation. The project then
explicitly accepted that architecture limitation and proceeded with the one
permitted integrated campaign rather than opening another layout exploration.

At footprints through 44,776 bytes, the absolute-pointer loop materially beat
object rows on both targets:

| Architecture | Pointer/object range | Pointer/native range |
|---|---:|---:|
| Intel | 0.716-0.776x | 1.007-1.019x |
| Graviton | 0.716-0.724x | 1.231-1.264x |

Intel passed both isolated gates. Graviton passed the `0.80x` object-row gate
but took about 1.25x the minimal direct C++ pointer loop because of the known
C2/AArch64 instruction-selection limitation. No cutoff confirmation was used.

The Intel object-row crossover selected a conservative 48 KiB total allocation
cap. Informational corpus analysis found that this cap covers 66 of 67 observed
compact routes.

Both direct C++ controls used the same deterministic transition topology and
input as Java. They were compiled with `-O3 -DNDEBUG` plus `-march=native` on
Intel or `-mcpu=native` on Graviton. Both AWS instances completed successfully
and were terminated.

Integrated session `20260716T233209Z-7064` measured the production
state-changing route at 0.709x object time and 0.996x native time on Intel, and
0.728x object time and 1.006x native time on Graviton. Paired protected routes
remained within the 2% gate. The sidecar is therefore accepted for eligible
forward one-byte DFA searches. Detailed cutoff and integrated evidence is in
[`history/2026-07-16-dfa-absolute-pointer-cutoff.md`](history/2026-07-16-dfa-absolute-pointer-cutoff.md)
and
[`history/2026-07-16-dfa-absolute-pointer-integrated.md`](history/2026-07-16-dfa-absolute-pointer-integrated.md).
