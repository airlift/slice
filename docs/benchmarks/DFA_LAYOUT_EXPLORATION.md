# Compact DFA Layout Exploration

**Status:** Complete. The exploration retains a narrow on-heap adaptive scanner
and a bounded absolute-pointer FFM sidecar. Its relative-offset, buffer,
universal-scanner, and primitive-heap alternatives are rejected. These results
are engineering evidence, not formal release qualification.

## Question

The campaign asked one narrow question:

> Can Java close the compact one-byte DFA gap to native RE2 on modern Intel and
> AWS Graviton without using an unshippable API or regressing ordinary regexes?

The existing engine cascade was not in question. Paired transitions,
fixed-distance scanning, prefix and start-byte acceleration, OnePass, BitState,
and NFA fallback continue to own the shapes they already handle well.

## Answer

No representation in the original campaign was a universal improvement. Its
bounded FFM and buffer access did not reproduce native pointer throughput, and
the best primitive heap layouts changed direction across Intel and Graviton.
That conclusion initially did not cover the later supported FFM
everything-segment idiom, which dereferences 64-bit absolute row addresses
instead of bounded relative offsets. The bounded follow-up rejected that
candidate because it remained 1.231-1.264x direct C++ time across useful
Graviton footprints.

The useful result was a code-shape specialization, not a new table layout. The
current `Object[]` transition rows are retained. For a long end-constrained
search with no paired-transition route, the engine samples 64 normal
transitions. If at least 90% are exact self-loops, it dispatches once to an
isolated self-loop-first scanner. Otherwise it continues in the existing
compact loop.

The retained policy is deliberately narrow:

- input remaining at dispatch must be at least 64 KiB
- the search must be end-constrained
- the search must not have entered paired execution
- the sample must complete 64 normal transitions
- at least 58 of those 64 transitions must be exact self-loops
- rejection adds no condition to the established compact loop
- selection is local to one search and allocates no memory

This leaves two production code shapes rather than replacing the DFA:

1. the established compact `Object[]` scanner for the general case
2. the self-loop-first scanner for measured stable self-loop traversals

## Final Integrated Result

The decisive production-path workload is an expanded version of RE2's `HARD`
shape. It uses varied printable input, the normal 29 byte classes, one hot
self-loop, and a 520-byte required suffix. Warming the matching suffix
materializes more than 500 DFA states. Complete paired expansion exceeds the
64 KiB cap, partial pairing is rejected, and fixed-distance scanning is
ineligible. The benchmark therefore exercises the actual unpaired compact-DFA
route rather than a synthetic table walker.

Session `20260716T051735Z-81238` compared candidate-before, an exact
source-disabled control, candidate-after, and direct host-tuned native RE2 on
the same hosts:

| Architecture | Java candidate | Java control | Candidate/control | Native RE2 | Candidate/native |
|---|---:|---:|---:|---:|---:|
| Intel | 13.222 ms | 30.643 ms | 0.432x | 22.056 ms | 0.599x |
| Graviton | 15.550 ms | 42.544 ms | 0.366x | 30.188 ms | 0.515x |

The native binary was built from pinned RE2 with `-O3 -DNDEBUG` and
`-march=native` on Intel or `-mcpu=native` on Graviton. Java and C++ use the
same xorshift-generated 16 MiB printable corpus and equivalent expressions.
Candidate-before and candidate-after agree closely on both architectures.

This does not mean Java universally beats native RE2. It means the retained
specialization beats native for the exact stable-self-loop shape it accepts.
The compact fallback remains slower than native for state-changing graphs that
cannot use paired transitions or another specialization.

## Why The Scanner Works

The ordinary Java loop loads the next `Object` reference, tests for `null`,
decodes the compressed reference, verifies the array type, and carries the
result into the next transition. On an exact self-loop, the next row is already
the current row. Testing identity first lets the common path avoid compressed
reference decoding and the array checkcast entirely.

Conceptually:

```java
Object next = state[byteClass];
if (next == state) {
    continue;
}
if (next == null) {
    break;
}
state = (Object[]) next;
```

Target-host perfasm showed that this shortens the serial dependency chain for
self-loop-heavy traversals. The benefit is not instruction count in isolation;
it comes from issuing the next dependent transition load sooner.

The identity branch is harmful on Intel when states change frequently. A
universal replacement made zero-self-loop `ALTERNATING` and `MULTIBYTE`
topologies about 1.129x slower. Keeping the old loop physically separate is
therefore part of the design, not cosmetic organization.

## Representation Results

### Current Object Rows

The current recursive `Object[]` rows remain the general representation. They
avoid row-index multiplication and allow a loaded reference to become the next
row directly. Compressed-oop decoding and the array checkcast remain costs on
state-changing traversals. The bounded absolute-pointer sidecar replaces these
loads for eligible forward compact searches; object rows remain the complete
fallback.

### Primitive Heap Tables

The campaign compared exact-stride `int[]`, power-of-two state IDs,
pre-shifted row indexes, and entry-width variants over synthetic grids and
warmed real DFA graphs.

- Exact-stride indexing was consistently slower than object rows.
- Power-of-two state IDs did not remove enough dependency-chain work.
- Pre-shifted indexes helped selected Intel points but did not provide a stable
  Graviton win.
- Wider entries increased footprint without providing native pointer
  semantics.

These exact loops are rejected as replacements. Primitive layouts remain a
possible future direction only if a materially different code shape is
proposed and tested against this evidence.

### Supported Off-Heap Access

Bounded FFM, `DirectByteBuffer`, and view-buffer access were measured with
32-bit and 64-bit relative row offsets. Results were neutral, mixed, or slower
depending on architecture and payload. None approached the raw-pointer
diagnostic on both targets.

Those relative and bounded layouts remain rejected. That result is not evidence
against the later supported FFM absolute-pointer candidate because the hot loop
uses a materially different address calculation and generated-code shape.

### 64-Bit Absolute-Pointer FFM Sidecar

The later diagnostic stores absolute native addresses of target transition rows
and dereferences them through a `static final` everything segment. At small and
medium table footprints, it removes enough relative-address and JVM-reference
work to take 0.716-0.776x object-row time on Intel and 0.716-0.724x on
Graviton. Intel also reaches 1.007-1.019x direct C++ time. Against the minimal
direct C++ walker, Graviton remains at 1.231-1.264x because of the known C2
instruction-selection limitation.

Follow-up assembly and a forced-native-address confirmation attributed the
small-table gap to JDK 25 C2 emitting a separate dependent `add` instead of
AArch64's scaled indexed `ldr`. Forcing that code shape in C++ reproduced the
Java result within 1%; removing Java's FFM alignment guard recovered only 0.4%.
This identifies a compiler code-generation limitation rather than a cache or
native-memory limitation.

The project accepted this known isolated-loop cost and qualified the complete
production route. A 48 KiB row-rounded sidecar took 0.709x object and 0.996x
native time on Intel, and 0.728x object and 1.006x native time on Graviton.
Pairing retains selection precedence, native access remains optional, and
object rows handle every unsupported or unavailable transition. See
[`history/2026-07-16-dfa-absolute-pointer-cutoff.md`](history/2026-07-16-dfa-absolute-pointer-cutoff.md)
and
[`history/2026-07-16-dfa-absolute-pointer-integrated.md`](history/2026-07-16-dfa-absolute-pointer-integrated.md).

### Unsafe Raw Pointers

A raw-address `sun.misc.Unsafe` walker was substantially faster than object rows
in the diagnostic matrix. It proved that removing relative address formation
and JVM reference handling can shorten the dependency chain. It did not prove
that an Unsafe production implementation is acceptable.

Unsafe is being removed from modern supported Java usage and is not shippable
for this project. The recorded prototype results remain diagnostic evidence;
the prototype source is not retained. If supported Java cannot meet a future
requirement, native binding is the honest alternative to shipping an Unsafe
memory manager.

## Rejected Scanner Policies

### Universal Self-Loop-First Loop

The universal loop produced large wins on `HARD`, `PARENS`, `SELF_LOOP`, and
`LATE_TRANSITION`, but regressed state-changing Intel topologies by about 13%.
Natural Rebar source A/B checks also found large losses, including Intel
`KEYWORDS` at 1.784x control. It is rejected.

### Four-KiB Sampling Threshold

Sampling only end-constrained searches restored natural Rebar parity, but a
4 KiB threshold made Intel's 32 KiB fixed-distance dense-false-positive route
1.114x control. Repeated short continuations could not amortize the adaptive
path. The threshold is 64 KiB so this class of search remains on the compact
loop.

### Sampling After Paired Execution

Allowing the scanner after a partial paired-table continuation added a stable
3.7% Graviton regression to the protected `HARD` route. Excluding paired
execution with a boolean gate was not sufficient: merely retaining the branch
in the shared continuation method left `HARD` at 1.056x control on Graviton.
The dispatcher now sends paired execution directly to the original compact
continuation and sends only eligible unpaired searches through the sampling
method. Final source-disabled ratios were 1.009x and 1.010x for Intel `HARD`
and `PARENS`, and 0.988x and 0.994x on Graviton.

### Synthetic Sampled-Helper Benchmark

A standalone JMH method that put both selected and rejected tails behind helper
calls produced a misleading 12.9% Intel fallback regression. Even after the
compact tail was moved inline, target C2 compiled the synthetic method into the
same slow shape. The actual production state-changing benchmark remained at
1.000-1.003x control.

This is a benchmark-validity lesson: helper topology must be tested through the
real engine when inlining and profile shape affect generated code. The
synthetic table walkers remain useful for causal diagnostics but are not
production acceptance gates.

## Correctness And Regression Gates

The retained implementation has deterministic tests for:

- the 64 KiB and end-constrained eligibility boundary
- exclusion after paired execution
- an eligible unpaired graph with more than 500 states
- a state-changing compact graph with no self-loops
- paired and partial-paired continuation behavior
- unmatched and end-of-text outcomes
- existing cache, fixed-distance, and paired-transition invariants

Source-disabled Intel and Graviton campaigns bracket candidate results around
the control. The protected matrix covers state-changing compact traversal,
partial paired match and no-match, `HARD`, `MEDIUM`, `PARENS`, and the
fixed-distance sparse, dense-false-positive, and no-match shapes.

The final physically separated implementation measured 0.999x control on the
Intel state-changing compact route and 1.001x on Graviton. The complete Maven
install passed 2,448 tests, including 658 RE2 tests.

## Decision Boundary

Retain this design if all of the following remain true:

- the integrated unpaired self-loop path materially beats compact control on
  both Intel and Graviton
- state-changing compact paths remain within the 2% regression gate
- paired paths remain within the 2% regression gate
- the complete RE2 selector and Maven install pass
- no unsupported JVM API appears in production code

If a future workload falls outside the specialization, do not broaden the
branch-first loop by default. First prove a stable route predicate and a
production-path win. If no supported Java design closes a material remaining
gap, compare a native binding rather than reviving Unsafe.

## Reproducibility

The reusable harness supports:

- synthetic compact-layout grids
- warmed real-layout topology screens
- Intel and Graviton source-disabled A/B campaigns
- target-host perfasm and hardware-counter capture
- the integrated self-loop/native comparison

The final raw artifacts are under
`benchmark-results/re2-engineering/20260716T051735Z-81238`, with exploration
artifacts under `benchmark-results/re2-engineering/dfa-*` and summary CSV files
under `benchmark-results/re2-engineering/dfa-layout-summary/`. These paths are
ignored by Git but retained outside Maven's `target` directory.

Related durable records:

- [`ENGINEERING_RESULTS.md`](ENGINEERING_RESULTS.md) contains the broader active
  performance ledger.
- [`ENGINE_RESEARCH.md`](ENGINE_RESEARCH.md) ranks transferable engine
  techniques.
- [`METHODOLOGY.md`](METHODOLOGY.md) defines benchmark interpretation rules.
- [`QUALIFICATION_PLAN.md`](QUALIFICATION_PLAN.md) defines the later formal
  release campaign.
