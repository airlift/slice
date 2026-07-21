# Vector Search Exploration

**Status:** Proposed engineering exploration. This document defines an ordered
investigation, not an implemented design or a performance claim.

## Question

This exploration asks:

> Where can the Java Vector API let the engine skip input or reject candidate
> match positions faster than the scalar RE2 execution paths on modern Intel
> and AWS Graviton?

The ordinary DFA transition loop is not the primary target. Its recurrence is
serial:

```text
state[position + 1] = transition(state[position], input[position])
```

The next transition cannot normally be computed until the previous transition
has produced its state. Vector search is useful where multiple input positions
can be tested independently, such as finding a required byte, finding candidate
literal positions, or skipping bytes that are known to self-loop in the current
DFA state.

This work is complementary to
[`DFA_LAYOUT_EXPLORATION.md`](DFA_LAYOUT_EXPLORATION.md). A fast general DFA is
still required for expressions that have no selective literal, candidate-byte
set, or stable self-loop.

## Model

Most useful SIMD regex optimizations follow the same two-stage model:

1. Inspect a vector of input bytes and cheaply identify positions that might be
   relevant.
2. Run a scalar verification or regex engine only at those candidate positions.

The optimization wins when vector scanning skips enough input to amortize
setup, candidate extraction, and verification. It loses when candidates are
dense or frequently false, because execution repeatedly crosses between the
scanner and the regex engine.

Every proposed scanner must therefore be evaluated with no-match, sparse-match,
dense-match, and dense-false-positive inputs. Peak scan bandwidth alone is not
an acceptance criterion.

## Relevant Rust Techniques

Rust's `regex` and `regex-automata` libraries use SIMD principally through
literal prefilters and state acceleration. They do not generally vectorize the
ordinary DFA recurrence.

### Single-Byte And Small-Set Search

The Rust prefilter selector first considers `memchr`, `memchr2`, and `memchr3`
when one, two, or three bytes can identify candidate positions. A vector load is
compared with one or more broadcast bytes, the masks are combined, and the
first matching lane identifies the next candidate.

This applies to literal prefixes, small start-byte sets, fixed-distance byte
sets, and DFA states with only a few non-self-loop bytes.

References:

- [Rust prefilter selection](https://raw.githubusercontent.com/rust-lang/regex/master/regex-automata/src/util/prefilter/mod.rs)
- [Rust `memchr` prefilters](https://raw.githubusercontent.com/rust-lang/regex/master/regex-automata/src/util/prefilter/memchr.rs)

### Single-Literal Search

For one required substring, Rust delegates to `memchr::memmem`. Its x86 generic
SIMD path selects two bytes at different offsets in the needle, favoring bytes
that are expected to be rare. It compares both offsets over a vector of
candidate starts and intersects the masks. A complete literal comparison is
performed only for surviving candidates.

This differs from searching only for the first byte and then checking the rest
of the literal. Two aligned vector comparisons can reject common first-byte
false positives before returning to scalar code.

Reference:

- [Rust literal search design](https://burntsushi.net/regex-internals/#searching-for-literals)

### Teddy Multi-Literal Search

For a small set of literals, Rust uses the packed Aho-Corasick searcher, whose
principal SIMD algorithm is Teddy. Teddy scans 16-byte or 32-byte input blocks
and uses precomputed fingerprints to find candidate literals.

For each fingerprint byte, Teddy splits the input into low and high four-bit
nibbles. Two 16-entry vector tables map those nibbles to literal buckets. A
vector shuffle performs all table lookups in parallel, and intersecting the two
results identifies possible fingerprint matches. Two- and three-byte
fingerprints align and intersect results from adjacent positions before scalar
verification.

Teddy is intended for a relatively small number of literals. It is a poor fit
when fingerprints are common, verification is frequent, the input is short, or
the literal set is too large.

References:

- [Teddy algorithm description](https://raw.githubusercontent.com/BurntSushi/aho-corasick/master/src/packed/teddy/README.md)
- [Aho-Corasick packed search](https://docs.rs/aho-corasick/latest/aho_corasick/packed/index.html)
- [Rust Teddy prefilter](https://raw.githubusercontent.com/rust-lang/regex/master/regex-automata/src/util/prefilter/teddy.rs)

### DFA State Acceleration

A DFA state may self-loop for most input bytes and leave the state for only a
small set. Instead of executing one DFA transition for every byte, Rust's full
DFA can scan directly for a byte that leaves the state and resume DFA execution
there. For example, a line-oriented `.*` state may be able to skip directly to
the next newline.

This does not vectorize dependent DFA transitions. It proves that the skipped
transitions all have the same result and avoids executing them.

Reference:

- [Rust DFA state acceleration](https://burntsushi.net/regex-internals/#engine-dfa)

### Finite-Language Bypass

When the complete language is a small finite set of literals, the literal
searcher can replace the regex engine rather than merely prefilter it. This is
particularly useful for alternations such as `foo|bar|baz`, provided match
ordering, captures, empty alternatives, and case folding remain correct.

## Java Vector API Fit

JDK 25's incubating Vector API exposes byte vectors, comparisons, masks, lane
selection, and rearrangement. It is designed to lower to AVX instructions on
x64 and NEON instructions on AArch64.

Reference:

- [JDK 25 Vector API](https://docs.oracle.com/en/java/javase/25/docs/api/jdk.incubator.vector/module-summary.html)
- [`ByteVector` operations](https://docs.oracle.com/en/java/javase/25/docs/api/jdk.incubator.vector/jdk/incubator/vector/ByteVector.html)

Expected feasibility:

| Technique | Vector API fit | Main concern |
|---|---|---|
| One-byte search | Direct | Setup cost on short scans |
| Two- or three-byte set search | Direct | Candidate density |
| Two-offset literal filter | Direct | Full verification frequency |
| Small self-loop exit set | Direct | Scanner/DFA handoff frequency |
| Arbitrary byte-set search | Feasible with nibble tables | Shuffle code quality |
| Teddy | Feasible but substantial | ISA-specific lowering and verification cost |
| Ordinary DFA transition loop | Not directly vectorizable | Loop-carried state dependency |

Teddy's nibble lookup can be expressed with `ByteVector.rearrange`, but semantic
expressibility is not sufficient. Target-host assembly must show an efficient
shuffle or table-lookup instruction. In particular, 256-bit x86 shuffles are
lane-local while Java rearrangement is specified over a complete vector. Both
explicit 128-bit species and the preferred species must be measured rather
than assuming that the wider species is faster.

## Current Starting Point

The port already contains several relevant components:

- `Prog.indexOfVectorAPI` implements a Vector API one-byte scan.
- `Prog.prefixAccel` searches for required prefixes and uses front/back checks
  for multi-byte case-sensitive literals.
- `Prog.buildStartByteCandidates` derives conservative candidate-byte sets.
- `Prog.buildFixedDistanceByteCandidates` looks for a more selective set at a
  fixed distance from the match start.
- `Dfa.searchForwardSampledSelfLoops` recognizes observed self-loop-heavy
  traversal and dispatches to a separate scanner.
- `Prefilter` extracts literal information for the filtered RE2 API.

These are starting points, not evidence that the vector opportunities are
already covered. Before adding a new algorithm, measure the current scalar,
SWAR, Vector API, and intrinsic-backed Slice paths for the exact operation.

## Ordered Exploration

Each phase should stop if isolated results do not justify integration. Do not
carry a losing primitive into the engine in the hope that integration will make
it faster.

### 1. Establish Scanner Controls

Build one isolated benchmark and scalar reference for candidate-byte scanning.
It should support identical input and result semantics for:

- one candidate byte
- two candidate bytes
- three candidate bytes
- forward and reverse search where required by the engine
- unaligned offsets and every tail length
- empty, tiny, cache-resident, and streaming inputs

Compare byte-at-a-time, SWAR, explicit 128-bit vectors, explicit 256-bit vectors
where available, and preferred-width vectors. Record generated assembly on
Intel and Graviton.

This phase validates the harness and determines the setup and break-even costs
that later selectors must use.

### 2. Extend One-Byte Search To Two And Three Bytes

Implement isolated `memchr2` and `memchr3` equivalents. Reuse one vector load,
OR the comparison masks, and return the first set lane.

Exercise these first in existing start-byte and fixed-distance benchmark
shapes. This is the lowest-complexity extension and directly supports later
self-loop acceleration.

Acceptance requires:

- higher throughput than the best scalar/SWAR control on both target hosts
- a measured input length at which vector dispatch becomes beneficial
- no material loss when candidates are dense
- exact first-position semantics for every byte value and tail length

### 3. Vectorize Two-Offset Literal Candidate Search

Add an isolated single-literal scanner modeled on Rust's generic SIMD search:

1. Select two needle bytes and their distance.
2. Load vectors at both corresponding haystack offsets.
3. Intersect equality masks.
4. Verify the complete literal only at surviving positions.

Compare these selection policies:

- first and last byte, matching the current conceptual strategy
- two bytes chosen by a static byte-frequency table
- rarest pair subject to a minimum useful separation

Benchmark literals of different lengths and alphabets against absent, sparse,
dense, and adversarial false-positive inputs. Include current `prefixAccel` as
the integrated control.

This is expected to have higher value than Teddy because one required literal
is common, implementation complexity is modest, and the result can improve
prefix, suffix, and inner-literal strategies.

### 4. Accelerate States With One To Three Exit Bytes

Use the established small-set scanner for DFA states where all other bytes are
exact non-reporting self-loops. Start only with states having one, two, or three
exit bytes so the scanner does not require a general set-membership algorithm.

Compare two selection policies:

- static state metadata produced when a transition row becomes complete
- the existing runtime sampling policy, followed by inspection of the current
  row's exit set

The scanner must preserve abnormal transition, match-boundary, end-of-text,
anchor, and direction semantics. It must also avoid repeated short
scanner/DFA handoffs.

Acceptance is based on actual engine paths, not only a synthetic byte scanner.
Protected state-changing, paired, partial-paired, fixed-distance, and capture
workloads must remain within the existing regression gate.

### 5. Explore General Byte-Set Scanning

If route census shows important candidate or self-loop exit sets larger than
three bytes, prototype a vector membership operation. Candidate designs are:

- repeated equality comparisons for very small sets
- range comparisons for one or two contiguous ranges
- nibble lookup tables using `ByteVector.rearrange`
- a hybrid selected by set shape

Do not begin with a universal 256-byte membership implementation. First record
the distributions of candidate counts and range shapes produced by real Trino,
Rebar, and RE2 workloads.

This phase is accepted only if the generated code is efficient on both Intel
and Graviton and the selector can avoid dense-set ping-pong.

### 6. Add Complete Finite-Language Bypass

Use the best validated scanner to bypass the regex engines for complete small
literal languages. Begin with simple, capture-free, case-sensitive byte
alternations whose literals cannot match empty.

Correctness gates must cover:

- leftmost-first alternation order
- prefix-related alternatives such as `sam|samwise` and `samwise|sam`
- overlapping matches
- repeated operations including count, split, extract, and replace
- Latin1 and UTF-8 byte semantics
- group zero and statically derivable captures, if captures are admitted later

Use a simple verification design before introducing Teddy. The goal of this
phase is to validate engine bypass and API semantics independently from a
complex SIMD fingerprint algorithm.

### 7. Prototype Teddy

Attempt Teddy only after vector shuffle code quality and finite-language
semantics are established.

Start with:

- two to eight literals
- minimum literal length of two or three bytes
- one-, two-, and three-byte fingerprints
- 128-bit vectors on both architectures
- scalar verification grouped by fingerprint bucket

Then compare preferred-width vectors and larger literal sets. Measure build
cost and retained memory as well as search throughput. The selector should
reject short inputs, weak fingerprints, common one-byte prefixes, and literal
sets that produce excessive verification.

Teddy should first serve the complete finite-language route. Using it as a
prefilter for a general regex adds another handoff and should be considered only
after the direct route is successful.

### 8. Generalize Literal Placement

After prefix scanning is proven, evaluate required suffixes and inner literals.
The scanner may find a literal quickly, but recovering the leftmost match start
can require reverse execution and can become quadratic if candidates are
verified independently.

Any inner- or suffix-literal design must process candidate intervals
monotonically, retain RE2's linear-time guarantee, and include dense false
candidate tests. Existing rejected required-suffix evidence in
[`ENGINEERING_RESULTS.md`](ENGINEERING_RESULTS.md) is a required control.

### 9. Integrate Selector Policy

Only after individual primitives win should the engine choose among them.
Selection inputs may include:

- input length
- candidate count and range shape
- literal count and minimum length
- estimated fingerprint rarity
- anchored versus unanchored execution
- forward versus reverse execution
- capture requirements
- observed scanner productivity

Prefer compile-time or cache-build-time decisions. Runtime sampling is
appropriate only when usefulness depends on the haystack and the sampling cost
is bounded and measured.

## Correctness Requirements

Follow the optimization test-first rule in `AGENTS.md` and
[`../audits/PROCEDURES.md`](../audits/PROCEDURES.md):

1. Add a deterministic test that fails before the optimized route exists.
2. Verify route selection directly rather than using timing as a test.
3. Compare every scanner with a simple scalar reference over generated inputs.
4. Cover all offsets, lengths, tail sizes, and byte values.
5. Verify full engine results against pinned native RE2 golden data where the
   optimization changes engine dispatch.
6. Run exhaustive agreement and the full RE2 suite before retaining an
   integrated route.

No optimization is complete if it works only in Latin1 or only in UTF-8. A
route may deliberately be ineligible for one mode, but its eligibility and
fallback behavior must be tested.

## Benchmark Matrix

### Inputs

At minimum, measure:

- lengths from zero through the vector tail boundary
- 64 B, 1 KiB, 32 KiB, 1 MiB, and 16 MiB inputs
- aligned and unaligned starting offsets
- no candidates
- one early candidate
- one late candidate
- sparse candidates
- dense true candidates
- dense false candidates
- random binary, ASCII text, and UTF-8 text

### Platforms

Engineering acceptance requires dedicated runs on:

- modern Intel x64
- AWS Graviton AArch64

Apple Silicon is useful only for local directional development. It is not an
acceptance platform for this server library.

### Evidence

For each retained primitive, record:

- isolated nanoseconds per byte and effective bandwidth
- candidate and verification counts
- break-even input length
- generated assembly for the hot loop
- cycles, instructions, branches, and cache behavior where counters are stable
- retained memory and construction cost for precomputed tables
- integrated source-disabled A/B results
- route census over representative corpora

The exact benchmark named by an optimization claim must improve. A scanner
microbenchmark cannot establish an end-to-end regex win.

## Acceptance Policy

A vector route may be retained only when:

- it is correct for every supported mode in which it is eligible
- it materially improves its intended production path on both Intel and
  Graviton
- target-host assembly explains the improvement
- its selector prevents dense-candidate and short-input regressions
- protected routes remain within the existing 2% regression gate
- allocation, retained memory, and compile cost are proportionate to the win
- the complete RE2 tests and Maven install pass

An Intel-only or Graviton-only win is evidence for a platform-specific strategy,
not a universal replacement. Platform dispatch should be considered only when
the benefit is substantial and the maintenance cost is justified.

## Expected Priority

The current expected value order is:

1. two- and three-byte candidate scanning
2. two-offset rare-byte filtering for one literal
3. one- to three-byte DFA self-loop exit scanning
4. shape-specific general byte-set scanning
5. complete small finite-language bypass
6. Teddy for small multi-literal sets
7. suffix and inner-literal meta strategies

The order deliberately starts with reusable, low-complexity primitives. Teddy
is potentially powerful, but it should not be the first vector experiment: its
fingerprint construction, shuffle lowering, verification policy, and
leftmost-first semantics create several variables at once.

## Decision Boundary

The exploration succeeds even if Teddy or general byte-set scanning is
rejected. The useful result may be a small set of focused scanners with clear
eligibility rules.

Do not use SIMD merely because a regex engine can contain vector code. Retain a
route only when it removes work from a measured production path. If an
expression has no safe selective predicate, it belongs on the optimized scalar
DFA, OnePass, BitState, or NFA path.
