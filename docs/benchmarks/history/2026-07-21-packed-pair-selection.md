# Ranked Literal Byte Selection

## Status

The bounded productionization campaign stopped at its Intel integration gate.
The ranked selector is not accepted. The existing fused front/back scanner
from
[`2026-07-20-vector-literal-scanning.md`](2026-07-20-vector-literal-scanning.md)
remains the stable baseline.

## Candidate Design

The compiler selects two literal byte offsets once and stores them as integers
on `Prog`. The SWAR and Vector hot loops read those concrete fields directly.
There is no input sampling, dynamic adaptation, rank-table lookup, interface
dispatch, or allocation while scanning.

The selector:

1. chooses the lowest-ranked literal byte, using the earliest offset for ties
2. chooses the lowest-ranked different byte value, again using the earliest
   offset for ties
3. chooses the farthest offset when the literal contains only one byte value

Every surviving position is still verified against the complete prefix. The
selector changes only the amount of rejection work, not matching semantics.

The candidate limits ranked selection to UTF-8 prefixes whose first byte is
non-ASCII. ASCII, Latin-1, case-folded, and single-byte prefixes retain their
existing selection behavior. Searches shorter than 1 KiB use fused SWAR;
longer searches use the Vector helper when the application resolves
`jdk.incubator.vector`, otherwise they use the same SWAR route.

## Rank Table

The 256-entry table was generated from pinned Rebar commit
`463d00f31887e84c38467805b9e3122c314b9521`. Nine text corpora are weighted
equally so a large file or language does not dominate the ordering:

- English, Russian, and Chinese OpenSubtitles
- Sherlock Holmes prose
- Rust source
- Python source
- Veryl source
- unstructured logs
- Unicode character data

The sampled OpenSubtitles files used for evaluation are not training inputs.
UTF-8 lead bytes `0xC0` through `0xFF` rank last because they are frequent and
poorly selective in homogeneous non-ASCII text. The generated table checksum
is `8f6bd2b1e4a148ce214f83a6c280ce2fed46620651d59ba51ecbe7263f8c5d52`.

The campaign's temporary corpus analyzer regenerated the table checksum and
candidate census. The rejected analyzer and selector implementation are not
retained in the production tree. A future table experiment requires all of the
following:

1. retain a pinned, reviewable corpus identity
2. exclude evaluation files from training
3. update the asserted checksum
4. inspect candidate counts against front/back, memchr, and the source oracle
5. rerun the integrated Intel and Graviton campaign

## Candidate Census

The exact evaluation files show why internal bytes matter for UTF-8 literals:

| Language | Front/back false candidates | Selected false candidates | Oracle false candidates |
|---|---:|---:|---:|
| English | 98 | 4 | 4 |
| Russian | 9,661 | 25 | 1 |
| Chinese | 721 | 20 | 0 |

The selected Russian pair is offsets 1 and 14. The selected Chinese pair is
offsets 19 and 1. English is included as a protected neutral case; production
continues to route its ASCII prefix through repeated-byte acceleration.

## Isolated Selector Qualification

Lower time is better. These three-fork measurements compare the old
front/back pair with the generated selector on the exact evaluation corpora.

| Platform | Scanner | English | Russian | Chinese |
|---|---|---:|---:|---:|
| Intel c8i | preferred Vector, front/back | 25.978 us | 131.457 us | 22.044 us |
| Intel c8i | preferred Vector, selected | 25.070 us | 45.393 us | 17.766 us |
| Intel c8i | SWAR, front/back | 122.206 us | 325.065 us | 113.986 us |
| Intel c8i | SWAR, selected | 122.328 us | 211.611 us | 108.085 us |
| Graviton4 c8g | preferred Vector, front/back | 97.664 us | 297.507 us | 90.431 us |
| Graviton4 c8g | preferred Vector, selected | 96.786 us | 169.123 us | 81.777 us |
| Graviton4 c8g | SWAR, front/back | 160.265 us | 362.792 us | 145.451 us |
| Graviton4 c8g | SWAR, selected | 158.284 us | 278.223 us | 140.650 us |

The selector is effectively neutral for English, reduces Russian scanning by
23% to 66%, and reduces Chinese scanning by 3% to 20%, depending on platform
and scanner. On Intel, the preferred 512-bit species remains faster than the
forced AVX2 control.

## Optional Vector Module

`Prog` has no constant-pool reference to `jdk.incubator.vector`. Vector linkage
is isolated in `VectorPrefixScanner`, and `VectorSupport` checks module presence
once. A child-JVM test proves both deployment modes:

- without `--add-modules jdk.incubator.vector`, `Prog` loads normally and uses
  SWAR without loading the helper or any Vector class
- with the module resolved, the same pattern uses the Vector helper

The helper is a direct static call rather than reflection or a strategy
interface. Splitting its scalar tail into a separate method lets C2 compile the
Vector loop as a small hot method; same-build helper and inline controls are
within the 2% acceptance gate.

## Integrated Qualification

The first integration benchmark used two independently compiled `Prog`
instances and was discarded because graph allocation and warm-state layout
were not controlled. Campaign `20260721T072411Z-53166` corrected the benchmark
so every fork constructs one program and varies only offset selection. It
confirmed the same Intel Chinese regression.

Keeping sparse full-prefix verification physically inside the Vector method
removed the helper-isolation loss. Campaign `20260721T073808Z-68868` confirmed
that `vectorHelper` and the source-inline control are within 1% on both
architectures. The integrated result still fails because Intel compiles the
Chinese DFA into two stable performance shapes.

Lower time is better:

| Platform | Language | Front/back | Selected | Selected/control |
|---|---|---:|---:|---:|
| Intel c8i | English | 103.823 us | 101.689 us | 0.979x |
| Intel c8i | Russian | 182.563 us | 92.615 us | 0.507x |
| Intel c8i | Chinese | 24.687 us | 28.444 us | 1.152x |
| Graviton4 c8g | English | 123.781 us | 128.065 us | 1.035x |
| Graviton4 c8g | Russian | 367.676 us | 235.859 us | 0.641x |
| Graviton4 c8g | Chinese | 89.088 us | 85.337 us | 0.958x |

The Intel Chinese selected forks are `32.048`, `32.598`, and `20.688`
microseconds. The first two are about 30% slower than control; the third is
about 16% faster. This is not ordinary benchmark noise. It shows that C2 can
produce both a good and a bad integrated shape from the same source and
workload. The campaign therefore stops without a final perfasm acceptance
capture or production commit.

The next investigation should isolate the good and bad compilations of
`Dfa.searchForwardScanAcceleration`, `Prog.prefixAccel`, and
`VectorPrefixScanner.find`. Candidate-only prefiltering may also avoid sparse
full-prefix verification, but it is a new algorithmic experiment and is not
part of this bounded goal.

## Campaign Correctness

The temporary campaign tests covered:

- exact 1,023, 1,024, and 1,025-byte dispatch boundaries
- vector lane and scalar-tail boundaries
- dense false candidates and complete-prefix verification
- repeated byte values and prefixes longer than the search window
- ASCII, case-folded, single-byte, and Latin-1 routes
- malformed UTF-8 haystacks and non-zero backing offsets
- DFA first/longest, DFA counting, NFA, BitState, captures, and matcher regions
- repeated matching through shared compiled programs on platform and virtual
  threads
- JVM startup with and without the optional Vector module

## Evidence

- selector campaign: `20260721T063737Z-19981`
- discarded two-program integration campaign: `20260721T070559Z-33714`
- corrected integration campaign: `20260721T072411Z-53166`
- optional confirmation: `20260721T073808Z-68868`
- corpus analysis: `target/packed-pair-corpus-analysis.csv`
