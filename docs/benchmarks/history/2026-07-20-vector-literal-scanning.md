# Vector Literal Scanning Campaign

## Status

The candidate passed isolated and exact-corpus qualification on Apple Silicon,
Intel, and AWS Graviton. Final pinned-native confirmation is recorded below.

## Question

The campaign tested where the Java Vector API can remove work from regex
search, rather than trying to vectorize the serial DFA recurrence. It focused
on two operations:

1. finding one, two, or three candidate bytes
2. finding a literal by comparing two fixed offsets before full verification

Apple Silicon is an acceptance platform for this work in addition to Intel and
Graviton. Measurements used an Apple M3 Max, an Intel `c8i.8xlarge`, and a
Graviton4 `c8g.4xlarge`. All results use JDK 25.

## Accepted Design

Case-sensitive multi-byte prefixes whose first byte is non-ASCII use a fused
front/back scanner. This shape targets UTF-8 literals, where the first leading
byte can occur at a very high rate even when the complete literal is rare.

- searches shorter than 1 KiB use fused SWAR
- searches of at least 1 KiB use `ByteVector.SPECIES_PREFERRED` when the
  application resolves `jdk.incubator.vector`, otherwise they use fused SWAR
- ASCII prefixes keep the existing repeated first-byte search
- case-insensitive prefixes keep the existing fused SWAR scanner
- every candidate is still checked against the complete prefix

The dispatch is based only on compiled-prefix shape, search length, and whether
the Vector module is available. It does not sample input, dispatch by CPU
model, or adapt while searching.

Vector linkage is isolated in `VectorPrefixScanner`. `Prog` has no Vector API
constant-pool references, so applications that do not resolve the incubating
module load and run normally on the SWAR route. The helper remains a direct
static call when the module is available; there is no reflection or strategy
interface on the scan path.

## Scanner Controls

The isolated candidate-byte benchmark compared scalar, SWAR, explicit 128-bit,
256-bit, and 512-bit vectors. The following table shows long absent scans; lower
is better.

| Platform | Candidates | SWAR | Best shippable vector | Vector/SWAR |
|---|---:|---:|---:|---:|
| Apple M-series | 1 | 1,296 ns | 426 ns, 128-bit | 0.329x |
| Apple M-series | 2 | 1,739 ns | 728 ns, 128-bit | 0.419x |
| Apple M-series | 3 | 2,595 ns | 991 ns, 128-bit | 0.382x |
| Intel c8i | 1 | 1,654 ns | 208 ns, 512-bit | 0.125x |
| Intel c8i | 2 | 3,349 ns | 275 ns, 512-bit | 0.082x |
| Intel c8i | 3 | 4,951 ns | 402 ns, 512-bit | 0.081x |
| Graviton c8g | 1 | 2,303 ns | 1,002 ns, 128-bit | 0.435x |
| Graviton c8g | 2 | 4,030 ns | 2,211 ns, 128-bit | 0.549x |
| Graviton c8g | 3 | 5,817 ns | 2,937 ns, 128-bit | 0.505x |

Explicit 256-bit and 512-bit species are emulated on AArch64 and are not usable.
For example, the 32 KiB one-byte scan takes 43 microseconds with explicit
512-bit vectors on Graviton, versus 1.0 microsecond with 128-bit vectors.

Candidate position matters. Three-candidate immediate-hit scans on Graviton
take 2.53 ns with vectors and 2.32 ns with SWAR, a 9% vector loss. This is why
the general one-to-three-byte primitive is retained as benchmark evidence but
is not wired into the DFA byte-set scanner in this change.

## Literal Controls

The old prefix scanner repeatedly searched for the first byte, returned to
scalar code, checked the last byte, and resumed. That is pathological for
UTF-8 text whose leading byte is common. The fused scanner loads the front and
back positions together, intersects their masks, and leaves the loop only for
surviving candidate starts.

Synthetic dense-first-byte controls showed the scale of this effect:

| Platform | Literal | Repeated SWAR | Fused vector | Improvement |
|---|---|---:|---:|---:|
| Apple M-series | Russian | 65,970 ns | 2,026 ns | 32.6x |
| Apple M-series | Chinese | 35,875 ns | 1,962 ns | 18.3x |
| Intel c8i | Russian | 150,304 ns | 405 ns | 371x |
| Intel c8i | Chinese | 75,836 ns | 400 ns | 190x |
| Graviton c8g | Russian | 194,005 ns | 3,110 ns | 62.4x |
| Graviton c8g | Chinese | 96,543 ns | 3,103 ns | 31.1x |

The controls also explain where fusion loses. If the first byte is genuinely
absent, the repeated search reads one byte stream while the fused search reads
two. ASCII prefixes therefore remain on the existing route.

On Apple Silicon, fused-vector and fused-SWAR literal scanning are equal at
about 1 KiB. Vector scanning becomes consistently faster above that point and
is 11% to 22% faster by 16 to 32 KiB. Intel and Graviton cross over earlier,
so 1 KiB is the conservative common cutoff.

## Exact Corpus Results

The exact Rebar Sherlock count rows were run candidate-before, source-disabled
control, and candidate-after. Lower is better.

| Platform | Workload | Control | Candidate bracket | Candidate/Control |
|---|---|---:|---:|---:|
| Apple M-series | Russian | 2.796 ms | 0.222 to 0.236 ms | 0.082x |
| Apple M-series | Chinese | 0.405 ms | 0.072 to 0.075 ms | 0.181x |
| Intel c8i | Russian | 2.710 ms | 0.171 to 0.181 ms | 0.065x |
| Intel c8i | Chinese | 0.696 ms | 0.0227 to 0.0230 ms | 0.033x |
| Graviton c8g | Russian | 4.300 ms | 0.353 to 0.363 ms | 0.083x |
| Graviton c8g | Chinese | 0.734 ms | 0.132 to 0.138 ms | 0.184x |

English case-sensitive and case-insensitive literals, literal alternations,
and case-insensitive Unicode controls remain within their before/after brackets.

## Final Native Comparison

The accepted source was then compared in the same session with the pinned,
host-tuned native RE2 build. The native before/after controls drifted by less
than 0.5% on every row. Lower Slice/native ratios are better.

| Platform | Workload | Slice | Native | Slice/Native |
|---|---|---:|---:|---:|
| Intel c8i | Russian | 177.6 us | 300.0 us | 0.592x |
| Intel c8i | Chinese | 23.0 us | 31.6 us | 0.726x |
| Graviton c8g | Russian | 332.8 us | 4.640 ms | 0.072x |
| Graviton c8g | Chinese | 135.7 us | 772.9 us | 0.176x |

The final route is therefore faster than host-tuned native RE2 for both
targeted workloads on both server architectures. The same narrowed campaign
also retained the existing English and case-insensitive rows, which do not use
the new route. The complete normalized rows are in
[`2026-07-20-vector-literal-native-comparison.csv`](2026-07-20-vector-literal-native-comparison.csv).

## Rejected Alternatives

### Short SWAR Probe Before Vector Search

A hybrid that scanned the first 8 or 16 bytes with SWAR before entering the
vector loop added setup cost without protecting a meaningful early-hit case.
Direct vectors were already tied or faster for early hits on Apple and Intel.

### Wider AArch64 Species

Explicit 256-bit and 512-bit vectors are emulated on Apple and Graviton and are
orders of magnitude slower. Production uses only the preferred species.

### Corpus-Specific Rare Offsets

Comparing two rare internal literal bytes can reduce false positives, but the
fused front/back loop already removes the expensive scalar handoff. The
synthetic results did not justify a byte-frequency table or language-specific
selection policy in production.

### Folded-Prefix Vector Route

The exact case-insensitive English corpus was 742 and 787 microseconds in the
SWAR brackets and 737 microseconds with vectors on Apple. This is not a clean
win, so the folded route remains SWAR.

### General Small-Set Integration

One- and two-byte candidate scans are promising on all three platforms, but
the current DFA consumer accepts arbitrary sets and has its own productivity
policy. Three-byte immediate hits also regress on Graviton. Integrating this
primitive requires a separate consumer-level campaign and is not part of the
literal fix.

## Correctness And Verification

- randomized scanner agreement covers every species, offset, tail, candidate
  count, and literal offset pair used by the controls
- randomized production-prefix agreement covers raw bytes, non-zero slices,
  no-match input, inserted matches, and both sides of the cutoff
- the focused RE2 selector passes 936 tests with zero failures or errors and
  one intentional skip
- the full Maven install passes 2,728 tests with zero failures or errors and
  one intentional skip
- exact Rebar verification reports the expected count for every measured row
- child-JVM coverage verifies startup and matching both with and without the
  optional Vector module resolved

## Evidence

- scanner campaign: `20260721T035201Z-65768`
- literal candidate/control campaign: `20260721T041649Z-43917`
- final pinned-native campaign: `20260721T043752Z-69851`
- Apple raw CSV files: `target/vector-exploration/`
