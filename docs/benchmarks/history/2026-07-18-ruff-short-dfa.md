# Ruff Short-Search DFA Campaign

**Status:** Closed. Repeated short forward searches promote to the absolute-pointer
sidecar after a bounded warmup. Redundant matcher capture-buffer clearing is also
removed. The exact Ruff `real` operation is at native parity on Intel and
Graviton. Ruff `tweaked` remains a line-window integration outlier rather than a
capture-engine or core-DFA outlier.

Lower elapsed-time ratios are better.

## Diagnosis

Baseline session `20260719T054652Z-ruff-pipeline` decomposed both Ruff capture
grep workloads. Each operation processes 890,906 lines and performs 890,926
forward searches, but only 20 reverse searches and 20 OnePass capture passes.
The input averages 35.5 bytes per line.

| Workload | Architecture | Public | Setup | Forward DFA |
|---|---|---:|---:|---:|
| `real` | Intel | 105.37 ms | 22.86 ms | 82.31 ms |
| `real` | Graviton | 115.33 ms | 20.12 ms | 89.78 ms |
| `tweaked` | Intel | 53.23 ms | 24.95 ms | 38.66 ms |
| `tweaked` | Graviton | 63.97 ms | 20.04 ms | 44.12 ms |

The capture phases are immaterial at this scale. Ruff `real` exposed a short
one-byte DFA deficit: its isolated Java forward stage was slower than native
RE2's complete 73.08/98.48 ms operation on Intel/Graviton. Ruff `tweaked`
already had a Java forward stage faster than native's complete operation; its
remaining cost is line scanning, `Slice` window construction, and matcher reset.

## Matcher Invalidation

`Re2Matcher` previously filled its capture array on reset, again in `matchInto`,
and again after a failed search. Match validity is already represented by
`hasMatch`, and `matchInto` initializes every requested slot before execution.
Reset and failure now invalidate the matcher without rewriting inaccessible
capture slots. A direct diagnostic test proves that reset preserves stale slots
while public group access remains invalid.

Session `20260719T055409Z-ruff-invalidation` reduced Ruff `real` public time by
2.7%/5.2% and Ruff `tweaked` by approximately 0%/11.4% on Intel/Graviton. Setup
was cheaper on all four rows.

## Short Absolute-Pointer Promotion

The accepted route keeps the paired-table threshold at 256 bytes. For shorter
eligible forward searches, a DFA counts repeated uses and requests its existing
absolute-pointer sidecar after 16 searches. Before promotion, and whenever
native access is unavailable, the established start-byte or object-row route
continues normally. Once promoted, warmed short searches use the same concrete
FFM pointer loop as larger one-byte searches.

The first candidate promoted on the first short search. It closed Ruff but
prevented a later long search on the same DFA instance from constructing its
paired table, because pointer and paired representations are intentionally
exclusive. The retained 16-search gate avoids that mixed-length lifecycle
regression. Direct tests cover both short promotion and preservation of later
paired-table construction.

## Final Result

Confirmation session `20260719T061153Z-33390` used source snapshot
`a634fda5a33f9e25a62cfe54235a94dc78b87583796785db026f556c6e0211b5`.
The public-before/public-after drift was 0.13% on Intel and 0.88% on Graviton.

| Architecture | Baseline Java | Final Java | Native RE2 | Final/native |
|---|---:|---:|---:|---:|
| Intel | 105.37 ms | 72.96 ms | 73.08 ms | 0.998x |
| Graviton | 115.33 ms | 96.83 ms | 98.48 ms | 0.983x |

The final isolated forward stages are 61.18 ms on Intel and 79.58 ms on
Graviton. The complete operation remains correct at 84 participating groups.

Ruff `tweaked` does not enter the short-pointer route because its prefix scanner
returns earlier. Its forward stage is already below native's whole-operation
time, so further work belongs to a separately justified line-window API or
adapter campaign. It is not evidence of a remaining capture algorithm deficit.

