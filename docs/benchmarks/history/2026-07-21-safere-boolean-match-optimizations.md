# SafeRE-Inspired Boolean Match Optimizations

**Status:** Accepted from one focused engineering session. This is a regression
check for two small changes, not a complete comparator qualification.

## Changes

Two startup-cost ideas were retained after reviewing SafeRE's UTF-8 boolean
find path:

- complete case-sensitive literals use the existing Slice exact-prefix scanner,
  even when captures wrap the literal;
- nullable patterns return before matcher setup when cached DFA metadata proves
  that the empty match at the initial position wins.

Both apply only to `Re2.partialMatch`. Capture-producing and boundary-producing
APIs retain the ordinary engine paths.

## Method

Session `20260721T201148Z-35792` ran once on dedicated on-demand
`c8i.2xlarge` and `c8g.2xlarge` instances. Both used Temurin 25.0.3+9, an 8 GiB
zero-based compressed-oops heap, one pinned physical core, three forks, five
500 ms warmup iterations, and five 500 ms measurement iterations. The source
content SHA-256 was
`38c409254749b0d2dc8843fc53cb42f4110a120a53f9fb5eeda97c661dd86b2f`.

The first matrix compares `partialMatch` with the unchanged `matchInto` engine
entry. Ratios are optimized time divided by engine time, so lower is better.

| Workload | Source | Intel | Graviton |
|---|---:|---:|---:|
| Sparse capture | 1 KiB | 1.000x | 1.021x |
| Sparse capture | 32 KiB | 1.012x | 1.016x |
| Dense delimiter | 1 KiB | 0.997x | 1.008x |
| Dense delimiter | 32 KiB | 1.005x | 1.009x |
| Empty matches | 1 KiB | 0.165x | 0.123x |
| Empty matches | 32 KiB | 0.168x | 0.124x |
| Sparse literal | 1 KiB | 0.518x | 0.442x |
| Sparse literal | 32 KiB | 0.879x | 0.940x |
| Sparse Unicode | 1 KiB | 0.223x | 0.274x |
| Sparse Unicode | 32 KiB | 0.224x | 0.279x |

The intended paths improve materially. The two unaffected workload classes are
within 1.2% on Intel and 2.1% on Graviton. The single 2.1% row is a 3 ns
difference, is not reproduced on Intel, and does not justify another campaign
for this focused check.

The second matrix brackets SafeRE with Slice before and after. Ratios are Slice
time divided by SafeRE time.

| Architecture | Rows | Slice wins | Geometric mean | Worst | Maximum Slice bracket |
|---|---:|---:|---:|---:|---:|
| Intel | 10 | 10 | 0.347x | 0.879x | 1.007x |
| Graviton | 10 | 10 | 0.321x | 0.691x | 1.007x |

The former SafeRE wins are closed. Intel empty containment moves from about
21 ns to 3.8 ns and captured Unicode from about 30 ns to 6.8 ns. Graviton empty
containment moves from about 32 ns to 4.5 ns and captured Unicode from about
49 ns to 14.6 ns. All measured Slice paths allocate zero bytes per operation.

## Correctness

Direct tests cover exact literals, captures, complete-literal verification,
Slice regions, UTF-8, Latin-1, case-fold and anchor exclusions, nullable starts,
and unchanged capture offsets. The complete local RE2 selector passes 942
tests with zero failures or errors and one intentional skip. Both AWS hosts also
passed the focused semantic tests with and without native access.
