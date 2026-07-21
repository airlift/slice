# RE2 Candidate Qualification

**Status:** Candidate evidence complete for the traditional native, Rebar native,
and Trino-shaped Joni campaigns. The complete release qualification gate is not
met because the full matrix did not produce three independent sessions per
architecture and the broader official Rebar intersection remains open.

**Candidate:** `c212684c4ad0a700e18c12ffca66ec1c33ff2160`

**Base:** `upstream/master` at `484d964`

**Pinned native RE2:** `972a15cedd008d846f1a39b2e88ce48d7f166cbd`

Ratios in this report are Slice time divided by comparator time, so lower is
better. General summaries exclude explicitly identified extreme outliers; every
qualified and underqualified row is retained in the companion CSV files.

## Assessment

The candidate is correctness-complete and has a strong performance foundation,
but it is not ready for an unconditional native-parity or release-readiness
claim.

- Public reusable matching is substantially faster than host-tuned native RE2
  in the traditional corpus. Public capture is at parity.
- The normal Rebar execution intersection is at parity with native RE2 on both
  Intel and Graviton.
- The initial matrix won 73 of 79 qualified Trino-shaped Intel operations
  against Joni, with a 0.481x median. Its two-session Graviton evidence won 67
  of 72 rows, with a 0.435x median. The later final Joni acceptance supersedes
  these incomplete aggregates.
- The initial candidate had five long sparse group-zero boundary losses against
  Joni. Follow-up session `20260720T182018Z-72726` closes all five with a
  bounded bulk candidate scan.
- Native RE2 retains specialized wins for Intel Unicode literal scanning,
  case-insensitive literal scanning, one long paired-DFA boundary shape, and
  `SplitBig2` stateless capture.
- Compilation is slower, especially on Graviton, but it is lower priority for
  the intended compile-once, reuse-many workload.

## Correctness Entry Gate

The candidate passed the complete Maven build with 2,705 tests, zero failures
or errors, and one intentional skip. The 913-test RE2 selector passed with the
object DFA implementation and again with strict native access enabled.

Coverage includes pinned upstream case tables, exhaustive generation,
randomized native differential tests, public and direct-engine agreement,
invalid UTF-8, concurrency, memory budgets, JVM-sourced Unicode authority, and
the Trino syntax and function corpora.

## Native RE2: Traditional Corpus

The traditional campaign ran 298 one-to-one Java/native pairs per architecture
over three independent sessions. A row is aggregate-qualified when at least two
sessions have no more than 2% native-before/native-after drift. It qualifies
589 of 596 rows. The seven underqualified rows are internal diagnostics; none
is a material public loss. One Graviton NFA row has zero stable sessions and is
retained explicitly rather than disappearing from the reduction.

| Public operation | Intel rows | Intel | Graviton rows | Graviton |
|---|---:|---:|---:|---:|
| Failed search | 63 | 0.521x | 62 | 0.452x |
| Successful search | 32 | 0.641x | 32 | 0.582x |
| Full match | 21 | 0.386x | 21 | 0.349x |
| Capture | 6 | 1.028x | 6 | 1.021x |
| Normal public execution | 103 | 0.730x | 102 | 0.638x |

The public capture aggregate is close enough to parity that individual material
rows are more informative:

| Capture row | Intel | Intel deficit | Graviton | Graviton deficit |
|---|---:|---:|---:|---:|
| `SplitBig2` | 1.235x | 0.589 ms | 1.102x | 0.315 ms |
| `SplitBig1` | 1.104x | 0.027 ms | 1.004x | immaterial |
| `split-hard` | 1.149x | 23 ns | 1.326x | 66 ns |

Direct engine rows remain useful diagnostics but are not the public score.
Direct NFA, OnePass, and BitState capture take 1.019x, 0.934x, and 1.150x native
time on Intel, and 1.319x, 1.226x, and 1.454x on Graviton. The reusable public
pipeline hides most of those Graviton deficits through engine selection and
workspace reuse.

`PossibleMatchRange` ratios of roughly 2,700x to 3,300x are excluded from the
matching summary. They compare a Java utility path with native high-level
prefix analysis and have at most a 0.13 ms absolute deficit; they do not measure
the public matching lifecycle.

## Native RE2: Rebar-Derived Corpus

The bounded Rebar-native campaign ran three independent Intel and Graviton
sessions plus one allowed focused confirmation. Workloads whose native bracket
drift exceeds 2% are excluded per session rather than invalidating unrelated
rows. Intel aggregate-qualifies 39 rows and Graviton aggregate-qualifies all 41.

| Scope | Intel | Graviton |
|---|---:|---:|
| Normal execution | 0.991x | 0.970x |
| Count captures | 1.066x | 0.971x |
| Count, normal rows | 1.027x | 0.996x |
| Count spans, normal rows | 0.944x | 0.799x |
| Grep captures | 0.888x | 0.774x |
| Compile | 1.206x | 1.577x |

The normal summary excludes ratios below 0.5x or at least 2.0x. It is a
descriptive view, not a substitute for individual rows.

### Material Residuals

| Workload | Intel | Intel deficit | Graviton | Graviton deficit |
|---|---:|---:|---:|---:|
| Veryl count captures | 1.066x | 9.90 ms | 0.971x | win |
| Ruff `tweaked` captures | 1.218x | 8.94 ms | 1.015x | 0.82 ms |
| AWS key grep | 1.189x | 7.32 ms | 0.865x | win |
| Dictionary compile | 1.604x | 3.27 ms | 1.663x | 3.71 ms |
| Date spans | 1.211x | 0.82 ms | 0.905x | win |

Veryl's 6.6% Intel ratio is material because the complete operation is large;
it remains the main native capture-counting residual. Ruff `tweaked` improved
substantially in the retained logical-region work but remains a material Intel
loss.

### Extreme Outliers

- Intel Russian literal count is 8.844x native time with a 2.395 ms deficit.
  Portable native RE2 matches Slice; host-tuned native RE2 uses an AVX2
  two-offset first/last-byte scanner that Slice does not have.
- Intel Chinese literal count repeats the same shape at approximately 20.7x and
  a 0.66 ms deficit, but only one session passes the native stability gate.
- Case-insensitive English literal count is 2.163x on Intel and 1.628x on
  Graviton. It uses the ShiftDFA path rather than the ordinary DFA loop.
- Cloudflare simplified long group-zero spans are 5.177x on Intel and 4.136x on
  Graviton, with 0.108 ms and 0.113 ms deficits. The greedy continuation shape
  falls outside the productive paired-table route.
- The shorter Cloudflare span rows reach 2.25x to 3.31x but have less than one
  microsecond absolute deficit.
- Intel dictionary single count is approximately 0.017x native time but has
  only one stable session. It remains underqualified rather than being used as
  a headline win.

## Trino-Shaped Operations Against Joni

The operation campaign brackets each Joni measurement with Slice before and
after measurements. A row is aggregate-qualified when at least two sessions
have a Slice bracket ratio no greater than 1.02. This two-session reduction is
used to identify stable rows, but the release protocol still requires three
complete host sessions.

| Operation | Intel median | Intel wins | Graviton median | Graviton wins |
|---|---:|---:|---:|---:|
| Contains | 0.543x | 10/10 | 0.514x | 10/10 |
| Count | 0.282x | 10/10 | 0.270x | 10/10 |
| Extract | 0.624x | 9/10 | 0.778x | 9/10 |
| Extract all | 0.412x | 9/10 | 0.426x | 8/9 |
| Third position | 0.307x | 9/10 | 0.203x | 9/10 |
| Replace | 0.525x | 9/10 | 0.553x | 7/8 |
| Lambda replace | 0.557x | 8/9 | 0.390x | 9/9 |
| Split | 0.420x | 9/10 | 0.601x | 5/6 |
| All qualified | 0.481x | 73/79 | 0.435x | 67/72 |

The Graviton table above contains two completed sessions and is retained as the
original candidate evidence. The later final Joni acceptance provides the
complete three-session aggregate.

### Superseded Sparse Boundary Outlier

Five 32 KiB `captureSparse` operations are materially slower than Joni:

| Operation | Intel | Slice | Joni | Graviton |
|---|---:|---:|---:|---:|
| Extract all | 3.569x | 84.6 us | 23.7 us | 1.977x |
| Replace | 2.999x | 79.3 us | 26.5 us | 1.941x |
| Split | 2.495x | 76.2 us | 30.6 us | 1.785x |
| Extract | 2.273x | 13.5 us | 5.94 us | 1.964x |
| Third position | 2.169x | 39.7 us | 18.3 us | 1.260x |

All five operations request only group-zero boundaries. A post-report trace
corrected the initial forward-plus-reverse diagnosis: these rows use the
candidate-start cursor for all three matches, record zero fallbacks, and never
compute the reverse program. Lambda replacement, which actually requests
subgroup captures, is at parity on Intel and faster on Graviton. The follow-up
campaign therefore targets the cursor's repeated boundary scan rather than the
NFA, OnePass, or BitState capture engines. See the
[`Joni sparse boundary campaign`](2026-07-20-joni-sparse-boundary-campaign.md).

That follow-up is now complete. The retained bulk scan wins all 32 focused rows
on each architecture. The eight 32 KiB `captureSparse` operations, including
the true-capture lambda control, take `0.335x`-`0.552x` Joni time on Intel and
`0.395x`-`0.469x` on Graviton. The table above remains useful provenance for
the regression but no longer describes the retained candidate.

The later complete three-session matrix qualifies 78 of 80 rows per
architecture and every qualified row wins. See the
[`final Joni acceptance`](2026-07-20-joni-final-acceptance.md).

## Memory Against Joni

Ordinary warm-pattern memory has a 0.474x median Slice/Joni ratio, compiled
patterns have a 0.903x median, and active matchers have a 0.484x median. Some
tiny compiled patterns use 2.0x to 2.75x Joni memory, but the excess is only
about 0.6 to 1.5 KiB. No ordinary row exceeds both 2x Joni memory and a 1 MiB
absolute excess.

The deliberately extreme 75,850-state bounded-context graph is different.
Slice retains approximately 29.8 MiB for the warm pattern and 31.6 MiB with an
active matcher, versus approximately 11 KiB and 27 KiB for Joni. Joni does not
retain an equivalent DFA. Slice charges the complete graph and its 7.96 MiB
off-heap sidecar to the configured DFA budget and records zero resets. This is
an explicit bounded memory/performance tradeoff, not ordinary pattern memory.

## Formal Gate Disposition

The strict release campaign required three independent complete host sessions
per architecture. The long `full` mode exceeded the old three-hour controller
timeout, and several Spot instances were interrupted after many hours. The
recoverable full-matrix results are retained as informational evidence, but the
three-session release gate is not met and no additional unbounded replacement
campaign was opened.

The official broader Rebar intersection across Slice, native RE2, Rust regex,
PCRE2 JIT, Java Pattern, and Go regexp also remains open. The Rebar-derived
native intersection in this report is not an overall regex-engine ranking.

Accordingly:

- correctness and API review can proceed;
- the five-commit reviewer-facing history is a sound initial review shape;
- the engine cannot yet claim every supported case is at least as fast as
  native RE2;
- the project is not at final release qualification.

## Next Focused Work

1. Investigate the Intel two-offset literal scanner and ShiftDFA generated code
   as separate bounded campaigns.
2. Investigate the Cloudflare long paired-DFA eligibility/continuation shape.
3. Revisit `SplitBig2` and Veryl capture lifecycle with stateless and reusable
   paths measured separately.
4. Run the broader official Rebar intersection and the final three-session
   release campaign only after retained production changes are complete.

## Provenance

The benchmark hosts were dedicated `c8i.2xlarge` Intel Xeon 6975P-C and
`c8g.2xlarge` Graviton4 instances in `us-west-2`, running Amazon Linux 2023 and
Temurin 25.0.3+9 LTS with an 8 GiB compressed-oops heap. Native RE2 was compiled
with GCC 11.5.0, `-O3 -DNDEBUG`, and host tuning (`-march=native` on Intel or
`-mcpu=native` on Graviton).

The full and Rebar source content SHA-256 is
`e5dbd52b74a617de4363f895746ca0b15976d2f72b1f981a5a48e6c885127810`.
The Joni source content SHA-256 is
`809c555008b10c338df18023866fe6a185b9d13ba66c87c334ade65df3830cb3`.
The snapshots differ only in qualification-tool changes; engine, benchmark, and
comparator code are identical.
