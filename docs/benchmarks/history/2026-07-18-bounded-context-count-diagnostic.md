# Bounded-Context Count Diagnostic

**Status:** Diagnostic complete. No production memory policy or transition
layout changed.

This focused campaign explains the remaining capture-free count deficit in
Rebar workload `curated/10-bounded-repeat/context`. It varies only the total
configured memory budget and records Java and native DFA cache behavior for the
same expression and 7,384,531-byte haystack. This is internal engineering
evidence, not release qualification.

## Ratio Convention

All ratios are elapsed-time ratios, so lower is better:

- `Java/native < 1.0` means Java is faster than native RE2;
- `Java/native > 1.0` means Java is slower than native RE2;
- `Java/Java-8-MiB < 1.0` means the larger Java budget is faster than the
  current 8 MiB default.

## Protocol

Session `20260718T180151Z-37147` ran concurrently on an AWS `c8i.2xlarge`
Intel Xeon 6975P-C and `c8g.2xlarge` Graviton4. Both used Temurin 25.0.3 with
compressed ordinary object pointers and an 8 GiB heap. Each point has five
untimed warmups and seven measured iterations pinned to one physical CPU.

Native RE2 was the pinned project revision built with `-O3 -DNDEBUG` and the
host-specific `-march=native` or `-mcpu=native` flag. The runner verifies the
exact pattern hash, haystack hash, haystack size, and expected count before
accepting results. Both native-access-disabled and native-access-enabled focused
Java test runs passed on both hosts.

The expression is:

```text
[A-Za-z]{10}\s+[\s\S]{0,100}Result[\s\S]{0,100}\s+[A-Za-z]{10}
```

Every measured operation returned 53 matches. Java used a forward
capture-free count path with 54 searches. Native Rebar requested group zero and
used one forward search plus 53 reverse searches. Java therefore did less
boundary work, so the remaining deficit is not caused by an extra reverse
pass.

## Target Results

The tables report medians. `Resets` is the number during one complete count,
not a cumulative process count.

### Intel

| Total budget | Java | Native | Java/native | Java/Java-8-MiB | Java resets | Native resets | Retained Java states |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 MiB | 135.312 ms | 93.822 ms | 1.442x | 1.000x | 31 | 8 | 5,751 |
| 12 MiB | 121.517 ms | 83.102 ms | 1.462x | 0.898x | 18 | 5 | 3,558 |
| 16 MiB | 116.779 ms | 78.952 ms | 1.479x | 0.863x | 12 | 3 | 8,010 |
| 24 MiB | 105.240 ms | 72.002 ms | 1.462x | 0.778x | 7 | 2 | 19,250 |
| 32 MiB | 97.809 ms | 66.282 ms | 1.476x | 0.723x | 5 | 1 | 14,525 |
| 48 MiB | 88.536 ms | 13.349 ms | 6.632x | 0.654x | 3 | 0 | 29,964 |
| 64 MiB | 83.798 ms | 13.330 ms | 6.286x | 0.619x | 2 | 0 | 28,377 |
| 96 MiB | 18.755 ms | 13.374 ms | 1.402x | 0.139x | 0 | 0 | 75,850 |
| 128 MiB | 18.570 ms | 13.330 ms | 1.393x | 0.137x | 0 | 0 | 75,850 |

### Graviton

| Total budget | Java | Native | Java/native | Java/Java-8-MiB | Java resets | Native resets | Retained Java states |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 MiB | 167.501 ms | 112.185 ms | 1.493x | 1.000x | 31 | 8 | 5,751 |
| 12 MiB | 151.630 ms | 107.764 ms | 1.407x | 0.905x | 18 | 5 | 3,558 |
| 16 MiB | 142.386 ms | 107.121 ms | 1.329x | 0.850x | 12 | 3 | 8,010 |
| 24 MiB | 128.609 ms | 102.860 ms | 1.250x | 0.768x | 7 | 2 | 19,250 |
| 32 MiB | 122.510 ms | 95.132 ms | 1.288x | 0.731x | 5 | 1 | 14,525 |
| 48 MiB | 115.505 ms | 18.113 ms | 6.377x | 0.690x | 3 | 0 | 29,964 |
| 64 MiB | 104.472 ms | 18.010 ms | 5.801x | 0.624x | 2 | 0 | 28,377 |
| 96 MiB | 28.573 ms | 17.842 ms | 1.601x | 0.171x | 0 | 0 | 75,850 |
| 128 MiB | 27.975 ms | 18.274 ms | 1.531x | 0.167x | 0 | 0 | 75,850 |

## Controls

The two control workloads were measured at 8, 32, and 64 MiB. Their Java
medians changed by at most 1.0% on Intel and 0.6% on Graviton relative to the
8 MiB point, so larger configured budgets do not intrinsically slow these
already-stable routes.

| Workload | Architecture | Java/native at 8 MiB | Java 64 MiB / Java 8 MiB |
|---|---|---:|---:|
| `curated/01-literal/capitals` | Intel | 0.761x | 0.994x |
| `curated/01-literal/capitals` | Graviton | 0.663x | 1.003x |
| `imported/regex-redux/letters-en` | Intel | 1.466x | 0.992x |
| `imported/regex-redux/letters-en` | Graviton | 0.906x | 1.000x |

`letters-en` uses the bounded character-class counter rather than this DFA
route. Its Intel deficit is real but outside this focused campaign.

## Findings

Cache capacity and steady-state transition throughput are separate problems:

1. Native becomes reset-free at a 48 MiB total planning budget. Java becomes
   reset-free at 96 MiB. At the same nominal budget, Java's conservative state
   charge therefore supports roughly half the useful state capacity of native
   for this graph.
2. Eliminating resets is an algorithmic-size win: the Java operation becomes
   7.2x faster on Intel and 6.0x faster on Graviton between 8 and 128 MiB.
3. Capacity alone does not establish native parity. With the complete 75,850
   Java states retained, the ordinary object-row loop still takes 1.393x native
   time on Intel and 1.531x on Graviton.
4. The route used neither paired transitions nor the absolute-pointer sidecar.
   Its complete 64-bit pointer table would be about 6.7 MiB, far beyond the
   qualified 48 KiB sidecar cap. Prior cutoff evidence does not justify simply
   raising that cap, and doing so globally would reserve the full larger arena
   for every eligible small DFA.

The current Java state estimate charges `12 * byteClassCount` bytes for the
primitive transition row and object-reference row before instruction and fixed
overhead. Both campaign JVMs used four-byte compressed references, so this is
intentionally conservative relative to the measured VM layout. The next round
must distinguish a better planning estimate from a larger public default
instead of treating them as one unexplained memory increase.

## Stopping Decision

The diagnostic question is answered, so this campaign stops without changing
production behavior. The next campaign should be a bounded cache-capacity
integration experiment. A later transition-loop campaign is justified only
after a reset-free production policy is selected, because reset thrashing would
otherwise dominate its measurement.
