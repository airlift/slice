# Line Capture Integration Campaign

**Status:** Closed. Offset-aware SWAR newline discovery is rejected; no
production change is retained.

**Baseline source commit:** `95349d63ed78658f8e329092b9684f2c83a1457a`

**Baseline source content hash:**
`82e614d847e3010d18df21c447bc6e5f1fc26dba04a02a9b93b6ebcf8458ce5b`

**Candidate source content hash:**
`a60df311efeebd5cdbb3ee720ea5ba1a3a77c3b4874012b779f94976ae79c18d`

**Baseline campaign:** `20260719T184554Z-line-region-baseline`

**Candidate campaign:** `20260719T185603Z-line-swar-candidate`

## Question

The current native census places Ruff `tweaked` capture grep at
`1.299x/1.078x` native time on Intel/Graviton and Unicode line capture at
`1.459x/1.053x`. Ruff executes 890,926 searches over separately constructed
line Slices, while Unicode executes 69,848 attempts over 34,924 lines.

The baseline decomposition showed that Ruff's forward DFA stage was already
faster than native's complete operation, while line discovery and matcher setup
remained timed. Java found every newline with a scalar byte loop, whereas the
native Rebar runner uses an optimized byte search. The candidate added an
offset-aware entry point to Slice's existing SWAR byte search and used it for
line discovery in the public and diagnostic runners.

## Result

Lower time is better. Public time is the mean of the public-before and
public-after medians. Candidate change is relative to the independent baseline
host; negative values are improvements.

| Workload | Architecture | Baseline public | Candidate public | Change | Baseline setup | Candidate setup | Candidate drift |
|---|---|---:|---:|---:|---:|---:|---:|
| Ruff `tweaked` | Intel | 51.476 ms | 52.571 ms | +2.13% | 21.254 ms | 17.081 ms | 0.02% |
| Ruff `tweaked` | Graviton | 57.277 ms | 59.957 ms | +4.68% | 17.568 ms | 18.348 ms | 0.99% |
| Unicode parse-line | Intel | 30.509 ms | 30.455 ms | -0.18% | 0.804 ms | 0.480 ms | 0.04% |
| Unicode parse-line | Graviton | 29.667 ms | 29.351 ms | -1.06% | 0.738 ms | 0.636 ms | 0.28% |

The complete stage comparison is retained in
[`2026-07-19-line-capture-integration.csv`](2026-07-19-line-capture-integration.csv).

Ruff's Intel baseline bracket drifted 5.10%, so its cross-session change is
diagnostic. The candidate bracket itself was stable. Ruff's Graviton baseline
and candidate brackets both remained below 1.1%, and the complete operation
regressed by 4.68%. This exceeds the protected 2% regression gate. Unicode's
setup stage improved, but setup accounts for less than 3% of its public
operation and complete time remained effectively unchanged.

## Interpretation

Replacing scalar newline discovery alone does not close Ruff's integration
gap. The independently timed setup stage can move without producing a public
win because the public operation still creates a Slice view and resets the
matcher for almost every line. The Unicode result confirms that line discovery
is not its material cost: its BitState capture stage takes 35.6 ms on Intel and
45.3 ms on Graviton, while candidate setup takes 0.48/0.64 ms.

Avoiding the per-line Slice view is not a local benchmark-runner cleanup.
Anchors, word boundaries, UTF-8 empty-width checks, and empty matches currently
derive their logical context from that view. A matcher region over a larger
backing Slice must carry explicit logical context boundaries through every
applicable engine or prove that a narrower route cannot observe surrounding
bytes. Treating only the Ruff pattern specially would not establish a safe
public design.

## Decision

The SWAR candidate is rejected and reverted. Its Ruff public regression fails
the retention gate, and its Unicode gain is immaterial. The campaign therefore
does not spend a native-comparison confirmation on this source.

Any further Ruff `tweaked` work must be a separate correctness-first logical
region campaign. Before timing, it must compare region behavior with Slice-view
behavior for start/end anchors, multiline anchors, word boundaries, empty
matches, invalid UTF-8, nonzero backing offsets, and CRLF line endings. It must
also demonstrate that the implementation avoids per-line allocation without
adding work to ordinary full-Slice hot loops. If that cannot be done without
changing protected engine loops, stop for design review rather than introducing
a workload-specific shortcut.

Unicode remains a direct BitState capture question on Intel, not a line
integration problem. It should not be combined with the logical-region work.

## Qualification

- The baseline and candidate each ran on independent `c8i.2xlarge` and
  `c8g.2xlarge` pairs with an 8 GiB compressed-oops heap.
- All four candidate hosts used the same source content hash.
- The native-access-enabled focused host suite passed 26 tests with zero
  failures or errors on every host.
- Every candidate public bracket drifted by less than 1%.
- All temporary AWS resources were removed.

This is internal engineering evidence, not formal publication qualification.
