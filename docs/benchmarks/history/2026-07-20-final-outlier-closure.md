# RE2 Final Search-Outlier Closure

**Status:** Both candidates retained. The long Cloudflare regression is closed
on Intel and Graviton. Case-insensitive English scanning is faster than native
on Graviton and retains a 15.7% Intel deficit.

**Session:** `20260720T224822Z-6754`

**Pinned native RE2:** `972a15cedd008d846f1a39b2e88ce48d7f166cbd`

Ratios are Slice time divided by the midpoint of the host-tuned native before
and after medians, so lower is better.

## Diagnoses

### Matching self-loop

The paired object table stores direct references only for ordinary non-match
transitions. A match transition is represented by a null reference so the
continuation can preserve its exact byte boundary and priority. For
`.*.*=.*`, the destination is also a matching self-loop. The old continuation
therefore decoded the same match outcome for every pair in the long suffix.

The retained path recognizes the exact primitive transition value
`state | match-bit`, consumes it directly while it remains a matching
self-loop, and updates the match boundary for each byte. The first different
transition remains unconsumed and returns to the existing decoder.

### Case-insensitive prefix

Upstream ShiftDFA performs eight table lookups followed by eight serial
variable shifts per unrolled block. The Java port reproduced that dependency
chain, but it did not map competitively to HotSpot on the supported hosts.

The retained scanner compares the folded first and last prefix bytes in
parallel across each candidate block, intersects those masks, and verifies the
complete prefix only at surviving positions. It processes sixteen candidate
starts with the Vector API or eight with SWAR. This removes the serial ShiftDFA
chain rather than attempting source rearrangements around the same dependency.

## Target Results

| Workload | Architecture | Prior Slice | Current Slice | Native | Current ratio |
|---|---|---:|---:|---:|---:|
| Sherlock case-insensitive count | Intel | 642.97 us | 356.68 us | 308.22 us | 1.157x |
| Sherlock case-insensitive count | Graviton | 704.09 us | 368.72 us | 431.39 us | 0.855x |
| Cloudflare simplified-long spans | Intel | 133.30 us | 24.45 us | 25.75 us | 0.950x |
| Cloudflare simplified-long spans | Graviton | 148.82 us | 33.93 us | 36.04 us | 0.942x |

The folded-prefix change improves the exact Slice operation by 1.80x on Intel
and 1.91x on Graviton. The matching-self-loop change improves it by 5.45x and
4.39x. Cloudflare now beats native by about 5%-6%. The remaining Intel Sherlock
gap is 48.46 us per 899,232-byte count operation; it is no longer an extreme
outlier but remains explicit qualification work.

Native before/after drift for all four target rows is below 0.3%. The Intel
session's report command exited nonzero because two unrelated guard rows
exceeded the strict 2% native-drift gate. The raw target rows and correctness
results remain valid; the Graviton session exited cleanly. Both instances
terminated after artifact upload.

## Correctness And Guards

- The complete RE2 selector passed locally and on both target architectures.
- Target hosts ran the selector both without native access and with strict
  native access enabled.
- A deterministic route test requires the matching-self-loop scan and verifies
  that a newline stops before the exceptional transition.
- Randomized folded-prefix tests compare candidate results with a scalar
  reference over varied offsets, lengths, repeated prefixes, and byte data.
- Unaffected count/span rows show no correlated regression. The largest
  cross-session change was a 3.9% Graviton date fluctuation; remaining
  intersecting guards were within 1%.

## Disposition

The two multi-fold regressions had different causes and are closed separately.
The paired fix is a state-shape optimization over the existing primitive table.
The prefix fix is an intentional algorithm deviation from upstream. Neither
changes public matching semantics, memory budgets, native-access requirements,
or fallback behavior.
