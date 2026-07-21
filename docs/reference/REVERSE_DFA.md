# Reverse DFA

A forward DFA reports the end boundary of a left-to-right match. When the public
API needs the start boundary and capture extraction is not already selecting an
NFA-style engine, RE2 searches the matching prefix with a reverse-compiled
program. The reverse DFA's boundary is the original match start.

## Public Search Flow

For an ordinary unanchored search, `Re2.matchInternal` performs:

1. A forward DFA search to find the selected match end.
2. A reverse DFA search over the prefix ending at that boundary to find the
   selected match start.
3. A capture engine only when the caller requested captures not already known
   from the two boundaries.

End-anchored programs can start with an anchored reverse search. This quickly
rejects text whose suffix cannot match and avoids scanning every possible start
position from the left.

If either DFA cannot be created or exhausts its cache without making progress,
the public cascade falls back to NFA, BitState, or OnePass as appropriate. DFA
failure is an execution-path signal, not a no-match result.

## Boundary Semantics

`Dfa.search` returns a packed `long` containing the boundary and search status.
The boundary's meaning depends on direction:

- forward search: exclusive match end relative to the searched Slice window
- reverse search: inclusive match start relative to the searched Slice window

The reverse search must retain the complete original text as context. Beginning
and end assertions, line boundaries, and word boundaries can depend on bytes just
outside the searched prefix or window.

For a match `[3, 6)` in `abc123xyz`, the forward search returns `6`. The reverse
search scans the prefix `[0, 6)` and returns `3`.

## Reverse Compilation

Reverse compilation reverses instruction order and swaps directional assertions:

| Forward assertion | Reverse assertion |
|---|---|
| begin text | end text |
| end text | begin text |
| begin line | end line |
| end line | begin line |

The runtime maps those assertions back to the original text context while
scanning bytes from right to left. Capturing-group numbering and byte offsets do
not change.

## Lifetime And Memory

The reverse program is compiled lazily because boolean and capture-engine paths
do not always need it. `Re2` stores the parsed required-prefix suffix so lazy
compilation does not parse the pattern again.

Following upstream RE2:

- the forward program receives two thirds of `Options.maxMemory`
- the reverse program receives one third
- a reverse longest-match DFA receives its program's complete remaining DFA
  allowance because a reverse program has only one DFA kind

An insufficient reverse compile budget is not fatal to an already valid `Re2`;
the current match falls back to another linear-time engine.

## Correct Use

- Run reverse matching only after a forward end boundary is known, unless using
  the explicit end-anchored shortcut.
- Search the exact prefix ending at the forward boundary, not the complete input.
- Preserve the complete context slice when the search window is a subrange.
- Treat `Dfa.SEARCH_FAILED` separately from no match.
- Keep positions as byte offsets; no conversion to Unicode code-point indices is
  performed.

## Tests

- `TestDfaReverseSearch` covers direct reverse matching.
- `TestDfaReversePosition` checks exact two-phase boundaries.
- `TestDfaReverseDemonstration` covers assertions, Unicode, and empty matches.
- `TestDfaContextFlags` covers context outside the searched window.
- `TestPublicEngineAgreement` compares the public cascade with direct engines
  across full and non-zero windows.
