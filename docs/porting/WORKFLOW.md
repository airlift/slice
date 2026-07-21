# RE2 Porting Workflow

This guide describes correctness work on the Java RE2 port. Current status and
remaining work are tracked in [`RE2_TASKS.md`](../../RE2_TASKS.md); exact test
coverage is recorded in [`TEST_MAP.md`](TEST_MAP.md).

## Before Changing Code

1. Read the corresponding source at pinned upstream commit
   `972a15cedd008d846f1a39b2e88ce48d7f166cbd`.
2. Read [`RE2_DECISIONS.md`](../../RE2_DECISIONS.md) and
   [`PROCEDURES.md`](../audits/PROCEDURES.md).
3. Identify the public behavior and every engine or encoding affected.
4. Add an upstream-grounded failing test before fixing a correctness defect.

Do not infer behavior from Java's `Pattern`; pinned RE2 is the semantic source of
truth. Java API adaptations may change the shape of a call, but not matching,
capture, parse, or fallback behavior.

## Verification Loop

Run the narrowest relevant tests while developing, then run the complete RE2
selector before committing:

```bash
# One class
./mvnw "-Dtest=TestUpstreamParseDump" test

# All RE2 tests
./mvnw "-Dtest=**/re2/**/Test*" test

# Repository release gate
./mvnw clean install
```

Correctness tests must assert outcomes, state transitions, or selected execution
paths. Wall-clock thresholds are not correctness tests.

## Pinned Native Oracle

`tools/re2-golden/` builds a small native helper from exact RE2 and Abseil
revisions fetched into `target/`. It is optional and is not run in CI.

```bash
cd tools/re2-golden
./build.sh
```

Use hexadecimal inputs for arbitrary pattern and text bytes. Commit stable input
cases and generated JSONL expectations, not the fetched source or build output.
See `tools/re2-golden/README.md` for modes and fixture formats.

## Differential Coverage

When behavior can vary by execution path, compare as many of these as apply:

- UTF-8 and Latin-1 parsing
- first, longest, full, and many-match execution
- anchored and unanchored searches
- full and non-zero context windows
- malformed UTF-8 and NUL bytes
- unmatched and excess capture slots
- public `Re2`, DFA, NFA, BitState, OnePass, and testing Backtrack

`io.airlift.slice.re2.Tester` is the shared differential harness. Extend
it instead of creating a standalone diagnostic program.

## Source Mapping

| Java | Pinned upstream |
|---|---|
| `Regexp.java`, `CharClass.java` | `re2/regexp.cc`, `re2/regexp.h` |
| `RegexpParser.java` | `re2/parse.cc` |
| `Simplifier.java` | `re2/simplify.cc` |
| `Compiler.java` | `re2/compile.cc` |
| `Prog.java` | `re2/prog.cc`, `re2/prog.h` |
| `Dfa.java` | `re2/dfa.cc` |
| `Nfa.java` | `re2/nfa.cc` |
| `OnePass.java` | `re2/onepass.cc` |
| `BitState.java` | `re2/bitstate.cc` |
| `Re2.java` | `re2/re2.cc`, `re2/re2.h` |
| `Re2Set.java` | `re2/set.cc`, `re2/set.h` |
| `FilteredRe2.java`, `Prefilter.java` | `re2/filtered_re2.cc`, `re2/prefilter.cc` |
| test-scope `Backtrack.java` | `re2/testing/backtrack.cc` |

## Scope And Review

- Keep production changes within `io.airlift.slice.re2` unless integration
  explicitly requires otherwise.
- Follow project naming and comment guidance; do not retain port artifacts merely
  to resemble C++ source.
- Preserve stack safety and non-capturing allocation behavior while cleaning code.
- Treat parser/compiler/runtime internals as implementation details unless the
  public Java API requires them.
- Keep generated output, fetched dependencies, raw profiles, and campaign scratch
  out of the initial PR. Retain reproducible inputs and concise conclusions.
- Record a decision only for a current intentional deviation. Superseded designs
  remain available in Git history.

Performance changes require direct path tests and later benchmark qualification,
but performance measurements are a separate campaign from correctness auditing.
