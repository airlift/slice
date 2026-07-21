# RE2 Port Readiness Review - 2026-07-13

> Historical record. This document is evidence, not current project status.
> See the repository-root `RE2_TASKS.md` for open work.

## Scope

This review assessed the complete RE2 branch against the pinned upstream commit
`972a15cedd008d846f1a39b2e88ce48d7f166cbd`. It covered the public API, parser,
compiler, execution engines, set matching, test architecture, memory policy,
documentation, and static performance properties.

The review used targeted semantic, differential, and concurrency diagnostics.
It did not rerun the Java/C++ benchmark matrix.

Reviewed branch state after rebasing:

- base: `upstream/master` at `95a137d3d1f6ce986c4b458b329e0ee5a0e42a00`
- head: `39b601f0554bdb32166f45bce067a66dfe52fb31`
- build: `./mvnw clean install`
- result: 2,134 tests passed

## Assessment

The port has broad functionality and substantial tests, but it is not ready for
an initial pull request. Five confirmed correctness or robustness defects block
release. The existing generated test matrix does not exercise the public DFA
dispatch path where several of those defects occur, so a green build is not
sufficient evidence of upstream parity.

## Blocking Findings

### 1. Full-match semantics are incorrect

The DFA has first-match, longest-match, and many-match modes, but no distinct
full-match mode equivalent to upstream. The public anchored path maps full match
to longest match while retaining the normal leftmost-first option.

Confirmed examples that incorrectly return false include:

- `a|ab` against `ab`
- `a*?` against `a`
- `a+?` against `aa`
- `a??` against `a`
- `(?:a|ab)+` against `ab`

An exhaustive differential diagnostic compared public boolean operations with
the direct NFA over approximately 2.4 million pattern/text combinations and
found 8,388 mismatches, predominantly in full-match behavior.

Relevant code:

- `src/main/java/io/airlift/slice/re2/prog/Dfa.java`
- `src/main/java/io/airlift/slice/re2/Re2.java`

### 2. Shared compiled patterns are not thread-safe

`DfaInstance` stores mutable work queues, a mutable stack, transitions, state
arrays, and cache metadata on the cached program instance. Locks protect parts
of cache lookup and insertion, but transition computation uses shared queues
outside those locks and hot-path reads are not protected from cache reset.

A diagnostic sharing one compiled `Re2` across 16 workers produced incorrect
results immediately and reproducibly. The existing multithreaded test avoids
the defect by compiling a separate `Prog` for every thread and explicitly notes
the shared-state limitation.

Relevant code:

- `src/main/java/io/airlift/slice/re2/prog/Dfa.java`
- `src/test/java/io/airlift/slice/re2/prog/TestDfaMultithreaded.java`

### 3. The UTF-8 match-any-string shortcut changes behavior

The `(?s).*` full-match shortcut returns true without validating that the input
is a valid UTF-8 sequence. A single byte `0xFF` is accepted by the public fast
path while the compiled UTF-8 NFA rejects it.

The optimization is valid for arbitrary bytes in Latin1 mode, but not for the
default UTF-8 mode unless invalid UTF-8 semantics are preserved.

Relevant code:

- `src/main/java/io/airlift/slice/re2/prog/Compiler.java`
- `src/main/java/io/airlift/slice/re2/Re2.java`

### 4. Unanchored `Re2Set` matching is quadratic

The Java implementation starts an NFA search at every possible byte position
and scans each suffix. For a pattern such as `a*b` against a long string of only
`a`, this is O(n^2). Pinned upstream executes one many-match DFA pass.

This violates RE2's central linear-time guarantee and creates a resource-risk
path for user-controlled input.

Relevant code:

- `src/main/java/io/airlift/slice/re2/Re2Set.java`

### 5. The parser is not stack-safe

Group parsing uses recursive descent through `parseGroup()` and `parseAlt()`.
Approximately 1,000 nested noncapturing groups produce `StackOverflowError`
with a 1 MiB Java stack. Pinned upstream uses an explicit parser stack.

The historical remediation document classified recursive descent as
functionally equivalent. The diagnostic disproves that classification.

Relevant code:

- `src/main/java/io/airlift/slice/re2/parse/RegexpParser.java`
- `docs/audits/history/2026-02-08-line-by-line-audit.remediation.md`

## Major Assurance and Resource Findings

### Public dispatch is absent from the full generated matrix

`Tester.Config.fullMatrix()` disables DFA. The exhaustive and randomized tests
therefore compare selected direct engines, not the complete public
`Re2.matchInternal` cascade. This allowed basic full-match and concurrency
defects to survive the suite.

The DFA multithreaded test also uses reduced upstream stress parameters and
separate compiled programs per thread.

### The documented test command selected zero tests

Tests were renamed from `FooTest` to `TestFoo`, but active documentation retained
the old suffix selector. The corrected selector is:

```bash
./mvnw "-Dtest=**/re2/**/Test*" test
```

### A public compile mode gives DFA unlimited memory

`Re2.compile(ByteSlice, int flags)` passes a zero memory budget. DFA interprets
zero as `Long.MAX_VALUE`, allowing unbounded cache growth for user-controlled
patterns and text. Forward/reverse accounting also requires comparison with
upstream's program-size deductions and cache split.

### The parser imposes an upstream-incompatible capture limit

The Java parser rejects more than 1,000 captures. Pinned upstream applies the
1,000 limit to repeat counts, not capture count, and the Java program stores
capture indices in an `int`.

## Static Performance Findings

These findings do not depend on timing measurements:

- non-capturing `fullMatch` allocates an `int[2]` on every call
- OnePass eligibility is lazily synchronized and may be queried twice per match
- required-prefix handling diverges from upstream and contains duplicate or
  apparently unreachable checks
- unanchored `Re2Set` is algorithmically quadratic
- the flags-only compile API permits unbounded DFA cache growth

Historical benchmark results also record unresolved slower paths, including
Easy1 DFA, BigFixed through `Re2`, full compile, parser overhead, and
`possibleMatchRange`. Those measurements predate the current base and must be
rerun only after correctness is restored.

## Documentation and History Findings

- `RE2_TASKS.md` claimed complete parity while listing already-fixed work as
  open and reporting obsolete test counts.
- `docs/porting/TEST_MAP.md` used old class names and contradicted the task
  board about which suites were complete.
- `RE2_DECISIONS.md` contains superseded logging, exception, allocation,
  memory, and thread-safety claims.
- historical campaign logs are useful experiment records but were being used
  as current project status.
- the branch contained 361 commits, including experiment/revert pairs, a
  literal fixup commit, raw benchmark output, and cleanup separated far from
  the code it clarified.

## Positive Evidence

- the port covers the major RE2 parser, compiler, engine, and public API areas
- the full Maven build passes after rebasing onto current upstream master
- direct partial-match differential checking found no mismatch in 62,320
  sampled cases
- extensive generated, golden, and engine-specific tests exist and provide a
  strong base for completing public-path parity testing
- historical benchmark campaigns used explicit no-regression gates and
  retained useful evidence about failed optimization approaches

## Required Follow-up

The authoritative prioritized work is tracked in
[`RE2_TASKS.md`](../../../RE2_TASKS.md). Correctness and test-assurance work must
precede renewed optimization, structural cleanup, and history reconstruction.
