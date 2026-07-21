# Trino Integration And Public API

**Status:** Slice API and operation adapter implemented and measured against
Joni on Intel and Graviton. The complete three-session operation matrix has no
qualified Joni loss. Native RE2 and broader Rebar release qualification remain.
Trino repository wiring is a separate follow-on project, not a release gate
for this library.

**Research snapshot:** 2026-07-13
**Implementation snapshot:** 2026-07-13

This document records the requirements discovered while evaluating this RE2
port as a possible replacement for Trino's Joni and historical RE2J regular
expression backends. It also records the implemented Slice facade,
`Re2Matcher`, compatibility corpora, and `TrinoRegexp` operation adapter. SQL
function registration and Trino engine selection are outside this repository.

## Conclusions

1. API design should be driven by repeated matching, captures, and byte-range
   operations, not only by one-shot `fullMatch` and `partialMatch` calls.
2. `Slice` is the appropriate public input and output type while this code is
   developed in the Slice project. It represents the required byte array,
   offset, and length without copying. The dependency must stop at the public
   facade so the engine can later move to a separate library without carrying
   Slice through parser, compiler, or execution internals.
3. The historical Trino RE2J fork mostly added execution and integration
   capabilities, not a separate regular expression language. This port already
   contains modern upstream equivalents for most of those engine features.
4. The main API gap for Trino is an allocation-conscious repeated-match cursor.
   Trino's count, position, extract-all, split, and replacement functions all
   iterate over non-overlapping matches and must handle empty matches at UTF-8
   code-point boundaries.
5. Trino replacement syntax and SQL value construction belong in a Trino
   adapter. Native RE2 rewrite syntax and empty-match replacement behavior are
   not the same as Trino's documented behavior.
6. Syntax compatibility is a larger adoption risk than the Java API. Trino
   documents Java `Pattern` syntax with exceptions, while RE2 intentionally
   supports only regular-language constructs. Some unsupported Java constructs
   are rejected, but others can be accepted with a different meaning. A Trino
   backend must define and test this boundary explicitly.
7. Final candidate measurements show broad superiority to Joni for
   Trino-shaped operations. The earlier five long sparse repeated-boundary
   losses were per-byte candidate-cursor accounting and are closed by a
   bounded bulk scan. Native RE2 and historical RE2J comparisons remain useful
   for engine diagnosis and adoption context.

## Scope

The proposed integration covers Trino's SQL regular expression functions and
their compiled regexp type:

- `regexp_like`
- `regexp_count`
- `regexp_position`
- `regexp_extract`
- `regexp_extract_all`
- `regexp_replace`, including lambda replacement
- `regexp_split`

The following Trino uses of regular expressions are separate systems and are
not automatically in scope:

- SQL `LIKE`, which has its own implementation and semantics
- JSON path `like_regex`, which implements XQuery regular expression rules and
  currently uses Joni after a separate syntax translation and validation step
- configuration, security, and connector code that directly uses
  `java.util.regex.Pattern`
- row-pattern matching used by `MATCH_RECOGNIZE`

Replacing those systems would require separate semantic and performance
research. In particular, the JSON path implementation must not be switched as
an incidental consequence of replacing the SQL regexp-function backend.

## Evidence Base

The research used these source snapshots and historical records:

- This port at `c64f24cb6f587e8afb97b53ed309003a0408d025`.
- Pinned upstream RE2 at
  [`972a15cedd008d846f1a39b2e88ce48d7f166cbd`](https://github.com/google/re2/tree/972a15cedd008d846f1a39b2e88ce48d7f166cbd),
  including its [syntax table](https://github.com/google/re2/blob/972a15cedd008d846f1a39b2e88ce48d7f166cbd/doc/syntax.txt).
- The local Trino checkout at
  `5c0efbe65ffeef8c64f17f02577bac79d7e677d6`, with `origin/master` at
  `695b824f87c1272849503486cabd58c719c63e2c`. The relevant current sources
  include the [SQL function documentation](https://github.com/trinodb/trino/blob/695b824f87c1272849503486cabd58c719c63e2c/docs/src/main/sphinx/functions/regexp.md),
  [backend configuration](https://github.com/trinodb/trino/blob/695b824f87c1272849503486cabd58c719c63e2c/docs/src/main/sphinx/admin/properties-regexp-function.md),
  and [shared function tests](https://github.com/trinodb/trino/blob/695b824f87c1272849503486cabd58c719c63e2c/core/trino-main/src/test/java/io/trino/operator/scalar/AbstractTestRegexpFunctions.java).
- The historical [`trinodb/re2j`](https://github.com/trinodb/re2j) fork at
  `6d2a746b1db0a6fed7875218f2a8ab668f0fc2d6`. The repository is not
  archived, but its last source change was in February 2024.
- Trino's local SafeRe backend prototype at
  [`c61f7529c95`](https://github.com/trinodb/trino/commit/c61f7529c95),
  which provides a useful example of integrating another linear-time engine.
- The original 2016 Trino integration commit, `f4e98aa8358`, which introduced
  RE2J as an optional backend and a shared Joni/RE2J function test suite.
- [Trino PR 2313](https://github.com/trinodb/trino/pull/2313), which fixed
  multi-byte and empty-match iteration across several regexp functions.
- [Trino PR 13064](https://github.com/trinodb/trino/pull/13064), which made Joni
  searches interruptible so failed queries do not leave pathological matches
  running.
- [Trino PR 20615](https://github.com/trinodb/trino/pull/20615) and
  [trinodb/re2j PR 4](https://github.com/trinodb/re2j/pull/4), which moved the
  fork to `io.trino.re2j` to avoid dependency package conflicts.
- [Trino PR 20638](https://github.com/trinodb/trino/pull/20638), which deprecated
  the RE2J backend. The PR records no technical rationale beyond the
  deprecation itself.
- Trino commit
  [`2030a2944c1`](https://github.com/trinodb/trino/commit/2030a2944c1), which
  removed whole-value copies from Joni functions and added non-zero Slice
  offset coverage.

## Trino's Runtime Model

Current Trino still contains both `JONI` and deprecated `RE2J` SQL-function
backends. Joni is the default. The configuration documentation says Joni is
generally faster for common usage but can take exponential time, while RE2J
guarantees linear time but is often slower. The deprecation change does not
record additional rationale, so performance and compatibility must be measured
rather than inferred from the deprecation.

Trino compiles a pattern while casting a SQL value to an engine-specific regexp
type. A compiled pattern is then reused while a scalar function processes many
input rows. This has several consequences:

- Compilation cost matters for non-constant patterns, but row matching is the
  dominant steady-state path for constant patterns.
- A compiled pattern must be safe to share across driver threads when Trino's
  expression infrastructure shares constants.
- Per-row matching should not copy the input or convert it to `String`.
- Boolean `regexp_like` must not allocate capture storage.
- Repeated operations should allocate their capture buffer once per function
  invocation and reuse it for every match in the input.
- Returned captures should be byte-range views or be written directly to a
  Trino block. Materializing Java strings is unnecessary and potentially
  incorrect for malformed UTF-8.

The current Joni implementation operates on `source.byteArray()` with
`source.byteArrayOffset()` and `source.length()`. The July 2026 no-copy change
showed why this matters: for a 32 KiB input with an early extraction match,
removing the input copy changed the recorded result from 2,075 ns to 98 ns.
The exact historical number is not a qualification result, but the shape proves
that copying can dominate an otherwise short search.

The SafeRe prototype converts patterns and every input to `String`. It also has
to translate UTF-16 matcher positions and skip empty matches in the middle of a
surrogate pair. That is useful evidence against a `CharSequence`-based facade
for this byte-oriented engine.

## Trino Behavioral Contract

Trino's shared `AbstractTestRegexpFunctions` suite runs for both Joni and RE2J.
It is the best existing executable specification for the SQL functions. The
important requirements are summarized below.

| Area | Required behavior |
|---|---|
| Search | `regexp_like` is a contains/search operation, not a full match. |
| Match order | Repeated operations return successive non-overlapping, leftmost-first matches. |
| Empty matches | Repeated matching advances by one UTF-8 code point after an empty match and may match at the end of input. |
| Offsets | Engine offsets are byte offsets relative to the logical Slice, even when its backing-array offset is non-zero. |
| SQL position | `regexp_position` accepts and returns one-based Unicode code-point positions, converting to and from engine byte offsets. |
| Captures | Group zero is the complete match. Unmatched optional groups become SQL null; matched empty groups remain empty values. |
| Extraction | `regexp_extract` returns the first match or capture; `regexp_extract_all` returns every requested capture and preserves nulls. |
| Split | Splits are non-overlapping and trailing empty fields are preserved. |
| Replacement | Every match is replaced. `$0`, numbered `$g`, and named `${name}` references are supported. Numbered references consume the longest valid group number. Backslash escapes the next replacement byte. |
| Lambda replacement | The lambda receives captures 1 through N, excluding group zero, for every match. A null lambda result makes the complete SQL result null. |
| Pattern errors | Invalid patterns and group references become `INVALID_FUNCTION_ARGUMENT`. |
| Character patterns | A pattern supplied as SQL `CHAR` is space padded before compilation. |
| Invalid UTF-8 | Matching malformed input must terminate safely. The shared test deliberately does not prescribe the boolean result. |
| Concurrency | A compiled pattern can be reused while many rows and drivers are evaluated. |

The current port reports match offsets relative to a logical Slice, including
non-zero windows, and its public/direct-engine differential tests cover
malformed UTF-8 and context windows. Execution engines receive explicit window
bounds so empty windows retain the complete Slice as boundary context.

### Empty-Match Replacement Difference

Trino follows Java/Joni repeated-match behavior for replacement. For example,
replacing `.*` in `x` with `xxxxx` performs a non-empty match followed by an
empty end-of-input match and produces ten `x` characters.

Native RE2 `GlobalReplace`, and the current `Re2.globalReplace` port, suppress
an empty match immediately adjacent to the preceding match. The current port
therefore performs one replacement and produces five `x` characters. This is
correct upstream behavior, but it is not Trino behavior. A Trino adapter must
iterate over matches itself instead of using the native rewrite API.

## Syntax Compatibility

Trino's user documentation says that SQL regexp functions use Java `Pattern`
syntax with a list of exceptions. Joni is constructed with `Syntax.Java`.
RE2, by design, accepts only constructs that can be implemented with bounded
memory and linear-time matching.

The historical optional RE2J backend already implemented a smaller syntax than
the documentation promised. That history does not remove the compatibility
risk for a new default backend. The new integration must choose one of these
honest contracts:

1. Expose an explicitly RE2-syntax backend and document that users select a
   different language.
2. Define a Java-compatible regular-language subset, reject every unsupported
   construct reliably, and document the subset.
3. Change Trino's default syntax contract, with migration analysis and explicit
   project approval.

It must not silently reinterpret Java syntax as a different RE2 expression.

### Initial Syntax Matrix

| Construct | Trino Java/Joni contract | Historical Trino RE2J | Pinned/current RE2 | Integration consequence |
|---|---|---|---|---|
| Literals, concatenation, alternation, classes, counted repetition | Supported | Supported | Supported | Common subset. |
| Greedy and reluctant quantifiers | Supported | Supported | Supported | Common subset. |
| Inline `i`, `m`, and `s` flags | Supported with documented newline/case rules | Supported | Supported | Verify exact anchor and case-fold behavior. |
| `(?U)` | Java Unicode-character-class mode | RE2 ungreedy mode | RE2 ungreedy mode | Same text has different meaning; must be rejected or translated in a Trino compatibility mode. |
| Named capture `(?<name>...)` | Supported | Added by the Trino fork | Supported by pinned RE2 | Common subset. Python-style `(?P<name>...)` is an additional RE2 spelling. |
| Backreferences such as `\1` | Supported by Java/Joni | Rejected | Rejected | Cannot be implemented without abandoning RE2's guarantee. |
| Lookahead and lookbehind | Supported by Java/Joni | Rejected | Rejected | Cannot generally be implemented by this engine. |
| Atomic groups and possessive quantifiers | Supported by Java/Joni | Rejected | Rejected | Must be rejected by an RE2 backend. |
| Java class intersection, such as `[a-z&&[^bc]]` | Supported | Not Java-compatible | Not Java-compatible | Current RE2 can parse this text with different structure and meaning; compatibility validation is mandatory. |
| POSIX ASCII classes, such as `[[:alpha:]]` | Accepted by Joni, despite not being ordinary Java syntax | Supported | Supported | Available and useful for compatibility. |
| `\d`, `\s`, `\w`, and boundaries | Java behavior, with Trino caveats | ASCII RE2 behavior | ASCII RE2 behavior | Test Unicode and boundary differences explicitly. |
| Unicode categories and scripts | Supported with Trino naming rules | Categories and scripts | Upstream categories and scripts | The accepted names differ; see below. |
| Unicode blocks and binary properties | Documented by Trino | Not implemented | JVM-backed | Supported directly by the current port. |
| Unicode names containing underscores | Trino documentation says underscores must be removed | Upstream names require underscores | Upstream names require underscores | `OldItalic` versus `Old_Italic` is a documented mismatch. |
| `\C` single-byte match | Not a Java construct | Not implemented by the old fork | Supported | Useful for byte-oriented callers, but not part of Java compatibility. |
| `\Q...\E` outside classes | Supported | Supported | Supported | Common subset. |
| `\Q` and `\E` inside classes | Trino documents them as literals | Treated according to old RE2 parser | Rejected by current RE2 | Preserve Trino's documented exception if compatibility mode is added. |
| Surrogate escape pairs | Trino says to use `\x{10000}` instead | Old byte-oriented parser follows RE2 rules | Surrogate escapes rejected; `\x{10000}` supported | Compatible with the documented exception. |
| `\Z`, `\G`, `\R`, horizontal-space classes, and Java-only flags | Generally available in Java syntax unless listed otherwise | Mostly rejected | Mostly rejected | Complete the case-by-case syntax corpus before claiming Trino compatibility. |

The matrix is deliberately not labeled complete. Java `Pattern` has a large
surface, and Trino's documentation describes syntax more broadly than its
shared function tests exercise. Completing this corpus is correctness work, not
documentation cleanup.

Direct probes against the current port confirmed both collision examples. It
accepts `(?U)` as ungreedy mode. It also accepts `[a-z&&[^bc]]`, but not as a
Java class intersection, so a caller cannot rely on compilation failure to
detect the incompatibility. The probes also confirmed support for both named
capture spellings, `\C`, Java Unicode blocks, and Java binary properties through
the executing JVM. They confirmed rejection of lookaround, backreferences,
atomic groups, and possessive quantifiers.

## Historical Trino RE2J Fork

The repository commonly remembered as an archived Trino port is
[`trinodb/re2j`](https://github.com/trinodb/re2j). It is not marked archived.
It was imported into the Trino organization in 2023 from the earlier
Teradata/Trino fork and published as `io.trino:trino-re2j:1.7`.

The fork began with Google RE2/J's NFA-oriented Java port and added these
important changes in 2015 and 2016:

| Commit | Capability | Relevance now |
|---|---|---|
| `07e9f9e`, `5be829a`, `5f89e80` | Slice input and byte-oriented matching without input copies | The new public facade needs the same zero-copy property. |
| `efb6cbe`, `e5b12a5`, `ab46e5e` | Thread-local machines and flatter NFA data structures | This port has independent modern NFA, BitState, OnePass, and DFA engines. |
| `e480e1b` | Reverse and unanchored programs | This port has reverse compilation and currently creates the reverse program lazily. |
| `d3f779e`, `093e61c` | Port of the native RE2 DFA and integration with RE2/J | This port contains the pinned upstream DFA design. |
| `60d3835`, `732bb09` | Selectable DFA/NFA execution, state limit, retries, and permanent fallback | This port instead follows upstream memory budgets and cache reset/fallback behavior. |
| `464234f` | `(?<name>...)` named capture syntax | Pinned RE2 and this port already support it. |
| `36dbef1` | Named groups in Java-style replacement strings | This remains a Trino adapter requirement. |
| `9fa5a99`, `7778b40` | Prefix acceleration and cheaper boolean `match`/`find` | This port has upstream prefix and boolean no-capture paths, but they require formal qualification. |

Trino's `Re2JRegexp` wrapper also strips a leading `.*` or `.*?` before a
contains search because that prefix prevented the old fork's first-byte
acceleration. This is a workaround for the old engine, not SQL semantics. It
should not be copied into the new adapter unless a benchmark and direct path
test show that the current upstream-based engine needs it.

The old backend exposes a maximum DFA state count and a retry count. Once the
state limit is exceeded enough times, the compiled pattern permanently uses
the NFA and optionally emits a fallback event. This port uses upstream-style
memory planning and bounded DFA caches instead. A Trino integration should map
configuration to a memory budget rather than preserve the old state-count API
without evidence that users depend on it.

No Trino-specific character classes were found in the fork. Its named ASCII
classes and Unicode category/script tables came from the RE2/J lineage. It did
not add the Java Unicode block or binary-property support described by Trino's
current Joni-oriented documentation. The remembered Trino changes are therefore
more likely the byte-oriented engine, DFA, named-capture, and replacement work
listed above than an additional character-class language.

## Current Port Assessment

### Capabilities Already Present

- Public Slice input and output over internal byte-array ranges.
- UTF-8 and Latin-1 parsing and execution.
- Leftmost-first, longest, and full-match execution modes.
- Boolean matching without a capture buffer.
- Caller-owned `[start,end,...]` capture arrays through `matchInto`.
- High-level `MatchResult` with numbered and named captures.
- Reusable `Re2Matcher` capture storage for repeated non-overlapping matches.
- Allocation-free `Re2Matcher.reset(input, start, end)` for repeated logical
  regions with Slice-view boundary semantics and region-relative offsets.
- DFA, NFA, OnePass, and BitState execution with bounded memory behavior.
- Lazy reverse-program construction.
- Required-prefix and first-byte acceleration.
- Safe concurrent use of a compiled `Re2`.
- Native RE2 rewrite, set, and filtered-set functionality.
- Trino-compatible contains, count, position, extraction, split, template
  replacement, and lambda-replacement iteration.
- Data-driven coverage for 113 Trino function cases and 26 syntax cases.

### API And Follow-On Integration Considerations

| Gap | Why it matters |
|---|---|
| Trino module wiring is a separate project | A future Trino integration must provide SQL registration, block construction, `CHAR` padding, configuration, and error-code translation in Trino. |
| Complete Java syntax compatibility is impossible | The backend must be presented as RE2 syntax; additional same-text/different-meaning collisions may need explicit rejection. |
| Pinned upstream changes after the current audit | Applicable pinned C++ tables and active generated parameters are ported; changing the pin requires a new audit. |
| High-level `MatchResult` allocates every capture pair | This is appropriate for ordinary Java callers; Trino hot paths should use `Re2Matcher` or caller-owned capture arrays. |
| Internal Java packages require public implementation types | The final artifact or package layout must decide how parser/compiler/engine types are hidden from users. |
| No cooperative cancellation contract | Linear time prevents catastrophic backtracking but does not make a very large scan instantaneous after query cancellation. The cost of polling must be evaluated before adding it to hot loops. |

Pattern bytes are copied during compilation so caller mutation cannot change a
compiled pattern. Input and group values remain zero-copy Slice views. Raw parse
flags and program diagnostics are not public facade APIs.

## Public API Requirements

The following are requirements, not final class or method names.

### Layering

```text
Trino SQL adapter
    -> public Slice-oriented compiled-pattern API
        -> package-private byte range (byte[], offset, length)
            -> parser, compiler, and execution engines
```

Rules for this boundary:

1. Public pattern, input, output, and group-view methods use `Slice`.
2. The facade extracts the backing array, offset, and length once per call and
   creates no copy.
3. Parser, compiler, and execution packages do not import Slice.
4. Byte offsets are relative to the logical input Slice, never the backing
   array.
5. Group Slice values are views of the input and retain its backing storage.
6. No implicit `String`, `CharSequence`, or UTF-16 conversion is provided.
7. If the engine later moves to a separate artifact, the internal byte-range API
   can become the core boundary and the Slice facade can remain a small adapter.

### One-Shot Operations

The compiled pattern needs clear one-shot operations for:

- full-input boolean matching
- unanchored contains/search without captures
- anchored-start matching
- low-level matching into a caller-owned capture array
- high-level nullable match results

Method names should communicate the operation directly. `matches` should mean
full input, while `contains` or `find` should mean unanchored search. The current
`partialMatch` name is accurate in RE2 terminology but less familiar to normal
Java users.

### Repeated Matching

A stateful matcher or cursor is justified here. It should:

- bind one compiled pattern to one input Slice
- allocate its capture buffer once and reuse it
- support `find()` and `find(startByteOffset)`
- expose current match start/end and numbered/named groups
- distinguish no current match from an unmatched optional group
- advance an empty match by one UTF-8 code point in UTF-8 mode and one byte in
  Latin-1 mode
- allow an empty match at end of input exactly once
- preserve full-input context for `^`, `$`, word boundaries, and non-zero search
  starts
- reset cheaply for reuse with another input if benchmarks show that useful
- reset to a logical input region without allocating a Slice view for every
  failed region search

The cursor should expose current match state rather than allocate an immutable
`MatchResult` for every `find()`. A separate snapshot/result API can remain for
ordinary one-shot use.

The low-level equivalent should allow a caller such as Trino to provide one
`int[]` and control iteration itself. That path is required for lambda
replacement and block construction, where creating a result object per match
would be avoidable overhead.

### Captures And Results

- Group zero is always the complete match.
- Unmatched pairs are `-1,-1`.
- The caller may request a prefix of capture groups rather than every group.
- Group access by name uses compiled metadata and does not rebuild the map per
  match.
- High-level group access returns Slice views.
- Numeric parsing helpers are not needed by Trino and should be reconsidered as
  part of public API minimization rather than driving the matcher design.

### Compilation And Resources

- Compilation uses static factories; constructors remain non-public.
- The ordinary factory accepts a pattern Slice and a small, named options type.
- Leftmost-first UTF-8 behavior remains the default.
- The memory budget is explicit and bounded. Trino can expose it as
  configuration if operational evidence requires a knob.
- Compile and parse failures remain typed exceptions so the Trino adapter can
  map them to `INVALID_FUNCTION_ARGUMENT`.
- Engine fallback events or metrics should be added only for a demonstrated
  observability use case; the old debug logger is not sufficient evidence.

### Trino Adapter Responsibilities

The adapter, not the core engine, should own:

- SQL function registration and engine selection
- SQL `CHAR` padding
- one-based code-point positions
- `$g` and `${name}` replacement parsing
- lambda invocation and Trino block construction
- SQL null propagation
- conversion to `TrinoException` and Trino error codes
- any Java-syntax compatibility validation or translation
- Trino-specific configuration names and deprecation behavior

This keeps the core library usable by non-Trino callers and prevents SQL
semantics from becoming engine semantics.

## Correctness Qualification Plan

Correctness work must precede API stabilization and formal benchmarking.

### 1. Trino Function Corpus

Convert Trino's shared `AbstractTestRegexpFunctions` cases into a data-driven
adapter acceptance corpus. Preserve at least:

- all function outputs and errors
- empty input and empty pattern cases
- ASCII and multi-byte UTF-8 cases
- non-zero Slice backing-array offsets
- unmatched and empty capture groups
- replacement group-number parsing
- named replacements
- trailing empty splits
- code-point-based positions
- malformed UTF-8 termination

The corpus should run against Joni, historical Trino RE2J, and the new adapter
where syntax is supported.

### 2. Syntax Corpus

Build a case table from Java `Pattern`, Trino's documented exceptions, pinned
RE2 syntax, and observed Joni behavior. Every construct must be classified as:

- same syntax and behavior
- accepted after explicit translation
- rejected with a clear error
- impossible under RE2's linear-time contract

Include semantic-collision cases such as `(?U)` and Java character-class
intersection. A compile-success test is insufficient; accepted patterns must be
matched against discriminating inputs.

### 3. Differential Matching

For the common regular-language subset, compare:

- boolean search and full match
- complete match boundaries
- all numbered and named capture boundaries
- repeated non-overlapping matches
- replacement, extraction, split, count, and position results

Use deterministic generated patterns and byte inputs, including malformed UTF-8
and non-zero windows. Joni is the Trino compatibility oracle; pinned native RE2
remains the engine-semantics oracle.

### 4. Historical Regressions

Retain direct tests for the failures represented by Trino's history:

- empty matching over multi-byte input
- word boundary at end of input
- invalid UTF-8 termination
- non-zero Slice offsets
- long-running Joni cancellation behavior, if the new API adopts a cancellation
  contract
- DFA memory exhaustion and fallback
- leading-dot-star search acceleration without pattern rewriting

### 5. Concurrency And Resource Tests

- Share one compiled pattern across many threads and inputs.
- Exercise DFA cache reset while searches are active.
- Verify compile and execution memory limits through direct path assertions.
- Measure, but do not infer correctness from, allocations for boolean, one-shot
  capture, and repeated-match operations.

## Formal Performance Qualification Plan

The original research phase ran no benchmarks. Candidate `c212684` now has a
three-session Trino-shaped Joni comparison on Intel and Graviton. The strict
reduction qualifies 78 of 80 rows per architecture, and every qualified row is
a Slice win. See the dated
[`final Joni acceptance`](../benchmarks/history/2026-07-20-joni-final-acceptance.md)
and the earlier
[`candidate qualification report`](../benchmarks/history/2026-07-20-candidate-qualification.md).

### Comparators

1. Pinned native RE2 built with reproducible release settings.
2. This Java port through its low-level engine API.
3. This Java port through the final Slice public API.
4. Trino's Joni version and SQL-function adapter.
5. `io.trino:trino-re2j:1.7` and its SQL-function adapter.

Unsupported syntax must be reported in a compatibility table, not assigned a
misleading performance result. Catastrophic Joni cases should use a fixed
deadline and report timeout/cancellation rather than allowing an unbounded
benchmark iteration.

### Benchmark Levels

**Engine microbenchmarks** isolate compilation, search engines, prefix scanning,
capture extraction, and cache behavior. They explain algorithmic differences.

**Trino-operation benchmarks** measure complete operations over Slice values:

- `regexp_like`
- first extraction with and without captures
- extract all
- count
- first and Nth position
- split
- literal and capture-based replacement
- lambda-replacement match iteration, excluding SQL lambda body cost where
  necessary to isolate matching

Both levels are required. A microbenchmark win that disappears behind adapter
allocation or copying is not an integration win.

### Workload Dimensions

| Dimension | Required cases |
|---|---|
| Input size | Tiny values through multi-megabyte values, with enough points to reveal constant, setup-plus-linear, and cache-transition behavior. |
| Match location | No match, beginning, middle, end, and full-input match. |
| Match count | Zero, one, sparse, dense, and empty matches. |
| Captures | None, group zero only, one group, and many groups with unmatched alternatives. |
| Pattern family | Literal, character class, alternation, counted repetition, anchored, dot-star, Unicode, case-insensitive, and high-DFA-state patterns. |
| Input encoding | ASCII UTF-8, multi-byte UTF-8, Latin-1 mode, NUL bytes, and malformed UTF-8 where the operation is defined. |
| Compilation | Cold compile, repeated compile, and match-only with a precompiled pattern. |
| Cache state | Cold DFA, warm DFA, cache reset/fallback, and shared concurrent use. |
| API layer | Core byte range, public Slice facade, and Trino adapter. |

Record throughput or time, allocation per operation, retained compiled-pattern
size, and scaling slope. Every material gap must be explained by algorithm,
loads, allocation, dispatch, or generated code; `JVM overhead` is not an
explanation.

### AWS Execution

Run the final matrix on dedicated, non-burstable AWS instances representing:

- a modern Intel server CPU
- a same-generation Arm Graviton server CPU

Use equivalent vCPU and memory configurations where practical. Record the
exact instance type, CPU model, microcode, architecture, JDK build, JVM flags,
native compiler, native flags, kernel, NUMA placement, and benchmark commit.
Disable unrelated background services and avoid running Intel and Arm results
at different benchmark revisions.

Use multiple JMH forks with verified C2 compilation and enough warmup operations
to reach steady state. Test multiple sizes and inspect the performance shape;
ratios without scaling information can hide missing optimizations. Validate
important microbenchmark conclusions in the Trino-operation benchmark.

### Success Criteria

- No allocation regression for non-capturing boolean search.
- Repeated matching allocates one reusable capture buffer, not one result per
  match, on the low-level path.
- No input copy or `String` conversion in the Slice or Trino paths.
- Every native RE2 gap is explained and either closed or explicitly approved.
- The new backend is better than Joni and historical RE2J over the agreed common
  Trino workload, with syntax-only differences reported separately.
- Results are reproducible on both Intel and Graviton.

## Open Design Decisions

Implementation settled the compiled type as `Re2`, repeated matching as
`Re2Matcher`, stable copied pattern bytes, Slice group views, adapter-owned
replacement syntax, and explicit rejection of known syntax collisions. The
remaining decisions are:

1. Whether cooperative cancellation belongs in the engine API, the Trino
   adapter, or is unnecessary after bounding a single input size. Any polling in
   a hot loop requires targeted performance evidence.
2. Whether the library remains in Slice or moves to a separate artifact. The
   facade/core boundary should make this a packaging decision rather than a
   rewrite.
3. Whether native RE2 rewrite operations remain public alongside a separate
   Java/Trino replacement utility.
4. Which additional Java-only syntax collisions should be rejected by a Trino adapter and
   which can be translated without changing semantics.
5. Whether malformed patterns should retain the adapter's narrow Latin-1
   fallback after behavior is compared directly with Joni and historical RE2J.

## Recommended Sequence

1. Complete package encapsulation and structural cleanup outside protected hot
   loops.
2. Freeze the compatibility corpus and performance workload manifest.
3. Review and execute
   [`QUALIFICATION_PLAN.md`](../benchmarks/QUALIFICATION_PLAN.md) on dedicated
   Intel and Graviton hosts.
4. Resolve measured gaps, then reconstruct the reviewer-facing commit stack.

Wiring the adapter into a Trino branch and running Trino's shared regexp suite
belongs to the separate Trino adoption project.
