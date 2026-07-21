# Upstream RE2 Test Map

This file records the correctness evidence for the Java `byte[]` port. A Java
test derived from an upstream file does not imply that every upstream case or
parameter was ported.

Pinned upstream commit: `972a15cedd008d846f1a39b2e88ce48d7f166cbd`

Validation gate:

```bash
./mvnw "-Dtest=**/re2/**/Test*" test
```

## Differential Coverage

`Tester.Config.fullMatrix()` applies every generated regexp and text to:

- parse modes: single-line UTF-8, single-line Latin-1, multiline UTF-8,
  multiline non-greedy UTF-8, and multiline Latin-1
- match kinds: first, longest, and full
- anchors: anchored and unanchored
- windows: the complete text, the text without its first byte, and the text
  without its last byte, with the complete text retained as context
- engines: testing Backtrack, NFA, eligible BitState, and eligible OnePass
- DFA: match and end-boundary agreement for first, longest, and full
- public `Re2`: match and capture agreement for first and full

Public longest-match behavior is compared directly with native RE2 in the
pinned oracle rather than through `Tester`, because longest is an execution
option rather than a parse flag.

The pinned native public oracle contains 62 byte-oriented cases. It covers
first/longest/full, invalid UTF-8, Latin-1, `\C`, NUL, Unicode, empty and
non-zero windows, anchors, unmatched captures, extra capture slots, and empty
matches. Inputs and generated expectations are in:

- `tools/re2-golden/public-match-cases.tsv`
- `src/test/resources/io/airlift/slice/re2/upstream_public_match.jsonl`
- `io.airlift.slice.re2.TestUpstreamPublicMatch`

A second reproducible native corpus contains 148 cases dedicated to public
longest-match and byte-boundary behavior. It includes first/longest pairs,
non-zero windows, anchors, malformed and truncated UTF-8, split byte sequences,
and Latin-1. Inputs and generated expectations are in:

- `tools/re2-golden/generate-longest-boundary-corpus.sh`
- `src/test/resources/io/airlift/slice/re2/testing/native_longest_boundary.jsonl`
- `io.airlift.slice.re2.TestNativeLongestBoundaryAgreement`

`TestPublicEngineAgreement` adds a direct-engine matrix over 11 patterns and
11 byte strings, including malformed and truncated UTF-8. Every input also
uses the three context windows described above.

## Upstream Tests

Coverage labels:

- **Ported corpus**: the upstream case table or test body is represented.
- **Selected**: important cases are represented, but the upstream file is not
  complete.
- **Adapted**: behavior is covered through a deliberately different Java API.
- **Not ported**: no equivalent coverage is currently claimed.

| Upstream file | Coverage | Java evidence | Remaining work |
|---|---|---|---|
| `string_generator_test.cc` | Ported corpus | `io.airlift.slice.re2.TestStringGenerator` | None identified. |
| `regexp_test.cc` | Ported corpus | `io.airlift.slice.re2.TestRegexpAst`, `TestRegexpWalker`, `TestRegexpDump` | Manual reference-count tests are not applicable to Java. |
| `parse_test.cc` | Ported corpus | `io.airlift.slice.re2.TestUpstreamParseDump`, `TestUpstreamBadRegexps`, `TestUpstreamParseErrorArguments`, `TestUpstreamParseEscape`, `TestUpstreamSyntaxModes`, `TestRegexpParser` | None identified. |
| `simplify_test.cc` | Ported corpus | `io.airlift.slice.re2.TestUpstreamSimplifyToString`, `TestSimplifier` | Verify future upstream additions when the pin changes. |
| `compile_test.cc` | Ported corpus | `io.airlift.slice.re2.TestUpstreamCompileDump`, `TestUpstreamCompileByteMap`, `TestUpstreamCompileMemoryBudget`, compiler bug regressions | None identified. |
| `possible_match_test.cc` | Ported corpus and parameters | `io.airlift.slice.re2.TestUpstreamPossibleMatchRange`, `TestUpstreamPossibleMatchRangeExhaustive` | None identified. |
| `search_test.cc` | Ported corpus and engine matrix | `io.airlift.slice.re2.TestUpstreamSearch`, direct-engine tests, `Tester` | All 236 rows run through five parse modes, first/longest/full matching, Backtrack, NFA, BitState, eligible OnePass, boolean DFA, and forward/reverse boundary DFA. |
| `dfa_test.cc` | Ported corpus and parameters | `io.airlift.slice.re2.TestUpstreamDfa`, `TestDfaMultithreaded`, reverse/context/full-match tests | Exact build, search, concurrency, memory, and reverse parameters are retained. Seven callback rows assert the exact graph dump; three accepting-loop rows assert the upstream state count and longest boundary because Java represents the accepting loop with its full-match sentinel. Upstream malloc-counter assertions are disabled in the pinned source and are not applicable. |
| `exhaustive_test.cc` | Ported parameters | `io.airlift.slice.re2.TestExhaustiveAgreement` | All four active Egrep invocations are represented. |
| `exhaustive1_test.cc` | Ported parameters | `io.airlift.slice.re2.TestExhaustiveRepetition` | All four active repetition invocations are represented. |
| `exhaustive2_test.cc` | Ported parameters | `io.airlift.slice.re2.TestExhaustiveEdgeCase` | All three active invocations are represented. |
| `exhaustive3_test.cc` | Ported parameters | `io.airlift.slice.re2.TestExhaustiveCharClass` | Both character-class and both interesting-UTF-8 invocations are represented. |
| `random_test.cc` | Ported parameters with native oracle | `io.airlift.slice.re2.TestRandomAgreement` | Five groups preserve seed 404, 100 constructions, four wrappers, and 100 texts per construction. The 200,000 pinned native cases record exact match and capture results and run through the full Java engine/public matrix. Upstream's null-input probe is not applicable to the null-rejecting Slice API. |
| `charclass_test.cc` | Ported corpus | `io.airlift.slice.re2.TestUpstreamCharClassBuilder`, `TestCharClassBuilder`, exhaustive character-class tests | All 18 unique rows are covered; upstream contains one duplicate row. |
| `required_prefix_test.cc` | Ported corpus | `io.airlift.slice.re2.TestUpstreamRequiredPrefix`, `io.airlift.slice.re2.TestUpstreamPrefixAccel` | All three upstream test bodies and both case tables are represented. |
| `set_test.cc` | Ported corpus | `io.airlift.slice.re2.TestRe2Set` | All matching, anchoring, empty-set, prefix, and lifecycle cases are covered. C++ move semantics are not applicable to Java. |
| `filtered_re2_test.cc` | Ported corpus | `io.airlift.slice.re2.TestFilteredRe2` | All atom, index, Latin-1, empty-pattern, lifecycle, and matching cases are covered. C++ move semantics are not applicable to Java. |
| `re2_test.cc` | Ported corpus and adapted APIs | `TestRe2Api`, `TestRe2Match`, `TestRe2Matcher`, `TestRe2Rewrite`, `TestRe2NumericParsing`, `TestRe2FullMatchTypes`, `TestRe2BugRegression`, and other `TestRe2*` classes | Applicable matching, rewrite, quoting, UTF-8, options, capture, metrics, error, Unicode, anchor, recursion, and numeric-boundary cases are covered. Consume and find-and-consume use repeated anchored matching and `Re2Matcher.find()`. C++ `Arg`, reference-wrapper, floating-point conversion, malloc-counter, and lazy-wrapper mechanics are not part of the Java API. |
| `re2_arg_test.cc` | Adapted | `TestRe2MatchResult`, `TestRe2MatchInto`, `TestRe2NumericParsing`, `TestRe2Matcher` | C++ `Arg` and consume out-parameters were intentionally replaced by capture buffers, `MatchResult`, and `Re2Matcher`. |
| `mimics_pcre_test.cc` | Not applicable | None | The unused `MimicsPCRE` diagnostic API was intentionally removed; it is not used by matching. |

## Test Harnesses

| Upstream helper | Java evidence | Status |
|---|---|---|
| `regexp_generator.cc/.h` | `RegexpGenerator`, `TestRegexpGenerator`, pinned native random corpus generator | Ported for the active random-test groups; Java unit tests also cover the local generator directly. |
| `string_generator.cc/.h` | `StringGenerator`, `TestStringGenerator` | Ported for exhaustive and deterministic random generation. |
| `tester.cc/.h` | `Tester` | Compares all production engines plus the public cascade; PCRE is intentionally absent. |
| `backtrack.cc` | testing-scope `Backtrack` and `TestUpstreamBacktrackSearch` | Testing reference only. |
| `exhaustive_tester.cc/.h` | `TestExhaustiveAgreement`, `TestExhaustiveRepetition`, `TestExhaustiveEdgeCase`, `TestExhaustiveCharClass` | Ported active parameters. |

## Open Correctness Gaps

No applicable pinned-suite case-table or active generated-parameter gap is
currently identified. A future upstream pin or supported-JDK change requires a
new audit rather than assuming this map remains complete.

The parser has no Java-only capture-count limit; a 10,000-capture stack-safety
test protects that contract. `TestUpstreamCompileMemoryBudget` protects the
upstream program/DFA budget split and the internal unbounded-compile default.
