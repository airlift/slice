# JVM-Sourced Unicode Policy

Status: implemented. `RE2_DECISIONS.md` records the intentional deviation from
pinned upstream RE2.

## Implemented Direction

Use the executing JVM as the only Unicode authority for the entire Java RE2
library.

The implementation removes the Unicode category, script, and case-fold data
copied from upstream RE2. It derives Unicode character classes and case
relationships from public JVM APIs, converts them to the library's existing
immutable range representation, and caches the results outside the matching
hot path.

This policy applies to the core RE2 implementation as well as Trino-compatible
syntax extensions. The library must not maintain one understanding of Unicode
for upstream RE2 syntax and another for Java or Trino syntax.

## Motivation

Callers of a Java library reasonably expect its Unicode behavior to agree with
the JVM on which it runs. A private Unicode snapshot can disagree with
`java.lang.Character`, `java.util.regex.Pattern`, and other Trino operations
after either RE2 or the JDK updates its Unicode version.

Using the JVM as the authority provides:

- one Unicode version for regex and other Java operations
- automatic support for Unicode updates delivered with the JDK
- no checked-in generated Unicode range tables
- a natural source for Java Unicode blocks, scripts, categories, binary
  properties, character names, and case behavior
- no matching-loop cost after JVM data has been converted to normal character
  classes

Current Trino uses Joni with `Syntax.Java`, but Joni/jcodings carries embedded
Unicode tables. Matching that particular table version is not the target. The
target is Java behavior as defined by the JVM used to run Trino.

## Intentional Upstream Difference

Pinned upstream RE2 carries Unicode data generated from a particular Unicode
release. JVM-derived data can differ for code points that were added or
reclassified in another release.

This is an intentional Java-platform adaptation. It changes only
Unicode-version-sensitive results; it does not justify changing RE2's matching
algorithm, resource guarantees, byte-oriented execution, or syntax semantics.
The implemented decision is recorded in `RE2_DECISIONS.md`.

The same library version may consequently produce different results after a
JDK upgrade. That is acceptable and consistent with other JVM Unicode APIs.
All nodes in a distributed Trino installation should use equivalent supported
JDK versions.

## Unicode Data Versus Regex Semantics

The JVM should answer facts about code points. The regex language must still
define how syntax uses those facts.

Examples of JVM-owned data include:

- general category
- script and block membership
- alphabetic, ideographic, whitespace, case, and related binary properties
- character names
- upper-case, lower-case, and title-case mappings

Examples of regex-language decisions include:

- whether `\d`, `\s`, and `\w` use ASCII or Unicode definitions in a given
  syntax mode
- which property spellings and aliases are accepted
- the meaning of `(?U)` in RE2 syntax and Java-compatible syntax
- malformed UTF-8 behavior
- which unsupported Java constructs are rejected

Keeping RE2's ASCII definition of a shorthand class is not a competing Unicode
database. It is a syntax rule. An explicit Java Unicode-character-class mode
can map the same shorthand to JVM-derived ranges.

## Production Design

The package-private JVM Unicode registry in `io.airlift.slice.re2` exposes
immutable `CharClass` values to the parser and character-class builder.

Property construction follows this flow:

1. Parse and normalize the property name according to the selected regex
   syntax.
2. Resolve the name to a JVM-backed code-point predicate.
3. Iterate through Unicode code points using the engine's existing scalar-value
   policy.
4. Coalesce adjacent matches into `RuneRange` values.
5. Cache the immutable result by canonical property identity.
6. Compile the result through the existing character-class path.

Production matching does not invoke `Character`, `Pattern`, a map lookup, or a
property predicate per input code point. The execution engines receive only the
same compiled instructions and character-class ranges used before this change.

### JVM Sources

The registry uses public JVM APIs wherever they directly express the property:

- `Character.getType(int)` for general categories
- `Character.UnicodeScript.of(int)` and `forName(String)` for scripts
- `Character.UnicodeBlock.of(int)` and `forName(String)` for blocks
- `Character.isAlphabetic(int)`, `isIdeographic(int)`, `isLetter(int)`, and
  related predicates for binary properties
- `Character.toUpperCase(int)` and `toLowerCase(int)` as the source for simple
  case relationships

The registry contains small definitions for properties that the JVM defines but
does not expose as one public predicate. For example,
`Noncharacter_Code_Point` is:

```java
(codePoint & 0xfffe) == 0xfffe ||
        (codePoint >= 0xfdd0 && codePoint <= 0xfdef)
```

Properties such as join control, hexadecimal digit, Unicode word, and some
whitespace classes are similarly composed from public JVM predicates and small
explicit sets. These definitions follow Java `Pattern` semantics and do not
introduce copied Unicode range tables.

### Property Names And Aliases

Property-name parsing is language metadata rather than Unicode data. The
registry maintains a small explicit mapping from accepted RE2, Java, and Trino
spellings to canonical property identities.

The parser distinguishes:

- general-category names and aliases
- script names and the `Is` form
- block names and the `In` form
- Java binary properties
- Trino's documented name-normalization rules

Alias handling is tested independently from range membership and does not
silently reinterpret a recognized Java spelling as a different RE2 construct.

### Case Folding

`UnicodeCaseFold` builds JVM-derived simple case-equivalence classes instead of
using an upstream-generated table. The parser, character-class builder,
required-match analysis, and single-byte optimizations continue to use an
efficient immutable cycle for each equivalence class.

The canonical relation uses the JVM's upper-case mapping followed by its
lower-case mapping rather than assuming that one `toLowerCase` call is complete.
Exhaustive cycle checks and focused comparisons with case-insensitive
`java.util.regex.Pattern` validate the resulting equivalence classes.
Multi-code-point full case folding is not introduced.

### Initialization And Caching

The implementation does not rescan all Unicode code points for every pattern
compilation:

- Each property is built lazily and cached by canonical identity.
- Published ranges are immutable and safe for concurrent compilation.
- Simple case equivalence is initialized once on first use.
- No production table is generated at build time, which would bind behavior to
  the build JDK instead of the executing JDK.

## Replaced Code

The previous implementation used upstream-generated data in:

- `UnicodeGroups`
- `UnicodeGroupsRanges0` through `UnicodeGroupsRanges7`
- `UnicodeCaseFold`

Migrated consumers include:

- `RegexpParser`
- `CharClassBuilder`
- required-match analysis
- single-byte matcher optimizations

The range files were removed after every consumer migrated to the JVM-backed
registry and the replacement gained exhaustive correctness coverage.

## Correctness Evidence

Tests were added before replacing the existing implementation.

1. Categories, scripts, every JDK block, supported binary properties, and Java
   properties are checked over the complete Unicode code-point domain against
   their corresponding `Character` APIs.
2. Canonical aliases are checked for identity, and unsupported names are
   rejected.
3. The derived case-equivalence relation is validated exhaustively. Focused
   `Pattern` comparisons cover dotted and dotless I, long S, Kelvin sign, and
   Greek sigma.
4. Unicode-version-only upstream differences have explicit expected results
   rather than unexplained golden-test exceptions.
5. Complete pinned-native differential, exhaustive, randomized, UTF-8,
   Latin-1, parser, and compatibility suites pass with the JVM-backed data.

Test failures identify the property or case-equivalence class being checked so
a future JDK upgrade produces a diagnosable difference rather than an
apparently random range-table change.

## Performance Requirements

This is primarily compilation-path behavior and must not regress match
execution.

- Compiled instructions and hot matching loops must retain their current shape.
- Existing specialized matchers must consume precomputed case and class data.
- Common pattern compilation after cache warmup should perform only property
  lookup and ordinary character-class construction.
- First-use property construction and case-table initialization require focused
  benchmarks, but no target-host performance campaign is required merely to
  establish the design.

## Related Feature Scope

JVM-sourced Unicode provides the data foundation for several possible language
extensions, but does not itself add them:

- Unicode blocks and binary properties
- Java property aliases
- Unicode-aware `\d`, `\s`, `\w`, and boundaries
- horizontal and vertical whitespace classes
- named Unicode characters
- character-class intersection and subtraction

Each extension requires a separate syntax and compatibility decision. General
lookaround remains a separate, potentially major engine project. General
backreferences are explicitly out of scope because they are incompatible with
the library's linear-time guarantee.

## Implemented Components

1. Exhaustive JVM-property authority and case-equivalence tests.
2. A cached JVM property registry for categories, scripts, blocks, binary
   properties, aliases, and Java properties.
3. JVM-derived simple case-equivalence classes.
4. JVM-backed parser, analysis, and specialized-matcher consumers.
5. Removal of the generated upstream Unicode range sources.
6. The intentional platform adaptation recorded in `RE2_DECISIONS.md`.
