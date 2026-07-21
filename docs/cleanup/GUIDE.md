# RE2 Cleanup Guide

This guide defines how to clean up the RE2 Java port safely, with explicit quality and performance gates.

## Goals

1. Improve readability and maintainability for Java engineers.
2. Preserve upstream RE2 behavior.
3. Preserve or improve runtime performance.

## Non-Goals

1. Introducing behavioral changes as part of style-only cleanup.
2. Sacrificing hot-path performance for stylistic consistency.
3. Leaving temporary compatibility shims for internal-only refactors.

## Core Rules

1. Follow upstream RE2 behavior by default.
2. For internal cleanup refactors, prefer direct renames over transition APIs.
3. Separate cleanup-only changes from behavior/performance changes whenever practical.
4. Validate changes at the risk tier required below before keeping them.

## Risk Tiers

### Tier 0: Mechanical Cleanup (Low Risk)

Examples:
- Variable/method renames
- Comment cleanup
- Method ordering
- Local loop readability improvements

Typical areas:
- AST helpers
- Test code
- Non-hot utility methods

### Tier 1: Data-Shape Cleanup (Medium Risk)

Examples:
- Class to record conversion
- Value-semantics cleanup (`equals`/`hashCode`)

Typical areas:
- AST value objects
- Parser status/result wrappers

### Tier 2: Structural Refactor (High Risk)

Examples:
- Extracting helpers from large methods
- Reorganizing parser/compiler control flow without intended behavior changes

Typical areas:
- parser and compiler code

### Tier 3: Hot-Path Refactor (Critical Risk)

Examples:
- Changes in DFA search loops
- Changes in `Re2.matchInto()` engine cascade
- Changes in prefix acceleration, bytemap, state caching

Typical areas:
- `Dfa`
- `Prog` hot methods
- `Re2.matchInto`

## Autonomous Cleanup Mode

Use this mode for repetitive cleanup so the agent can execute full passes with minimal prompts.

### Default Autonomous Scope

Without asking for step-by-step confirmation, proceed with:
1. Tier 0 cleanup across targeted packages/classes.
2. Tier 1 cleanup where semantics are clearly preserved.
3. Test updates required by renamed or removed internal APIs.
4. Local docs updates needed to keep guidance and code aligned.

### Basic Cleanup Bundle (Run By Default)

When doing broad cleanup passes, include these by default:
1. Rename abbreviated locals/params to descriptive names (except tiny loop indexes).
2. Normalize camelCase split-word naming for fields, locals, accessors, and booleans.
3. Convert value-producing classic `switch` blocks to switch expressions.
4. Replace obvious hand-rolled helpers with clear JDK equivalents when behavior is unchanged.
5. Remove dead/private-unused helpers surfaced by the refactor.
6. Clean Javadocs to follow `<p>` and non-empty-line conventions.
7. Keep type-owned equality (`equals`/`hashCode`) and remove duplicate ad hoc equality helpers.

### Stop-And-Ask Boundaries

Pause and ask before proceeding when any of these are true:
1. Intended change crosses into Tier 3 hot-path behavior.
2. Behavior is ambiguous versus upstream RE2 semantics.
3. Public API semantics would change in a user-visible way not already agreed.
4. A change requires choosing between two plausible semantics, not just style.

### Autonomous Validation Defaults

For autonomous cleanup batches, run:
1. `./mvnw -DskipTests test-compile`
2. `./mvnw "-Dtest=**/re2/**/Test*" test` (Tier 2+ or any cross-package cleanup)
3. `./mvnw install` before final handoff for large multi-file batches

### Autonomous Handoff Format

Summaries should always include:
1. What changed (files/classes and cleanup categories)
2. Why it is safe (behavior/performance expectation)
3. Validation commands run and outcomes
4. Any deferred follow-ups or open risks

## Required Validation Matrix

### Tier 0

1. `./mvnw -q -DskipTests test-compile`
2. Focused tests for touched package/classes

### Tier 1

1. Tier 0 checks
2. Broader subsystem tests (AST/parse/prog slices as relevant)

### Tier 2

1. Tier 1 checks
2. RE2 suite:
   - `./mvnw "-Dtest=**/re2/**/Test*" test`

### Tier 3

1. Tier 2 checks
2. Benchmark guard runs for impacted workloads
3. Rerun noisy regressions before decision
4. Do not keep persistent regressions beyond policy threshold

## Benchmark Guard Policy

Use the same strict policy used in campaign work:

1. Treat persistent regressions over `2%` as reject for protected metrics.
2. Re-run suspected regressions before final decision.
3. Prefer reverting a candidate over keeping uncertain wins.
4. Compare against the exact benchmark affected by the code path.

## Java Cleanup Conventions

1. Prefer descriptive names:
   - `runeCount`, `rangeCount`, `subCount`
2. Prefer standard library primitives where clear and correct:
   - `Arrays.binarySearch`, `Collections.binarySearch`, etc.
3. Keep hot loops simple and predictable; avoid readability changes that alter generated machine code in critical paths without guard benchmarks.
4. Keep comments focused on invariants and intent, not line-by-line narration.
5. Prefer switch expressions over classic switch statements when computing a value (clearer exhaustiveness and less fall-through noise).
6. Remove redundant parameter/field alias locals inherited from porting style (for example, `localX = x`) when no transformation is occurring.
7. Keep locals only when they provide real semantic value:
   - normalized/defaulted working values
   - volatile/concurrency snapshots
   - caching of potentially expensive repeated calls
8. In byte-oriented RE2 logic, avoid converting core matching/parsing data to `String` for intermediate computation; prefer operating on `Slice`, `byte[]`, runes, or code points directly.
9. If a `String` conversion is required at an API boundary, keep it localized and document why.

## Naming Conventions

1. Use Java camelCase for local variables, parameters, fields, and record components.
2. Keep parse/flag constants in upper snake case (for example, `FOLD_CASE`, `NON_GREEDY`) but use camelCase names for values derived from them (`foldCase`, `nonGreedy`).
3. Prefer expanded words over compressed names (`foldCase`, not `foldcase`; `nonGreedy`, not `nongreedy`).
4. Default rule: do not use abbreviations. Name things what they are (`instruction`, `prefilter`, `regexp`, `builder`, `characterClass`, `runQueue`, `nextQueue`, `stackPointer`).
5. Treat abbreviation examples as guidance, not a complete whitelist/blacklist; follow the spirit of readability over short names.
6. Single-character variable names are exceptional:
   - allow `i` for a simple, single-level index loop
   - allow `c` when it clearly means a character/code point/byte value in tight parsing logic
   - otherwise use descriptive names
7. For nested loops, avoid `j`/`k` style names; use descriptive names for the inner loop index/position.
8. `op` is acceptable when the variable/field type is an operation enum/tag type (for example, `RegexpOp`, `PrefilterOp`).
9. `id` is acceptable when the value is an identifier (for example, `matchId`, `entryId`, `parentId`).
10. Do not use `len`; use `length` or `count` based on meaning.
11. Avoid abbreviated bound names in non-trivial logic (`lo`, `hi`, `prec`); prefer descriptive names such as `lowerBound`, `upperBound`, and `precedence`.
12. Prefer full type names for domain classes over abbreviations (for example, `Instruction` over `Inst`) unless constrained by third-party API shape.

## Javadoc and Comment Conventions

1. In Javadocs, use explicit paragraph markers (`<p>`) for paragraph breaks.
2. Do not use empty Javadoc lines (`*` alone). If a paragraph break is needed, use `* <p>`.
3. Keep required separation before block tags (`@param`, `@return`, `@see`) to satisfy checkstyle.
4. Avoid generic provenance comments like "Ported from RE2" on classes/methods.
5. Keep source references only when they materially help future C++ cross-checks (for complex logic, tricky invariants, or performance-sensitive behavior).

## AST Boundary Convention

1. Treat `Regexp` as the recursive AST node type.
2. Treat `CharClass`, `RuneRange`, and parser status/result wrappers as supporting value types, not AST node kinds.

## AST Traversal Convention

1. Prefer direct traversal with modern Java control flow (`switch`, explicit stack/queue) for simple read-only AST scans.
2. Keep `RegexpWalker` for complex traversals that need post-order child-result composition, bounded traversal (`walkExponential`), or short-visit fallback behavior.
3. When using an explicit LIFO stack but requiring left-to-right child visitation, push children in reverse order and document that intent with a brief comment.

## Capture Map Convention

1. Do not imply iteration-order guarantees unless the API contract explicitly requires them.
2. For named-capture lookup maps, prefer `HashMap` when only key lookup semantics are required.
3. If deterministic iteration order is intentionally required, use `LinkedHashMap` and state the reason in a short comment or Javadoc.

## Equality Convention

1. Prefer type-owned `equals`/`hashCode` for value-like types (`Regexp`, `Slice`, etc.).
2. Avoid static ad hoc equality helpers once value equality exists on the type.
3. Use `Objects.equals(left, right)` at call sites that require null-safe comparison of possibly-null references.
4. Prefer normal imports for common JDK utility types (`Arrays`, `Objects`) over repeated fully-qualified usage in method bodies.

## AST API Surface Convention

1. Keep AST public surface minimal and focused on runtime/compiler needs.
2. Remove public AST helper methods that are only used by dedicated tests and not by production code paths.
3. If compatibility-analysis helpers are still needed, prefer keeping them in test/tooling code instead of core AST API.

## Change Planning Template

For each cleanup PR or commit, include:

1. `Tier`: `0`, `1`, `2`, or `3`
2. `Scope`: files/classes touched
3. `Intent`: what readability/maintainability issue is being fixed
4. `Behavior impact`: expected `none` or explicit change
5. `Performance impact`: expected `none` or explicit benchmark plan
6. `Validation run`: commands executed and results

## Keep/Reject Decision Template

Use this format in PR description or campaign logs:

1. `Change`: one-line summary
2. `Expected impact`: readability/perf/behavior
3. `Observed behavior`: pass/fail against targeted tests
4. `Observed performance`: pass/fail against benchmark guard
5. `Decision`: `keep` or `revert`

## Full Build Command

Run full project verification with:

```bash
./mvnw -q install
```

If intentionally skipping tests:

```bash
./mvnw -q -DskipTests install
```

Do not skip tests for Tier 2/Tier 3 cleanup unless explicitly directed.
