# Cleanup Decisions Log

This file tracks cleanup-specific architectural/style decisions.

Policy: keep this focused on cleanup conventions, not behavior deviations (those belong in `RE2_DECISIONS.md`).

## 2026-02-15: Direct Internal Renames (No Transition Shims)

- **Decision**: for internal cleanup refactors in this tree, use direct renames and direct API cleanup; do not add temporary compatibility adapters.
- **Context**: this RE2 tree is new and not published.
- **Rationale**:
  - avoids prolonged mixed naming
  - keeps code simpler and easier to read
  - reduces one-off compatibility maintenance

## 2026-02-15: Java Naming for Count Accessors

- **Decision**: prefer Java-style count names (`runeCount`, `rangeCount`, `subCount`) over upstream port spellings (`nrunes`, `nranges`, `nsub`).
- **Rationale**: improves readability for Java engineers and aligns with local coding norms.

## 2026-02-15: AST Boundary Model

- **Decision**: treat `Regexp` as the recursive AST node; treat `CharClass`, `RuneRange`, and parse-status types as supporting values.
- **Rationale**:
  - aligns type structure with actual tree structure
  - reduces conceptual noise for maintainers
  - avoids implying multiple independent AST node families when only one exists

## 2026-02-15: Split-Word Java Naming

- **Decision**: use split-word Java camelCase names for multi-word concepts (`foldCase`, `nonGreedy`) and keep constant flags as upper snake case (`FOLD_CASE`, `NON_GREEDY`).
- **Rationale**:
  - consistent Java naming style across fields, parameters, locals, and accessors
  - clearer relationship between constant flags and derived booleans

## 2026-02-15: Javadoc and Provenance Comment Style

- **Decision**:
  - use explicit `<p>` markers for Javadoc paragraph breaks
  - do not use empty `*` lines in Javadocs
  - avoid generic "Ported from ..." comments in routine class/method docs
- **Rationale**:
  - consistent with local style/checkstyle expectations
  - keeps docs focused on intent and invariants
  - preserves source references only where they are useful for future upstream bug/behavior audits

## 2026-02-15: RuneRange Natural Ordering

- **Decision**: make `RuneRange` naturally orderable (`Comparable<RuneRange>`) by `(lo, hi)`.
- **Rationale**:
  - removes repeated custom comparators
  - simplifies use of standard binary search utilities
  - centralizes ordering semantics in the type itself

## 2026-02-15: CharClass Equality Encapsulation

- **Decision**: define deep equality and hash code on `CharClass` and remove external char-class-specific equality helpers.
- **Rationale**:
  - keeps value semantics with the value object
  - avoids duplication and divergence in consumers
  - improves encapsulation

## 2026-02-16: Selective Walker Usage

- **Decision**: use direct traversal for simple AST scans; keep `RegexpWalker` only where traversal mechanics are genuinely needed (post-order child-result composition, bounded visits, short-visit behavior).
- **Rationale**:
  - reduces abstraction overhead for straightforward logic
  - improves readability for Java-first maintainers
  - preserves existing safety/performance properties where walker mechanics matter

## 2026-02-16: Capture Map Ordering Is Non-Contractual

- **Decision**: treat capture map iteration order as non-contractual unless explicitly documented; default to `HashMap` for capture-name lookups.
- **Rationale**:
  - communicates that lookup is the API contract, not iteration order
  - avoids accidental reliance on insertion order
  - keeps implementation intent aligned with current usage

## 2026-02-16: Explicit Traversal Order in Stack Walks

- **Decision**: when using explicit LIFO stacks for left-to-right AST traversal, push children in reverse order and include a short explanatory comment.
- **Rationale**:
  - prevents subtle ordering regressions (for example, first-win semantics)
  - makes traversal intent obvious during code review

## 2026-02-16: Prefer Switch Expressions

- **Decision**: prefer switch expressions over classic switch statements when computing a value.
- **Rationale**:
  - improves readability with explicit exhaustiveness
  - avoids accidental fall-through patterns
  - keeps value-producing control flow concise

## 2026-02-16: Type-Owned Equality and Null-Safe Call Sites

- **Decision**:
  - implement `equals`/`hashCode` on value-like domain types (`Regexp`, `Slice`, `CharClass`, etc.)
  - remove static ad hoc equality helpers once value equality exists on the type
  - use `Objects.equals(left, right)` at nullable call sites
- **Rationale**:
  - encapsulates value semantics with the owning type
  - reduces duplicate comparison logic and drift
  - keeps null-handling explicit where needed

## 2026-02-16: Minimize Core AST Public Surface

- **Decision**: remove AST helper methods from core runtime types when they are only exercised by dedicated tests and not used by production code paths.
- **Rationale**:
  - keeps the API focused on parser/compiler/runtime needs
  - lowers maintenance burden for non-user-facing helpers
  - avoids preserving legacy analysis hooks by default

## 2026-02-16: Expand Non-Index Abbreviations

- **Decision**: prefer descriptive local names for non-trivial logic and avoid short bound/precedence abbreviations (`lo`, `hi`, `prec`).
- **Rationale**:
  - improves readability for Java-first maintainers
  - reduces ambiguity during maintenance and code review
