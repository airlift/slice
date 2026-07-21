# Code Comments Guide

For the broader philosophy behind this guide, see
[Development Philosophy](development-philosophy.md).

## Purpose

Comments should reduce reader effort where code alone is not enough. They are required when they
carry information that is hard to infer from local code.

Good comments explain:

- Why a design choice exists.
- What invariant must remain true.
- What non-obvious behavior is intentional.
- High-level steps in dense or unusual logic.
- Protocol/file-format details that are easy to miss.

Avoid comments that only restate obvious code.

## Baseline Rule

Prefer self-explanatory code first (naming, structure, small methods). Add comments where a normal
maintainer would still have to reverse-engineer intent.

Use this test:

- If removed, would a reviewer likely misread the code, miss a constraint, or need to chase
  references/usages to understand intent?
  - If yes, keep/add the comment.
  - If no, remove it.

## Comment Density Scales With Complexity

Comment sparingly in straightforward code.
Comment more in complex, dense, or non-idiomatic code.

Examples where higher comment density is expected:

- Bit-level math and hashing internals.
- Locking/concurrency patterns that are uncommon or subtle.
- Parser/planner logic with multiple transformation stages.
- Binary/file-format readers with reserved/skipped regions.
- Advanced framework patterns that are unusual in normal Java code.

## Patterns To Keep

Keep comments like these when accurate:

- Non-obvious double-check behavior in a lock path to avoid unnecessary work.
- Meaning of ambiguous constants (for example, what a hash seed is used for).
- Math rationale for non-trivial mapping formulas (for example, unsigned multiply-high mapping to
  avoid modulo bias).
- Notes that explain reserved/skipped sections when reading structured file formats.
- Step markers that outline the phases of a complicated algorithm.

## Required Comments: Empty Catch Blocks

Empty `catch` blocks must contain a comment to satisfy style checks.

Rules:

- Do not remove the comment from an empty `catch` block.
- Do not "clean up" these comments away during comment refactors.
- Keep the comment short and factual (for example, why swallowing is intentional).

## Review Checklist For Comment Changes

When editing comments in a PR or fixup commit:

1. Remove only comments that are truly obvious/noise.
2. Keep or improve comments that explain non-obvious intent.
3. Preserve required empty-catch comments.
4. Verify comment text still matches behavior after code changes.
