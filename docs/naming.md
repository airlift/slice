# Naming Guide

For the broader philosophy behind this guide, see
[Development Philosophy](development-philosophy.md).

## Purpose

Name things so a reader can understand intent without tribal knowledge. Prefer names that reduce
review time, debugging time, and accidental misuse.

## Core Rules

- Prefer descriptive names over short or clever names.
- Avoid obscure abbreviations, slang, and inside jokes.
- Use names that describe meaning, not Java type.
- Keep terminology consistent for the same concept across nearby code.
- Prefer consistency with established local conventions over global style preferences.

## Abbreviations

Avoid abbreviations unless they are already standard and widely understood.

Acceptable examples:

- `max`, `min`, `ttl`
- product/protocol terms used as proper nouns: `sql`, `csv`, `rest`

Avoid unclear abbreviations:

- `cfg` -> use `config`
- `mgr` -> use `manager` (or the specific role name)
- `obj` -> use the actual domain name
- `val` -> use `value` (or a specific value name)

Exception for established conventions:

- If a module or pattern consistently uses a conventional short name, keep it for local consistency.
- `ctx` is acceptable in code paths where context objects are conventionally named `ctx`
  (for example parser/visitor context parameters).
- Do not introduce a new local convention just to shorten names; this exception is for existing patterns.

## Name Meaning, Not Type

Prefer semantic names:

- `nextPlayerId` instead of `nextIdInt`
- `lookupVersion` instead of `lookupVersionLong`
- `hasPermission` instead of `permissionBoolean`

## Scope and Length

- Keep names as short as possible, but no shorter than clarity allows.
- Larger scope requires more descriptive names.
- Small local scopes can use shorter names when meaning is obvious.

## Temporary Variables

Avoid generic placeholders like `temp` or `tmp`.

Prefer names that describe the actual content or stage:

- `downloadedIndexPath`
- `parsedLookupSchema`
- `previousLookupVersion`

## Booleans

Boolean names must make `true`/`false` obvious.

Prefer forms like:

- `isReady`
- `hasEntries`
- `foundPlayer`
- `shouldRetry`

Avoid ambiguous names:

- `status`
- `check`
- `flag`

## Constants and Loop Variables

- Use `UPPER_CASE` for constants.
- In simple one-level loops, `i` is acceptable.
- Do not rename `i` to a longer name unless it materially improves clarity.
- For nested loops, multi-index logic, or non-trivial indexing, use descriptive names such as
  `playerIndex`, `bucketIndex`, `entryIndex`.
- Prefer names that are grep-friendly and easy to search.

## Consistency in APIs and Modules

For similar operations, choose one naming family and keep it consistent:

- `getX` vs `fetchX`
- `createX` vs `buildX`
- `lookupX` vs `resolveX`

Do not mix terms for the same concept without a clear semantic reason.

## Quick Review Checklist

When adding or renaming variables:

1. Can a new engineer infer meaning without code archaeology?
2. Is this name domain-oriented rather than type-oriented?
3. Is abbreviation usage obvious and standard?
4. Is terminology consistent with nearby code?
5. For booleans, is the `true` case unambiguous?
