# Line-by-Line Audit Remediation Log

> Historical record. This document is evidence, not current project status.
> See the repository-root `RE2_TASKS.md` for open work.

This file tracks remediation of issues identified in the [2026-02-08 line-by-line audit](2026-02-08-line-by-line-audit.md).

**Status Legend:**
- ✅ FIXED - Issue resolved
- 🔄 IN PROGRESS - Work underway
- 📋 PLANNED - Scheduled for future work
- ⏸️ DEFERRED - Intentionally postponed
- ❌ WONTFIX - Intentionally not fixing (documented in RE2_DECISIONS.md)

---

## Critical Findings Remediation

### 1. DFA: Missing Features

| Finding | Status | Notes |
|---------|--------|-------|
| No Mark support | ✅ FIXED | Implemented in Run 003 (commit e11402d) |
| No kManyMatch support | ✅ FIXED | Implemented in Run 003 (commit e11402d) |
| No needflags optimization | ✅ FIXED | Implemented in Run 003 (commit e11402d) - 59-69% performance improvement on Easy1 |
| ALT_MATCH handled differently | ⏸️ DEFERRED | Current implementation correct for supported modes |
| stateToWorkq doesn't expand | ✅ FIXED | Now calls addToQueue for instruction expansion |
| FullMatchState condition differs | ✅ FIXED | Condition now matches upstream for FIRST_MATCH/LONGEST_MATCH |
| Dead state check differs | ✅ FIXED | Using `(flag & FLAG_MATCH) == 0` (Run 003) |

### 2. Re2: Missing Prefix Optimization

| Finding | Status | Notes |
|---------|--------|-------|
| No prefix_, prefix_foldcase_, suffix_regexp_ fields | ✅ FIXED | Added `suffixRegexp`, `prefixBytes`, `prefixFoldcase` fields. Forward program uses full pattern; reverse program compiled from stored `suffixRegexp` (avoids re-parsing). |
| Java re-parses pattern for reverse program | ✅ FIXED | `compileReverse()` now uses stored `suffixRegexp` instead of re-parsing |
| Java does not upgrade anchor mode | ✅ FIXED | Already implemented - anchor mode upgrade at Re2.java:1187-1191 (test: `Re2MatchTest#testAnchorModeUpgrade`) |
| No error logging | ❌ WONTFIX | Design decision - Java callers check return values |

### 3. Prog: Missing Features

| Finding | Status | Notes |
|---------|--------|-------|
| Java adds FAIL at position 0 in constructor | ❌ WONTFIX | Intentional design choice |
| Java missing inst_count_ tracking in flatten() | ✅ FIXED | Added `int[] instCount` field, populated in `flatten()` fourth pass. Added `getInstCount()` getters. Test: `ProgFlattenTest#testInstCountTracking` |
| Java missing ShiftDFA for foldcase prefix | ✅ FIXED | Implemented `buildShiftDfa()` and `prefixAccelShiftDfa()` ported from upstream. Processes 8 bytes at a time for case-insensitive prefix search. Test: `UpstreamPrefixAccelTest#testFoldcasePrefixAccelShiftDfa` |
| Java uses separate fields vs C++ union in Inst | ❌ WONTFIX | Java doesn't have unions; memory acceptable |
| Java Inst.matches() adds bounds check | ❌ WONTFIX | Safety > performance for edge case |

### 4. Compiler: Missing Optimizations

| Finding | Status | Notes |
|---------|--------|-------|
| ANCHOR_BOTH handling in HAVE_MATCH case | ✅ FIXED | Handled at match time in Re2Set (line 279: `!anchoredAtEnd \|\| atEnd` check), test: `Re2SetTest#testAnchored` |
| C++ checks re->cap() < 0 for non-capturing groups | ✅ FIXED | Parser handles this - `(?:...)` returns sub directly without CAPTURE (test: `RegexpParserTest#testParseNonCapturingGroup`) |
| DFA memory budget calculation | ✅ FIXED | Implemented in DFA caching (see RE2_DECISIONS.md) |
| CompileSet for RE2::Set | ✅ FIXED | Re2Set implemented |

### 5. Parser: Architectural Differences

| Finding | Status | Notes |
|---------|--------|-------|
| Stack-based vs recursive descent | ❌ WONTFIX | Architectural choice, functionally equivalent |
| Latin1 to UTF-8 conversion | ❌ WONTFIX | Java keeps bytes as-is, correct behavior |
| Leading zeros in repeat counts | ✅ FIXED | `isValidRepeatBrace()` now validates full repeat syntax (commit d1338ec, test: `RegexpParserTest#testLeadingZerosInRepeatTreatedAsLiteral`) |
| Missing [:ascii:] POSIX class | ✅ FIXED | Added `case "ascii"` in `posixCharClass()` (commit c8b4a4f, test: `RegexpParserTest#testPosixAsciiCharClass`) |
| Alternation prefix factoring | ✅ FIXED | Already implemented in `Simplifier.factorAlternationForParse()` (test: `RegexpParserTest#testAlternationPrefixFactoring`) |
| Squashing duplicate repeat operators | ✅ FIXED | Already implemented in `Regexp.starPlusOrQuest()` (test: `RegexpParserTest#testSquashDuplicateRepeatOperators`) |

### 6. Simplifier: Extra Features in Java

| Finding | Status | Notes |
|---------|--------|-------|
| Mode.PARSE concept | ❌ WONTFIX | Java addition, beneficial |
| Alternation factoring logic | ❌ WONTFIX | Java addition, beneficial |
| Missing ComputeSimple() short-circuit | ⏸️ DEFERRED | Cannot implement as-is. Java sets `simple=true` at construction time (for immutability), while C++ sets `simple_` after simplification. Short-circuiting would skip needed transformations (e.g., empty CHAR_CLASS → NO_MATCH). See code comments in Simplifier.java. |

---

## Java Improvements Over C++

These are intentional enhancements where Java goes beyond C++ RE2:

| Enhancement | Date | Description |
|-------------|------|-------------|
| UTF-8 `(?s).*` O(1) | 2026-02-09 | `matchesAnyString` flag enables O(1) FullMatch for UTF-8 mode. C++ UTF-8 is O(n). See RE2_DECISIONS.md. |

---

## Remediation History

### 2026-02-09: UTF-8 DotStar O(1) Optimization

**Issue:** DotStar UTF-8 was O(n) in Java while C++ benchmark (LATIN1) showed O(1).

**Investigation:** C++ benchmark explicitly uses LATIN1 mode. C++ UTF-8 is also O(n) because ALT_MATCH optimization only works for single-byte programs.

**Resolution:** Implemented higher-level `matchesAnyString` detection that works for both UTF-8 and LATIN1.

**Files changed:**
- `Prog.java` - Added `matchesAnyString` flag
- `Compiler.java` - Added `isMatchesAnyStringPattern()` detection
- `Re2.java` - Added O(1) short-circuit in `match()`
- `FullMatchStateTest.java` - Added verification tests

**Result:** Java UTF-8 is now O(1), better than C++ UTF-8 (O(n)).

### 2026-02-08: Run 003 DFA Improvements

**Issues addressed:**
- Mark support for LONGEST_MATCH mode
- kManyMatch support
- needflags optimization
- FLAG_MATCH vs flag==0 dead state check
- stateToWorkq instruction expansion

**Files changed:** `Dfa.java`, `Prog.java`

**Result:** Easy1 59-69% faster, FullMatch DotStar 42-53% faster.

---

## Next Steps

All planned optimizations have been implemented. Remaining item:

1. **ComputeSimple() short-circuit** - Cannot be implemented due to architectural difference between Java (immutable Regexp with `simple=true` at construction) and C++ (mutable `simple_` set after simplification).

---

### 2026-02-09: Audit Remediation Implementation

**Issues addressed:**
- [:ascii:] POSIX class - Added support
- Leading zeros in repeat counts - Now treated as literals per C++ behavior
- Verified existing implementations: alternation prefix factoring, repeat operator squashing, anchor mode upgrade, non-capturing group optimization, ANCHOR_BOTH handling

**Files changed:**
- `RegexpParser.java` - Added [:ascii:], improved isValidRepeatBrace()
- `RegexpParserTest.java` - Added tests for all parser features
- `Re2MatchTest.java` - Added tests for anchor mode and prefix optimization

**Result:** All 11 planned items addressed. 8 items fixed/verified, 3 items deferred as lower-priority optimizations.

---

### 2026-02-10: Full Audit Remediation Implementation

**Issues addressed (all planned optimizations):**

1. **Prefix/suffix_regexp_ structure** - Added `suffixRegexp`, `prefixBytes`, `prefixFoldcase` fields to Re2.java. Forward program compiled from full pattern; reverse program compiled from stored `suffixRegexp` (no re-parsing).

2. **ShiftDFA for foldcase prefix** - Implemented `buildShiftDfa()` and `prefixAccelShiftDfa()` in Prog.java, ported from upstream prog.cc. Uses 256-entry lookup table to process 8 bytes at a time for ~2x speedup on case-insensitive prefix search.

3. **inst_count_ tracking in flatten()** - Added `int[] instCount` field to Prog.java, populated during flatten() fourth pass. Added `getInstCount(InstOp)` and `getInstCount()` accessor methods.

4. **ComputeSimple() short-circuit** - Attempted but cannot implement. Java's immutable Regexp sets `simple=true` at construction, while C++ sets `simple_` after simplification. Short-circuiting would skip transformations like empty CHAR_CLASS → NO_MATCH.

5. **numCaptures() method** - Added `numCaptures()` method to Regexp.java (ported from upstream `NumCaptures()`), uses walker to count CAPTURE nodes.

**Files changed:**
- `Re2.java` - Added prefix/suffix fields, updated constructor, `build()`, `compileReverse()`
- `Prog.java` - Added `instCount[]`, `prefixDfa[]`, `buildShiftDfa()`, `prefixAccelShiftDfa()`
- `Regexp.java` - Added `numCaptures()` method and `NumCapturesWalker` class
- `Simplifier.java` - Documented why ComputeSimple() short-circuit cannot work

**Tests added:**
- `ProgFlattenTest#testInstCountTracking`
- `UpstreamPrefixAccelTest#testFoldcasePrefixAccelShiftDfa`
- `SimplifierTest#testSimpleFieldIsSet`

**Result:** All 2107 tests pass. All planned optimizations implemented except ComputeSimple() short-circuit (documented as architecturally infeasible).

---

**Last Updated:** 2026-02-10
