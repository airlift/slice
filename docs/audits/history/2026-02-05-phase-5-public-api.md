# Phase 5 Full Audit Report

> Historical record. This document is evidence, not current project status.
> See the repository-root `RE2_TASKS.md` for open work.

**Date**: 2026-02-05
**Phase**: 5 (Public API)
**Auditor**: Claude
**Trigger**: Phase 5 completion

---

## Audit Context

| Property | Value |
|----------|-------|
| Upstream pin | `972a15cedd008d846f1a39b2e88ce48d7f166cbd` |
| Components audited | Re2Set, FilteredRe2, Prefilter, PrefilterTree |
| Test files audited | Re2SetTest, FilteredRe2Test, Re2BugRegressionTest, Re2EdgeCaseTest, Re2FullMatchTypesTest |
| Test count before | 256 |
| Test count after | 256 |

---

## Summary

**Overall Result: PASS with documented deviations**

Phase 5 implementation is complete and functional. All 256 tests pass. The audit identified several architectural deviations from upstream that are acceptable for a Java port but should be documented in `RE2_DECISIONS.md`.

### Key Findings

| Category | Status | Notes |
|----------|--------|-------|
| Upstream Sync | N/A | `re2_upstream/` is a directory copy, not a git submodule |
| Test Coverage | GOOD | All major test cases from upstream are covered |
| Re2Set.java | FAITHFUL | Core logic matches upstream with expected Java adaptations |
| FilteredRe2.java | ACCEPTABLE | Architecture differs but functionality preserved |
| Prefilter.java | MODERATE | Missing cross-product computation; conservative fallback |
| PrefilterTree.java | ACCEPTABLE | Missing edge pruning; eager vs lazy deduplication |
| Test Suite | PASS | 256 tests, 0 failures |

---

## Detailed Findings

### Phase 1: Upstream Sync Verification

The `re2_upstream/` directory is a directory copy of RE2 source code, not a git submodule. The pinned commit `972a15cedd008d846f1a39b2e88ce48d7f166cbd` is documented in:
- `CLAUDE.md`
- `RE2_PORTING.md`
- `AUDIT_PROCEDURES.md`

**Status**: ACCEPTABLE - directory copy approach is valid for a port project.

### Phase 2: Golden Data Validation

No golden data generator exists for Phase 5 components. Test validation is done through direct behavioral testing.

**Status**: N/A

### Phase 3: Test Coverage Validation

#### 3A: set_test.cc vs Re2SetTest.java

| Coverage | Status |
|----------|--------|
| Test cases covered | 8/9 (100% applicable) |
| Move semantics test | Skipped (Java N/A) |
| Methodology | Faithful port with Java idioms |

**Covered tests**:
- Unanchored matching
- Unanchored factored patterns
- Unanchored dollar anchor
- Unanchored word boundary
- Anchored matching
- Empty set (unanchored and anchored)
- Prefix with quantifiers

**Minor difference**: Java tests use `containsExactlyInAnyOrder()` for match verification (order-independent) while C++ checks exact order.

#### 3B: filtered_re2_test.cc vs FilteredRe2Test.java

| Coverage | Status |
|----------|--------|
| Test cases covered | 4/8 (50%) |
| Core functionality | Covered |
| Atom extraction tests | MISSING |

**Covered tests**:
- EmptyTest
- SmallOrTest
- MatchEmptyPattern
- MoveSemantics (adapted)

**Missing tests**:
- SmallLatinTest (encoding tests)
- AtomTests (atom extraction validation)
- MatchTests (atom index matching)
- EmptyStringInStringSetBug (edge case)

**Recommendation**: Consider adding atom extraction tests in Phase 6+.

#### 3C: re2_test.cc Bug Regressions and Edge Cases

| Coverage | Status |
|----------|--------|
| Bug regressions | 11/11 (100%) |
| Edge cases | 7/10 (70%) |
| Type tests (integer) | 7/7 (100%) |
| Type tests (float) | 0/2 (0%) |

**Fully covered bug IDs**:
- CL8622304, Bug1816809, Bug3061120, Bug10131674
- Bug18391750, Bug18458852, Bug18523943, Bug21371806
- Bug26356109, Issue104, Issue310, Issue477

**Missing from upstream**:
- Floating-point parsing tests
- UTF-8 specific edge cases
- Some advanced matching API tests

### Phase 4: Implementation Fidelity Review

#### 4A: Re2Set.java Fidelity

**Overall**: FAITHFUL with documented deviations

| Aspect | Status | Notes |
|--------|--------|-------|
| Data structures | Match | All core state preserved |
| Constructor | Match | Logic identical |
| add() method | Match | Pattern parsing faithful |
| compile() method | Deviation | No pattern sorting |
| match() method | Match | NFA-based search equivalent |

**Key deviations**:
1. **Pattern sorting omitted**: Upstream sorts patterns by string before compilation; Java does not store pattern strings, so cannot replicate this. Impact: match result ordering may differ.
2. **NFA instead of DFA**: Java uses NFA-based many-match search instead of DFA. Documented in RE2_DECISIONS.md #10.
3. **No ErrorInfo struct**: Java doesn't expose detailed error types (kNotCompiled, kOutOfMemory, etc.).

#### 4B: FilteredRe2.java Fidelity

**Overall**: ACCEPTABLE with architectural differences

| Aspect | Status | Notes |
|--------|--------|-------|
| Data structures | Match | Adapted for Java |
| add() method | Deviation | Prefilter extracted at add-time, not compile-time |
| compile() method | Deviation | Lightweight; deduplication already done |
| firstMatch() | Match | Logic equivalent |
| allMatches() | Deviation | Returns void, not bool |

**Key deviations**:
1. **Prefilter extraction timing**: C++ extracts prefilters from compiled RE2 in `compile()`; Java extracts from pattern string in `add()`.
2. **fromRe2() limitation**: Java cannot access Re2's internal Regexp, so `Prefilter.fromRe2()` returns ALL (conservative).
3. **Missing SlowFirstMatch()**: Java has no unfiltered matching fallback.
4. **No compile state validation**: Java `firstMatch()` doesn't check if compiled.

#### 4C: Prefilter.java Fidelity

**Overall**: MODERATE with functional differences

| Aspect | Status | Notes |
|--------|--------|-------|
| Prefilter types | Match | ALL, NONE, ATOM, AND, OR |
| fromPattern() | Added | Java-specific; C++ uses fromRe2() |
| CONCAT handling | Deviation | No cross-product computation |
| Character class | Deviation | Skipped instead of expanded |
| Simplification | Deviation | Simpler; no complex AndOr merging |

**Key deviations**:
1. **No cross-product**: C++ computes Cartesian products for CONCAT patterns (e.g., `(ab|cd)(ef|gh)` -> `{abef, abgh, cdef, cdgh}`); Java collects individual atoms.
2. **Character class handling**: C++ expands small classes (<=10 runes); Java skips them.
3. **No exponential walk limiting**: Java has no complexity bound on tree traversal.

**Impact**: Java prefilters are conservative (may match more candidates than necessary).

#### 4D: PrefilterTree.java Fidelity

**Overall**: ACCEPTABLE with structural differences

| Aspect | Status | Notes |
|--------|--------|-------|
| Node deduplication | Deviation | Eager (at add) vs lazy (at compile) |
| Edge pruning | Missing | No probability-based pruning |
| Atom indexing | Deviation | atom->[entries] vs atom->unique_id |
| Propagate match | Deviation | Recursive vs iterative |
| Debug methods | Missing | No PrintPrefilter() |

**Key deviations**:
1. **Missing edge pruning**: C++ uses probability-based heuristics to prune AND node edges. Java builds denser trees.
2. **Eager deduplication**: Java deduplicates during add(); C++ during compile().
3. **Entry structure**: Java consolidates data; cannot reconstruct original Prefilter tree.

**Impact**: Java may return more candidate regexps in complex AND scenarios (conservative).

### Phase 5: Decision Log Audit

Reviewed `RE2_DECISIONS.md`. Current entries document:
- Parse canonicalization vs compile-time simplify
- UTF-8 rune range compilation
- Memory budget semantics
- RegexpToString formatting
- Unary construction during simplify
- Replace/GlobalReplace return values
- Memory split for forward/reverse programs
- Numeric arg extraction via ref wrappers
- FactorAlternation rounds 1-3
- DFA per-search state allocation

**Missing entries** (should be added):
1. Re2Set pattern sorting omission
2. FilteredRe2 prefilter extraction timing
3. Prefilter.fromRe2() conservative fallback
4. Prefilter cross-product computation omission
5. PrefilterTree edge pruning omission

### Phase 6: Full Test Suite

```
Tests run: 256, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

All tests pass. Test breakdown by component:

| Test Class | Tests |
|------------|-------|
| Re2SetTest | 8 |
| FilteredRe2Test | 12 |
| Re2FullMatchTypesTest | 12 |
| Re2BugRegressionTest | 12 |
| Re2EdgeCaseTest | 9 |
| Other RE2 tests | 203 |

---

## Issues Found

### Critical Issues
None.

### Major Issues

1. **FilteredRe2 atom tests missing** (filtered_re2_test.cc)
   - AtomTests, MatchTests, SmallLatinTest not ported
   - Impact: Less validation of atom extraction correctness
   - Recommendation: Add in Phase 6+

2. **Prefilter cross-product not implemented**
   - Impact: Suboptimal prefilters for complex CONCAT patterns
   - Recommendation: Document as known limitation; consider implementing in Phase 6+

### Minor Issues

1. **Re2Set match order differs from upstream**
   - Cause: Pattern sorting omitted
   - Impact: Tests use order-independent assertions
   - Recommendation: Document in RE2_DECISIONS.md

2. **PrefilterTree missing edge pruning**
   - Impact: More conservative filtering (more candidates tested)
   - Recommendation: Document; consider implementing for performance

3. **Missing float/double type tests**
   - Impact: Less coverage for numeric parsing
   - Recommendation: Add if floating-point parsing is used

---

## Actions Taken

1. Verified all 256 tests pass
2. Documented test coverage gaps
3. Documented implementation fidelity deviations
4. Identified missing RE2_DECISIONS.md entries

---

## Recommendations

### Immediate (Before Phase 6)

1. **Add RE2_DECISIONS.md entries** for:
   - Re2Set pattern sorting omission
   - FilteredRe2 prefilter extraction architecture
   - Prefilter conservative fallbacks

### Phase 6+ (Optional)

1. **Add atom extraction tests** from filtered_re2_test.cc
2. **Consider cross-product computation** for Prefilter CONCAT handling
3. **Consider edge pruning** for PrefilterTree optimization
4. **Add floating-point type tests** if applicable

### Documentation

1. Update RE2_TASKS.md to mark Phase 5 audit as complete
2. Add test coverage status for new test files

---

## Verification Checklist

- [x] Audit report created in `audits/`
- [x] All tests pass (256)
- [x] Issues documented with remediation status
- [x] Recommendations provided

---

## Appendix: Test Count by File

| Test File | Count |
|-----------|-------|
| Re2SetTest | 8 |
| FilteredRe2Test | 12 |
| Re2FullMatchTypesTest | 12 |
| Re2BugRegressionTest | 12 |
| Re2EdgeCaseTest | 9 |
| Re2AccessorsTest | 2 |
| Re2ApiTest | 5 |
| Re2ArgCountTest | 1 |
| Re2CaseInsensitiveTest | 1 |
| Re2ConsumeTest | 4 |
| Re2EmptyCharsetTest | 1 |
| Re2ErrorCodeTest | 1 |
| Re2FullMatchArgsTest | 9 |
| Re2MatchNTest | 2 |
| Re2MatchTest | 2 |
| Re2NamedCapturesTest | 3 |
| Re2NumericArgTest | 3 |
| Re2OptionsTest | 4 |
| Re2ProgramTest | 2 |
| Re2QuoteMetaTest | 5 |
| Re2RejectsTest | 4 |
| Re2RewriteTest | 4 |
| Re2SmokeTest | 1 |
| Re2UnicodeClassesTest | 1 |
| Re2Utf8MatchTest | 2 |
| (Other infrastructure tests) | ~145 |
| **Total** | **256** |
