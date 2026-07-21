# Benchmark Encoding Audit Remediation Log

> Historical record. This document is evidence, not current project status.
> See the repository-root `RE2_TASKS.md` for open work.

**Audit Date**: 2026-02-09
**Upstream pin**: `972a15cedd008d846f1a39b2e88ce48d7f166cbd`

**Status Legend:**
- ✅ FIXED - Issue resolved
- 🔄 IN PROGRESS - Work underway
- 📋 PLANNED - Scheduled for future work
- ⏸️ DEFERRED - Intentionally postponed
- ❌ WONTFIX - Intentionally not fixing

---

## Critical Findings Remediation

### 1. FullMatch Encoding

| Finding | Status | Notes |
|---------|--------|-------|
| FullMatch_DotStar encoding | ✅ FIXED | Java has both `fullMatchDotStar` (UTF-8) and `fullMatchDotStarLatin1` (LATIN1) |
| FullMatch_DotStarDollar encoding | ✅ FIXED | Java has both `fullMatchDotStarDollar` (UTF-8) and `fullMatchDotStarDollarLatin1` (LATIN1) |
| FullMatch_DotStarCapture encoding | ✅ FIXED | Java has both `fullMatchDotStarCapture` (UTF-8) and `fullMatchDotStarCaptureLatin1` (LATIN1) |

**Notes**: Java actually has MORE coverage than C++ - we have UTF-8 variants for all three patterns, while C++ only has a UTF-8 variant for `(?s).*`.

### 2. Parse1 Pattern Mismatch

| Finding | Status | Notes |
|---------|--------|-------|
| Parse1 pattern | ✅ FIXED | Java aligned to C++ pattern `[0-9]+-(.*)`; renamed `parse1Digit*` → `parse1Split*` |

### 3. Search Benchmark Encoding

| Finding | Status | Notes |
|---------|--------|-------|
| Search_Easy0_* encoding | ✅ N/A | Both C++ and Java use UTF-8 default |
| Search_Easy1_* encoding | ✅ N/A | Both C++ and Java use UTF-8 default |
| Search_Medium_* encoding | ✅ N/A | Both C++ and Java use UTF-8 default |
| Search_Hard_* encoding | ✅ N/A | Both C++ and Java use UTF-8 default |
| Search_Parens_* encoding | ✅ N/A | Both C++ and Java use UTF-8 default |

### 4. Text Generation

| Finding | Status | Notes |
|---------|--------|-------|
| Character range | ✅ MATCH | Both use 0x20-0x7F (printable ASCII) |
| Fixed seed | ✅ MATCH | Both use seed=1 |
| RNG algorithm | ⏸️ DEFERRED | Different algorithms (`rand()` vs `Random.nextInt()`), but same distribution |
| Caching strategy | ✅ NO IMPACT | C++ caches 16MB globally; Java generates per-setup |

**Notes**:
- The RNG algorithms produce different sequences, but the character distribution is identical. For performance benchmarking (not correctness testing), this is acceptable.
- **Caching difference does NOT affect benchmark timing**: C++ generates text in static initialization (before `main()`), Java generates in `@Setup(Level.Trial)` (before warmup/measurement). Neither includes text generation in the timed `@Benchmark` method loop.

---

## Coverage Gaps

These are not critical issues - they represent areas where Java has less granular coverage than C++.

### Missing LATIN1 Variants (Future Work - LOW Priority)

Most benchmarks in C++ also use default UTF-8 encoding, so these are not mismatches. Adding LATIN1 variants would enable additional performance analysis but is not required for baseline comparison.

| Benchmark Category | Has UTF-8 | Has LATIN1 | Notes |
|-------------------|-----------|------------|-------|
| Search Easy0/1/Medium/Hard/Parens | ✓ | - | Could add for completeness |
| Search NFA | ✓ | - | Could add for completeness |
| Parse (phone number) | ✓ | - | Not typically encoding-sensitive |
| Compile benchmarks | ✓ | - | Pattern-focused, not encoding-sensitive |

### Missing C++ Benchmarks (Ported to Java)

| C++ Benchmark | Status | Java Class | Notes |
|---------------|--------|------------|-------|
| `Search_Easy2_*` | ✅ FIXED | `BenchmarkRe2SearchExtra` | `(?i)ABCDEFGHIJKLMNOPQRSTUVWXYZ$` - case insensitive |
| `Search_Fanout_*` | ✅ FIXED | `BenchmarkRe2SearchExtra` | Unicode fanout, high NFA load |
| `Search_BigFixed_*` | ✅ FIXED | `BenchmarkRe2SearchExtra` | Dynamic large fixed prefix |
| `Search_Success_*` | ✅ FIXED | `BenchmarkRe2SearchExtra` | Anchored `.*$` success |
| `Search_Success1_*` | ✅ FIXED | `BenchmarkRe2SearchExtra` | Anchored `.*\C$` with BitState |
| `Search_AltMatch_*` | ✅ FIXED | `BenchmarkRe2SearchExtra` | `\C*` anchored match |
| `Search_Digits_*` | ✅ FIXED | `BenchmarkRe2SearchExtra` | Anchored digits with submatches |
| `Parse_SplitHard_*` | ✅ FIXED | `BenchmarkRe2Parse` | `[0-9]+.(.*)` ambiguous pattern |
| `Parse_SplitBig1_*` | ✅ FIXED | `BenchmarkRe2Parse` | 100K 'x' + phone input |
| `Parse_SplitBig2_*` | ✅ FIXED | `BenchmarkRe2Parse` | "650-253-" + 100K '0' input |
| `SearchPhone_*` | ✅ FIXED | `BenchmarkRe2Parse` | Phone pattern search, scaled sizes |
| `EmptyPartialMatch*` | ✅ FIXED | `BenchmarkRe2Misc` | Empty pattern edge case |
| `DotMatch*` | ✅ FIXED | `BenchmarkRe2Misc` | `(?-s)^(.+)` on HTTP text |
| `ASCIIMatch*` | ✅ FIXED | `BenchmarkRe2Misc` | `(?-s)^([ -~]+)` printable ASCII |
| `PossibleMatchRange_*` | ✅ FIXED | `BenchmarkRe2Misc` | 4 pattern variants |
| `FindAndConsume` | ❌ WONTFIX | - | API not available in Java port |
| `CacheFill_*` | ❌ WONTFIX | - | Disabled in C++ upstream (TODO marker) |

**Summary**: 15 of 17 missing benchmarks ported. Remaining 2 cannot be ported (API not available or disabled in C++).

---

## Text Size Coverage

| Benchmark | C++ Sizes | Java Sizes | Gap |
|-----------|-----------|------------|-----|
| Search DFA/Re2 | 8 → 16M (22 points) | 8 → 16M (8 points) | Sparser sampling |
| Search NFA | 8 → 256K (16 points) | 8 → 256K (6 points) | Sparser sampling |
| FullMatch | 8 → 2M (19 points) | 8 → 2M (7 points) | Sparser sampling |

**Status**: ⏸️ DEFERRED - The sparser sampling is intentional to reduce JMH benchmark runtime. Key inflection points are still covered.

---

## Action Items

None required. The audit found no critical issues requiring remediation.

### Completed Actions (Pre-Audit)

1. ✅ Added LATIN1 variants to `BenchmarkRe2FullMatch.java`
2. ✅ Implemented `compileRe2Latin1()` helper in `Re2BenchmarkRunner.java`
3. ✅ Aligned `randomText()` character range with C++ `RandomText()`
4. ✅ Used matching fixed seed for reproducibility

### Optional Future Work (Not Blocking)

1. 📋 Add LATIN1 variants to Search benchmarks (for completeness)
2. ✅ ~~Add missing C++ benchmark variants~~ - **COMPLETED 2026-02-09**: Ported 15 benchmarks in 3 files
3. 📋 Increase text size granularity (if more detailed scaling analysis needed)

### New Benchmark Files Added (2026-02-09)

| File | Benchmarks | Notes |
|------|------------|-------|
| `BenchmarkRe2SearchExtra.java` | 21 methods | Easy2, Fanout, BigFixed, Success, AltMatch, Digits |
| `BenchmarkRe2Parse.java` | +7 methods | SplitHard (4), SplitBig1/2 (2), SearchPhone (1) |
| `BenchmarkRe2Misc.java` | 7 methods | Empty, DotMatch, ASCIIMatch, PossibleMatchRange (4) |
