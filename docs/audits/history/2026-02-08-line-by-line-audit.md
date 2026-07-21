# Line-by-Line Audit Report - 2026-02-08

> Historical record. This document is evidence, not current project status.
> See the repository-root `RE2_TASKS.md` for open work.

## Summary

| File Pair | Differences Found | Priority | Detail File |
|-----------|------------------|----------|-------------|
| Dfa.java vs dfa.cc | 64 | HIGH | [01-dfa.md](2026-02-08-details/01-dfa.md) |
| Prog.java vs prog.cc | 92 | HIGH | [02-prog.md](2026-02-08-details/02-prog.md) |
| Re2.java vs re2.cc | 78 | HIGH | [03-re2.md](2026-02-08-details/03-re2.md) |
| Compiler.java vs compile.cc | 93 | MEDIUM | [04-compiler.md](2026-02-08-details/04-compiler.md) |
| OnePass.java vs onepass.cc | 37 | MEDIUM | [05-onepass.md](2026-02-08-details/05-onepass.md) |
| Nfa.java vs nfa.cc | 38 | MEDIUM | [06-nfa.md](2026-02-08-details/06-nfa.md) |
| BitState.java vs bitstate.cc | 32 | MEDIUM | [07-bitstate.md](2026-02-08-details/07-bitstate.md) |
| RegexpParser.java vs parse.cc | 67 | LOW | [08-regexpparser.md](2026-02-08-details/08-regexpparser.md) |
| Simplifier.java vs simplify.cc | 56 | LOW | [09-simplifier.md](2026-02-08-details/09-simplifier.md) |
| **TOTAL** | **557** | |

**Upstream pin**: `972a15cedd008d846f1a39b2e88ce48d7f166cbd`

---

## Critical Findings

### 1. DFA: Missing Features (HIGH IMPACT)

**Location**: Dfa.java vs dfa.cc

| Missing Feature | Impact | C++ Location |
|----------------|--------|--------------|
| **No Mark support** | kLongestMatch mode relies on Marks to separate thread groups by match start position | dfa.cc:368-418 |
| **No kManyMatch support** | Match IDs, MatchSep, mq parameter all missing | dfa.cc:592-741 |
| **No needflags optimization** | C++ stores needflags in upper bits, conditionally runs empty string pass; Java always runs it | dfa.cc:1022-1114 |
| **ALT_MATCH handled differently** | C++ just advances id; Java explores both branches like ALT | dfa.cc:837-919 |
| **stateToWorkq doesn't expand** | C++ calls AddToQueue for each inst; Java just inserts directly | dfa.cc:821-834 |

**FullMatchState condition differs**:
- C++: `kind_ != Prog::kManyMatch && (kind_ != Prog::kFirstMatch || (it == q->begin() && ip->greedy(prog_))) && (kind_ != Prog::kLongestMatch || !sawmark) && (flag & kFlagMatch)`
- Java: `kind != Kind.FIRST_MATCH || (isFirstEntry && ip.greedy(prog)) && (kind == Kind.LONGEST_MATCH || (flag & FLAG_MATCH) != 0)`

**Dead state check differs**:
- C++: `if (n == 0 && flag == 0) return DeadState;`
- Java: `if (n == 0 && (flag & FLAG_MATCH) == 0) return DEAD;`

---

### 2. Re2: Missing Prefix Optimization (HIGH IMPACT)

**Location**: Re2.java vs re2.cc

| Missing Feature | Impact | C++ Location |
|----------------|--------|--------------|
| **No prefix_, prefix_foldcase_, suffix_regexp_ fields** | Java does not implement the prefix optimization structure | re2.cc:210-284 |
| **Java re-parses pattern for reverse program** | C++ uses stored suffix_regexp_ | re2.cc:287-303 |
| **Java does not upgrade anchor mode** | C++ upgrades anchor based on prog.anchorStart()/anchorEnd() | re2.cc:658-916 |
| **No error logging** | C++ logs with ABSL_LOG; Java silent | Throughout |

**Anchor mode upgrade missing**:
```cpp
// C++ (re2.cc:698-704)
if (prog_->anchor_start() && prog_->anchor_end())
    re_anchor = ANCHOR_BOTH;
else if (prog_->anchor_start() && re_anchor != ANCHOR_BOTH)
    re_anchor = ANCHOR_START;
```
Java has no equivalent.

---

### 3. Prog: Missing Features (MEDIUM IMPACT)

**Location**: Prog.java vs prog.cc

| Difference | Impact |
|-----------|--------|
| Java adds FAIL at position 0 in constructor | Behavioral difference |
| Java missing `inst_count_` tracking in flatten() | No instruction count stats |
| Java missing ShiftDFA for foldcase prefix | Performance for case-insensitive prefix |
| Java uses separate fields vs C++ union in Inst | Memory overhead |
| Java `Inst.matches()` adds bounds check | C++ has undefined behavior for c outside [0,255] |

---

### 4. Compiler: Missing Optimizations (MEDIUM IMPACT)

**Location**: Compiler.java vs compile.cc

| Difference | C++ Location |
|-----------|--------------|
| C++ has ANCHOR_BOTH handling in HAVE_MATCH case | compile.cc:965-970 |
| C++ checks `re->cap() < 0` for non-capturing groups | compile.cc:972-975 |
| C++ has DFA memory budget calculation | compile.cc:1163-1203 |
| C++ has CompileSet for RE2::Set | compile.cc:1219-1263 |

---

### 5. Parser: Architectural Differences (MEDIUM IMPACT)

**Location**: RegexpParser.java vs parse.cc

| Difference | Impact |
|-----------|--------|
| C++ uses stack-based parsing; Java uses recursive descent | Fundamental architecture |
| C++ converts Latin1 to UTF-8; Java keeps bytes as-is | Encoding handling |
| C++ disallows leading zeros in repeat counts; Java allows | `{01,02}` parses differently |
| Java missing `[:ascii:]` POSIX class | Feature gap |
| C++ has alternation prefix factoring; Java does not | Optimization |
| C++ squashes duplicate repeat operators (`**`, `++`); Java does not | Optimization |

---

### 6. Simplifier: Extra Features in Java (LOW IMPACT)

**Location**: Simplifier.java vs simplify.cc

Java has features C++ doesn't:
- `Mode.PARSE` concept for parse-time simplification
- Alternation factoring logic (`factorAlternationForParse`)
- Adjacent literal merging in concat
- Char class merging in alternations
- Flattening nested concat/alternate

C++ has features Java doesn't:
- `ComputeSimple()` short-circuit for already-simple regexps
- `PreVisit` optimization to skip already-simplified

---

## Common Patterns Across Files

### Memory Management
- C++ uses reference counting (Incref/Decref); Java uses GC
- C++ uses PODArray with manual growth; Java uses ArrayList or Arrays.copyOf

### Sentinel Values
- C++ uses NULL pointers; Java uses -1 for unset positions
- C++ uses `flag == 0`; Java uses `(flag & FLAG_MATCH) == 0`

### Debug Assertions
- C++ has `ABSL_DCHECK`, `ABSL_LOG(DFATAL)`; Java omits or throws exceptions

### Loop Control
- C++ uses `goto` labels; Java uses `while(true)` with break or helper methods

### Atomics
- C++ uses `memory_order_acquire/release`; Java uses plain assignments

### Unsigned Types
- C++ uses `uint8_t`, `uint32_t`; Java masks with `& 0xFF`

---

## Behavioral Impact Assessment

### HIGH RISK Differences

1. **DFA FullMatchState condition** - Different conditions may affect when FullMatch optimization triggers
2. **DFA Mark support missing** - kLongestMatch mode may behave differently
3. **Re2 anchor mode upgrade missing** - Some patterns may not get optimal anchor handling
4. **Re2 prefix optimization missing** - Performance impact for prefix-heavy patterns

### MEDIUM RISK Differences

1. **Parser leading zeros** - `{01,02}` parses as `{1,2}` in Java but fails in C++
2. **Compiler ANCHOR_BOTH HAVE_MATCH** - May affect certain anchored patterns
3. **Dead state check** - `flag == 0` vs `(flag & FLAG_MATCH) == 0`

### LOW RISK Differences

1. **Debug assertions removed** - No runtime checks for invalid states
2. **Error logging removed** - Silent failures instead of DFATAL logs
3. **Simplifier extra features** - Java may simplify more aggressively

---

## Recommendations

1. **Investigate DFA FullMatchState condition** - Verify the Java condition is equivalent
2. **Document missing kManyMatch/Mark support** - Add to RE2_DECISIONS.md
3. **Consider adding anchor mode upgrade** - May improve performance
4. **Add [:ascii:] POSIX class** - Feature parity
5. **Consider disallowing leading zeros** - Match C++ behavior

---

## Files Audited

| Java File | C++ File(s) |
|-----------|------------|
| `src/main/java/io/airlift/slice/re2/prog/Dfa.java` | `re2_upstream/re2/dfa.cc` |
| `src/main/java/io/airlift/slice/re2/prog/Prog.java` | `re2_upstream/re2/prog.cc`, `prog.h` |
| `src/main/java/io/airlift/slice/re2/Re2.java` | `re2_upstream/re2/re2.cc`, `re2.h` |
| `src/main/java/io/airlift/slice/re2/prog/Compiler.java` | `re2_upstream/re2/compile.cc` |
| `src/main/java/io/airlift/slice/re2/prog/OnePass.java` | `re2_upstream/re2/onepass.cc` |
| `src/main/java/io/airlift/slice/re2/prog/Nfa.java` | `re2_upstream/re2/nfa.cc` |
| `src/main/java/io/airlift/slice/re2/prog/BitState.java` | `re2_upstream/re2/bitstate.cc` |
| `src/main/java/io/airlift/slice/re2/parse/RegexpParser.java` | `re2_upstream/re2/parse.cc` |
| `src/main/java/io/airlift/slice/re2/ast/Simplifier.java` | `re2_upstream/re2/simplify.cc` |

**Audit Date**: 2026-02-08
**Methodology**: Parallel agent line-by-line comparison per AUDIT_PROCEDURES.md
