# Golden Output Comparison Template

Use this template to verify that C++ and Java produce identical intermediate representations for the same pattern.

---

## Golden Comparison Report

**Pattern Category**: [e.g., DotStar, Prefix, Character Classes]
**Audit Date**: YYYY-MM-DD
**Auditor**: [agent/person]

---

## Patterns Tested

| # | Pattern | Options | C++/Java Match? |
|---|---------|---------|-----------------|
| 1 | `(?s).*` | LATIN1 | YES/NO |
| 2 | `ABCD.*` | UTF-8 | YES/NO |

---

## Pattern #1: `[pattern]`

### Compilation Options

| Option | Value |
|--------|-------|
| Encoding | UTF-8 / LATIN1 |
| posix_syntax | true / false |
| longest_match | true / false |
| max_mem | [value] |

### C++ Program Dump

```
# Generated with:
# echo "pattern" | tools/re2-golden/re2_golden --mode=compile_dump [--encoding=...]

0: byte [00-ff] -> 1
1: altmatch -> 2 | 3
2: byte [00-ff] -> 1
3: match! 0
```

### Java Program Dump

```
# Generated with test harness or debugger

0: byte [00-ff] -> 1
1: altmatch -> 2 | 3
2: byte [00-ff] -> 1
3: match! 0
```

### Comparison Result

| Metric | C++ | Java | Match? |
|--------|-----|------|--------|
| Instruction count | N | M | YES/NO |
| First instruction | X | X | YES/NO |
| Has ALT_MATCH | YES/NO | YES/NO | YES/NO |
| Byte ranges | [ranges] | [ranges] | YES/NO |

**Status**: IDENTICAL / STRUCTURALLY DIFFERENT / SEMANTICALLY EQUIVALENT

### Differences (if any)

| Instruction | C++ | Java | Notes |
|-------------|-----|------|-------|
| N | `byte [00-ff]` | `byte [00-7f] + byte [c0-ff]` | UTF-8 split |

---

## Engine Selection Comparison

For each pattern, verify the same engine is selected.

| Pattern | C++ Engine | Java Engine | Match? |
|---------|------------|-------------|--------|
| `^a+$` | OnePass | OnePass | YES |
| `(a+)+` | BitState | BitState | YES |
| `.*xyz` | DFA | DFA | YES |

---

## Default Options Comparison

Verify default option values match between C++ and Java.

| Option | C++ Default | Java Default | Match? |
|--------|-------------|--------------|--------|
| encoding | EncodingUTF8 | UTF8 | YES |
| posix_syntax | false | false | YES |
| longest_match | false | false | YES |
| log_errors | true | - | N/A |
| max_mem | 8<<20 | 8<<20 | YES |
| literal | false | false | YES |
| never_nl | false | false | YES |
| dot_nl | false | false | YES |
| never_capture | false | false | YES |
| case_sensitive | true | true | YES |
| perl_classes | false | false | YES |
| word_boundary | false | false | YES |
| one_line | false | false | YES |

---

## Initial State Comparison

For each pattern, verify the initial DFA state matches.

| Pattern | C++ analyzeStart | Java analyzeStart | Match? |
|---------|-----------------|-------------------|--------|
| `(?s).*` | FullMatchState | FullMatchState | YES |
| `^ABCD` | [state hash] | [state hash] | YES |
| `impossible^` | DeadState | DeadState | YES |

---

## Summary

| Category | Patterns Tested | Identical | Different |
|----------|-----------------|-----------|-----------|
| DotStar | N | X | Y |
| Prefix | N | X | Y |
| Character Classes | N | X | Y |
| **Total** | **N** | **X** | **Y** |

**Overall Status**: PASS (all identical) / FAIL (differences found)

---

## Issues Found

### Issue #1: [title]

**Pattern**: `[pattern]`
**Options**: [options]

**C++ Output**:
```
[dump]
```

**Java Output**:
```
[dump]
```

**Impact**: [behavioral consequence]

**Root Cause**: [why they differ]

---

## Notes

[Any additional observations]
