# Condition-by-Condition Comparison Template

Use this template when comparing a C++ function with its Java counterpart. Every condition must be compared exactly.

---

## Function Comparison Report

**C++ Function**: `[name]`
**C++ Location**: `[file]:[start_line]-[end_line]`

**Java Method**: `[name]`
**Java Location**: `[file]:[start_line]-[end_line]`

**Audit Date**: YYYY-MM-DD
**Auditor**: [agent/person]

---

## Summary

| Metric | Count |
|--------|-------|
| Total conditions compared | 0 |
| Identical (semantically) | 0 |
| Different | 0 |
| Missing in Java | 0 |
| Added in Java | 0 |

**Overall Status**: PASS / FAIL / NEEDS REVIEW

---

## Condition Comparison

### Condition #1: [description]

**C++ (line N)**:
```cpp
if (condition_here) {
    // effect
}
```

**Java (line M)**:
```java
if (condition_here) {
    // effect
}
```

**Status**: IDENTICAL / DIFFERENT / MISSING / ADDED

**Notes**: [if different, explain the difference and its impact]

---

### Condition #2: [description]

**C++ (line N)**:
```cpp
// code
```

**Java (line M)**:
```java
// code
```

**Status**: IDENTICAL / DIFFERENT / MISSING / ADDED

**Notes**:

---

## Loop Conditions

### Loop #1: [description]

**C++ (line N)**:
```cpp
for/while (condition) { ... }
```

**Java (line M)**:
```java
for/while (condition) { ... }
```

**Status**: IDENTICAL / DIFFERENT

---

## Switch/Case Statements

### Switch #1: [description]

**C++**: Cases covered: [list]
**Java**: Cases covered: [list]

**Status**: IDENTICAL / DIFFERENT

**Missing cases in Java**: [list or "None"]
**Extra cases in Java**: [list or "None"]

---

## Default Values and Constants

| C++ Constant | Value | Java Constant | Value | Status |
|--------------|-------|---------------|-------|--------|
| `NAME` | `value` | `NAME` | `value` | MATCH/DIFF |

---

## Early Return Conditions

| # | C++ Condition | Java Condition | Status |
|---|---------------|----------------|--------|
| 1 | `if (x) return y;` | `if (x) return y;` | MATCH |

---

## Optimization Triggers

| Optimization | C++ Trigger | Java Trigger | Status |
|--------------|-------------|--------------|--------|
| FullMatchState | `if (...)` | `if (...)` | MATCH/DIFF |

---

## Differences Found

### Difference #1: [title]

**Severity**: CRITICAL / HIGH / MEDIUM / LOW

**C++ Code**:
```cpp
// exact code
```

**Java Code**:
```java
// exact code
```

**Impact**: [describe behavioral difference]

**Recommendation**: [fix suggestion]

---

## Checklist

Before marking this function as audited, verify:

- [ ] All if-conditions compared exactly
- [ ] All loop conditions compared exactly
- [ ] All switch cases compared exactly
- [ ] All flag/enum values compared exactly
- [ ] All default values compared exactly
- [ ] All early-return conditions compared exactly
- [ ] All optimization trigger conditions compared exactly
- [ ] All differences documented above

**Function Audit Status**: PASS / FAIL

---

## Notes

[Any additional observations about this function]
