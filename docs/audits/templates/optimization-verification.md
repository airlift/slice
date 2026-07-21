# Optimization Verification Template

Use this template to verify that a specific C++ optimization is correctly implemented in Java.

---

## Optimization Verification Report

**Optimization ID**: O-XXX-NNN (from OPTIMIZATION_REGISTRY.md)
**Optimization Name**: [human-readable name]
**Audit Date**: YYYY-MM-DD
**Auditor**: [agent/person]

---

## C++ Reference

**File**: `[file]:[line_range]`
**Function**: `[function_name]`

### Trigger Condition (EXACT)

```cpp
// Copy the EXACT trigger condition from C++
if (condition) {
    // optimization code
}
```

### Expected Effect

[Describe what the optimization does when triggered]

### Performance Impact

[Expected speedup: O(1) vs O(n), constant factor, etc.]

---

## Java Implementation

**File**: `[file]:[line_range]`
**Method**: `[method_name]`

### Trigger Condition (EXACT)

```java
// Copy the EXACT trigger condition from Java
if (condition) {
    // optimization code
}
```

---

## Condition Comparison

### Character-by-Character Analysis

| Element | C++ | Java | Match? |
|---------|-----|------|--------|
| Operand 1 | `expr` | `expr` | YES/NO |
| Operator | `&&` | `&&` | YES/NO |
| Operand 2 | `expr` | `expr` | YES/NO |
| ... | ... | ... | ... |

### Semantic Equivalence

| C++ Expression | Java Expression | Semantically Identical? |
|----------------|-----------------|------------------------|
| `ip->opcode() == kInstAltMatch` | `op == InstOp.ALT_MATCH` | YES |
| `!ip->last()` | `!ip.last()` | YES |
| `kind_ != Prog::kFirstMatch` | `kind != Kind.FIRST_MATCH` | YES |

### Extra/Missing Conditions

| Type | Expression | Present In |
|------|------------|------------|
| EXTRA | `(flag & FLAG_MATCH) != 0` | Java only |
| MISSING | `first && ip->greedy()` | C++ only |

---

## Behavioral Verification

### Test Pattern

**Pattern**: `[test pattern that triggers this optimization]`
**Options**: [encoding, flags, etc.]

### C++ Behavior

```
# How to verify in C++
echo "pattern" | tools/re2-golden/re2_golden --mode=search --input="test" --trace

Expected: [describe expected optimization path]
```

### Java Behavior

```java
// How to verify in Java
Re2 re2 = Re2.compile("pattern");
// Add debug tracing or use debugger

Expected: [describe expected optimization path]
```

### Verification Result

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Optimization triggers | YES/NO | YES/NO | YES/NO |
| Same code path taken | [path] | [path] | YES/NO |
| Same result produced | [result] | [result] | YES/NO |

---

## Performance Validation

### Benchmark Pattern

**Pattern**: `[benchmark pattern]`
**Input size**: 8B, 4KB, 2MB (test at multiple sizes)

### Results

| Size | C++ Time | Java Time | Ratio | Expected Ratio |
|------|----------|-----------|-------|----------------|
| 8B | Xns | Yns | Z.Zx | <3x |
| 4KB | Xns | Yns | Z.Zx | <3x |
| 2MB | Xns | Yns | Z.Zx | <3x |

### Scaling Analysis

| Metric | C++ | Java | Match? |
|--------|-----|------|--------|
| Big-O complexity | O(?) | O(?) | YES/NO |
| Time increases with size? | YES/NO | YES/NO | - |

**Performance Status**: PASS (all ratios < 3x) / FAIL (ratio > 3x indicates bug)

---

## Verification Checklist

- [ ] Trigger condition copied exactly from both C++ and Java
- [ ] All sub-expressions compared
- [ ] Extra/missing conditions identified
- [ ] Test pattern exercised in both implementations
- [ ] Same code path verified
- [ ] Performance benchmarked at 3+ sizes
- [ ] All ratios within expected range

---

## Status

**Condition Match**: EXACT / DIFFERENT / PARTIAL
**Behavioral Match**: YES / NO / UNTESTED
**Performance Match**: YES / NO / UNTESTED

**Overall Status**: VERIFIED / NEEDS FIX / NEEDS INVESTIGATION

---

## Issues Found

### Issue #1: [title]

**Severity**: CRITICAL / HIGH / MEDIUM / LOW

**Description**: [what's wrong]

**Evidence**:
```
[code or data showing the issue]
```

**Recommendation**: [how to fix]

---

## Notes

[Any additional observations]
