# RE2 Audit and Verification Procedures

This document combines code audit methodology and verification procedures for RE2 optimizations.

---

# Part 1: Line-by-Line Code Audit

This section describes how to compare Java implementation files against their C++ upstream counterparts. This is purely about code comparison, not performance benchmarking or behavioral testing.

## Key Principle

**The audit identifies ALL differences. It does not fix them. It does not filter them.**

- Compare every line of Java to corresponding C++ line
- Record EVERY difference, no matter how small
- Syntax translations get recorded (we sort later)
- No judgments about "this looks okay" - just record it
- The audit produces a complete report for later review

---

## File Mapping

| Java File | C++ File | Priority |
|-----------|----------|----------|
| `Dfa.java` | `dfa.cc` | HIGH (FullMatchState bug was here) |
| `Prog.java` | `prog.cc`, `prog.h` | HIGH |
| `Re2.java` | `re2.cc`, `re2.h` | HIGH |
| `Compiler.java` | `compile.cc` | MEDIUM |
| `OnePass.java` | `onepass.cc` | MEDIUM |
| `Nfa.java` | `nfa.cc` | MEDIUM |
| `BitState.java` | `bitstate.cc` | MEDIUM |
| `RegexpParser.java` | `parse.cc` | LOW |
| `Simplifier.java` | `simplify.cc` | LOW |
| `Regexp.java` | `regexp.h`, `regexp.cc` | LOW |

---

## What to Compare

For each Java method, find the corresponding C++ function and compare:

1. **Every `if` condition** - exact boolean expression
2. **Every `while` condition** - loop termination
3. **Every `for` loop** - init, condition, increment
4. **Every `switch` case** - all cases and default
5. **Every variable assignment** - right-hand side expressions
6. **Every return statement** - return value expressions
7. **Every function call** - arguments passed
8. **Every flag/enum value** - default values, constants
9. **Every early-return condition** - guard clauses

---

## Recording Format

Each difference gets recorded exactly as found:

```markdown
## File: [Java file] vs [C++ file]

### Method: [method_name]
C++ Location: [file:line]
Java Location: [file:line]

#### Difference 1
C++:  `[exact C++ code verbatim]`
Java: `[exact Java code verbatim]`
Note: [brief factual description - no judgment]

#### Difference 2
C++:  `[exact C++ code verbatim]`
Java: `[exact Java code verbatim]`
Note: [brief factual description - no judgment]

### Summary for [file]
- Methods compared: X
- Total differences found: Y
```

**Rules:**
- Copy code EXACTLY - don't paraphrase
- Include line numbers
- No filtering - record everything
- No "this is probably fine" - just record it

---

## Parallel Agent Strategy

**One agent per file pair.** All agents run simultaneously.

| Agent | Java File | C++ File | Focus |
|-------|-----------|----------|-------|
| Agent 1 | `Dfa.java` | `dfa.cc` | DFA state machine, search loops |
| Agent 2 | `Prog.java` | `prog.cc` | Program structure, flattening |
| Agent 3 | `Re2.java` | `re2.cc` | High-level API, engine selection |
| Agent 4 | `Compiler.java` | `compile.cc` | AST to program compilation |
| Agent 5 | `OnePass.java` | `onepass.cc` | One-pass engine |
| Agent 6 | `Nfa.java` | `nfa.cc` | NFA simulation |
| Agent 7 | `BitState.java` | `bitstate.cc` | BitState backtracking |
| Agent 8 | `RegexpParser.java` | `parse.cc` | Regex parsing |
| Agent 9 | `Simplifier.java` | `simplify.cc` | AST simplification |

**Each agent:**
1. Reads the Java file completely
2. Reads the C++ file completely
3. Matches each Java method to its C++ function
4. Compares every condition, loop, switch, etc.
5. Records EVERY difference in the format above
6. Outputs a markdown report

**Total agents: 9** (can all run in parallel)

---

## Critical Functions (Extra Scrutiny)

These functions have historically been sources of bugs. Compare with extra care:

| C++ Function | Java Method | File | Why Critical |
|-------------|-------------|------|--------------|
| `WorkqToCachedState` | `workqToCachedState` | Dfa | FullMatchState, DeadState triggers |
| `RunWorkqOnByte` | `runWorkqOnByte` | Dfa | Core DFA transition logic |
| `InlinedSearchLoop` | `searchLoop` | Dfa | 8 specialized variants |
| `AnalyzeSearch` | `analyzeStart` | Dfa | Initial state selection |
| `Optimize` | `optimize` | Prog | ALT_MATCH generation |
| `Flatten` | `flatten` | Prog | Program structure |
| `IsOnePass` | `isOnePass` | Prog | Engine selection |
| `ComputeByteMap` | `computeByteMap` | Prog | Byte equivalence classes |
| `AddRuneRangeUTF8` | `addRuneRangeUtf8` | Compiler | UTF-8 byte sequences |

---

## Audit Output

Each audit produces files in `docs/audits/history/`:

### Directory Structure

```
docs/audits/
├── PROCEDURES.md                 # This file
├── templates/                    # Reusable audit formats
└── history/                      # Historical audit reports
    ├── YYYY-MM-DD-audit.md       # Summary with links
    └── YYYY-MM-DD-details/       # Per-file detailed reports
        ├── 01-dfa.md
        ├── 02-prog.md
        └── ...
```

### Summary File Format

```markdown
# Line-by-Line Audit Report - [DATE]

## Summary

| File Pair | Differences Found | Priority | Detail File |
|-----------|------------------|----------|-------------|
| Dfa.java vs dfa.cc | N | HIGH | [01-dfa.md](YYYY-MM-DD-details/01-dfa.md) |
| ... | ... | ... | ... |
| **TOTAL** | **N** | | |

## Critical Findings
[High-level summary of most important differences]

## Recommendations
[Action items based on findings]
```

---

## What This Audit Does NOT Do

- Does NOT run benchmarks (that's in `docs/benchmarks/`)
- Does NOT validate performance (separate concern)
- Does NOT fix code (audit only identifies)
- Does NOT filter "acceptable" differences (record everything)
- Does NOT reference optimization registry during comparison (that's `docs/reference/OPTIMIZATION_REGISTRY.md`)

---

# Part 2: Verification Procedures for Optimizations

This section defines procedures for verifying that optimizations actually work.

## Core Problem

Code was added but behavior wasn't verified. The audit correctly identified issues, but remediation didn't prove fixes worked. Example: `(?s).*` fullMatch should be O(1), but remained O(n) after "fix" because the code path was never reached.

**New rule:** A fix is NOT complete until a test proves it works.

---

## Test Types (Preferred Order)

### Type A: Direct Verification (STRONGLY PREFERRED)

Test that specific code path was taken by asserting on internal state or return values.

**Example: FullMatchState optimization**
```java
@Test
public void testFullMatchDotStarReturnsFullMatchState() {
    // The optimization should return FULL_MATCH sentinel
    Re2 re = Re2.compile("(?s).*");
    DfaSearchResult result = re.searchWithState(input);
    assertEquals(DfaState.FULL_MATCH, result.getTerminalState());
}
```

**Why preferred:**
- Fast: milliseconds per test
- Deterministic: no timing flakiness
- Diagnostic: tells you exactly what went wrong
- Debuggable: you can step through and see why

**How to add test hooks:**
1. Add package-private field/method in production class
2. Test in same package accesses directly
3. Or use sentinel return values that can be checked

### Type B: Golden Tests

Run C++ with specific input, capture output. Java test asserts identical output.

**Example:**
```java
@Test
public void testGoldenMatchResult() {
    // Expected values from running C++ RE2:
    // re2::RE2 re("a(b*)c"); re.Match("abbbbc", ..., submatch, 2);
    // submatch[0] = "abbbbc", submatch[1] = "bbbb"

    Re2 re = Re2.compile("a(b*)c");
    String[] match = re.match("abbbbc", 2);
    assertEquals("abbbbc", match[0]);
    assertEquals("bbbb", match[1]);
}
```

### Type C: Generated Expected Results

Run C++ to compute expected values, hardcode in Java test.

**Example:**
```java
@Test
public void testDfaStateTransitions() {
    // Generated by running C++ DFA tracer on pattern "abc"
    // State 0 + 'a' -> State 1
    // State 1 + 'b' -> State 2
    // State 2 + 'c' -> Match

    Dfa dfa = buildDfa("abc");
    assertEquals(1, dfa.transition(0, 'a'));
    assertEquals(2, dfa.transition(1, 'b'));
    assertTrue(dfa.isMatch(dfa.transition(2, 'c')));
}
```

### Type D: Complexity Tests (LAST RESORT)

Measure ratio of small vs large input time. Only use when direct verification not possible.

**Example:**
```java
@Test
public void testFullMatchDotStarIsConstantTime() {
    Re2 re = Re2.compile("(?s).*");

    byte[] small = new byte[8];
    byte[] large = new byte[2_000_000];

    // Warm up
    for (int i = 0; i < 1000; i++) {
        re.fullMatch(small);
        re.fullMatch(large);
    }

    // Measure
    long smallTime = measureNanos(() -> re.fullMatch(small), 10000);
    long largeTime = measureNanos(() -> re.fullMatch(large), 10000);

    // O(1) means ratio should be close to 1, not 250000x (the size ratio)
    double ratio = (double) largeTime / smallTime;
    assertTrue("Expected O(1), got ratio " + ratio, ratio < 100);
}
```

**Why last resort:**
- Flaky: timing varies with system load
- Non-diagnostic: doesn't tell you WHY it failed
- Slow: needs warmup and many iterations

---

## JMH Benchmarks

JMH benchmarks are for final validation and continuous performance monitoring, NOT for per-fix verification.

**Use JMH for:**
- Validating multiple fixes together
- Detecting regressions in CI
- Publishing performance numbers

**Don't use JMH for:**
- Testing individual fixes during development
- First verification that code works

---

## Process for Fixing Performance Issues

### Step 1: Write Failing Test FIRST

Based on C++ behavior. Prefer direct verification over timing.

```java
@Test
public void testOptimizationWorks() {
    // This test should FAIL before the fix
    // and PASS after the fix
}
```

### Step 2: Verify Test Fails

Run the test. If it passes, your test isn't testing the right thing.

### Step 3: Investigate Root Cause

Understand WHY the optimization doesn't work. Don't guess - trace through the code.

### Step 4: Implement Fix

Make the minimal change needed.

### Step 5: Verify Test Passes

Run the test. It must pass. If it doesn't, the fix isn't complete.

### Step 6: Run Full Test Suite

Ensure no regressions.

---

## Adding Test Hooks

When internal state needs verification, add minimal test hooks:

**Option 1: Package-private accessor**
```java
// In Dfa.java
State getLastTerminalState() {
    return lastTerminalState;
}
```

**Option 2: Enum/sentinel return value**
```java
// Search returns terminal state in result object
class SearchResult {
    boolean matched;
    State terminalState; // FULL_MATCH, DEAD_STATE, etc.
}
```

**Option 3: Package-private counter**
```java
// In Dfa.java - incremented when optimization triggers
static int fullMatchOptimizationCount = 0;
```

Choose the least invasive option that allows verification.
