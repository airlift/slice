# Parse Benchmark Detailed Comparison

## Files Compared

| C++ | Java |
|-----|------|
| `re2_upstream/re2/testing/regexp_benchmark.cc` | `BenchmarkRe2Parse.java` |

---

## Parse_Digits vs parse3Digits

### C++ (regexp_benchmark.cc:472-509)

```cpp
void Parse3Digits(benchmark::State& state,
                  void (*parse3)(benchmark::State&, const char*,
                                 absl::string_view)) {
  parse3(state, "([0-9]+)-([0-9]+)-([0-9]+)", "650-253-0001");
  state.SetItemsProcessed(state.iterations());
}

void Parse_Digits_NFA(benchmark::State& state)      { Parse3Digits(state, Parse3NFA); }
void Parse_Digits_OnePass(benchmark::State& state)  { Parse3Digits(state, Parse3OnePass); }
void Parse_Digits_RE2(benchmark::State& state)      { Parse3Digits(state, Parse3RE2); }
void Parse_Digits_BitState(benchmark::State& state) { Parse3Digits(state, Parse3BitState); }

BENCHMARK(Parse_Digits_NFA)->ThreadRange(1, NumCPUs());
BENCHMARK(Parse_Digits_OnePass)->ThreadRange(1, NumCPUs());
BENCHMARK(Parse_Digits_RE2)->ThreadRange(1, NumCPUs());
BENCHMARK(Parse_Digits_BitState)->ThreadRange(1, NumCPUs());
```

### Java (BenchmarkRe2Parse.java:57, 60, 92-125)

```java
private static final String PARSE_3_DIGITS = "([0-9]+)-([0-9]+)-([0-9]+)";
private static final String PHONE_INPUT = "650-253-0001";

@Benchmark
public boolean parse3DigitsNfa(ParseState state) {
    int[] submatch = new int[8]; // (full + 3 captures) * 2
    return Nfa.search(state.prog3Digits, state.phoneInput, state.phoneInput,
                      false, false, submatch);
}

@Benchmark
public boolean parse3DigitsOnePass(ParseState state) {
    int[] submatch = new int[8];
    return OnePass.search(state.prog3Digits, state.phoneInput, state.phoneInput,
                          true, false, submatch);
}

@Benchmark
public boolean parse3DigitsBitState(ParseState state) {
    int[] submatch = new int[8];
    return BitState.search(state.prog3Digits, state.phoneInput, state.phoneInput,
                           false, false, submatch);
}

@Benchmark
public boolean parse3DigitsBacktrack(ParseState state) {
    int[] submatch = new int[8];
    return Backtrack.search(state.prog3Digits, state.phoneInput, state.phoneInput,
                            false, false, submatch);
}

@Benchmark
public boolean parse3DigitsRe2(ParseState state) {
    int[] submatch = new int[8];
    return state.re2Parse3.match(state.phoneInput, Anchor.UNANCHORED, submatch);
}
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Pattern | `([0-9]+)-([0-9]+)-([0-9]+)` | Same | ✓ |
| Input | `650-253-0001` | Same | ✓ |
| Encoding | UTF-8 (default) | UTF-8 (default) | ✓ |
| Capture groups | 3 + full match | 3 + full match | ✓ |
| Engines | NFA, OnePass, BitState, RE2 | NFA, OnePass, BitState, Backtrack, RE2 | Java has extra |

---

## Parse_DigitDs vs parse3DigitDs

### C++ (regexp_benchmark.cc:511-548)

```cpp
void Parse3DigitDs(benchmark::State& state,
                   void (*parse3)(benchmark::State&, const char*,
                                  absl::string_view)) {
  parse3(state, "(\\d+)-(\\d+)-(\\d+)", "650-253-0001");
  state.SetItemsProcessed(state.iterations());
}

void Parse_DigitDs_NFA(benchmark::State& state)      { Parse3DigitDs(state, Parse3NFA); }
void Parse_DigitDs_OnePass(benchmark::State& state)  { Parse3DigitDs(state, Parse3OnePass); }
void Parse_DigitDs_RE2(benchmark::State& state)      { Parse3DigitDs(state, Parse3RE2); }
void Parse_DigitDs_BitState(benchmark::State& state) { Parse3DigitDs(state, Parse3BitState); }
```

### Java (BenchmarkRe2Parse.java:58, 129-162)

```java
private static final String PARSE_3_DIGIT_DS = "(\\d+)-(\\d+)-(\\d+)";

@Benchmark
public boolean parse3DigitDsNfa(ParseState state) {
    int[] submatch = new int[8];
    return Nfa.search(state.prog3DigitDs, state.phoneInput, state.phoneInput,
                      false, false, submatch);
}

@Benchmark
public boolean parse3DigitDsOnePass(ParseState state) {
    int[] submatch = new int[8];
    return OnePass.search(state.prog3DigitDs, state.phoneInput, state.phoneInput,
                          true, false, submatch);
}

@Benchmark
public boolean parse3DigitDsBitState(ParseState state) {
    int[] submatch = new int[8];
    return BitState.search(state.prog3DigitDs, state.phoneInput, state.phoneInput,
                           false, false, submatch);
}

@Benchmark
public boolean parse3DigitDsBacktrack(ParseState state) {
    int[] submatch = new int[8];
    return Backtrack.search(state.prog3DigitDs, state.phoneInput, state.phoneInput,
                            false, false, submatch);
}

@Benchmark
public boolean parse3DigitDsRe2(ParseState state) {
    int[] submatch = new int[8];
    return state.re2Parse3Ds.match(state.phoneInput, Anchor.UNANCHORED, submatch);
}
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Pattern | `(\\d+)-(\\d+)-(\\d+)` | Same | ✓ |
| Input | `650-253-0001` | Same | ✓ |
| Encoding | UTF-8 (default) | UTF-8 (default) | ✓ |

---

## Parse_Split vs parse1Split

### C++ (regexp_benchmark.cc:552-585)

```cpp
void Parse1Split(benchmark::State& state,
                 void (*parse1)(benchmark::State&, const char*,
                                absl::string_view)) {
  parse1(state, "[0-9]+-(.*)", "650-253-0001");
  state.SetItemsProcessed(state.iterations());
}

void Parse_Split_NFA(benchmark::State& state)      { Parse1Split(state, Parse1NFA); }
void Parse_Split_OnePass(benchmark::State& state)  { Parse1Split(state, Parse1OnePass); }
void Parse_Split_RE2(benchmark::State& state)      { Parse1Split(state, Parse1RE2); }
void Parse_Split_BitState(benchmark::State& state) { Parse1Split(state, Parse1BitState); }
```

### Java (BenchmarkRe2Parse.java:59, 166-192)

```java
private static final String PARSE_1_SPLIT = "[0-9]+-(.*)";

@Benchmark
public boolean parse1SplitNfa(ParseState state) {
    int[] submatch = new int[4]; // (full + 1 capture) * 2
    return Nfa.search(state.prog1Split, state.phoneInput, state.phoneInput,
                      false, false, submatch);
}

@Benchmark
public boolean parse1SplitOnePass(ParseState state) {
    int[] submatch = new int[4];
    return OnePass.search(state.prog1Split, state.phoneInput, state.phoneInput,
                          true, false, submatch);
}

@Benchmark
public boolean parse1SplitBitState(ParseState state) {
    int[] submatch = new int[4];
    return BitState.search(state.prog1Split, state.phoneInput, state.phoneInput,
                           false, false, submatch);
}

@Benchmark
public boolean parse1SplitRe2(ParseState state) {
    int[] submatch = new int[4];
    return state.re2Split.match(state.phoneInput, Anchor.UNANCHORED, submatch);
}
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Pattern | `[0-9]+-(.*)` | Same | ✓ |
| Input | `650-253-0001` | Same | ✓ |
| Capture groups | 1 (captures `253-0001`) | 1 (captures `253-0001`) | ✓ |
| Encoding | UTF-8 (default) | UTF-8 (default) | ✓ |

---

## Summary

| Benchmark | C++ Pattern | Java Pattern | Encoding Match | Status |
|-----------|-------------|--------------|----------------|--------|
| Parse3Digits | `([0-9]+)-([0-9]+)-([0-9]+)` | Same | UTF-8 = UTF-8 | ✓ |
| Parse3DigitDs | `(\\d+)-(\\d+)-(\\d+)` | Same | UTF-8 = UTF-8 | ✓ |
| Parse1Split | `[0-9]+-(.*)`  | Same | UTF-8 = UTF-8 | ✓ |

**Conclusion**: All parse benchmarks are correctly aligned with C++ patterns and encodings.
