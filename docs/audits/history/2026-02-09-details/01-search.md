# Search Benchmark Detailed Comparison

## Files Compared

| C++ | Java |
|-----|------|
| `re2_upstream/re2/testing/regexp_benchmark.cc` | `BenchmarkRe2Search.java` |
| | `BenchmarkRe2SearchNfa.java` |

---

## Search_Easy0 vs searchEasy0

### C++ (regexp_benchmark.cc:190, 212-222)

```cpp
#define EASY0      "ABCDEFGHIJKLMNOPQRSTUVWXYZ$"

void Search_Easy0_CachedDFA(benchmark::State& state)  { Search(state, EASY0, SearchCachedDFA); }
void Search_Easy0_CachedNFA(benchmark::State& state)  { Search(state, EASY0, SearchCachedNFA); }
void Search_Easy0_CachedRE2(benchmark::State& state)  { Search(state, EASY0, SearchCachedRE2); }

BENCHMARK_RANGE(Search_Easy0_CachedDFA,  8, 16<<20)->ThreadRange(1, NumCPUs());
BENCHMARK_RANGE(Search_Easy0_CachedNFA,  8, 256<<10)->ThreadRange(1, NumCPUs());
BENCHMARK_RANGE(Search_Easy0_CachedRE2,  8, 16<<20)->ThreadRange(1, NumCPUs());
```

### Java (BenchmarkRe2Search.java:51, 98-108)

```java
private static final String EASY0 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ$";

@Benchmark
public Object searchEasy0Dfa(SearchState state) {
    return Dfa.search(state.progEasy0, state.text, state.text, false, false);
}

@Benchmark
public boolean searchEasy0Re2(SearchState state) {
    return state.re2Easy0.partialMatch(state.text);
}
```

### Java NFA (BenchmarkRe2SearchNfa.java:50, 83-87)

```java
private static final String EASY0 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ$";

@Benchmark
public boolean searchEasy0Nfa(NfaSearchState state) {
    return Nfa.search(state.progEasy0, state.text, state.text, false, false, null);
}
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Pattern | `ABCDEFGHIJKLMNOPQRSTUVWXYZ$` | Same | ✓ |
| Encoding | UTF-8 (default via RE2()) | UTF-8 (default via compileRe2()) | ✓ |
| Anchor mode | `Prog::kUnanchored` (line 184) | `false, false` (unanchored) | ✓ |
| Expect match | `false` (line 184) | N/A (no assertion) | ✓ |
| Text sizes DFA | 8 → 16M (geometric) | 8 → 16M (sparse) | ~✓ |
| Text sizes NFA | 8 → 256K (geometric) | 8 → 256K (sparse) | ~✓ |

---

## Search_Easy1 vs searchEasy1

### C++ (regexp_benchmark.cc:191, 224-234)

```cpp
#define EASY1      "A[AB]B[BC]C[CD]D[DE]E[EF]F[FG]G[GH]H[HI]I[IJ]J$"

void Search_Easy1_CachedDFA(benchmark::State& state)  { Search(state, EASY1, SearchCachedDFA); }
void Search_Easy1_CachedRE2(benchmark::State& state)  { Search(state, EASY1, SearchCachedRE2); }
```

### Java (BenchmarkRe2Search.java:52, 112-122)

```java
private static final String EASY1 = "A[AB]B[BC]C[CD]D[DE]E[EF]F[FG]G[GH]H[HI]I[IJ]J$";

@Benchmark
public Object searchEasy1Dfa(SearchState state) {
    return Dfa.search(state.progEasy1, state.text, state.text, false, false);
}

@Benchmark
public boolean searchEasy1Re2(SearchState state) {
    return state.re2Easy1.partialMatch(state.text);
}
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Pattern | `A[AB]B[BC]C[CD]D[DE]E[EF]F[FG]G[GH]H[HI]I[IJ]J$` | Same | ✓ |
| Encoding | UTF-8 (default) | UTF-8 (default) | ✓ |

---

## Search_Medium vs searchMedium

### C++ (regexp_benchmark.cc:197, 248-258)

```cpp
#define MEDIUM     "[XYZ]ABCDEFGHIJKLMNOPQRSTUVWXYZ$"

void Search_Medium_CachedDFA(benchmark::State& state)  { Search(state, MEDIUM, SearchCachedDFA); }
void Search_Medium_CachedRE2(benchmark::State& state)  { Search(state, MEDIUM, SearchCachedRE2); }
```

### Java (BenchmarkRe2Search.java:53, 126-136)

```java
private static final String MEDIUM = "[XYZ]ABCDEFGHIJKLMNOPQRSTUVWXYZ$";

@Benchmark
public Object searchMediumDfa(SearchState state) {
    return Dfa.search(state.progMedium, state.text, state.text, false, false);
}

@Benchmark
public boolean searchMediumRe2(SearchState state) {
    return state.re2Medium.partialMatch(state.text);
}
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Pattern | `[XYZ]ABCDEFGHIJKLMNOPQRSTUVWXYZ$` | Same | ✓ |
| Encoding | UTF-8 (default) | UTF-8 (default) | ✓ |

---

## Search_Hard vs searchHard

### C++ (regexp_benchmark.cc:202, 260-270)

```cpp
#define HARD       "[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$"

void Search_Hard_CachedDFA(benchmark::State& state)  { Search(state, HARD, SearchCachedDFA); }
void Search_Hard_CachedRE2(benchmark::State& state)  { Search(state, HARD, SearchCachedRE2); }
```

### Java (BenchmarkRe2Search.java:54, 140-150)

```java
private static final String HARD = "[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$";

@Benchmark
public Object searchHardDfa(SearchState state) {
    return Dfa.search(state.progHard, state.text, state.text, false, false);
}

@Benchmark
public boolean searchHardRe2(SearchState state) {
    return state.re2Hard.partialMatch(state.text);
}
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Pattern | `[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$` | Same | ✓ |
| Encoding | UTF-8 (default) | UTF-8 (default) | ✓ |

---

## Search_Parens vs searchParens

### C++ (regexp_benchmark.cc:209-210, 284-294)

```cpp
#define PARENS     "([ -~])*(A)(B)(C)(D)(E)(F)(G)(H)(I)(J)(K)(L)(M)" \
                   "(N)(O)(P)(Q)(R)(S)(T)(U)(V)(W)(X)(Y)(Z)$"

void Search_Parens_CachedDFA(benchmark::State& state)  { Search(state, PARENS, SearchCachedDFA); }
void Search_Parens_CachedRE2(benchmark::State& state)  { Search(state, PARENS, SearchCachedRE2); }
```

### Java (BenchmarkRe2Search.java:55, 154-164)

```java
private static final String PARENS = "([ -~])*(A)(B)(C)(D)(E)(F)(G)(H)(I)(J)(K)(L)(M)" +
    "(N)(O)(P)(Q)(R)(S)(T)(U)(V)(W)(X)(Y)(Z)$";

@Benchmark
public Object searchParensDfa(SearchState state) {
    return Dfa.search(state.progParens, state.text, state.text, false, false);
}

@Benchmark
public boolean searchParensRe2(SearchState state) {
    return state.re2Parens.partialMatch(state.text);
}
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Pattern | `([ -~])*(A)(B)...(Y)(Z)$` | Same | ✓ |
| Encoding | UTF-8 (default) | UTF-8 (default) | ✓ |

---

## Summary

All Search benchmarks are correctly aligned:

| Pattern | C++ Encoding | Java Encoding | Pattern Match |
|---------|--------------|---------------|---------------|
| EASY0 | UTF-8 | UTF-8 | ✓ |
| EASY1 | UTF-8 | UTF-8 | ✓ |
| MEDIUM | UTF-8 | UTF-8 | ✓ |
| HARD | UTF-8 | UTF-8 | ✓ |
| PARENS | UTF-8 | UTF-8 | ✓ |
