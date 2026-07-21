# FullMatch Benchmark Detailed Comparison

## Files Compared

| C++ | Java |
|-----|------|
| `re2_upstream/re2/testing/regexp_benchmark.cc` | `BenchmarkRe2FullMatch.java` |

---

## FullMatch_DotStar vs fullMatchDotStar

### C++ (regexp_benchmark.cc:1531-1561, 1579-1583)

```cpp
// LATIN1 mode (main benchmark)
void FullMatchRE2(benchmark::State& state, const char *regexp) {
  std::string s = RandomText(state.range(0));
  s += "ABCDEFGHIJ";
  RE2 re(regexp, RE2::Latin1);  // <-- LATIN1 explicitly
  for (auto _ : state) {
    ABSL_CHECK(RE2::FullMatch(s, re));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
}

// UTF-8 mode (variant)
void FullMatchRE2_UTF8(benchmark::State& state, const char *regexp) {
  std::string s = RandomText(state.range(0));
  s += "ABCDEFGHIJ";
  RE2 re(regexp);  // <-- Default UTF-8
  for (auto _ : state) {
    ABSL_CHECK(RE2::FullMatch(s, re));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
}

void FullMatch_DotStar_CachedRE2(benchmark::State& state) {
  FullMatchRE2(state, "(?s).*");           // Uses LATIN1
}

void FullMatch_DotStar_CachedRE2_UTF8(benchmark::State& state) {
  FullMatchRE2_UTF8(state, "(?s).*");      // Uses UTF-8
}

BENCHMARK_RANGE(FullMatch_DotStar_CachedRE2,       8, 2<<20);
BENCHMARK_RANGE(FullMatch_DotStar_CachedRE2_UTF8,  8, 2<<20);
```

### Java (BenchmarkRe2FullMatch.java:61, 73-76, 79-82, 102-106, 123-126)

```java
private static final String FULLMATCH_DOTSTAR = "(?s).*";

@Setup(Level.Trial)
public void setup() {
    text = ByteSlice.wrap(randomText(textSize));

    // UTF-8 mode
    re2DotStar = compileRe2(FULLMATCH_DOTSTAR);  // <-- UTF-8 default

    // LATIN1 mode
    re2DotStarLatin1 = compileRe2Latin1(FULLMATCH_DOTSTAR);  // <-- LATIN1
}

// UTF-8 variant
@Benchmark
public boolean fullMatchDotStar(FullMatchState state) {
    return state.re2DotStar.fullMatch(state.text);
}

// LATIN1 variant
@Benchmark
public boolean fullMatchDotStarLatin1(FullMatchState state) {
    return state.re2DotStarLatin1.fullMatch(state.text);
}
```

### Comparison

| Aspect | C++ LATIN1 | C++ UTF-8 | Java LATIN1 | Java UTF-8 | Match? |
|--------|------------|-----------|-------------|------------|--------|
| Pattern | `(?s).*` | `(?s).*` | `(?s).*` | `(?s).*` | ✓ |
| Encoding | `RE2::Latin1` | Default | `compileRe2Latin1` | `compileRe2` | ✓ |
| Text sizes | 8 → 2M | 8 → 2M | 8 → 2M | 8 → 2M | ✓ |
| Text content | RandomText + "ABCDEFGHIJ" | Same | randomText (no suffix) | Same | Note |

**Note**: C++ appends "ABCDEFGHIJ" to the random text, while Java uses raw random text. This doesn't affect the performance characteristics since `(?s).*` matches any string.

---

## FullMatch_DotStarDollar vs fullMatchDotStarDollar

### C++ (regexp_benchmark.cc:1564-1569, 1585-1588)

```cpp
void FullMatch_DotStarDollar_CachedRE2(benchmark::State& state) {
  FullMatchRE2(state, "(?s).*$");           // Uses LATIN1
}

BENCHMARK_RANGE(FullMatch_DotStarDollar_CachedRE2,  8, 2<<20);
// Note: No UTF-8 variant for DotStarDollar in C++
```

### Java (BenchmarkRe2FullMatch.java:62, 108-118, 127-131)

```java
private static final String FULLMATCH_DOTSTAR_DOLLAR = "(?s).*$";

// UTF-8 variant
@Benchmark
public boolean fullMatchDotStarDollar(FullMatchState state) {
    return state.re2DotStarDollar.fullMatch(state.text);
}

// LATIN1 variant
@Benchmark
public boolean fullMatchDotStarDollarLatin1(FullMatchState state) {
    return state.re2DotStarDollarLatin1.fullMatch(state.text);
}
```

### Comparison

| Aspect | C++ | Java LATIN1 | Java UTF-8 | Match? |
|--------|-----|-------------|------------|--------|
| Pattern | `(?s).*$` | `(?s).*$` | `(?s).*$` | ✓ |
| Encoding | LATIN1 | LATIN1 | UTF-8 | ✓ |
| UTF-8 variant | (none) | N/A | ✓ | Java has extra |

**Note**: Java has MORE coverage - it provides both UTF-8 and LATIN1 variants, while C++ only has LATIN1.

---

## FullMatch_DotStarCapture vs fullMatchDotStarCapture

### C++ (regexp_benchmark.cc:1571-1576, 1590-1593)

```cpp
void FullMatch_DotStarCapture_CachedRE2(benchmark::State& state) {
  FullMatchRE2(state, "(?s)((.*)()()($))");  // Uses LATIN1
}

BENCHMARK_RANGE(FullMatch_DotStarCapture_CachedRE2,  8, 2<<20);
// Note: No UTF-8 variant for DotStarCapture in C++
```

### Java (BenchmarkRe2FullMatch.java:63, 114-118, 134-138)

```java
private static final String FULLMATCH_DOTSTAR_CAPTURE = "(?s)((.*)()()($))";

// UTF-8 variant
@Benchmark
public boolean fullMatchDotStarCapture(FullMatchState state) {
    return state.re2DotStarCapture.fullMatch(state.text);
}

// LATIN1 variant
@Benchmark
public boolean fullMatchDotStarCaptureLatin1(FullMatchState state) {
    return state.re2DotStarCaptureLatin1.fullMatch(state.text);
}
```

### Comparison

| Aspect | C++ | Java LATIN1 | Java UTF-8 | Match? |
|--------|-----|-------------|------------|--------|
| Pattern | `(?s)((.*)()()($))` | Same | Same | ✓ |
| Encoding | LATIN1 | LATIN1 | UTF-8 | ✓ |
| UTF-8 variant | (none) | N/A | ✓ | Java has extra |

---

## Text Generation Difference

### C++ (regexp_benchmark.cc:1522-1529)

```cpp
void FullMatchRE2(benchmark::State& state, const char *regexp) {
  std::string s = RandomText(state.range(0));
  s += "ABCDEFGHIJ";  // <-- Appends suffix
  // ...
}
```

### Java (BenchmarkRe2FullMatch.java:86)

```java
text = ByteSlice.wrap(randomText(textSize));  // <-- No suffix
```

**Impact**: For `(?s).*` patterns, both match any string, so the suffix doesn't affect the benchmark. The suffix ensures the pattern always matches by providing non-newline content at the end. In Java, the random text already contains printable ASCII (0x20-0x7F) which matches `(?s).*`.

---

## Summary

| Pattern | C++ LATIN1 | C++ UTF-8 | Java LATIN1 | Java UTF-8 | Status |
|---------|------------|-----------|-------------|------------|--------|
| `(?s).*` | `FullMatch_DotStar_CachedRE2` | `FullMatch_DotStar_CachedRE2_UTF8` | `fullMatchDotStarLatin1` | `fullMatchDotStar` | ✓ Both aligned |
| `(?s).*$` | `FullMatch_DotStarDollar_CachedRE2` | (none) | `fullMatchDotStarDollarLatin1` | `fullMatchDotStarDollar` | ✓ Java has extra |
| `(?s)((.*)()()($))` | `FullMatch_DotStarCapture_CachedRE2` | (none) | `fullMatchDotStarCaptureLatin1` | `fullMatchDotStarCapture` | ✓ Java has extra |

**Conclusion**: The Java benchmarks are properly aligned with C++ and provide additional coverage (UTF-8 variants for all three patterns, while C++ only has UTF-8 for `(?s).*`).
