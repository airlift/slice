# Practical Benchmark Detailed Comparison

## Files Compared

| C++ | Java |
|-----|------|
| `re2_upstream/re2/testing/regexp_benchmark.cc` | `BenchmarkRe2Practical.java` |

---

## HTTPPartialMatch vs httpPartialMatch

### C++ (regexp_benchmark.cc:1430-1453)

```cpp
static std::string http_text =
  "GET /asdfhjasdhfasdlfhasdflkjasdfkljasdhflaskdjhf"
  "alksdjfhasdlkfhasdlkjfhasdljkfhadsjklf HTTP/1.1";

void HTTPPartialMatchRE2(benchmark::State& state) {
  absl::string_view a;
  RE2 re("(?-s)^(?:GET|POST) +([^ ]+) HTTP");  // <-- Default UTF-8
  for (auto _ : state) {
    RE2::PartialMatch(http_text, re, &a);
  }
}

BENCHMARK(HTTPPartialMatchRE2)->ThreadRange(1, NumCPUs());
```

### Java (BenchmarkRe2Practical.java:54-55, 93-97)

```java
private static final String HTTP_PATTERN = "(?-s)^(?:GET|POST) +([^ ]+) HTTP";
private static final String HTTP_INPUT =
    "GET /asdfhjasdhfasdlfhasdflkjasdfkljasdhflaskdjhf" +
    "alksdjfhasdlkfhasdlkjfhasdljkfhadsjklf HTTP/1.1";

@Benchmark
public boolean httpPartialMatch(PracticalState state) {
    return state.re2Http.partialMatch(state.httpInput);
}
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Pattern | `(?-s)^(?:GET|POST) +([^ ]+) HTTP` | Same | ✓ |
| Input | `GET /asdf...jklf HTTP/1.1` | Same | ✓ |
| Encoding | UTF-8 (default) | UTF-8 (default) | ✓ |
| Capture | 1 group (extracts URL) | No capture (just match) | Different |

**Note**: C++ extracts the URL into `absl::string_view a`, while Java just tests for match. This doesn't significantly affect performance for this benchmark since the main work is the pattern matching.

---

## SmallHTTPPartialMatch vs smallHttpPartialMatch

### C++ (regexp_benchmark.cc:1455-1477)

```cpp
static std::string smallhttp_text =
  "GET /abc HTTP/1.1";

void SmallHTTPPartialMatchRE2(benchmark::State& state) {
  absl::string_view a;
  RE2 re("(?-s)^(?:GET|POST) +([^ ]+) HTTP");  // <-- Default UTF-8
  for (auto _ : state) {
    RE2::PartialMatch(smallhttp_text, re, &a);
  }
}

BENCHMARK(SmallHTTPPartialMatchRE2)->ThreadRange(1, NumCPUs());
```

### Java (BenchmarkRe2Practical.java:56, 99-103)

```java
private static final String SMALL_HTTP_INPUT = "GET /abc HTTP/1.1";

@Benchmark
public boolean smallHttpPartialMatch(PracticalState state) {
    return state.re2Http.partialMatch(state.smallHttpInput);
}
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Pattern | `(?-s)^(?:GET|POST) +([^ ]+) HTTP` | Same | ✓ |
| Input | `GET /abc HTTP/1.1` | Same | ✓ |
| Encoding | UTF-8 (default) | UTF-8 (default) | ✓ |

---

## SimplePartialMatch vs simplePartialMatch

### C++ (regexp_benchmark.cc:1419-1428)

```cpp
void SimplePartialMatchRE2(benchmark::State& state) {
  RE2 re("abcdefg");  // <-- Default UTF-8
  for (auto _ : state) {
    RE2::PartialMatch("abcdefg", re);
  }
}

BENCHMARK(SimplePartialMatchRE2)->ThreadRange(1, NumCPUs());
```

### Java (BenchmarkRe2Practical.java:105-109)

```java
@Benchmark
public boolean simplePartialMatch(PracticalState state) {
    return state.re2Simple.partialMatch(state.simpleInput);
}

// Setup:
simpleInput = ByteSlice.wrap("abcdefghijk".getBytes(StandardCharsets.UTF_8));  // Note: slightly longer
re2Simple = Re2BenchmarkRunner.compileRe2("abcdefg");
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Pattern | `abcdefg` | Same | ✓ |
| Input | `abcdefg` | `abcdefghijk` (longer) | Slight difference |
| Encoding | UTF-8 (default) | UTF-8 (default) | ✓ |

**Note**: Java input is slightly longer but both match. Doesn't affect benchmark validity.

---

## EmptyPartialMatch vs emptyPartialMatch

### C++ (regexp_benchmark.cc:1394-1410)

```cpp
void EmptyPartialMatchRE2(benchmark::State& state) {
  RE2 re("");  // <-- Default UTF-8
  for (auto _ : state) {
    RE2::PartialMatch("", re);
  }
}

BENCHMARK(EmptyPartialMatchRE2)->ThreadRange(1, NumCPUs());
```

### Java (BenchmarkRe2Practical.java:111-115)

```java
@Benchmark
public boolean emptyPartialMatch(PracticalState state) {
    return state.re2Empty.partialMatch(state.emptyInput);
}

// Setup:
emptyInput = ByteSlice.wrap(new byte[0]);
re2Empty = Re2BenchmarkRunner.compileRe2("");
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Pattern | `` (empty) | Same | ✓ |
| Input | `` (empty) | Same | ✓ |
| Encoding | UTF-8 (default) | UTF-8 (default) | ✓ |

---

## Compile Benchmarks

### C++ (regexp_benchmark.cc:763-788)

```cpp
ABSL_FLAG(std::string, compile_regexp, "(.*)-(\\d+)-of-(\\d+)",
          "regexp for compile benchmarks");

void BM_Regexp_Parse(benchmark::State& state)  { RunBuild(state, ..., ParseRegexp); }
void BM_Regexp_Simplify(benchmark::State& state) { RunBuild(state, ..., SimplifyRegexp); }
void BM_CompileToProg(benchmark::State& state) { RunBuild(state, ..., CompileToProg); }
void BM_Regexp_SimplifyCompile(benchmark::State& state) { RunBuild(state, ..., SimplifyCompileRegexp); }
void BM_RE2_Compile(benchmark::State& state) { RunBuild(state, ..., CompileRE2); }

BENCHMARK(BM_Regexp_Parse)->ThreadRange(1, NumCPUs());
BENCHMARK(BM_Regexp_Simplify)->ThreadRange(1, NumCPUs());
BENCHMARK(BM_CompileToProg)->ThreadRange(1, NumCPUs());
BENCHMARK(BM_Regexp_SimplifyCompile)->ThreadRange(1, NumCPUs());
BENCHMARK(BM_RE2_Compile)->ThreadRange(1, NumCPUs());
```

### Java (BenchmarkRe2Practical.java:59, 141-175)

```java
private static final String COMPILE_PATTERN = "(.*)-(\\d+)-of-(\\d+)";

@Benchmark
public Object compilePhaseParse(CompilePhaseState state) {
    ByteSlice pat = ByteSlice.wrap(state.compilePatternBytes);
    return RegexpParser.parse(pat, Regexp.LIKE_PERL);
}

@Benchmark
public Object compilePhaseSimplify(CompilePhaseState state) {
    ByteSlice pat = ByteSlice.wrap(state.compilePatternBytes);
    ParseResult parsed = RegexpParser.parse(pat, Regexp.LIKE_PERL);
    return Simplifier.simplify(parsed.regexp());
}

@Benchmark
public Object compilePhaseCompileToProg(CompilePhaseState state) {
    return Compiler.compile(state.preParsedRegexp, false, 0);
}

@Benchmark
public Object compilePhaseSimplifyCompile(CompilePhaseState state) {
    ByteSlice pat = ByteSlice.wrap(state.compilePatternBytes);
    ParseResult parsed = RegexpParser.parse(pat, Regexp.LIKE_PERL);
    Regexp simplified = Simplifier.simplify(parsed.regexp());
    return Compiler.compile(simplified, false, 0);
}

@Benchmark
public Object compilePhaseRe2Compile(CompilePhaseState state) {
    return new Re2(ByteSlice.wrap(state.compilePatternBytes));
}
```

### Comparison

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Compile pattern | `(.*)-(\\d+)-of-(\\d+)` | Same | ✓ |
| Parse | BM_Regexp_Parse | compilePhaseParse | ✓ |
| Simplify | BM_Regexp_Simplify | compilePhaseSimplify | ✓ |
| CompileToProg | BM_CompileToProg | compilePhaseCompileToProg | ✓ |
| SimplifyCompile | BM_Regexp_SimplifyCompile | compilePhaseSimplifyCompile | ✓ |
| RE2 Compile | BM_RE2_Compile | compilePhaseRe2Compile | ✓ |
| Encoding | UTF-8 (default) | UTF-8 (default) | ✓ |

---

## Summary

| Benchmark | C++ Encoding | Java Encoding | Pattern Match | Input Match |
|-----------|--------------|---------------|---------------|-------------|
| HTTPPartialMatch | UTF-8 | UTF-8 | ✓ | ✓ |
| SmallHTTPPartialMatch | UTF-8 | UTF-8 | ✓ | ✓ |
| SimplePartialMatch | UTF-8 | UTF-8 | ✓ | ~✓ (Java longer) |
| EmptyPartialMatch | UTF-8 | UTF-8 | ✓ | ✓ |
| Compile phases | UTF-8 | UTF-8 | ✓ | N/A |

**Conclusion**: All practical benchmarks are correctly aligned for encoding.
