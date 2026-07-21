# Text Generation Detailed Comparison

## Files Compared

| C++ | Java |
|-----|------|
| `re2_upstream/re2/testing/regexp_benchmark.cc` | `Re2BenchmarkRunner.java` |

---

## Key Finding: Caching Difference Does NOT Affect Benchmark Timing

**C++ caches globally**: Generates 16 MB once in static initializer, returns substrings.

**Java generates per-setup**: Generates fresh text in `@Setup(Level.Trial)`.

**This does NOT affect benchmark timing** because:
- C++ text generation happens in static initialization (before `main()`)
- Java text generation happens in `@Setup(Level.Trial)` which runs **once before** warmup/measurement
- Neither includes text generation in the `@Benchmark` method timing loop

Both approaches generate text **outside** the timed benchmark loop. The performance comparison is valid.

---

## RandomText Implementation

### C++ (regexp_benchmark.cc:161-178)

```cpp
// Generate random text that won't contain the search string,
// to test worst-case search behavior.
std::string RandomText(int64_t nbytes) {
  static const std::string* const text = []() {
    std::string* text = new std::string;
    srand(1);                              // Fixed seed
    text->resize(16<<20);                  // Pre-allocate 16 MB
    for (int64_t i = 0; i < 16<<20; i++) {
      // Generate a one-byte rune that isn't a control character (e.g. '\n').
      // Clipping to 0x20 introduces some bias, but we don't need uniformity.
      int byte = rand() & 0x7F;            // 0-127
      if (byte < 0x20)
        byte = 0x20;                       // Clip to 0x20
      (*text)[i] = byte;
    }
    return text;
  }();
  ABSL_CHECK_LE(nbytes, 16<<20);
  return text->substr(0, nbytes);          // Return prefix
}
```

### Java (Re2BenchmarkRunner.java:86-98)

```java
/**
 * Port of upstream RandomText() with same seed for reproducibility.
 */
public static byte[] randomText(int nbytes) {
    Random rng = new Random(1);             // Fixed seed
    byte[] text = new byte[nbytes];
    for (int i = 0; i < nbytes; i++) {
        int b = rng.nextInt(128);           // 0-127
        if (b < 0x20) {
            b = 0x20;                       // Clip to 0x20
        }
        text[i] = (byte) b;
    }
    return text;
}
```

---

## Detailed Comparison

### Seed

| Aspect | C++ | Java |
|--------|-----|------|
| Function | `srand(1)` | `new Random(1)` |
| Value | 1 | 1 |
| Match | ✓ |

**Note**: Same seed value, but different RNG implementations.

### Random Number Generation

| Aspect | C++ | Java |
|--------|-----|------|
| Function | `rand()` | `rng.nextInt(128)` |
| Algorithm | C standard library (implementation-defined) | Linear congruential generator |
| Range | 0 to RAND_MAX | 0 to 127 |
| Masking | `& 0x7F` → 0-127 | Direct 0-127 |
| Match | ~✓ (same range, different sequences) |

### Character Clipping

| Aspect | C++ | Java |
|--------|-----|------|
| Check | `if (byte < 0x20)` | `if (b < 0x20)` |
| Action | `byte = 0x20` | `b = 0x20` |
| Match | ✓ |

### Output Range

| Aspect | C++ | Java |
|--------|-----|------|
| Minimum | 0x20 (space) | 0x20 (space) |
| Maximum | 0x7F (DEL) | 0x7F (DEL) |
| Character count | 96 (0x20-0x7F) | 96 (0x20-0x7F) |
| Match | ✓ |

### Caching Strategy

| Aspect | C++ | Java |
|--------|-----|------|
| Strategy | Static, generate once (16 MB) | Per-call, no caching |
| First call | Generate 16 MB | Generate nbytes |
| Subsequent calls | Return substring | Generate nbytes |
| Memory | 16 MB permanent | nbytes temporary |

**Implication**: C++ generates text once and reuses it. Java generates fresh text each time. For benchmark purposes, this is acceptable since:
1. JMH's setup phase generates text before measurement
2. The character distribution is identical
3. Fresh generation allows different text sizes without substring overhead

---

## Character Distribution Analysis

Both implementations produce the same character distribution (uniform over 0x20-0x7F), but with different sequences due to different RNG algorithms.

### Distribution Characteristics

```
Original range: 0-127 (128 values)
After clipping: 0x20-0x7F (96 values)

For bytes 0x00-0x1F (32 values): All map to 0x20
For bytes 0x20-0x7F (96 values): Map to themselves

Probability of 0x20: 33/128 ≈ 25.8%
Probability of 0x21-0x7F: 1/128 ≈ 0.78% each
```

This bias is identical in both implementations. As noted in the C++ comment: "Clipping to 0x20 introduces some bias, but we don't need uniformity."

---

## Text Sizes

### C++ Benchmark Sizes

```cpp
BENCHMARK_RANGE(..., 8, 16<<20)
// Generates geometric progression: 8, 16, 32, 64, ..., 16777216
// Total sizes: 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192,
//              16384, 32768, 65536, 131072, 262144, 524288, 1048576,
//              2097152, 4194304, 8388608, 16777216
// Count: 22 sizes
```

### Java Benchmark Sizes

```java
// BenchmarkRe2Search.java
@Param({"8", "64", "512", "4096", "32768", "262144", "2097152", "16777216"})
// Count: 8 sizes

// BenchmarkRe2SearchNfa.java
@Param({"8", "64", "512", "4096", "32768", "262144"})
// Count: 6 sizes (smaller max for NFA)

// BenchmarkRe2FullMatch.java
@Param({"8", "64", "512", "4096", "32768", "262144", "2097152"})
// Count: 7 sizes
```

### Size Comparison Table

| Size | C++ DFA | C++ NFA | Java DFA | Java NFA | Java FullMatch |
|------|---------|---------|----------|----------|----------------|
| 8 | ✓ | ✓ | ✓ | ✓ | ✓ |
| 16 | ✓ | ✓ | - | - | - |
| 32 | ✓ | ✓ | - | - | - |
| 64 | ✓ | ✓ | ✓ | ✓ | ✓ |
| 128 | ✓ | ✓ | - | - | - |
| 256 | ✓ | ✓ | - | - | - |
| 512 | ✓ | ✓ | ✓ | ✓ | ✓ |
| 1K | ✓ | ✓ | - | - | - |
| 2K | ✓ | ✓ | - | - | - |
| 4K | ✓ | ✓ | ✓ | ✓ | ✓ |
| 8K | ✓ | ✓ | - | - | - |
| 16K | ✓ | ✓ | - | - | - |
| 32K | ✓ | ✓ | ✓ | ✓ | ✓ |
| 64K | ✓ | ✓ | - | - | - |
| 128K | ✓ | ✓ | - | - | - |
| 256K | ✓ | ✓ | ✓ | ✓ | ✓ |
| 512K | ✓ | - | - | - | - |
| 1M | ✓ | - | - | - | - |
| 2M | ✓ | - | ✓ | - | ✓ |
| 4M | ✓ | - | - | - | - |
| 8M | ✓ | - | - | - | - |
| 16M | ✓ | - | ✓ | - | - |

**Rationale for sparser Java sampling**:
1. JMH has higher per-iteration overhead than Google Benchmark
2. Key inflection points (8, 64, 512, 4K, 32K, 256K, 2M, 16M) cover the important ranges
3. Sparser sampling reduces total benchmark runtime significantly

---

## Summary

| Aspect | C++ | Java | Match? |
|--------|-----|------|--------|
| Seed | 1 | 1 | ✓ |
| Range | 0x20-0x7F | 0x20-0x7F | ✓ |
| Distribution | Biased toward 0x20 | Same bias | ✓ |
| RNG algorithm | `rand()` | `Random.nextInt()` | Different (acceptable) |
| Caching | Static 16 MB | None | Different (acceptable) |
| Size coverage | 22 sizes | 6-8 sizes | Sparser (acceptable) |

**Conclusion**: Text generation is functionally equivalent. The different RNG algorithms produce different byte sequences, but with identical character distributions. For performance benchmarking (not correctness testing), this is acceptable.
