/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.slice;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@BenchmarkMode(AverageTime)
@OutputTimeUnit(NANOSECONDS)
@Warmup(iterations = 3, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 4, time = 500, timeUnit = MILLISECONDS)
@Fork(3)
public class BenchmarkFullCaseMapping
{
    private static volatile Slice sink;

    private static final String[] INPUT_KINDS = {
            "ascii_lower", "ascii_upper", "ascii_mixed", "ascii_uncased",
            "mixed_prefix", "mixed_latin", "uncased_unicode", "supplementary",
            "upper_expansion", "lower_expansion", "sigma", "sigma_ignored", "malformed"};

    @Param({"ascii_lower", "ascii_upper", "ascii_mixed", "ascii_uncased", "mixed_prefix", "mixed_latin", "uncased_unicode", "supplementary", "upper_expansion", "lower_expansion", "sigma", "sigma_ignored", "malformed"})
    public String inputKind;

    @Param({"32", "1024"})
    public int targetBytes;

    private byte[] input;
    private int length;

    @Setup
    public void setup()
    {
        byte[] bytes = createInput(inputKind, targetBytes);
        input = new byte[bytes.length + 10];
        Arrays.fill(input, (byte) 'A');
        System.arraycopy(bytes, 0, input, 7, bytes.length);
        length = bytes.length;

        // Populate shared JIT profiles with both APIs, both directions, and every
        // workload in each fork, before JMH's workload-specific warmup begins.
        for (String kind : INPUT_KINDS) {
            byte[] pollution = createInput(kind, 1024);
            for (int iteration = 0; iteration < 1000; iteration++) {
                sink = SliceUtf8.toUpperCase(pollution, 0, pollution.length);
                sink = SliceUtf8.toLowerCase(pollution, 0, pollution.length);
                sink = SliceUtf8.toUpperCaseFull(pollution, 0, pollution.length);
                sink = SliceUtf8.toLowerCaseFull(pollution, 0, pollution.length);
            }
        }
        sink = null;
    }

    @Benchmark
    public Slice simpleUpper()
    {
        return SliceUtf8.toUpperCase(input, 7, length);
    }

    @Benchmark
    public Slice simpleLower()
    {
        return SliceUtf8.toLowerCase(input, 7, length);
    }

    @Benchmark
    public Slice fullUpper()
    {
        return SliceUtf8.toUpperCaseFull(input, 7, length);
    }

    @Benchmark
    public Slice fullLower()
    {
        return SliceUtf8.toLowerCaseFull(input, 7, length);
    }

    public static byte[] createInput(String kind, int targetBytes)
    {
        String pattern = switch (kind) {
            case "ascii_lower" -> "the quick brown fox jumps over the lazy dog. ";
            case "ascii_upper" -> "THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG. ";
            case "ascii_mixed" -> "The Quick Brown Fox Jumps Over The Lazy Dog. ";
            case "ascii_uncased" -> "0123456789:., /!?- ";
            case "mixed_prefix" -> "éThe quick brown fox jumps over the lazy dog. ";
            case "mixed_latin" -> "Été à Zürich: ÖL, déjà vu. ";
            case "uncased_unicode" -> "中文日本語😀 ";
            case "supplementary" -> "𐐀𐐨𐐁𐐩 ";
            case "upper_expansion" -> "Straße ﬃ ΐ hello ";
            case "lower_expansion" -> "İSTANBUL İ HELLO ";
            case "sigma" -> "ΟΣ ΟΣΑ AΣ:A AΣ-A ";
            case "sigma_ignored" -> "AΣ" + "\u0301\u0345\u200D".repeat(16) + " ";
            case "malformed" -> "AßİΣ ";
            default -> throw new IllegalArgumentException("Unknown input kind: " + kind);
        };
        StringBuilder text = new StringBuilder();
        int bytes = 0;
        int[] codePoints = pattern.codePoints().toArray();
        for (int position = 0; bytes < targetBytes; position++) {
            int codePoint = codePoints[position % codePoints.length];
            int width = SliceUtf8.lengthOfCodePoint(codePoint);
            if (bytes + width > targetBytes) {
                break;
            }
            text.appendCodePoint(codePoint);
            bytes += width;
        }
        byte[] result = text.toString().getBytes(UTF_8);
        if (kind.equals("malformed")) {
            for (int position = 5; position < result.length; position += 17) {
                result[position] = (byte) 0xFF;
            }
        }
        return result;
    }
}
