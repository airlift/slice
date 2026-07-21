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
package io.airlift.slice.re2;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.re2.Re2BenchmarkRunner.buildOptions;
import static java.nio.charset.StandardCharsets.UTF_8;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkTrinoRegexpSearchEdges
{
    private static final byte[] CAPTURE = "abc-123".getBytes(UTF_8);
    private static final Slice REPLACEMENT = Slices.utf8Slice("_".repeat(128));

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({
                "singleByteNoMatch",
                "singleByteLateMatch",
                "captureNoMatch",
                "captureLateMatch",
                "unsupportedEndNoMatch",
                "unsupportedEndLateMatch",
                "unsupportedBoundaryNoMatch",
                "unsupportedBoundaryLateMatch"})
        String workload;

        @Param({"1024", "32768"})
        int sourceLength;

        private TrinoRegexp regexp;
        private Slice pattern;
        private Slice source;
        private Slice expectedExtract;
        private Slice expectedReplacement;

        @Setup
        public void setup()
        {
            byte[] sourceBytes = new byte[sourceLength];
            String patternText;
            int matchStart = -1;
            byte[] match = null;
            switch (workload) {
                case "singleByteNoMatch" -> {
                    patternText = "[;x]";
                    Arrays.fill(sourceBytes, (byte) 'a');
                }
                case "singleByteLateMatch" -> {
                    patternText = "[;x]";
                    Arrays.fill(sourceBytes, (byte) 'a');
                    match = new byte[] {'x'};
                    matchStart = sourceBytes.length - match.length;
                }
                case "captureNoMatch" -> {
                    patternText = "([a-z]+)-([0-9]+)";
                    Arrays.fill(sourceBytes, (byte) '.');
                }
                case "captureLateMatch" -> {
                    patternText = "([a-z]+)-([0-9]+)";
                    Arrays.fill(sourceBytes, (byte) '.');
                    match = CAPTURE;
                    matchStart = sourceBytes.length - match.length;
                }
                case "unsupportedEndNoMatch" -> {
                    patternText = "a$";
                    Arrays.fill(sourceBytes, (byte) 'b');
                }
                case "unsupportedEndLateMatch" -> {
                    patternText = "a$";
                    Arrays.fill(sourceBytes, (byte) 'b');
                    match = new byte[] {'a'};
                    matchStart = sourceBytes.length - match.length;
                }
                case "unsupportedBoundaryNoMatch" -> {
                    patternText = "\\ba\\b";
                    Arrays.fill(sourceBytes, (byte) 'b');
                }
                case "unsupportedBoundaryLateMatch" -> {
                    patternText = "\\ba\\b";
                    Arrays.fill(sourceBytes, (byte) 'b');
                    sourceBytes[sourceBytes.length - 2] = ' ';
                    match = new byte[] {'a'};
                    matchStart = sourceBytes.length - match.length;
                }
                default -> throw new IllegalArgumentException("unknown workload: " + workload);
            }

            if (match != null) {
                System.arraycopy(match, 0, sourceBytes, matchStart, match.length);
            }
            pattern = Slices.utf8Slice(patternText);
            regexp = TrinoRegexp.compile(pattern);
            source = Slices.wrappedBuffer(sourceBytes);
            expectedExtract = match == null ? null : Slices.wrappedBuffer(match);
            expectedReplacement = replace(source, matchStart, match == null ? 0 : match.length);
        }

        private static Slice replace(Slice source, int matchStart, int matchLength)
        {
            if (matchStart < 0) {
                return source;
            }
            DynamicSliceOutput output = new DynamicSliceOutput(source.length() - matchLength + REPLACEMENT.length());
            output.writeBytes(source, 0, matchStart);
            output.writeBytes(REPLACEMENT);
            output.writeBytes(source, matchStart + matchLength, source.length() - matchStart - matchLength);
            return output.slice();
        }

        Slice expectedExtract()
        {
            return expectedExtract;
        }

        Slice expectedReplacement()
        {
            return expectedReplacement;
        }
    }

    @Benchmark
    public boolean contains(BenchmarkData data)
    {
        return data.regexp.contains(data.source);
    }

    @Benchmark
    public TrinoRegexp compile(BenchmarkData data)
    {
        return TrinoRegexp.compile(data.pattern);
    }

    @Benchmark
    public Slice extract(BenchmarkData data)
    {
        return data.regexp.extract(data.source);
    }

    @Benchmark
    public Slice replace(BenchmarkData data)
    {
        return data.regexp.replace(data.source, REPLACEMENT);
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = buildOptions(BenchmarkTrinoRegexpSearchEdges.class, args);
        new Runner(options).run();
    }
}
