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

import static io.airlift.slice.re2.Re2.Anchor.UNANCHORED;
import static io.airlift.slice.re2.Re2BenchmarkRunner.buildOptions;
import static java.nio.charset.StandardCharsets.UTF_8;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkRe2PublicApi
{
    private static final byte[] MATCH = "abc-123".getBytes(UTF_8);

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"tinyMatch", "tinyNoMatch", "largeMatch", "largeNoMatch"})
        String workload;

        private Re2 pattern;
        private Slice source;
        private int[] groups;
        private Re2Matcher matcher;

        @Setup
        public void setup()
        {
            boolean hasMatch;
            int sourceLength;
            switch (workload) {
                case "tinyMatch" -> {
                    sourceLength = 64;
                    hasMatch = true;
                }
                case "tinyNoMatch" -> {
                    sourceLength = 64;
                    hasMatch = false;
                }
                case "largeMatch" -> {
                    sourceLength = 32_768;
                    hasMatch = true;
                }
                case "largeNoMatch" -> {
                    sourceLength = 32_768;
                    hasMatch = false;
                }
                default -> throw new IllegalArgumentException("unknown workload: " + workload);
            }

            byte[] sourceBytes = new byte[sourceLength];
            Arrays.fill(sourceBytes, (byte) '.');
            if (hasMatch) {
                inject(sourceBytes, MATCH, sourceLength / 4, sourceLength / 2, sourceLength * 3 / 4);
            }

            pattern = Re2.compile(Slices.utf8Slice("([a-z]+)-([0-9]+)"));
            source = Slices.wrappedBuffer(sourceBytes);
            groups = new int[6];
            matcher = pattern.matcher(source);
        }

        private static void inject(byte[] target, byte[] value, int... positions)
        {
            for (int position : positions) {
                System.arraycopy(value, 0, target, position, value.length);
            }
        }
    }

    @Benchmark
    public boolean partialMatch(BenchmarkData data)
    {
        return data.pattern.partialMatch(data.source);
    }

    @Benchmark
    public boolean matchIntoReusedBuffer(BenchmarkData data)
    {
        return data.pattern.matchInto(data.source, UNANCHORED, data.groups);
    }

    @Benchmark
    public MatchResult partialMatchResult(BenchmarkData data)
    {
        return data.pattern.partialMatchResult(data.source);
    }

    @Benchmark
    public Re2Matcher createMatcher(BenchmarkData data)
    {
        return data.pattern.matcher(data.source);
    }

    @Benchmark
    public boolean findWithNewMatcher(BenchmarkData data)
    {
        return data.pattern.matcher(data.source).find();
    }

    @Benchmark
    public boolean findWithReusedMatcher(BenchmarkData data)
    {
        return data.matcher.reset(data.source).find();
    }

    @Benchmark
    public long findAllWithNewMatcher(BenchmarkData data)
    {
        return findAll(data.pattern.matcher(data.source));
    }

    @Benchmark
    public long findAllWithReusedMatcher(BenchmarkData data)
    {
        return findAll(data.matcher.reset(data.source));
    }

    private static long findAll(Re2Matcher matcher)
    {
        long checksum = 0;
        while (matcher.find()) {
            checksum += matcher.start() + matcher.end();
        }
        return checksum;
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = buildOptions(BenchmarkRe2PublicApi.class, args);
        new Runner(options).run();
    }
}
