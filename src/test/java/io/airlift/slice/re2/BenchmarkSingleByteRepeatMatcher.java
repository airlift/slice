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

import static io.airlift.slice.re2.Re2BenchmarkRunner.buildOptions;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkSingleByteRepeatMatcher
{
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"allMiss", "allMatch", "alternating", "longRuns", "unicode"})
        String shape;

        @Param({"1024", "32768"})
        int sourceLength;

        private Re2 re2;
        private SingleByteRepeatMatcher repeatMatcher;
        private Slice source;

        @Setup
        public void setup()
        {
            byte[] bytes = new byte[sourceLength];
            switch (shape) {
                case "allMiss" -> Arrays.fill(bytes, (byte) 'a');
                case "allMatch" -> Arrays.fill(bytes, (byte) 'x');
                case "alternating" -> {
                    for (int position = 0; position < bytes.length; position++) {
                        bytes[position] = (position & 1) == 0 ? (byte) 'x' : (byte) 'a';
                    }
                }
                case "longRuns" -> {
                    for (int position = 0; position < bytes.length; position++) {
                        bytes[position] = (position & 127) < 64 ? (byte) 'x' : (byte) 'a';
                    }
                }
                case "unicode" -> {
                    byte[] rune = Slices.utf8Slice("💰").getBytes();
                    for (int position = 0; position < bytes.length; position += rune.length) {
                        System.arraycopy(rune, 0, bytes, position, Math.min(rune.length, bytes.length - position));
                    }
                }
                default -> throw new IllegalArgumentException("unknown shape: " + shape);
            }

            re2 = Re2.compile(Slices.utf8Slice("x*"));
            repeatMatcher = re2.createSingleByteRepeatMatcher();
            source = Slices.wrappedBuffer(bytes);
        }
    }

    @Benchmark
    public long regularMatcher(BenchmarkData data)
    {
        Re2Matcher matcher = data.re2.matcher(data.source);
        long count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    @Benchmark
    public long singleByteRepeatCounter(BenchmarkData data)
    {
        return data.repeatMatcher.count(data.source);
    }

    @Benchmark
    public long regularMatcherBoundaries(BenchmarkData data)
    {
        Re2Matcher matcher = data.re2.matcher(data.source);
        long boundaries = 0;
        while (matcher.find()) {
            boundaries += ((long) matcher.start() << 32) | matcher.end();
        }
        return boundaries;
    }

    @Benchmark
    public long singleByteRepeatMatcherBoundaries(BenchmarkData data)
    {
        SingleByteRepeatMatcher.Cursor matcher = data.repeatMatcher.matcher(data.source);
        long boundaries = 0;
        while (matcher.find()) {
            boundaries += ((long) matcher.start() << 32) | matcher.end();
        }
        return boundaries;
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = buildOptions(BenchmarkSingleByteRepeatMatcher.class, args);
        new Runner(options).run();
    }
}
