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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.re2.Re2.Anchor.UNANCHORED;
import static io.airlift.slice.re2.Re2BenchmarkRunner.buildOptions;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkDfaFixedDistanceByte
{
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param
        InputShape inputShape;

        @Param({"1024", "32768"})
        int sourceLength;

        private Re2 pattern;
        private Slice source;
        private final int[] matchBoundary = new int[2];

        @Setup
        public void setup()
        {
            pattern = Re2.compile(Slices.utf8Slice("[a-z]{8}-[0-9]{4}"));
            byte[] sourceBytes = new byte[sourceLength];
            Arrays.fill(sourceBytes, (byte) 'a');

            switch (inputShape) {
                case SPARSE_MATCH -> {
                    byte[] match = "abcdefgh-1234".getBytes(StandardCharsets.UTF_8);
                    for (int position : new int[] {sourceLength / 8, sourceLength / 2, sourceLength * 7 / 8 - match.length}) {
                        System.arraycopy(match, 0, sourceBytes, position, match.length);
                    }
                }
                case DENSE_FALSE_POSITIVE -> {
                    for (int position = 8; position < sourceBytes.length; position += 13) {
                        sourceBytes[position] = '-';
                    }
                }
                case NO_MATCH -> {}
            }
            source = Slices.wrappedBuffer(sourceBytes);
        }
    }

    public enum InputShape
    {
        SPARSE_MATCH,
        DENSE_FALSE_POSITIVE,
        NO_MATCH,
    }

    @Benchmark
    public boolean partialMatch(BenchmarkData data)
    {
        return data.pattern.partialMatch(data.source);
    }

    @Benchmark
    public boolean matchBoundary(BenchmarkData data)
    {
        return data.pattern.matchInto(data.source, UNANCHORED, data.matchBoundary);
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = buildOptions(BenchmarkDfaFixedDistanceByte.class, args);
        new Runner(options).run();
    }
}
