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

import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.re2.Re2BenchmarkRunner.buildOptions;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkBoundedCharacterClassCounter
{
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"ascii", "russian", "mixed"})
        String workload;

        @Param("32768")
        int sourceLength;

        private TrinoRegexp regexp;
        private Slice source;

        @Setup
        public void setup()
        {
            Slice token;
            switch (workload) {
                case "ascii" -> token = utf8Slice("abcdefgh 12 ");
                case "russian" -> token = utf8Slice("абвгдежз 12 ");
                case "mixed" -> token = utf8Slice("abcdefgh абвгдежз 12 💰 ");
                default -> throw new IllegalArgumentException("unknown workload: " + workload);
            }
            regexp = TrinoRegexp.compile(utf8Slice("\\p{L}{8,13}"));
            source = repeat(token, sourceLength);
        }
    }

    @Benchmark
    public long specialized(BenchmarkData data)
    {
        return data.regexp.count(data.source);
    }

    @Benchmark
    public long generalMatcher(BenchmarkData data)
    {
        Re2Matcher matcher = data.regexp.pattern().groupZeroMatcher(data.source, null);
        long count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    private static Slice repeat(Slice token, int length)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(length);
        while (output.size() + token.length() <= length) {
            output.writeBytes(token);
        }
        while (output.size() < length) {
            output.writeByte(' ');
        }
        return output.slice();
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = buildOptions(BenchmarkBoundedCharacterClassCounter.class, args);
        new Runner(options).run();
    }
}
