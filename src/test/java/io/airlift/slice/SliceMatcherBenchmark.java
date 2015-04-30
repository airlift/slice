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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 4, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = MILLISECONDS)
public class SliceMatcherBenchmark
{
    @Benchmark
    public int benchmarkIndexOf(BenchmarkData data)
    {
        return data.getData().indexOf(data.getNeedle(), 0);
    }

    @Benchmark
    public int benchmarkMatcherFind(BenchmarkData data)
    {
        return data.getMatcher().find(data.getData(), 0);
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Thread)
    public static class BenchmarkData
    {
        @Param({"10", "100", "1500", "200", "500", "1000", "10000" })
        private int length;

        @Param({"1", "5", "10", "20", "50", "100" })
        private int needleLength;

        @Param({ "true", "false" })
        private boolean match = true;

        private Slice data;
        private Slice needle;
        private SliceMatcher matcher;

        public BenchmarkData()
        {
        }

        public BenchmarkData(int length, boolean match)
                throws IOException
        {
            this.length = length;
            this.match = match;
            setup();
        }

        @Setup
        public void setup()
                throws IOException
        {
            data = Slices.copyOf(wrappedBuffer(toByteArray(getResource("corpus.txt"))), 0, length);
            needle = Slices.allocate(needleLength);
            needle.fill((byte) 'A');
            matcher = SliceMatcher.sliceMatcher(needle);
            if (match) {
                data.setBytes(data.length() - needle.length(), needle);
            }
        }

        public Slice getData()
        {
            return data;
        }

        public Slice getNeedle()
        {
            return needle;
        }

        public SliceMatcher getMatcher()
        {
            return matcher;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + SliceMatcherBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
