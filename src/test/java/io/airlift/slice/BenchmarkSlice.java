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
import org.openjdk.jmh.annotations.Level;
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
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("MethodMayBeStatic")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(5)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkSlice
{
    @Benchmark
    public Object compareTo(BenchmarkData data)
    {
        return data.slice1.compareTo(0, data.slice1.length(), data.slice2, 0, data.slice2.length());
    }

    @Benchmark
    public Object equalsUnchecked(BenchmarkData data)
    {
        return data.slice1.equals(0, data.slice1.length(), data.slice2, 0, data.slice1.length());
    }

    @Benchmark
    public Object equalsObject(BenchmarkData data)
    {
        return data.slice1.equals(data.slice2);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1", "7", "8", "16", "32", "64", "127", "32779"})
        private int size = 1;

        private Slice slice1;
        private Slice slice2;

        @Setup(Level.Iteration)
        public void setup()
        {
            slice1 = Slices.allocate(size);
            slice2 = Slices.allocate(size);

            if (ThreadLocalRandom.current().nextBoolean()) {
                slice1.setByte(size - 1, 1);
            }
            else {
                slice2.setByte(size - 1, 1);
            }
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkSlice().equalsObject(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSlice.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
