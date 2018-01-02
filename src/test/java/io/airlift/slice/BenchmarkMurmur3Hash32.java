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

import com.google.common.hash.Hashing;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(5)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkMurmur3Hash32
{
    @Benchmark
    public int hash(BenchmarkData data, ByteCounter counter)
    {
        counter.add(data.getSlice().length());
        return Murmur3Hash32.hash(data.getSlice());
    }

    @Benchmark
    public int guava(BenchmarkData data, ByteCounter counter)
    {
        counter.add(data.getSlice().length());
        return Hashing.murmur3_32().hashBytes(data.getBytes()).asInt();
    }

    @Benchmark
    public int specializedHashInt(SingleLong data, ByteCounter counter)
    {
        counter.add(SizeOf.SIZE_OF_INT);
        return Murmur3Hash32.hash((int) data.getValue());
    }

    @Benchmark
    public int hashInt(BenchmarkData data, ByteCounter counter)
    {
        counter.add(SizeOf.SIZE_OF_INT);
        return Murmur3Hash32.hash(data.getSlice(), 0, 4);
    }

    @Benchmark
    public int specializedHashLong(SingleLong data, ByteCounter counter)
    {
        counter.add(SizeOf.SIZE_OF_LONG);
        return Murmur3Hash32.hash(data.getValue());
    }

    @Benchmark
    public int hashLong(BenchmarkData data, ByteCounter counter)
    {
        counter.add(SizeOf.SIZE_OF_LONG);
        return Murmur3Hash32.hash(data.getSlice(), 0, 8);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkMurmur3Hash32.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
