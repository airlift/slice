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
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(5)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkMurmur3
{
    @GenerateMicroBenchmark
    public long hash64(Data data)
    {
        return Murmur3.hash64(data.getSlice());
    }

    @GenerateMicroBenchmark
    public Slice hash(Data data)
    {
        return Murmur3.hash(data.getSlice());
    }

    @GenerateMicroBenchmark
    public long guava(Data data)
    {
        return Hashing.murmur3_128().hashBytes(data.getBytes()).asLong();
    }

    @GenerateMicroBenchmark
    public long specializedHashLong(Data data)
    {
        return Murmur3.hash64(data.getLong());
    }

    @GenerateMicroBenchmark
    public long hashLong(Data data)
    {
        return Murmur3.hash64(data.getSlice(), 0, 8);
    }

    @State(Scope.Thread)
    public static class Data
    {
        private byte[] bytes;
        private Slice slice;
        private long value;

        @Setup
        public void setup()
        {
            bytes = new byte[1024 * 1024]; // 1 MB
            ThreadLocalRandom.current().nextBytes(bytes);
            slice = Slices.wrappedBuffer(bytes);

            value = ThreadLocalRandom.current().nextLong();
        }

        public Slice getSlice()
        {
            return slice;
        }

        public byte[] getBytes()
        {
            return bytes;
        }

        public long getLong()
        {
            return value;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkMurmur3.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}

