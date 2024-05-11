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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("restriction")
@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkEquals
{
    @Benchmark
    public void arraysEquals(BenchmarkData data)
    {
        if (Arrays.equals(data.bytes, 0, data.bytes.length, data.bytes2, 0, data.bytes.length)) {
            throw new IllegalStateException("Not true");
        }
    }

    @Benchmark
    public void sliceEquals(BenchmarkData data)
    {
        if (data.slice.equals(data.slice2)) {
            throw new IllegalStateException("Not true");
        }
    }

    @Benchmark
    public void sliceDirectEquals(BenchmarkData data)
    {
        if (Arrays.equals(data.slice.byteArray(), data.slice.byteArrayOffset(), data.slice.byteArrayOffset() + data.size, data.slice2.byteArray(), data.slice2.byteArrayOffset(), data.slice2.byteArrayOffset() + data.size)) {
            throw new IllegalStateException("Not true");
        }
    }

    @Benchmark
    public void sliceUnwrap(BenchmarkData data)
    {
        if (data.slice.byteArray().length != data.size) {
            throw new IllegalStateException("Not true");
        }
    }

    @Benchmark
    public void segmentMismatchEquals(BenchmarkData data)
    {
        if (MemorySegment.mismatch(data.segment, 0, data.size, data.segment2, 0, data.size) == -1) {
            throw new IllegalStateException("Not true");
        }
    }

    @Benchmark
    public void forLoopOverBytes(BenchmarkData data)
    {
        for (int i = 0; i < data.size; i++) {
            if (data.bytes[i] != data.bytes2[i]) {
                return;
            }
        }

        throw new IllegalStateException("Not true");
    }

    @Benchmark
    public void forLoopOverSegment(BenchmarkData data)
    {
        for (int i = 0; i < data.size; i++) {
            if (data.segment.get(JAVA_BYTE, i) != data.segment2.get(JAVA_BYTE, i)) {
                return;
            }
        }

        throw new IllegalStateException("Not true");
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({
                "512",
                "1024",
                "16411",
        })
        public int size;

        private byte[] bytes;
        private byte[] bytes2;

        private MemorySegment segment;
        private MemorySegment segment2;

        private Slice slice;
        private Slice slice2;

        @Setup
        public void setup()
        {
            bytes = ("a".repeat(size - 3) + "def").getBytes();
            bytes2 = ("a".repeat(size - 4) + "abcd").getBytes();

            segment = MemorySegment.ofArray(bytes);
            segment2 = MemorySegment.ofArray(bytes2);

            slice = Slices.wrappedBuffer(bytes);
            slice2 = Slices.wrappedBuffer(bytes2);
        }

        public byte[] bytes()
        {
            return bytes;
        }

        public MemorySegment segment()
        {
            return segment;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkEquals.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
