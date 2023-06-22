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
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.slice.MemoryLayout.SIZE_OF_LONG;

@SuppressWarnings("restriction")
@BenchmarkMode(Mode.Throughput)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class MemoryCopyBenchmark
{
    private static final Unsafe unsafe;

    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static final int BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);

    static final int PAGE_SIZE = 4 * 1024;
    static final int N_PAGES = 256 * 1024;
    static final int ALLOC_SIZE = PAGE_SIZE * N_PAGES;

    @State(Scope.Thread)
    public static class Buffers
    {
        byte[] bytes;
        Slice data;
        long startOffset;
        long destOffset;

        @Setup
        public void fillWithBogusData()
        {
            bytes = new byte[ALLOC_SIZE];
            data = Slices.wrappedBuffer(bytes);
            for (int idx = 0; idx < data.length() / 8; idx++) {
                data.setLong(idx, ThreadLocalRandom.current().nextLong());
            }

            long startOffsetPages = ThreadLocalRandom.current().nextInt(N_PAGES / 4);
            long destOffsetPages = ThreadLocalRandom.current().nextInt(N_PAGES / 4) + N_PAGES / 2;

            startOffset = startOffsetPages * PAGE_SIZE;
            destOffset = destOffsetPages * PAGE_SIZE;
        }
    }

    @Benchmark
    public Slice b00sliceZero(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 0);
    }

    @Benchmark
    public Slice b01customLoopZero(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 0);
    }

    @Benchmark
    public Slice b02unsafeZero(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 0);
    }

    @Benchmark
    public Slice b03slice32B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 32);
    }

    @Benchmark
    public Slice b04customLoop32B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 32);
    }

    @Benchmark
    public Slice b05unsafe32B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 32);
    }

    @Benchmark
    public Slice b06slice128B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 128);
    }

    @Benchmark
    public Slice b07customLoop128B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 128);
    }

    @Benchmark
    public Slice b08unsafe128B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 128);
    }

    @Benchmark
    public Slice b09slice512B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 512);
    }

    @Benchmark
    public Slice b10customLoop512B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 512);
    }

    @Benchmark
    public Slice b11unsafe512B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 512);
    }

    @Benchmark
    public Slice b12slice1K(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 1024);
    }

    @Benchmark
    public Slice b13customLoop1K(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 1024);
    }

    @Benchmark
    public Slice b14unsafe1K(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 1024);
    }

    @Benchmark
    public Slice b15slice1M(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 1024 * 1024);
    }

    @Benchmark
    public Slice b16customLoop1M(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 1024 * 1024);
    }

    @Benchmark
    public Slice b17unsafe1M(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 1024 * 1024);
    }

    @Benchmark
    public Slice b18slice128M(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 128 * 1024 * 1024);
    }

    @Benchmark
    public Slice b19customLoop128M(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 128 * 1024 * 1024);
    }

    @Benchmark
    public Slice b20unsafe128M(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 128 * 1024 * 1024);
    }

    static Slice doCopy(Buffers buffers, CopyStrategy strategy, int length)
    {
        strategy.doCopy(buffers, length);
        return buffers.data;
    }

    private enum CopyStrategy
    {
        SLICE {
            @Override
            public void doCopy(Buffers buffers, int length)
            {
                buffers.data.setBytes((int) buffers.destOffset, buffers.data, (int) buffers.startOffset, length);
            }
        },

        CUSTOM_LOOP {
            @Override
            public void doCopy(Buffers buffers, int length)
            {
                Object base = buffers.bytes;
                long src = buffers.startOffset;
                long dest = buffers.destOffset;
                long offset = BYTE_ARRAY_BASE_OFFSET;
                while (length >= SIZE_OF_LONG) {
                    long srcLong = unsafe.getLong(base, src + offset);
                    unsafe.putLong(base, dest + offset, srcLong);

                    offset += SIZE_OF_LONG;
                    length -= SIZE_OF_LONG;
                }

                while (length > 0) {
                    byte srcByte = unsafe.getByte(base, src + offset);
                    unsafe.putByte(base, dest + offset, srcByte);

                    offset++;
                    length--;
                }
            }
        },

        UNSAFE {
            @Override
            public void doCopy(Buffers buffers, int length)
            {
                Object base = buffers.bytes;
                long srcOffset = buffers.startOffset + BYTE_ARRAY_BASE_OFFSET;
                long destOffset = buffers.destOffset + BYTE_ARRAY_BASE_OFFSET;
                int bytesToCopy = length - (length % 8);
                unsafe.copyMemory(base, srcOffset, base, destOffset, bytesToCopy);
                unsafe.copyMemory(base, srcOffset + bytesToCopy, base, destOffset + bytesToCopy, length - bytesToCopy);
            }
        };

        public abstract void doCopy(Buffers buffers, int length);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + MemoryCopyBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
