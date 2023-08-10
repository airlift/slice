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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.JvmUtils.unsafe;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

@SuppressWarnings("restriction")
@BenchmarkMode(Mode.Throughput)
@Fork(1)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class MemoryCopyBenchmark
{
    private static final int PAGE_SIZE = 4 * 1024;
    private static final int N_PAGES = 256 * 1024;
    private static final int ALLOC_SIZE = PAGE_SIZE * N_PAGES;

    @State(Scope.Thread)
    public static class Buffers
    {
        @Param({
                "0",
                "32",
                "128",
                "512",
                "1024",
                "1048576",
                "134217728",
        })
        public int size;

        @Param({
                "ARRAY_COPY",
                "SLICE",
                "CUSTOM_LOOP",
                "UNSAFE",
        })
        public CopyStrategy copyStrategy;

        Slice data;
        long startOffset;
        long destOffset;

        @Setup
        public void fillWithBogusData()
        {
            data = Slices.allocate(ALLOC_SIZE);
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
    public Slice copy(Buffers buffers)
    {
        buffers.copyStrategy.doCopy(buffers.data, buffers.startOffset, buffers.destOffset, buffers.size);
        return buffers.data;
    }

    public enum CopyStrategy
    {
        ARRAY_COPY {
            @Override
            public void doCopy(Slice data, long src, long dest, int length)
            {
                byte[] byteArray = data.byteArray();
                int byteArrayOffset = data.byteArrayOffset();
                System.arraycopy(byteArray, (int) (byteArrayOffset + src), byteArray, (int) (byteArrayOffset + src), length);
            }
        },

        SLICE {
            @Override
            public void doCopy(Slice data, long src, long dest, int length)
            {
                data.setBytes((int) dest, data, (int) src, length);
            }
        },

        CUSTOM_LOOP {
            @Override
            public void doCopy(Slice data, long src, long dest, int length)
            {
                byte[] base = data.byteArray();
                long offset = data.byteArrayOffset() + ARRAY_BYTE_BASE_OFFSET;
                while (length >= SizeOf.SIZE_OF_LONG) {
                    long srcLong = unsafe.getLong(base, src + offset);
                    unsafe.putLong(base, dest + offset, srcLong);

                    offset += SizeOf.SIZE_OF_LONG;
                    length -= SizeOf.SIZE_OF_LONG;
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
            public void doCopy(Slice data, long srcOffset, long destOffset, int length)
            {
                byte[] base = data.byteArray();
                long address = data.byteArrayOffset() + ARRAY_BYTE_BASE_OFFSET;
                srcOffset += address;
                destOffset += address;
                int bytesToCopy = length - (length % 8);
                unsafe.copyMemory(base, srcOffset, base, destOffset, bytesToCopy);
                unsafe.copyMemory(base, srcOffset + bytesToCopy, base, destOffset + bytesToCopy, length - bytesToCopy);
            }
        };

        public abstract void doCopy(Slice data, long src, long dest, int length);
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
