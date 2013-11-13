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

import com.google.common.primitives.Ints;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
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

import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.slice.Slice.getUnsafe;

@SuppressWarnings("restriction")
@BenchmarkMode(Mode.Throughput)
@Fork(5)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class MemoryCopyBenchmark
{
    static final long PAGE_SIZE = 4 * 1024;
    static final int N_PAGES = 256 * 1024;
    static final long ALLOC_SIZE = PAGE_SIZE * N_PAGES;

    @State(Scope.Thread)
    public static class Buffers
    {
        Slice data;
        long startOffset;
        long destOffset;

        @Setup
        public void fillWithBogusData()
        {
            data = Slices.allocate(Ints.checkedCast(ALLOC_SIZE));
            for (int idx = 0; idx < data.length() / 8; idx++) {
                data.setLong(idx, ThreadLocalRandom.current().nextLong());
            }

            long startOffsetPages = ThreadLocalRandom.current().nextInt(N_PAGES / 4);
            long destOffsetPages = ThreadLocalRandom.current().nextInt(N_PAGES / 4) + N_PAGES / 2;

            startOffset = startOffsetPages * PAGE_SIZE;
            destOffset = destOffsetPages * PAGE_SIZE;
        }
    }

    @GenerateMicroBenchmark
    public Slice b00sliceZero(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 0);
    }

    @GenerateMicroBenchmark
    public Slice b01customLoopZero(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 0);
    }

    @GenerateMicroBenchmark
    public Slice b02unsafeZero(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 0);
    }

    @GenerateMicroBenchmark
    public Slice b03slice32B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 32);
    }

    @GenerateMicroBenchmark
    public Slice b04customLoop32B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 32);
    }

    @GenerateMicroBenchmark
    public Slice b05unsafe32B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 32);
    }

    @GenerateMicroBenchmark
    public Slice b06slice128B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 128);
    }

    @GenerateMicroBenchmark
    public Slice b07customLoop128B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 128);
    }

    @GenerateMicroBenchmark
    public Slice b08unsafe128B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 128);
    }

    @GenerateMicroBenchmark
    public Slice b09slice512B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 512);
    }

    @GenerateMicroBenchmark
    public Slice b10customLoop512B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 512);
    }

    @GenerateMicroBenchmark
    public Slice b11unsafe512B(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 512);
    }

    @GenerateMicroBenchmark
    public Slice b12slice1K(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 1024);
    }

    @GenerateMicroBenchmark
    public Slice b13customLoop1K(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 1024);
    }

    @GenerateMicroBenchmark
    public Slice b14unsafe1K(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 1024);
    }

    @GenerateMicroBenchmark
    public Slice b15slice1M(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 1024 * 1024);
    }

    @GenerateMicroBenchmark
    public Slice b16customLoop1M(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 1024 * 1024);
    }

    @GenerateMicroBenchmark
    public Slice b17unsafe1M(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 1024 * 1024);
    }

    @GenerateMicroBenchmark
    public Slice b18slice128M(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.SLICE, 128 * 1024 * 1024);
    }

    @GenerateMicroBenchmark
    public Slice b19customLoop128M(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.CUSTOM_LOOP, 128 * 1024 * 1024);
    }

    @GenerateMicroBenchmark
    public Slice b20unsafe128M(Buffers buffers)
    {
        return doCopy(buffers, CopyStrategy.UNSAFE, 128 * 1024 * 1024);
    }

    static Slice doCopy(Buffers buffers, CopyStrategy strategy, int length)
    {
        assert buffers.startOffset >= 0 : "startOffset < 0";
        assert buffers.destOffset >= 0 : "destOffset < 0";
        assert buffers.startOffset + length < ALLOC_SIZE : "startOffset + length >= ALLOC_SIZE";
        assert buffers.destOffset + length < ALLOC_SIZE : "destOffset + length >= ALLOC_SIZE";

        strategy.doCopy(buffers.data, buffers.startOffset, buffers.destOffset, length);
        return buffers.data;
    }

    private enum CopyStrategy
    {
        SLICE
                {
                    @Override
                    public void doCopy(Slice data, long src, long dest, int length)
                    {
                        data.setBytes((int) dest, data, (int) src, length);
                    }
                },

        CUSTOM_LOOP
                {
                    @Override
                    public void doCopy(Slice data, long src, long dest, int length)
                    {
                        Object base = data.getBase();
                        long offset = data.getAddress();
                        while (length >= SizeOf.SIZE_OF_LONG) {
                            long srcLong = getUnsafe().getLong(base, src + offset);
                            getUnsafe().putLong(base, dest + offset, srcLong);

                            offset += SizeOf.SIZE_OF_LONG;
                            length -= SizeOf.SIZE_OF_LONG;
                        }

                        while (length > 0) {
                            byte srcByte = getUnsafe().getByte(base, src + offset);
                            getUnsafe().putByte(base, dest + offset, srcByte);

                            offset++;
                            length--;
                        }
                    }
                },

        UNSAFE
                {
                    @Override
                    public void doCopy(Slice data, long srcOffset, long destOffset, int length)
                    {
                        Object base = data.getBase();
                        srcOffset += data.getAddress();
                        destOffset += data.getAddress();
                        int bytesToCopy = length - (length % 8);
                        getUnsafe().copyMemory(base, srcOffset, base, destOffset, bytesToCopy);
                        getUnsafe().copyMemory(base, srcOffset + bytesToCopy, base, destOffset + bytesToCopy, length - bytesToCopy);
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
