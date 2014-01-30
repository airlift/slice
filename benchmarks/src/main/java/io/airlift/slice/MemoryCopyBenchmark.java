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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

import com.google.common.primitives.Ints;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
@BenchmarkMode(Mode.Throughput)
public class MemoryCopyBenchmark
{
    static final long PAGE_SIZE = 4 * 1024;
    static final int N_PAGES = 256 * 1024;
    static final long ALLOC_SIZE = PAGE_SIZE * N_PAGES;

    @State(Scope.Thread)
    public static class Buffers
    {
        Slice data;
        final Random random = new Random();

        @Setup
        public void fillWithBogusData() {
            data = Slice.toUnsafeSlice(ByteBuffer.allocateDirect(Ints.checkedCast(ALLOC_SIZE)).order(ByteOrder.nativeOrder()));
            for (int idx = 0; idx < data.length() / 8; idx++) {
                data.setLong(idx, random.nextLong());
            }
        }
    }

    @GenerateMicroBenchmark
    public Slice b00sliceZero(Buffers buffers)
    {
        return doCopy(buffers, SliceCopyStrategy.INSTANCE, 0);
    }

    @GenerateMicroBenchmark
    public Slice b01customLoopZero(Buffers buffers)
    {
        return doCopy(buffers, CustomLoopCopyStrategy.INSTANCE, 0);
    }

    @GenerateMicroBenchmark
    public Slice b02unsafeZero(Buffers buffers)
    {
        return doCopy(buffers, UnsafeCopyStrategy.INSTANCE, 0);
    }

    @GenerateMicroBenchmark
    public Slice b03slice32B(Buffers buffers)
    {
        return doCopy(buffers, SliceCopyStrategy.INSTANCE, 32);
    }

    @GenerateMicroBenchmark
    public Slice b04customLoop32B(Buffers buffers)
    {
        return doCopy(buffers, CustomLoopCopyStrategy.INSTANCE, 32);
    }

    @GenerateMicroBenchmark
    public Slice b05unsafe32B(Buffers buffers)
    {
        return doCopy(buffers, UnsafeCopyStrategy.INSTANCE, 32);
    }

    @GenerateMicroBenchmark
    public Slice b06slice128B(Buffers buffers)
    {
        return doCopy(buffers, SliceCopyStrategy.INSTANCE, 128);
    }

    @GenerateMicroBenchmark
    public Slice b07customLoop128B(Buffers buffers)
    {
        return doCopy(buffers, CustomLoopCopyStrategy.INSTANCE, 128);
    }

    @GenerateMicroBenchmark
    public Slice b08unsafe128B(Buffers buffers)
    {
        return doCopy(buffers, UnsafeCopyStrategy.INSTANCE, 128);
    }

    @GenerateMicroBenchmark
    public Slice b09slice512B(Buffers buffers)
    {
        return doCopy(buffers, SliceCopyStrategy.INSTANCE, 512);
    }

    @GenerateMicroBenchmark
    public Slice b10customLoop512B(Buffers buffers)
    {
        return doCopy(buffers, CustomLoopCopyStrategy.INSTANCE, 512);
    }

    @GenerateMicroBenchmark
    public Slice b11unsafe512B(Buffers buffers)
    {
        return doCopy(buffers, UnsafeCopyStrategy.INSTANCE, 512);
    }

    @GenerateMicroBenchmark
    public Slice b12slice1K(Buffers buffers)
    {
        return doCopy(buffers, SliceCopyStrategy.INSTANCE, 1024);
    }

    @GenerateMicroBenchmark
    public Slice b13customLoop1K(Buffers buffers)
    {
        return doCopy(buffers, CustomLoopCopyStrategy.INSTANCE, 1024);
    }

    @GenerateMicroBenchmark
    public Slice b14unsafe1K(Buffers buffers)
    {
        return doCopy(buffers, UnsafeCopyStrategy.INSTANCE, 1024);
    }

    @GenerateMicroBenchmark
    public Slice b15slice1M(Buffers buffers)
    {
        return doCopy(buffers, SliceCopyStrategy.INSTANCE, 1024 * 1024);
    }

    @GenerateMicroBenchmark
    public Slice b16customLoop1M(Buffers buffers)
    {
        return doCopy(buffers, CustomLoopCopyStrategy.INSTANCE, 1024 * 1024);
    }

    @GenerateMicroBenchmark
    public Slice b17unsafe1M(Buffers buffers)
    {
        return doCopy(buffers, UnsafeCopyStrategy.INSTANCE, 1024 * 1024);
    }

    @GenerateMicroBenchmark
    public Slice b18slice128M(Buffers buffers)
    {
        return doCopy(buffers, SliceCopyStrategy.INSTANCE, 128 * 1024 * 1024);
    }

    @GenerateMicroBenchmark
    public Slice b19customLoop128M(Buffers buffers)
    {
        return doCopy(buffers, CustomLoopCopyStrategy.INSTANCE, 128 * 1024 * 1024);
    }

    @GenerateMicroBenchmark
    public Slice b20unsafe128M(Buffers buffers)
    {
        return doCopy(buffers, UnsafeCopyStrategy.INSTANCE, 128 * 1024 * 1024);
    }

    static Slice doCopy(Buffers buffers, CopyStrategy strategy, int length)
    {
        long startOffsetPages = buffers.random.nextInt(N_PAGES / 4);
        long destOffsetPages = buffers.random.nextInt(N_PAGES / 4) + N_PAGES / 2;

        final long startOffset = startOffsetPages * PAGE_SIZE;
        final long destOffset = destOffsetPages * PAGE_SIZE;

        assert startOffset >= 0 : "startOffset < 0";
        assert destOffset >= 0 : "destOffset < 0";
        assert startOffset + length < ALLOC_SIZE : "startOffset + length >= ALLOC_SIZE";
        assert destOffset + length < ALLOC_SIZE : "destOFfset + length >= ALLOC_SIZE";

        strategy.doCopy(buffers.data, startOffset, destOffset, length);
        return buffers.data;
    }
}

interface CopyStrategy
{
    void doCopy(Slice data, long src, long dest, int length);
}

enum SliceCopyStrategy implements CopyStrategy
{
    INSTANCE;

    @Override
    public void doCopy(Slice data, long src, long dest, int length)
    {
        data.setBytes((int)dest, data, (int)src, length);
    }
}

@SuppressWarnings("restriction")
enum CustomLoopCopyStrategy implements CopyStrategy
{
    INSTANCE;

    static final int SIZE_OF_LONG = 8;
    static Unsafe unsafe = Slice.getUnsafe();

    @Override
    public void doCopy(Slice data, long src, long dest, int length)
    {
        Object base = data.getBase();
        long offset = data.getAddress();
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
}

@SuppressWarnings("restriction")
enum UnsafeCopyStrategy implements CopyStrategy
{
    INSTANCE;

    static Unsafe unsafe = Slice.getUnsafe();

    @Override
    public void doCopy(Slice data, long srcOffset, long destOffset, int length)
    {
        Object base = data.getBase();
        srcOffset += data.getAddress();
        destOffset += data.getAddress();
        final int bytesToCopy = length - (length % 8);
        unsafe.copyMemory(base, srcOffset, base, destOffset, bytesToCopy);
        unsafe.copyMemory(base, srcOffset + bytesToCopy, base, destOffset + bytesToCopy, length - bytesToCopy);
    }
}
