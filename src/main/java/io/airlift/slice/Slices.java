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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;

import static io.airlift.slice.MemoryLayout.SIZE_OF_DOUBLE;
import static io.airlift.slice.MemoryLayout.SIZE_OF_FLOAT;
import static io.airlift.slice.MemoryLayout.SIZE_OF_INT;
import static io.airlift.slice.MemoryLayout.SIZE_OF_LONG;
import static io.airlift.slice.MemoryLayout.SIZE_OF_SHORT;
import static io.airlift.slice.Slice.createSlice;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public final class Slices
{
    private static final SegmentAllocator ALLOCATOR = Arena.global();

    /**
     * A slice with size {@code 0}.
     */
    public static final Slice EMPTY_SLICE = Slice.EMPTY_SLICE;

    // see java.util.ArrayList for an explanation
    static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    static final int SLICE_ALLOC_THRESHOLD = 524_288; // 2^19
    static final double SLICE_ALLOW_SKEW = 1.25; // must be > 1!

    private Slices() {}

    public static Slice ensureSize(Slice existingSlice, long minWritableBytes)
    {
        if (existingSlice == null) {
            return allocate(minWritableBytes);
        }

        if (minWritableBytes <= existingSlice.length()) {
            return existingSlice;
        }

        int newCapacity;
        if (existingSlice.length() == 0) {
            newCapacity = 1;
        }
        else {
            newCapacity = existingSlice.length();
        }
        while (newCapacity < minWritableBytes) {
            if (newCapacity < SLICE_ALLOC_THRESHOLD) {
                newCapacity <<= 1;
            }
            else {
                newCapacity *= SLICE_ALLOW_SKEW; // double to int cast is saturating
                if (newCapacity > MAX_ARRAY_SIZE && minWritableBytes <= MAX_ARRAY_SIZE) {
                    newCapacity = MAX_ARRAY_SIZE;
                }
            }
        }

        Slice newSlice = allocate(newCapacity);
        newSlice.setBytes(0, existingSlice, 0, existingSlice.length());
        return newSlice;
    }

    /**
     * Allocates a new byte array backed heap slice with the specified capacity.
     */
    public static Slice allocate(long capacity)
    {
        if (capacity == 0) {
            return EMPTY_SLICE;
        }
        if (capacity > MAX_ARRAY_SIZE) {
            throw new SliceTooLargeException(format("Cannot allocate slice larger than %s bytes", MAX_ARRAY_SIZE));
        }
        return createSlice(MemorySegment.ofArray(new byte[toIntExact(capacity)]));
    }

    /**
     * Allocates a new native slice with the specified capacity.
     *
     * @deprecated Allocate the MemorySegment directly using global SegmentAllocator and use {@link #wrap(MemorySegment)}
     */
    @Deprecated(forRemoval = true)
    public static Slice allocateDirect(int capacity)
    {
        if (capacity == 0) {
            return EMPTY_SLICE;
        }
        return createSlice(ALLOCATOR.allocate(capacity));
    }

    public static Slice copyOf(Slice slice)
    {
        return copyOf(slice, 0, slice.length());
    }

    public static Slice copyOf(Slice slice, int offset, int length)
    {
        checkFromIndexSize(offset, length, slice.length());

        Slice copy = Slices.allocate(length);
        copy.setBytes(0, slice, offset, length);

        return copy;
    }

    public static Slice wrap(MemorySegment memory)
    {
        return createSlice(memory);
    }

    /**
     * Wrap the visible portion of a {@link java.nio.ByteBuffer}.
     */
    public static Slice wrappedBuffer(ByteBuffer buffer)
    {
        return createSlice(MemorySegment.ofBuffer(buffer));
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedBuffer(byte... array)
    {
        return createSlice(MemorySegment.ofArray(array));
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    public static Slice wrappedBuffer(byte[] array, int offset, int length)
    {
        return createSlice(MemorySegment.ofArray(array).asSlice(offset, length));
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedShortArray(short... array)
    {
        return createSlice(MemorySegment.ofArray(array));
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    public static Slice wrappedShortArray(short[] array, int offset, int length)
    {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return createSlice(MemorySegment.ofArray(array).asSlice((long) offset * SIZE_OF_SHORT, (long) length * SIZE_OF_SHORT));
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedIntArray(int... array)
    {
        return createSlice(MemorySegment.ofArray(array));
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    public static Slice wrappedIntArray(int[] array, int offset, int length)
    {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return createSlice(MemorySegment.ofArray(array).asSlice((long) offset * SIZE_OF_INT, (long) length * SIZE_OF_INT));
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedLongArray(long... array)
    {
        return createSlice(MemorySegment.ofArray(array));
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    public static Slice wrappedLongArray(long[] array, int offset, int length)
    {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return createSlice(MemorySegment.ofArray(array).asSlice((long) offset * SIZE_OF_LONG, (long) length * SIZE_OF_LONG));
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedFloatArray(float... array)
    {
        return createSlice(MemorySegment.ofArray(array));
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    public static Slice wrappedFloatArray(float[] array, int offset, int length)
    {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return createSlice(MemorySegment.ofArray(array).asSlice((long) offset * SIZE_OF_FLOAT, (long) length * SIZE_OF_FLOAT));
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedDoubleArray(double... array)
    {
        return createSlice(MemorySegment.ofArray(array));
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    public static Slice wrappedDoubleArray(double[] array, int offset, int length)
    {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return createSlice(MemorySegment.ofArray(array).asSlice((long) offset * SIZE_OF_DOUBLE, (long) length * SIZE_OF_DOUBLE));
    }

    public static Slice copiedBuffer(String string, Charset charset)
    {
        return wrappedBuffer(string.getBytes(charset));
    }

    public static Slice utf8Slice(String string)
    {
        return copiedBuffer(string, UTF_8);
    }

    public static Slice mapFileReadOnly(File file)
            throws IOException
    {
        requireNonNull(file, "file is null");

        if (!file.exists()) {
            throw new FileNotFoundException(file.toString());
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                FileChannel channel = randomAccessFile.getChannel()) {
            return wrappedBuffer(channel.map(MapMode.READ_ONLY, 0, file.length()));
        }
    }
}
