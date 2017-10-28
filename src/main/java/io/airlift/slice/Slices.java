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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;

import static io.airlift.slice.JvmUtils.getAddress;
import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.Preconditions.checkPositionIndexes;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class Slices
{
    /**
     * A slice with size {@code 0}.
     */
    public static final Slice EMPTY_SLICE = new Slice();

    // see java.util.ArrayList for an explanation
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private static final int SLICE_ALLOC_THRESHOLD = 524_288; // 2^19
    private static final double SLICE_ALLOW_SKEW = 1.25; // must be > 1!

    private Slices() {}

    public static Slice ensureSize(Slice existingSlice, int minWritableBytes)
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
        int minNewCapacity = minWritableBytes;
        while (newCapacity < minNewCapacity) {
            if (newCapacity < SLICE_ALLOC_THRESHOLD) {
                newCapacity <<= 1;
            }
            else {
                newCapacity *= SLICE_ALLOW_SKEW;
            }
        }

        Slice newSlice = allocate(newCapacity);
        newSlice.setBytes(0, existingSlice, 0, existingSlice.length());
        return newSlice;
    }

    public static Slice allocate(int capacity)
    {
        if (capacity == 0) {
            return EMPTY_SLICE;
        }
        checkArgument(capacity <= MAX_ARRAY_SIZE, "Cannot allocate slice larger than " + MAX_ARRAY_SIZE + " bytes");
        return new Slice(new byte[capacity]);
    }

    public static Slice allocateDirect(int capacity)
    {
        if (capacity == 0) {
            return EMPTY_SLICE;
        }
        return wrappedBuffer(ByteBuffer.allocateDirect(capacity));
    }

    public static Slice copyOf(Slice slice)
    {
        return copyOf(slice, 0, slice.length());
    }

    public static Slice copyOf(Slice slice, int offset, int length)
    {
        checkPositionIndexes(offset, offset + length, slice.length());

        Slice copy = Slices.allocate(length);
        copy.setBytes(0, slice, offset, length);

        return copy;
    }

    /**
     * Wrap the visible portion of a {@link java.nio.ByteBuffer}.
     */
    public static Slice wrappedBuffer(ByteBuffer buffer)
    {
        if (buffer.isDirect()) {
            long address = getAddress(buffer);
            return new Slice(null, address + buffer.position(), buffer.remaining(), buffer.capacity(), buffer);
        }

        if (buffer.hasArray()) {
            return new Slice(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }

        throw new IllegalArgumentException("cannot wrap " + buffer.getClass().getName());
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedBuffer(byte... array)
    {
        if (array.length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array);
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    public static Slice wrappedBuffer(byte[] array, int offset, int length)
    {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array, offset, length);
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedBooleanArray(boolean... array)
    {
        return wrappedBooleanArray(array, 0, array.length);
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    public static Slice wrappedBooleanArray(boolean[] array, int offset, int length)
    {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array, offset, length);
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedShortArray(short... array)
    {
        return wrappedShortArray(array, 0, array.length);
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
        return new Slice(array, offset, length);
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedIntArray(int... array)
    {
        return wrappedIntArray(array, 0, array.length);
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
        return new Slice(array, offset, length);
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedLongArray(long... array)
    {
        return wrappedLongArray(array, 0, array.length);
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
        return new Slice(array, offset, length);
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedFloatArray(float... array)
    {
        return wrappedFloatArray(array, 0, array.length);
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
        return new Slice(array, offset, length);
    }

    /**
     * Creates a slice over the specified array.
     */
    public static Slice wrappedDoubleArray(double... array)
    {
        return wrappedDoubleArray(array, 0, array.length);
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
        return new Slice(array, offset, length);
    }

    public static Slice copiedBuffer(String string, Charset charset)
    {
        requireNonNull(string, "string is null");
        requireNonNull(charset, "charset is null");

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
            MappedByteBuffer byteBuffer = channel.map(MapMode.READ_ONLY, 0, file.length());
            return wrappedBuffer(byteBuffer);
        }
    }
}
