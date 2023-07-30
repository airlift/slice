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
import java.nio.charset.Charset;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public final class Slices
{
    /**
     * A slice with size {@code 0}.
     */
    public static final Slice EMPTY_SLICE = new Slice();

    // see java.util.ArrayList for an explanation
    static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    static final int SLICE_ALLOC_THRESHOLD = 524_288; // 2^19
    static final double SLICE_ALLOW_SKEW = 1.25; // must be > 1!

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

    public static Slice allocate(int capacity)
    {
        if (capacity == 0) {
            return EMPTY_SLICE;
        }
        if (capacity > MAX_ARRAY_SIZE) {
            throw new SliceTooLargeException(format("Cannot allocate slice larger than %s bytes", MAX_ARRAY_SIZE));
        }
        return new Slice(new byte[capacity]);
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

    public static Slice wrappedHeapBuffer(ByteBuffer buffer)
    {
        if (!buffer.hasArray()) {
            throw new IllegalArgumentException("cannot wrap " + buffer.getClass().getName());
        }

        return new Slice(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
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
}
