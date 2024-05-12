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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public final class Slice
        implements Comparable<Slice>
{
    private static final int INSTANCE_SIZE = instanceSize(Slice.class);
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE.withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfShort SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfFloat FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfDouble DOUBLE = ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(LITTLE_ENDIAN);

    private static final int SIZE_OF_INT = toIntExact(INT.byteSize());
    private static final int SIZE_OF_LONG = toIntExact(LONG.byteSize());

    // Do not move this field above the constants used in the empty constructor
    static final Slice EMPTY_SLICE = new Slice();

    private final byte[] base;

    private final MemorySegment segment;

    private final int baseOffset;

    /**
     * Size of the slice
     */
    private final int size;

    /**
     * Bytes retained by the slice
     */
    private final long retainedSize;

    private int hash;

    /**
     * This is only used to create the EMPTY_SLICE constant.
     */
    private Slice()
    {
        // Since this is used to create a constant in this class, be careful to not use
        // other uninitialized constants.
        this.base = new byte[0];
        this.segment = MemorySegment.ofArray(base);
        this.baseOffset = 0;
        this.size = 0;
        this.retainedSize = INSTANCE_SIZE;
    }

    /**
     * Creates a slice over the specified array.
     */
    Slice(byte[] base)
    {
        requireNonNull(base, "base is null");
        if (base.length == 0) {
            throw new IllegalArgumentException("Empty array");
        }
        this.base = base;
        this.segment = MemorySegment.ofArray(base);
        this.baseOffset = 0;
        this.size = base.length;
        this.retainedSize = INSTANCE_SIZE + sizeOf(base);
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    Slice(byte[] base, int offset, int length)
    {
        this(base, offset, length, INSTANCE_SIZE + sizeOf(base));
    }

    /**
     * Creates a slice for directly accessing the base object.
     */
    Slice(byte[] base, int baseOffset, int size, long retainedSize)
    {
        requireNonNull(base, "base is null");
        if (base.length == 0) {
            throw new IllegalArgumentException("Empty array");
        }
        this.base = base;
        this.segment = MemorySegment.ofArray(base).asSlice(baseOffset, size);
        this.baseOffset = baseOffset;
        this.size = size;
        // INSTANCE_SIZE is not included, as the caller is responsible for including it.
        this.retainedSize = retainedSize;
    }

    /**
     * Length of this slice.
     */
    public int length()
    {
        return size;
    }

    /**
     * Approximate number of bytes retained by this slice.
     */
    public long getRetainedSize()
    {
        return retainedSize;
    }

    /**
     * A slice is considered compact if the base object is an array, and it contains the whole array.
     * As a result, it cannot be a view of a bigger slice.
     */
    public boolean isCompact()
    {
        return baseOffset == 0 && size == base.length;
    }

    /**
     * Returns the byte array wrapped by this Slice. Callers should also take care to use {@link Slice#byteArrayOffset()}
     * since the contents of this Slice may not start at array index 0.
     */
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] byteArray()
    {
        return segment.heapBase()
                .map(value -> (byte[]) value)
                .orElseThrow();
    }

    /**
     * Returns the start index the content of this slice within the byte array wrapped by this slice.
     */
    public int byteArrayOffset()
    {
        return baseOffset;
    }

    /**
     * Fill the slice with the specified value;
     */
    public void fill(byte value)
    {
        segment.fill(value);
    }

    /**
     * Fill the slice with zeros;
     */
    public void clear()
    {
        clear(0, size);
    }

    public void clear(int offset, int length)
    {
        segment.asSlice(offset, length).fill((byte) 0);
    }

    /**
     * Gets a byte at the specified absolute {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    public byte getByte(int index)
    {
        return getByteUnchecked(index);
    }

    public byte getByteUnchecked(int index)
    {
        return segment.get(BYTE, index);
    }

    /**
     * Gets an unsigned byte at the specified absolute {@code index} in this
     * buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    public short getUnsignedByte(int index)
    {
        return (short) (getByte(index) & 0xFF);
    }

    /**
     * Gets a 16-bit short integer at the specified absolute {@code index} in
     * this slice.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 2} is greater than {@code this.length()}
     */
    public short getShort(int index)
    {
        return getShortUnchecked(index);
    }

    public short getShortUnchecked(int index)
    {
        return segment.get(SHORT, index);
    }

    /**
     * Gets an unsigned 16-bit short integer at the specified absolute {@code index}
     * in this slice.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 2} is greater than {@code this.length()}
     */
    public int getUnsignedShort(int index)
    {
        return getShort(index) & 0xFFFF;
    }

    /**
     * Gets a 32-bit integer at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.length()}
     */
    public int getInt(int index)
    {
        return getIntUnchecked(index);
    }

    public int getIntUnchecked(int index)
    {
        return segment.get(INT, index);
    }

    /**
     * Gets an unsigned 32-bit integer at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.length()}
     */
    public long getUnsignedInt(int index)
    {
        return getInt(index) & 0xFFFFFFFFL;
    }

    /**
     * Gets a 64-bit long integer at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    public long getLong(int index)
    {
        return getLongUnchecked(index);
    }

    public long getLongUnchecked(int index)
    {
        return segment.get(LONG, index);
    }

    /**
     * Gets a 32-bit float at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.length()}
     */
    public float getFloat(int index)
    {
        return getFloatUnchecked(index);
    }

    public float getFloatUnchecked(int index)
    {
        return segment.get(FLOAT, index);
    }

    /**
     * Gets a 64-bit double at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    public double getDouble(int index)
    {
        return getDoubleUnchecked(index);
    }

    public double getDoubleUnchecked(int index)
    {
        return segment.get(DOUBLE, index);
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + destination.length()} is greater than {@code this.length()}
     */
    public void getBytes(int index, Slice destination)
    {
        getBytes(index, destination, 0, destination.length());
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code destinationIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code destinationIndex + length} is greater than
     * {@code destination.length()}
     */
    public void getBytes(int index, Slice destination, int destinationIndex, int length)
    {
        MemorySegment.copy(segment, index, destination.segment, destinationIndex, length);
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + destination.length} is greater than {@code this.length()}
     */
    public void getBytes(int index, byte[] destination)
    {
        getBytes(index, destination, 0, destination.length);
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code destinationIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code destinationIndex + length} is greater than
     * {@code destination.length}
     */
    public void getBytes(int index, byte[] destination, int destinationIndex, int length)
    {
        MemorySegment.copy(segment, BYTE, index, destination, destinationIndex, length);
    }

    /**
     * Returns a copy of this buffer as a byte array.
     */
    public byte[] getBytes()
    {
        return getBytes(0, length());
    }

    /**
     * Returns a copy of this buffer as a byte array.
     *
     * @param index the absolute index to start at
     * @param length the number of bytes to return
     * @throws IndexOutOfBoundsException if the specified {@code index} is less then {@code 0},
     * or if the specified {@code index + length} is greater than {@code this.length()}
     */
    public byte[] getBytes(int index, int length)
    {
        byte[] bytes = new byte[length];
        getBytes(index, bytes, 0, length);
        return bytes;
    }

    /**
     * Transfers a portion of data from this slice into the specified stream starting at the
     * specified absolute {@code index}.
     *
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + length} is greater than
     * {@code this.length()}
     * @throws java.io.IOException if the specified stream threw an exception during I/O
     */
    public void getBytes(int index, OutputStream out, int length)
            throws IOException
    {
        checkFromIndexSize(index, length, length());
        out.write(byteArray(), byteArrayOffset() + index, length);
    }

    /**
     * Returns a copy of this buffer as a short array.
     *
     * @param index the absolute index to start at
     * @param length the number of shorts to return
     * @throws IndexOutOfBoundsException if the specified {@code index} is less then {@code 0},
     * or if the specified {@code index + length} is greater than {@code this.length()}
     */
    public short[] getShorts(int index, int length)
    {
        short[] shorts = new short[length];
        getShorts(index, shorts, 0, length);
        return shorts;
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + destination.length} is greater than {@code this.length()}
     */
    public void getShorts(int index, short[] destination)
    {
        getShorts(index, destination, 0, destination.length);
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of shorts to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code destinationIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code destinationIndex + length} is greater than
     * {@code destination.length}
     */
    public void getShorts(int index, short[] destination, int destinationIndex, int length)
    {
        MemorySegment.copy(segment, SHORT, index, destination, destinationIndex, length);
    }

    /**
     * Returns a copy of this buffer as an int array.
     *
     * @param index the absolute index to start at
     * @param length the number of ints to return
     * @throws IndexOutOfBoundsException if the specified {@code index} is less then {@code 0},
     * or if the specified {@code index + length} is greater than {@code this.length()}
     */
    public int[] getInts(int index, int length)
    {
        int[] ints = new int[length];
        getInts(index, ints, 0, length);
        return ints;
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + destination.length} is greater than {@code this.length()}
     */
    public void getInts(int index, int[] destination)
    {
        getInts(index, destination, 0, destination.length);
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of ints to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code destinationIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code destinationIndex + length} is greater than
     * {@code destination.length}
     */
    public void getInts(int index, int[] destination, int destinationIndex, int length)
    {
        MemorySegment.copy(segment, INT, index, destination, destinationIndex, length);
    }

    /**
     * Returns a copy of this buffer as a long array.
     *
     * @param index the absolute index to start at
     * @param length the number of longs to return
     * @throws IndexOutOfBoundsException if the specified {@code index} is less then {@code 0},
     * or if the specified {@code index + length} is greater than {@code this.length()}
     */
    public long[] getLongs(int index, int length)
    {
        long[] longs = new long[length];
        getLongs(index, longs, 0, length);
        return longs;
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + destination.length} is greater than {@code this.length()}
     */
    public void getLongs(int index, long[] destination)
    {
        getLongs(index, destination, 0, destination.length);
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of longs to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code destinationIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code destinationIndex + length} is greater than
     * {@code destination.length}
     */
    public void getLongs(int index, long[] destination, int destinationIndex, int length)
    {
        MemorySegment.copy(segment, LONG, index, destination, destinationIndex, length);
    }

    /**
     * Returns a copy of this buffer as a float array.
     *
     * @param index the absolute index to start at
     * @param length the number of floats to return
     * @throws IndexOutOfBoundsException if the specified {@code index} is less then {@code 0},
     * or if the specified {@code index + length} is greater than {@code this.length()}
     */
    public float[] getFloats(int index, int length)
    {
        float[] floats = new float[length];
        getFloats(index, floats, 0, length);
        return floats;
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + destination.length} is greater than {@code this.length()}
     */
    public void getFloats(int index, float[] destination)
    {
        getFloats(index, destination, 0, destination.length);
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of floats to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code destinationIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code destinationIndex + length} is greater than
     * {@code destination.length}
     */
    public void getFloats(int index, float[] destination, int destinationIndex, int length)
    {
        MemorySegment.copy(segment, FLOAT, index, destination, destinationIndex, length);
    }

    /**
     * Returns a copy of this buffer as a double array.
     *
     * @param index the absolute index to start at
     * @param length the number of doubles to return
     * @throws IndexOutOfBoundsException if the specified {@code index} is less then {@code 0},
     * or if the specified {@code index + length} is greater than {@code this.length()}
     */
    public double[] getDoubles(int index, int length)
    {
        double[] doubles = new double[length];
        getDoubles(index, doubles, 0, length);
        return doubles;
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + destination.length} is greater than {@code this.length()}
     */
    public void getDoubles(int index, double[] destination)
    {
        getDoubles(index, destination, 0, destination.length);
    }

    /**
     * Transfers a portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of doubles to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code destinationIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code destinationIndex + length} is greater than
     * {@code destination.length}
     */
    public void getDoubles(int index, double[] destination, int destinationIndex, int length)
    {
        MemorySegment.copy(segment, DOUBLE, index, destination, destinationIndex, length);
    }

    /**
     * Sets the specified byte at the specified absolute {@code index} in this
     * buffer.  The 24 high-order bits of the specified value are ignored.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    public void setByte(int index, int value)
    {
        setByteUnchecked(index, value);
    }

    void setByteUnchecked(int index, int value)
    {
        segment.set(BYTE, index, (byte) (value & 0xFF));
    }

    /**
     * Sets the specified 16-bit short integer at the specified absolute
     * {@code index} in this buffer.  The 16 high-order bits of the specified
     * value are ignored.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 2} is greater than {@code this.length()}
     */
    public void setShort(int index, int value)
    {
        setShortUnchecked(index, value);
    }

    void setShortUnchecked(int index, int value)
    {
        segment.set(SHORT, index, (short) (value & 0xFFFF));
    }

    /**
     * Sets the specified 32-bit integer at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.length()}
     */
    public void setInt(int index, int value)
    {
        setIntUnchecked(index, value);
    }

    void setIntUnchecked(int index, int value)
    {
        segment.set(INT, index, value);
    }

    /**
     * Sets the specified 64-bit long integer at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    public void setLong(int index, long value)
    {
        setLongUnchecked(index, value);
    }

    void setLongUnchecked(int index, long value)
    {
        segment.set(LONG, index, value);
    }

    /**
     * Sets the specified 32-bit float at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.length()}
     */
    public void setFloat(int index, float value)
    {
        segment.set(FLOAT, index, value);
    }

    /**
     * Sets the specified 64-bit double at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    public void setDouble(int index, double value)
    {
        segment.set(DOUBLE, index, value);
    }

    /**
     * Transfers data from the specified slice into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + source.length()} is greater than {@code this.length()}
     */
    public void setBytes(int index, Slice source)
    {
        setBytes(index, source, 0, source.length());
    }

    /**
     * Transfers data from the specified slice into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @param sourceIndex the first index of the source
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code sourceIndex + length} is greater than
     * {@code source.length()}
     */
    public void setBytes(int index, Slice source, int sourceIndex, int length)
    {
        MemorySegment.copy(source.segment, sourceIndex, segment, index, length);
    }

    /**
     * Transfers data from the specified slice into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + source.length} is greater than {@code this.length()}
     */
    public void setBytes(int index, byte[] source)
    {
        setBytes(index, source, 0, source.length);
    }

    /**
     * Transfers data from the specified array into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code sourceIndex + length} is greater than {@code source.length}
     */
    public void setBytes(int index, byte[] source, int sourceIndex, int length)
    {
        MemorySegment.copy(source, sourceIndex, segment, BYTE, index, length);
    }

    /**
     * Transfers data from the specified input stream into this slice starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + source.length} is greater than {@code this.length()}
     */
    public void setBytes(int index, InputStream in, int length)
            throws IOException
    {
        checkFromIndexSize(index, length, length());
        byte[] bytes = byteArray();
        int offset = byteArrayOffset() + index;
        while (length > 0) {
            int bytesRead = in.read(bytes, offset, length);
            if (bytesRead < 0) {
                throw new IndexOutOfBoundsException("End of stream");
            }
            length -= bytesRead;
            offset += bytesRead;
        }
    }

    /**
     * Transfers data from the specified slice into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + source.length} is greater than {@code this.length()}
     */
    public void setShorts(int index, short[] source)
    {
        setShorts(index, source, 0, source.length);
    }

    /**
     * Transfers data from the specified array into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code sourceIndex + length} is greater than {@code source.length}
     */
    public void setShorts(int index, short[] source, int sourceIndex, int length)
    {
        MemorySegment.copy(source, sourceIndex, segment, SHORT, index, length);
    }

    /**
     * Transfers data from the specified slice into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + source.length} is greater than {@code this.length()}
     */
    public void setInts(int index, int[] source)
    {
        setInts(index, source, 0, source.length);
    }

    /**
     * Transfers data from the specified array into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code sourceIndex + length} is greater than {@code source.length}
     */
    public void setInts(int index, int[] source, int sourceIndex, int length)
    {
        MemorySegment.copy(source, sourceIndex, segment, INT, index, length);
    }

    /**
     * Transfers data from the specified slice into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + source.length} is greater than {@code this.length()}
     */
    public void setLongs(int index, long[] source)
    {
        setLongs(index, source, 0, source.length);
    }

    /**
     * Transfers data from the specified array into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code sourceIndex + length} is greater than {@code source.length}
     */
    public void setLongs(int index, long[] source, int sourceIndex, int length)
    {
        MemorySegment.copy(source, sourceIndex, segment, LONG, index, length);
    }

    /**
     * Transfers data from the specified slice into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + source.length} is greater than {@code this.length()}
     */
    public void setFloats(int index, float[] source)
    {
        setFloats(index, source, 0, source.length);
    }

    /**
     * Transfers data from the specified array into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code sourceIndex + length} is greater than {@code source.length}
     */
    public void setFloats(int index, float[] source, int sourceIndex, int length)
    {
        MemorySegment.copy(source, sourceIndex, segment, FLOAT, index, length);
    }

    /**
     * Transfers data from the specified slice into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + source.length} is greater than {@code this.length()}
     */
    public void setDoubles(int index, double[] source)
    {
        setDoubles(index, source, 0, source.length);
    }

    /**
     * Transfers data from the specified array into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code sourceIndex + length} is greater than {@code source.length}
     */
    public void setDoubles(int index, double[] source, int sourceIndex, int length)
    {
        MemorySegment.copy(source, sourceIndex, segment, DOUBLE, index, length);
    }

    /**
     * Returns a slice of this buffer's sub-region. Modifying the content of
     * the returned buffer, or this buffer affects each other's content.
     */
    public Slice slice(int index, int length)
    {
        if ((index == 0) && (length == length())) {
            return this;
        }
        checkFromIndexSize(index, length, length());
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }

        return new Slice(base, index, length, retainedSize);
    }

    /**
     * Returns a copy of this buffer's sub-region. Modifying the content of
     * the returned buffer does not affect this buffer, and vice versa.
     */
    public Slice copy()
    {
        if (size == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new Slice(Arrays.copyOfRange(base, baseOffset, baseOffset + size));
    }

    /**
     * Returns a copy of the specified region. Modifying the content of
     * the returned buffer does not affect this buffer, and vice versa.
     */
    public Slice copy(int index, int length)
    {
        checkFromIndexSize(index, length, size);
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new Slice(Arrays.copyOfRange(base, baseOffset + index, baseOffset + index + length));
    }

    public int indexOfByte(int b)
    {
        checkArgument((b >> Byte.SIZE) == 0, "byte value out of range");
        return indexOfByte((byte) b);
    }

    public int indexOfByte(byte b)
    {
        for (int i = 0; i < size; i++) {
            if (getByteUnchecked(i) == b) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the index of the first occurrence of the pattern with this slice.
     * If the pattern is not found -1 is returned. If patten is empty, zero is
     * returned.
     */
    public int indexOf(Slice slice)
    {
        return indexOf(slice, 0);
    }

    /**
     * Returns the index of the first occurrence of the pattern with this slice.
     * If the pattern is not found -1 is returned. If patten is empty, the offset
     * is returned.
     */
    public int indexOf(Slice pattern, int offset)
    {
        if (size == 0 || offset >= size || offset < 0) {
            return -1;
        }

        if (pattern.length() == 0) {
            return offset;
        }

        // Do we have enough characters?
        if (pattern.length() < SIZE_OF_INT || size < SIZE_OF_LONG) {
            return indexOfBruteForce(pattern, offset);
        }

        // Using the first four bytes for faster search. We are not using eight bytes for long
        // because we want more strings to get use of fast search.
        int head = pattern.getIntUnchecked(0);

        // Take the first byte of head for faster skipping
        int firstByteMask = head & 0xff;
        firstByteMask |= firstByteMask << 8;
        firstByteMask |= firstByteMask << 16;

        int lastValidIndex = size - pattern.length();
        int index = offset;
        while (index <= lastValidIndex) {
            // Read four bytes in sequence
            int value = getIntUnchecked(index);

            // Compare all bytes of value with the first byte of search data
            // see https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
            int valueXor = value ^ firstByteMask;
            int hasZeroBytes = (valueXor - 0x01010101) & ~valueXor & 0x80808080;

            // If valueXor doesn't have any zero bytes, then there is no match and we can advance
            if (hasZeroBytes == 0) {
                index += SIZE_OF_INT;
                continue;
            }

            // Try fast match of head and the rest
            if (value == head && equalsUnchecked(index, pattern, 0, pattern.length())) {
                return index;
            }

            index++;
        }

        return -1;
    }

    int indexOfBruteForce(Slice pattern, int offset)
    {
        if (size == 0 || offset >= size || offset < 0) {
            return -1;
        }

        if (pattern.length() == 0) {
            return offset;
        }

        byte firstByte = pattern.getByteUnchecked(0);
        int lastValidIndex = size - pattern.length();
        int index = offset;
        while (true) {
            // seek to first byte match
            while (index < lastValidIndex && getByteUnchecked(index) != firstByte) {
                index++;
            }
            if (index > lastValidIndex) {
                break;
            }

            if (equalsUnchecked(index, pattern, 0, pattern.length())) {
                return index;
            }

            index++;
        }

        return -1;
    }

    /**
     * Compares the content of the specified buffer to the content of this
     * buffer.  This comparison is performed byte by byte using an unsigned
     * comparison.
     */
    @SuppressWarnings("ObjectEquality")
    @Override
    public int compareTo(Slice that)
    {
        if (this == that) {
            return 0;
        }
        return compareTo(0, size, that, 0, that.size);
    }

    /**
     * Compares a portion of this slice with a portion of the specified slice.  Equality is
     * solely based on the contents of the slice.
     */
    @SuppressWarnings("ObjectEquality")
    public int compareTo(int offset, int length, Slice that, int otherOffset, int otherLength)
    {
        if ((this == that) && (offset == otherOffset) && (length == otherLength)) {
            return 0;
        }

        MemorySegment thisSlice = segment.asSlice(offset, length);
        MemorySegment thatSlice = that.segment.asSlice(otherOffset, otherLength);

        // Find index of the first mismatched byte
        long mismatch = thisSlice.mismatch(thatSlice);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch >= thisSlice.byteSize()) {
            return -1;
        }
        if (mismatch >= thatSlice.byteSize()) {
            return 1;
        }
        return Byte.compareUnsigned(thisSlice.get(BYTE, mismatch), thatSlice.get(BYTE, mismatch));
    }

    /**
     * Compares the specified object with this slice for equality.  Equality is
     * solely based on the contents of the slice.
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Slice that)) {
            return false;
        }

        if (length() != that.length()) {
            return false;
        }

        return equalsUnchecked(0, that, 0, length());
    }

    /**
     * Returns the hash code of this slice.  The hash code is cached once calculated,
     * and any future changes to the slice will not affect the hash code.
     */
    @SuppressWarnings("NonFinalFieldReferencedInHashCode")
    @Override
    public int hashCode()
    {
        if (hash != 0) {
            return hash;
        }

        hash = hashCode(0, size);
        return hash;
    }

    /**
     * Returns the hash code of a portion of this slice.
     */
    public int hashCode(int offset, int length)
    {
        return (int) XxHash64.hash(this, offset, length);
    }

    /**
     * Compares a portion of this slice with a portion of the specified slice.  Equality is
     * solely based on the contents of the slice.
     */
    @SuppressWarnings("ObjectEquality")
    public boolean equals(int offset, int length, Slice that, int otherOffset, int otherLength)
    {
        if (length != otherLength) {
            return false;
        }

        if ((this == that) && (offset == otherOffset)) {
            return true;
        }

        return equalsUnchecked(offset, that, otherOffset, length);
    }

    boolean equalsUnchecked(int offset, Slice that, int otherOffset, int length)
    {
        return segment.asSlice(offset, length)
                .mismatch(that.segment.asSlice(otherOffset, length)) == -1;
    }

    /**
     * Creates a slice input backed by this slice.  Any changes to this slice
     * will be immediately visible to the slice input.
     */
    public BasicSliceInput getInput()
    {
        return new BasicSliceInput(this);
    }

    /**
     * Creates a slice output backed by this slice.  Any data written to the
     * slice output will be immediately visible in this slice.
     */
    public SliceOutput getOutput()
    {
        return new BasicSliceOutput(this);
    }

    /**
     * Decodes the contents of this slice into a string with the specified
     * character set name.
     */
    public String toString(Charset charset)
    {
        return toString(0, length(), charset);
    }

    /**
     * Decodes the contents of this slice into a string using the UTF-8
     * character set.
     */
    public String toStringUtf8()
    {
        return toString(UTF_8);
    }

    /**
     * Decodes the contents of this slice into a string using the US_ASCII
     * character set.  The low-order 7 bits of each byte are converted directly
     * into a code point for the string.
     */
    public String toStringAscii()
    {
        return toStringAscii(0, size);
    }

    public String toStringAscii(int index, int length)
    {
        checkFromIndexSize(index, length, length());
        if (length == 0) {
            return "";
        }

        return new String(byteArray(), byteArrayOffset() + index, length, StandardCharsets.US_ASCII);
    }

    /**
     * Decodes the specified portion of this slice into a string with the specified
     * character set name.
     */
    public String toString(int index, int length, Charset charset)
    {
        if (length == 0) {
            return "";
        }
        return new String(byteArray(), byteArrayOffset() + index, length, charset);
    }

    public ByteBuffer toByteBuffer()
    {
        return toByteBuffer(0, size);
    }

    public ByteBuffer toByteBuffer(int index, int length)
    {
        if (length() == 0) {
            return EMPTY_BYTE_BUFFER;
        }

        return segment.asSlice(index, length).asByteBuffer();
    }

    /**
     * Returns information about the slice offset, and length
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Slice{");
        builder.append("base=").append(identityToString(base)).append(", ");
        builder.append("baseOffset=").append(baseOffset);
        builder.append(", length=").append(length());
        builder.append('}');
        return builder.toString();
    }

    private static String identityToString(Object o)
    {
        if (o == null) {
            return null;
        }
        return o.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(o));
    }
}
