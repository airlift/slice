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
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static io.airlift.slice.JvmUtils.unsafe;
import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.invoke.MethodHandles.byteArrayViewVarHandle;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_DOUBLE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_FLOAT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_INT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_LONG_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_SHORT_BASE_OFFSET;

public final class Slice
        implements Comparable<Slice>
{
    private static final int INSTANCE_SIZE = instanceSize(Slice.class);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

    private static final VarHandle SHORT_HANDLE = byteArrayViewVarHandle(short[].class, LITTLE_ENDIAN);
    private static final VarHandle INT_HANDLE = byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN);
    private static final VarHandle LONG_HANDLE = byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);
    private static final VarHandle FLOAT_HANDLE = byteArrayViewVarHandle(float[].class, LITTLE_ENDIAN);
    private static final VarHandle DOUBLE_HANDLE = byteArrayViewVarHandle(double[].class, LITTLE_ENDIAN);

    private final byte[] base;

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
     * Creates an empty slice.
     */
    Slice()
    {
        this.base = EMPTY_BYTE_ARRAY;
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
        this.base = base;
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
        requireNonNull(base, "base is null");
        checkFromIndexSize(offset, length, base.length);

        this.base = base;
        this.baseOffset = offset;
        this.size = length;
        this.retainedSize = INSTANCE_SIZE + sizeOf(base);
    }

    /**
     * Creates a slice for directly accessing the base object.
     */
    Slice(byte[] base, int baseOffset, int size, long retainedSize)
    {
        requireNonNull(base, "base is null");
        checkFromIndexSize(baseOffset, size, base.length);

        this.base = requireNonNull(base, "base is null");
        this.baseOffset = baseOffset;
        this.size = size;
        // INSTANCE_SIZE is not included, as the caller is responsible for including it.
        this.retainedSize = retainedSize;
    }

    /**
     * Returns the base object of this Slice, or null.  This is appropriate for use
     * with {@link Unsafe} if you wish to avoid all the safety belts e.g., bounds checks.
     */
    public byte[] getBase()
    {
        return byteArray();
    }

    /**
     * Return the address offset of this Slice.  This is appropriate for use
     * with {@link Unsafe} if you wish to avoid all the safety belts e.g., bounds checks.
     */
    public long getAddress()
    {
        return baseOffset + ARRAY_BYTE_BASE_OFFSET;
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
        return base;
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
        Arrays.fill(base, baseOffset, baseOffset + size, value);
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
        Arrays.fill(base, baseOffset, baseOffset + size, (byte) 0);
    }

    /**
     * Gets a byte at the specified absolute {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    public byte getByte(int index)
    {
        checkFromIndexSize(index, SIZE_OF_BYTE, length());
        return getByteUnchecked(index);
    }

    byte getByteUnchecked(int index)
    {
        return base[baseOffset + index];
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
        checkFromIndexSize(index, SIZE_OF_SHORT, length());
        return getShortUnchecked(index);
    }

    short getShortUnchecked(int index)
    {
        return (short) SHORT_HANDLE.get(base, baseOffset + index);
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
        checkFromIndexSize(index, SIZE_OF_INT, length());
        return getIntUnchecked(index);
    }

    int getIntUnchecked(int index)
    {
        return (int) INT_HANDLE.get(base, baseOffset + index);
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
        checkFromIndexSize(index, SIZE_OF_LONG, length());
        return getLongUnchecked(index);
    }

    long getLongUnchecked(int index)
    {
        return (long) LONG_HANDLE.get(base, baseOffset + index);
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
        checkFromIndexSize(index, SIZE_OF_FLOAT, length());
        return (float) FLOAT_HANDLE.get(base, baseOffset + index);
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
        checkFromIndexSize(index, SIZE_OF_DOUBLE, length());
        return (double) DOUBLE_HANDLE.get(base, baseOffset + index);
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
        checkFromIndexSize(destinationIndex, length, destination.length());
        checkFromIndexSize(index, length, length());

        System.arraycopy(base, baseOffset + index, destination.base, destination.baseOffset + destinationIndex, length);
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
        checkFromIndexSize(index, length, length());
        checkFromIndexSize(destinationIndex, length, destination.length);

        System.arraycopy(base, baseOffset + index, destination, destinationIndex, length);
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
        checkFromIndexSize(index, length * Short.BYTES, length());
        checkFromIndexSize(destinationIndex, length, destination.length);

        copyFromBase(index, destination, ARRAY_SHORT_BASE_OFFSET + ((long) destinationIndex * Short.BYTES), length * Short.BYTES);
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
        checkFromIndexSize(index, length * Integer.BYTES, length());
        checkFromIndexSize(destinationIndex, length, destination.length);

        copyFromBase(index, destination, ARRAY_INT_BASE_OFFSET + ((long) destinationIndex * Integer.BYTES), length * Integer.BYTES);
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
        checkFromIndexSize(index, length * Long.BYTES, length());
        checkFromIndexSize(destinationIndex, length, destination.length);

        copyFromBase(index, destination, ARRAY_LONG_BASE_OFFSET + ((long) destinationIndex * Long.BYTES), length * Long.BYTES);
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
        checkFromIndexSize(index, length * Float.BYTES, length());
        checkFromIndexSize(destinationIndex, length, destination.length);

        copyFromBase(index, destination, ARRAY_FLOAT_BASE_OFFSET + ((long) destinationIndex * Float.BYTES), length * Float.BYTES);
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
        checkFromIndexSize(index, length * Double.BYTES, length());
        checkFromIndexSize(destinationIndex, length, destination.length);

        copyFromBase(index, destination, ARRAY_DOUBLE_BASE_OFFSET + ((long) destinationIndex * Double.BYTES), length * Double.BYTES);
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
        checkFromIndexSize(index, SIZE_OF_BYTE, length());
        setByteUnchecked(index, value);
    }

    void setByteUnchecked(int index, int value)
    {
        base[baseOffset + index] = (byte) (value & 0xFF);
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
        checkFromIndexSize(index, SIZE_OF_SHORT, length());
        setShortUnchecked(index, value);
    }

    void setShortUnchecked(int index, int value)
    {
        SHORT_HANDLE.set(base, baseOffset + index, (short) (value & 0xFFFF));
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
        checkFromIndexSize(index, SIZE_OF_INT, length());
        setIntUnchecked(index, value);
    }

    void setIntUnchecked(int index, int value)
    {
        INT_HANDLE.set(base, baseOffset + index, value);
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
        checkFromIndexSize(index, SIZE_OF_LONG, length());
        setLongUnchecked(index, value);
    }

    void setLongUnchecked(int index, long value)
    {
        LONG_HANDLE.set(base, baseOffset + index, value);
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
        checkFromIndexSize(index, SIZE_OF_FLOAT, length());
        FLOAT_HANDLE.set(base, baseOffset + index, value);
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
        checkFromIndexSize(index, SIZE_OF_DOUBLE, length());
        DOUBLE_HANDLE.set(base, baseOffset + index, value);
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
        checkFromIndexSize(index, length, length());
        checkFromIndexSize(sourceIndex, length, source.length());

        System.arraycopy(source.base, source.baseOffset + sourceIndex, base, baseOffset + index, length);
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
        checkFromIndexSize(index, length, length());
        checkFromIndexSize(sourceIndex, length, source.length);
        System.arraycopy(source, sourceIndex, base, baseOffset + index, length);
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
        checkFromIndexSize(index, length, length());
        checkFromIndexSize(sourceIndex, length, source.length);
        copyToBase(index, source, ARRAY_SHORT_BASE_OFFSET + ((long) sourceIndex * Short.BYTES), length * Short.BYTES);
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
        checkFromIndexSize(index, length, length());
        checkFromIndexSize(sourceIndex, length, source.length);
        copyToBase(index, source, ARRAY_INT_BASE_OFFSET + ((long) sourceIndex * Integer.BYTES), length * Integer.BYTES);
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
        checkFromIndexSize(index, length, length());
        checkFromIndexSize(sourceIndex, length, source.length);
        copyToBase(index, source, ARRAY_LONG_BASE_OFFSET + ((long) sourceIndex * Long.BYTES), length * Long.BYTES);
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
        checkFromIndexSize(index, length, length());
        checkFromIndexSize(sourceIndex, length, source.length);
        copyToBase(index, source, ARRAY_FLOAT_BASE_OFFSET + ((long) sourceIndex * Float.BYTES), length * Float.BYTES);
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
        checkFromIndexSize(index, length, length());
        checkFromIndexSize(sourceIndex, length, source.length);
        copyToBase(index, source, ARRAY_DOUBLE_BASE_OFFSET + ((long) sourceIndex * Double.BYTES), length * Double.BYTES);
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

        return new Slice(base, baseOffset + index, length, retainedSize);
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

        checkFromIndexSize(offset, length, length());
        checkFromIndexSize(otherOffset, otherLength, that.length());

        return Arrays.compareUnsigned(
                base,
                baseOffset + offset,
                baseOffset + offset + length,
                that.base,
                that.baseOffset + otherOffset,
                that.baseOffset + otherOffset + otherLength);
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

        checkFromIndexSize(offset, length, length());
        checkFromIndexSize(otherOffset, otherLength, that.length());

        return equalsUnchecked(offset, that, otherOffset, length);
    }

    boolean equalsUnchecked(int offset, Slice that, int otherOffset, int length)
    {
        return Arrays.equals(
                base,
                baseOffset + offset,
                baseOffset + offset + length,
                that.base,
                that.baseOffset + otherOffset,
                that.baseOffset + otherOffset + length);
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
        checkFromIndexSize(index, length, length());

        if (length() == 0) {
            return EMPTY_BYTE_BUFFER;
        }

        return ByteBuffer.wrap(byteArray(), byteArrayOffset() + index, length).slice();
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

    private void copyFromBase(int index, Object dest, long destAddress, int length)
    {
        int baseAddress = ARRAY_BYTE_BASE_OFFSET + baseOffset + index;
        // The Unsafe Javadoc specifies that the transfer size is 8 iff length % 8 == 0
        // so ensure that we copy big chunks whenever possible, even at the expense of two separate copy operations
        // todo the optimization only works if the baseOffset is is a multiple of 8 for both src and dest
        int bytesToCopy = length - (length % 8);
        unsafe.copyMemory(base, baseAddress, dest, destAddress, bytesToCopy);
        unsafe.copyMemory(base, baseAddress + bytesToCopy, dest, destAddress + bytesToCopy, length - bytesToCopy);
    }

    private void copyToBase(int index, Object src, long srcAddress, int length)
    {
        int baseAddress = ARRAY_BYTE_BASE_OFFSET + baseOffset + index;
        // The Unsafe Javadoc specifies that the transfer size is 8 iff length % 8 == 0
        // so ensure that we copy big chunks whenever possible, even at the expense of two separate copy operations
        // todo the optimization only works if the baseOffset is is a multiple of 8 for both src and dest
        int bytesToCopy = length - (length % 8);
        unsafe.copyMemory(src, srcAddress, base, baseAddress, bytesToCopy);
        unsafe.copyMemory(src, srcAddress + bytesToCopy, base, baseAddress + bytesToCopy, length - bytesToCopy);
    }
}
