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

import org.openjdk.jol.info.ClassLayout;
import sun.misc.Unsafe;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static io.airlift.slice.JvmUtils.newByteBuffer;
import static io.airlift.slice.JvmUtils.unsafe;
import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.Preconditions.checkPositionIndexes;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfBooleanArray;
import static io.airlift.slice.SizeOf.sizeOfDoubleArray;
import static io.airlift.slice.SizeOf.sizeOfFloatArray;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.airlift.slice.SizeOf.sizeOfLongArray;
import static io.airlift.slice.SizeOf.sizeOfShortArray;
import static io.airlift.slice.StringDecoder.decodeString;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BOOLEAN_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_DOUBLE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_FLOAT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;

public final class Slice
        implements Comparable<Slice>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Slice.class).instanceSize();

    /**
     * @deprecated use {@link Slices#wrappedBuffer(java.nio.ByteBuffer)}
     */
    @Deprecated
    public static Slice toUnsafeSlice(ByteBuffer byteBuffer)
    {
        return Slices.wrappedBuffer(byteBuffer);
    }

    /**
     * Base object for relative addresses.  If null, the address is an
     * absolute location in memory.
     */
    private final Object base;

    /**
     * If base is null, address is the absolute memory location of data for
     * this slice; otherwise, address is the offset from the base object.
     * This base plus relative offset addressing is taken directly from
     * the Unsafe interface.
     * <p>
     * Note: if base object is a byte array, this address ARRAY_BYTE_BASE_OFFSET,
     * since the byte array data starts AFTER the byte array object header.
     */
    private final long address;

    /**
     * Size of the slice
     */
    private final int size;

    /**
     * Bytes retained by the slice
     */
    private final long retainedSize;

    /**
     * Reference is typically a ByteBuffer object, but can be any object this
     * slice must hold onto to assure that the underlying memory is not
     * freed by the garbage collector.
     */
    private final Object reference;

    private int hash;

    /**
     * Creates an empty slice.
     */
    Slice()
    {
        this.base = null;
        this.address = 0;
        this.size = 0;
        this.retainedSize = INSTANCE_SIZE;
        this.reference = null;
    }

    /**
     * Creates a slice over the specified array.
     */
    Slice(byte[] base)
    {
        requireNonNull(base, "base is null");
        this.base = base;
        this.address = ARRAY_BYTE_BASE_OFFSET;
        this.size = base.length;
        this.retainedSize = INSTANCE_SIZE + sizeOf(base);
        this.reference = null;
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
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = ARRAY_BYTE_BASE_OFFSET + offset;
        this.size = length;
        this.retainedSize = INSTANCE_SIZE + sizeOf(base);
        this.reference = null;
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    Slice(boolean[] base, int offset, int length)
    {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = sizeOfBooleanArray(offset);
        this.size = multiplyExact(length, ARRAY_BOOLEAN_INDEX_SCALE);
        this.retainedSize = INSTANCE_SIZE + sizeOf(base);
        this.reference = null;
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    Slice(short[] base, int offset, int length)
    {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = sizeOfShortArray(offset);
        this.size = multiplyExact(length, ARRAY_SHORT_INDEX_SCALE);
        this.retainedSize = INSTANCE_SIZE + sizeOf(base);
        this.reference = null;
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    Slice(int[] base, int offset, int length)
    {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = sizeOfIntArray(offset);
        this.size = multiplyExact(length, ARRAY_INT_INDEX_SCALE);
        this.retainedSize = INSTANCE_SIZE + sizeOf(base);
        this.reference = null;
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    Slice(long[] base, int offset, int length)
    {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = sizeOfLongArray(offset);
        this.size = multiplyExact(length, ARRAY_LONG_INDEX_SCALE);
        this.retainedSize = INSTANCE_SIZE + sizeOf(base);
        this.reference = null;
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    Slice(float[] base, int offset, int length)
    {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = sizeOfFloatArray(offset);
        this.size = multiplyExact(length, ARRAY_FLOAT_INDEX_SCALE);
        this.retainedSize = INSTANCE_SIZE + sizeOf(base);
        this.reference = null;
    }

    /**
     * Creates a slice over the specified array range.
     *
     * @param offset the array position at which the slice begins
     * @param length the number of array positions to include in the slice
     */
    Slice(double[] base, int offset, int length)
    {
        requireNonNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = sizeOfDoubleArray(offset);
        this.size = multiplyExact(length, ARRAY_DOUBLE_INDEX_SCALE);
        this.retainedSize = INSTANCE_SIZE + sizeOf(base);
        this.reference = null;
    }

    /**
     * Creates a slice for directly accessing the base object.
     */
    Slice(@Nullable Object base, long address, int size, long retainedSize, @Nullable Object reference)
    {
        if (address <= 0) {
            throw new IllegalArgumentException(format("Invalid address: %s", address));
        }
        if (size <= 0) {
            throw new IllegalArgumentException(format("Invalid size: %s", size));
        }
        checkArgument((address + size) >= size, "Address + size is greater than 64 bits");

        this.reference = reference;
        this.base = base;
        this.address = address;
        this.size = size;
        // INSTANCE_SIZE is not included, as the caller is responsible for including it.
        this.retainedSize = retainedSize;
    }

    /**
     * Returns the base object of this Slice, or null.  This is appropriate for use
     * with {@link Unsafe} if you wish to avoid all the safety belts e.g. bounds checks.
     */
    public Object getBase()
    {
        return base;
    }

    /**
     * Return the address offset of this Slice.  This is appropriate for use
     * with {@link Unsafe} if you wish to avoid all the safety belts e.g. bounds checks.
     */
    public long getAddress()
    {
        return address;
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
     * Fill the slice with the specified value;
     */
    public void fill(byte value)
    {
        int offset = 0;
        int length = size;
        long longValue = fillLong(value);
        while (length >= SIZE_OF_LONG) {
            unsafe.putLong(base, address + offset, longValue);
            offset += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            unsafe.putByte(base, address + offset, value);
            offset++;
            length--;
        }
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
        while (length >= SIZE_OF_LONG) {
            unsafe.putLong(base, address + offset, 0);
            offset += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            unsafe.putByte(base, address + offset, (byte) 0);
            offset++;
            length--;
        }
    }

    /**
     * Gets a byte at the specified absolute {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    public byte getByte(int index)
    {
        checkIndexLength(index, SIZE_OF_BYTE);
        return getByteUnchecked(index);
    }

    byte getByteUnchecked(int index)
    {
        return unsafe.getByte(base, address + index);
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
        checkIndexLength(index, SIZE_OF_SHORT);
        return getShortUnchecked(index);
    }

    short getShortUnchecked(int index)
    {
        return unsafe.getShort(base, address + index);
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
        checkIndexLength(index, SIZE_OF_INT);
        return getIntUnchecked(index);
    }

    int getIntUnchecked(int index)
    {
        return unsafe.getInt(base, address + index);
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
        checkIndexLength(index, SIZE_OF_LONG);
        return getLongUnchecked(index);
    }

    long getLongUnchecked(int index)
    {
        return unsafe.getLong(base, address + index);
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
        checkIndexLength(index, SIZE_OF_FLOAT);
        return unsafe.getFloat(base, address + index);
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
        checkIndexLength(index, SIZE_OF_DOUBLE);
        return unsafe.getDouble(base, address + index);
    }

    /**
     * Transfers portion of data from this slice into the specified destination starting at
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
     * Transfers portion of data from this slice into the specified destination starting at
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
        destination.setBytes(destinationIndex, this, index, length);
    }

    /**
     * Transfers portion of data from this slice into the specified destination starting at
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
     * Transfers portion of data from this slice into the specified destination starting at
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
        checkIndexLength(index, length);
        checkPositionIndexes(destinationIndex, destinationIndex + length, destination.length);

        copyMemory(base, address + index, destination, (long) ARRAY_BYTE_BASE_OFFSET + destinationIndex, length);
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
        checkIndexLength(index, length);

        if (base instanceof byte[]) {
            out.write((byte[]) base, (int) ((address - ARRAY_BYTE_BASE_OFFSET) + index), length);
            return;
        }

        byte[] buffer = new byte[4096];
        while (length > 0) {
            int size = min(buffer.length, length);
            getBytes(index, buffer, 0, size);
            out.write(buffer, 0, size);
            length -= size;
            index += size;
        }
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
        checkIndexLength(index, SIZE_OF_BYTE);
        setByteUnchecked(index, value);
    }

    void setByteUnchecked(int index, int value)
    {
        unsafe.putByte(base, address + index, (byte) (value & 0xFF));
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
        checkIndexLength(index, SIZE_OF_SHORT);
        setShortUnchecked(index, value);
    }

    void setShortUnchecked(int index, int value)
    {
        unsafe.putShort(base, address + index, (short) (value & 0xFFFF));
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
        checkIndexLength(index, SIZE_OF_INT);
        setIntUnchecked(index, value);
    }

    void setIntUnchecked(int index, int value)
    {
        unsafe.putInt(base, address + index, value);
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
        checkIndexLength(index, SIZE_OF_LONG);
        setLongUnchecked(index, value);
    }

    void setLongUnchecked(int index, long value)
    {
        unsafe.putLong(base, address + index, value);
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
        checkIndexLength(index, SIZE_OF_FLOAT);
        unsafe.putFloat(base, address + index, value);
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
        checkIndexLength(index, SIZE_OF_DOUBLE);
        unsafe.putDouble(base, address + index, value);
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
        checkIndexLength(index, length);
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length());

        copyMemory(source.base, source.address + sourceIndex, base, address + index, length);
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
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length);
        copyMemory(source, (long) ARRAY_BYTE_BASE_OFFSET + sourceIndex, base, address + index, length);
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
        checkIndexLength(index, length);
        if (base instanceof byte[]) {
            byte[] bytes = (byte[]) base;
            int offset = (int) ((address - ARRAY_BYTE_BASE_OFFSET) + index);
            while (length > 0) {
                int bytesRead = in.read(bytes, offset, length);
                if (bytesRead < 0) {
                    throw new IndexOutOfBoundsException("End of stream");
                }
                length -= bytesRead;
                offset += bytesRead;
            }
            return;
        }

        byte[] bytes = new byte[4096];

        while (length > 0) {
            int bytesRead = in.read(bytes, 0, min(bytes.length, length));
            if (bytesRead < 0) {
                throw new IndexOutOfBoundsException("End of stream");
            }
            copyMemory(bytes, ARRAY_BYTE_BASE_OFFSET, base, address + index, bytesRead);
            length -= bytesRead;
            index += bytesRead;
        }
    }

    /**
     * Returns a slice of this buffer's sub-region. Modifying the content of
     * the returned buffer or this buffer affects each other's content.
     */
    public Slice slice(int index, int length)
    {
        if ((index == 0) && (length == length())) {
            return this;
        }
        checkIndexLength(index, length);
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new Slice(base, address + index, length, retainedSize, reference);
    }

    public int indexOfByte(int b)
    {
        b = b & 0xFF;
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
     * If the pattern is not found -1 is returned If patten is empty, the offset
     * is returned.
     */
    public int indexOf(Slice pattern, int offset)
    {
        if (size == 0 || offset >= size) {
            return -1;
        }

        if (pattern.length() == 0) {
            return offset;
        }

        // Do we have enough characters
        if (pattern.length() < SIZE_OF_INT || size < SIZE_OF_LONG) {
            return indexOfBruteForce(pattern, offset);
        }

        // Using first four bytes for faster search. We are not using eight bytes for long
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

            // Compare all bytes of value with first byte of search data
            // see https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
            int valueXor = value ^ firstByteMask;
            int hasZeroBytes = (valueXor - 0x01010101) & ~valueXor & 0x80808080;

            // If valueXor doesn't not have any zero byte then there is no match and we can advance
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
        if (size == 0 || offset >= size) {
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

        checkIndexLength(offset, length);
        that.checkIndexLength(otherOffset, otherLength);

        long thisAddress = address + offset;
        long thatAddress = that.address + otherOffset;

        int compareLength = min(length, otherLength);
        while (compareLength >= SIZE_OF_LONG) {
            long thisLong = unsafe.getLong(base, thisAddress);
            long thatLong = unsafe.getLong(that.base, thatAddress);

            if (thisLong != thatLong) {
                return longBytesToLong(thisLong) < longBytesToLong(thatLong) ? -1 : 1;
            }

            thisAddress += SIZE_OF_LONG;
            thatAddress += SIZE_OF_LONG;
            compareLength -= SIZE_OF_LONG;
        }

        while (compareLength > 0) {
            byte thisByte = unsafe.getByte(base, thisAddress);
            byte thatByte = unsafe.getByte(that.base, thatAddress);

            int v = compareUnsignedBytes(thisByte, thatByte);
            if (v != 0) {
                return v;
            }
            thisAddress++;
            thatAddress++;
            compareLength--;
        }

        return Integer.compare(length, otherLength);
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
        if (!(o instanceof Slice)) {
            return false;
        }

        Slice that = (Slice) o;
        if (length() != that.length()) {
            return false;
        }

        return equalsUnchecked(0, that, 0, length());
    }

    /**
     * Returns the hash code of this slice.  The hash code is cached once calculated
     * and any future changes to the slice will not effect the hash code.
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

        checkIndexLength(offset, length);
        that.checkIndexLength(otherOffset, otherLength);

        return equalsUnchecked(offset, that, otherOffset, length);
    }

    boolean equalsUnchecked(int offset, Slice that, int otherOffset, int length)
    {
        long thisAddress = address + offset;
        long thatAddress = that.address + otherOffset;

        while (length >= SIZE_OF_LONG) {
            long thisLong = unsafe.getLong(base, thisAddress);
            long thatLong = unsafe.getLong(that.base, thatAddress);

            if (thisLong != thatLong) {
                return false;
            }

            thisAddress += SIZE_OF_LONG;
            thatAddress += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            byte thisByte = unsafe.getByte(base, thisAddress);
            byte thatByte = unsafe.getByte(that.base, thatAddress);
            if (thisByte != thatByte) {
                return false;
            }
            thisAddress++;
            thatAddress++;
            length--;
        }

        return true;
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
     * character set.  The low order 7 bits if each byte are converted directly
     * into a code point for the string.
     */
    public String toStringAscii()
    {
        return toStringAscii(0, size);
    }

    public String toStringAscii(int index, int length)
    {
        checkIndexLength(index, length);
        if (length == 0) {
            return "";
        }

        if (base instanceof byte[]) {
            //noinspection deprecation
            return new String((byte[]) base, 0, (int) ((address - ARRAY_BYTE_BASE_OFFSET) + index), length);
        }

        char[] chars = new char[length];
        for (int pos = index; pos < length; pos++) {
            chars[pos] = (char) (getByteUnchecked(pos) & 0x7F);
        }
        return new String(chars);
    }

    /**
     * Decodes the a portion of this slice into a string with the specified
     * character set name.
     */
    public String toString(int index, int length, Charset charset)
    {
        if (length == 0) {
            return "";
        }
        if (base instanceof byte[]) {
            return new String((byte[]) base, (int) ((address - ARRAY_BYTE_BASE_OFFSET) + index), length, charset);
        }
        // direct memory can only be converted to a string using a ByteBuffer
        return decodeString(toByteBuffer(index, length), charset);
    }

    public ByteBuffer toByteBuffer()
    {
        return toByteBuffer(0, size);
    }

    public ByteBuffer toByteBuffer(int index, int length)
    {
        checkIndexLength(index, length);

        if (base instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) base, (int) ((address - ARRAY_BYTE_BASE_OFFSET) + index), length);
        }

        try {
            return (ByteBuffer) newByteBuffer.invokeExact(address + index, length, (Object) reference);
        }
        catch (Throwable throwable) {
            if (throwable instanceof Error) {
                throw (Error) throwable;
            }
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            }
            if (throwable instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(throwable);
        }
    }

    /**
     * Decodes the a portion of this slice into a string with the specified
     * character set name.
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Slice{");
        if (base != null) {
            builder.append("base=").append(identityToString(base)).append(", ");
        }
        builder.append("address=").append(address);
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

    private static void copyMemory(Object src, long srcAddress, Object dest, long destAddress, int length)
    {
        // The Unsafe Javadoc specifies that the transfer size is 8 iff length % 8 == 0
        // so ensure that we copy big chunks whenever possible, even at the expense of two separate copy operations
        int bytesToCopy = length - (length % 8);
        unsafe.copyMemory(src, srcAddress, dest, destAddress, bytesToCopy);
        unsafe.copyMemory(src, srcAddress + bytesToCopy, dest, destAddress + bytesToCopy, length - bytesToCopy);
    }

    private void checkIndexLength(int index, int length)
    {
        checkPositionIndexes(index, index + length, length());
    }

    //
    // The following methods were forked from Guava primitives
    //

    private static long fillLong(byte value)
    {
        return (value & 0xFFL) << 56
                | (value & 0xFFL) << 48
                | (value & 0xFFL) << 40
                | (value & 0xFFL) << 32
                | (value & 0xFFL) << 24
                | (value & 0xFFL) << 16
                | (value & 0xFFL) << 8
                | (value & 0xFFL);
    }

    private static int compareUnsignedBytes(byte thisByte, byte thatByte)
    {
        return unsignedByteToInt(thisByte) - unsignedByteToInt(thatByte);
    }

    private static int unsignedByteToInt(byte thisByte)
    {
        return thisByte & 0xFF;
    }

    /**
     * Turns a long representing a sequence of 8 bytes read in little-endian order
     * into a number that when compared produces the same effect as comparing the
     * original sequence of bytes lexicographically
     */
    private static long longBytesToLong(long bytes)
    {
        return Long.reverseBytes(bytes) ^ Long.MIN_VALUE;
    }
}
