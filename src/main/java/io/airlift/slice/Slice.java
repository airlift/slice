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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.foreign.ValueLayout.OfByte;
import java.lang.foreign.ValueLayout.OfDouble;
import java.lang.foreign.ValueLayout.OfFloat;
import java.lang.foreign.ValueLayout.OfInt;
import java.lang.foreign.ValueLayout.OfLong;
import java.lang.foreign.ValueLayout.OfShort;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Optional;

import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.checkFromIndexSize;

public final class Slice
        implements Comparable<Slice>
{
    private static final OfByte BYTE = ValueLayout.JAVA_BYTE.withOrder(LITTLE_ENDIAN);
    private static final OfShort SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final OfInt INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final OfFloat FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final OfDouble DOUBLE = ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(LITTLE_ENDIAN);

    private static final int INSTANCE_SIZE = instanceSize(Slice.class);

    static final Slice EMPTY_SLICE = new Slice();

    private final MemorySegment memory;

    /**
     * Bytes retained by the slice
     */
    private final long retainedSize;

    /**
     * Precomputed hash code for performance. 0 means the hash code has not been computed yet.
     */
    private int hash;

    /**
     * Creates a slice over the specified memory segment.
     * If memory is zero length, a shared empty slice is returned.
     */
    static Slice createSlice(MemorySegment memory)
    {
        if (memory.byteSize() == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(memory);
    }

    /**
     * Creates an empty slice.
     */
    private Slice()
    {
        this(MemorySegment.NULL, INSTANCE_SIZE);
    }

    /**
     * Creates a slice over the specified memory segment.
     */
    private Slice(MemorySegment memory)
    {
        this(memory, INSTANCE_SIZE + estimatedSizeOf(memory));
    }

    /**
     * Creates a slice over the specified memory segment with a predeclared retained size.
     */
    private Slice(MemorySegment memory, long retainedSize)
    {
        if (memory.byteSize() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("MemorySegment can not be larger than Integer.MAX_VALUE bytes");
        }
        this.memory = memory;
        if (!memory.isNative() && retainedSize < memory.byteSize()) {
            throw new IllegalArgumentException("Retained size is smaller than the size of the memory segment");
        }
        this.retainedSize = retainedSize;
    }

    /**
     * Returns the internal memory segment.
     */
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public MemorySegment getMemory()
    {
        return memory;
    }

    /**
     * Returns the base object of this Slice, or null.  This is appropriate for use
     * with {@link Unsafe} if you wish to avoid all the safety belts e.g. bounds checks.
     *
     * @deprecated Use {@link #getMemory()} instead.
     */
    @Deprecated(forRemoval = true)
    public Object getBase()
    {
        return getMemory();
    }

    /**
     * Always zero.
     *
     * @deprecated Address can be fetched from {@link #getMemory()}, but that address is relative to the raw memory and not the view.
     */
    @Deprecated(forRemoval = true)
    public long getAddress()
    {
        return 0;
    }

    /**
     * Length of this slice.
     */
    public int length()
    {
        return (int) memory.byteSize();
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
        long size = length();
        if (size == 0) {
            return true;
        }
        if (memory.address() != 0) {
            return false;
        }
        Optional<Object> arrayReference = memory.heapBase();
        if (arrayReference.isEmpty()) {
            return false;
        }
        Object array = arrayReference.orElseThrow();
        int elementCount = Array.getLength(array);
        if (array instanceof byte[]) {
            return elementCount == size;
        }
        if (array instanceof short[]) {
            return (long) elementCount * SizeOf.SIZE_OF_SHORT == size;
        }
        if (array instanceof int[]) {
            return (long) elementCount * SizeOf.SIZE_OF_INT == size;
        }
        if (array instanceof long[]) {
            return (long) elementCount * SizeOf.SIZE_OF_LONG == size;
        }
        if (array instanceof float[]) {
            return (long) elementCount * SizeOf.SIZE_OF_FLOAT == size;
        }
        if (array instanceof double[]) {
            return (long) elementCount * SizeOf.SIZE_OF_DOUBLE == size;
        }
        throw new UnsupportedOperationException("Unsupported array type: " + array.getClass().getName());
    }

    private void checkHasByteArray()
            throws UnsupportedOperationException
    {
        if (!hasByteArray()) {
            throw new UnsupportedOperationException("Slice is not backed by a byte array");
        }
    }

    public boolean hasByteArray()
    {
        Optional<Object> array = memory.heapBase();
        return array.isPresent() && array.orElseThrow() instanceof byte[];
    }

    /**
     * Returns the byte array wrapped by this Slice, if any. Callers are expected to check {@link Slice#hasByteArray()} before calling
     * this method since not all instances are backed by a byte array. Callers should also take care to use {@link Slice#byteArrayOffset()}
     * since the contents of this Slice may not start at array index 0.
     *
     * @throws UnsupportedOperationException if this Slice has no underlying byte array
     */
    public byte[] byteArray()
            throws UnsupportedOperationException
    {
        checkHasByteArray();
        return (byte[]) memory.heapBase().orElseThrow();
    }

    /**
     * Returns the start index the content of this slice within the byte array wrapped by this slice. Callers should
     * check {@link Slice#hasByteArray()} before calling this method since not all Slices wrap a heap byte array
     *
     * @throws UnsupportedOperationException if this Slice has no underlying byte array
     */
    public int byteArrayOffset()
            throws UnsupportedOperationException
    {
        checkHasByteArray();
        return toIntExact(memory.address());
    }

    /**
     * Fill the slice with the specified value;
     */
    public void fill(byte value)
    {
        memory.fill(value);
    }

    /**
     * Fill the slice with zeros;
     */
    public void clear()
    {
        memory.fill((byte) 0);
    }

    public void clear(int offset, int length)
    {
        memory.asSlice(offset, length).fill((byte) 0);
    }

    /**
     * Gets a byte at the specified absolute {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    public byte getByte(int index)
    {
        return memory.get(BYTE, index);
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
        return memory.get(SHORT, index);
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
        return memory.get(INT, index);
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
        return memory.get(LONG, index);
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
        return memory.get(FLOAT, index);
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
        return memory.get(DOUBLE, index);
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
        MemorySegment.copy(memory, index, MemorySegment.ofArray(destination), destinationIndex, length);
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

        if (hasByteArray()) {
            out.write(byteArray(), byteArrayOffset() + index, length);
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
        memory.set(BYTE, index, (byte) value);
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
        memory.set(SHORT, index, (short) value);
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
        memory.set(INT, index, value);
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
        memory.set(LONG, index, value);
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
        memory.set(FLOAT, index, value);
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
        memory.set(DOUBLE, index, value);
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
        MemorySegment.copy(source.memory, sourceIndex, memory, index, length);
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
        MemorySegment.copy(source, sourceIndex, memory, BYTE, index, length);
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

        Optional<Object> arrayRef = memory.heapBase();
        if (arrayRef.isPresent()) {
            Object array = arrayRef.orElseThrow();
            if (array instanceof byte[]) {
                int bytesRead = in.readNBytes((byte[]) array, (int) (memory.address() + index), length);
                if (bytesRead != length) {
                    throw new IndexOutOfBoundsException("End of stream");
                }
                return;
            }
        }

        byte[] bytes = new byte[4096];

        while (length > 0) {
            int bytesRead = in.read(bytes, 0, min(bytes.length, length));
            if (bytesRead < 0) {
                throw new IndexOutOfBoundsException("End of stream");
            }
            setBytes(index, bytes, 0, bytesRead);
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
        checkFromIndexSize(index, length, length());
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }

        return new Slice(memory.asSlice(index, length), retainedSize);
    }

    public int indexOfByte(int b)
    {
        checkArgument((b >> Byte.SIZE) == 0, "byte value out of range");
        return indexOfByte((byte) b);
    }

    public int indexOfByte(byte b)
    {
        int size = length();
        for (int i = 0; i < size; i++) {
            if (getByte(i) == b) {
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
        int size = length();
        if (offset >= size || offset < 0) {
            return -1;
        }

        int patternLength = pattern.length();
        if (patternLength == 0) {
            return offset;
        }

        // Do we have enough characters
        if (patternLength < SIZE_OF_INT || size < SIZE_OF_LONG) {
            return indexOfBruteForce(pattern, offset);
        }

        // Using first four bytes for faster search. We are not using eight bytes for long
        // because we want more strings to get use of fast search.
        int head = pattern.getInt(0);

        // Take the first byte of head for faster skipping
        int firstByteMask = head & 0xff;
        firstByteMask |= firstByteMask << 8;
        firstByteMask |= firstByteMask << 16;

        int lastValidIndex = size - patternLength;
        int index = offset;
        while (index <= lastValidIndex) {
            // Read four bytes in sequence
            int value = getInt(index);

            // Compare all bytes of value with first byte of search data
            // see https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
            int valueXor = value ^ firstByteMask;
            int hasZeroBytes = (valueXor - 0x01010101) & ~valueXor & 0x80808080;

            // If valueXor doesn't have any zero byte then there is no match and we can advance
            if (hasZeroBytes == 0) {
                index += SIZE_OF_INT;
                continue;
            }

            // Try fast match of head and the rest
            if (value == head && mismatch(index, patternLength, pattern, 0, patternLength) == -1) {
                return index;
            }

            index++;
        }

        return -1;
    }

    int indexOfBruteForce(Slice pattern, int offset)
    {
        int size = length();
        if (offset >= size || offset < 0) {
            return -1;
        }

        int patternLength = pattern.length();
        if (patternLength == 0) {
            return offset;
        }

        byte firstByte = pattern.getByte(0);
        int lastValidIndex = size - patternLength;
        int index = offset;
        while (true) {
            // seek to first byte match
            while (index < lastValidIndex && getByte(index) != firstByte) {
                index++;
            }
            if (index > lastValidIndex) {
                break;
            }

            if (mismatch(index, patternLength, pattern, 0, patternLength) == -1) {
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
        return compareTo(0, length(), that, 0, that.length());
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

        int compareLength = min(length, otherLength);
        // for large comparisons, mismatch is faster
        if (compareLength >= 128) {
            // find where the string differ and then check the byte at the difference
            int mismatch = mismatch(offset, compareLength, that, otherOffset, compareLength);
            if (mismatch == -1) {
                return Integer.compare(length, otherLength);
            }
            return compareUnsignedBytes(getByte(offset + mismatch), that.getByte(otherOffset + mismatch));
        }

        int position = 0;
        while (compareLength >= SIZE_OF_LONG) {
            long thisLong = memory.get(LONG, position);
            long thatLong = that.memory.get(LONG, position);

            if (thisLong != thatLong) {
                return longBytesToLong(thisLong) < longBytesToLong(thatLong) ? -1 : 1;
            }

            position += SIZE_OF_LONG;
            compareLength -= SIZE_OF_LONG;
        }

        while (compareLength > 0) {
            byte thisByte = memory.get(BYTE, position);
            byte thatByte = that.memory.get(BYTE, position);

            int v = compareUnsignedBytes(thisByte, thatByte);
            if (v != 0) {
                return v;
            }
            position++;
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
        if (!(o instanceof Slice that)) {
            return false;
        }

        int length = length();
        if (length != that.length()) {
            return false;
        }
        return mismatch(0, length, that, 0, length) == -1;
    }

    /**
     * Returns the hash code of this slice.  The hash code is cached once calculated
     * and any future changes to the slice will not affect the hash code.
     */
    @Override
    public int hashCode()
    {
        if (hash != 0) {
            return hash;
        }

        hash = hashCode(0, length());
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
    public boolean equals(int offset, int length, Slice that, int otherOffset, int otherLength)
    {
        if (length != otherLength) {
            return false;
        }
        return mismatch(offset, length, that, otherOffset, otherLength) == -1;
    }

    public int mismatch(int offset, int length, Slice that, int otherOffset, int otherLength)
    {
        return (int) MemorySegment.mismatch(memory, offset, offset + length, that.memory, otherOffset, otherOffset + otherLength);
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
        return toString(US_ASCII);
    }

    public String toStringAscii(int index, int length)
    {
        return toString(0, length, US_ASCII);
    }

    /**
     * Decodes the portion of this slice into a string with the specified
     * character set name.
     */
    public String toString(int index, int length, Charset charset)
    {
        if (length == 0) {
            return "";
        }
        if (hasByteArray()) {
            return new String(byteArray(), byteArrayOffset() + index, length, charset);
        }
        return new String(getBytes(index, length), charset);
    }

    public ByteBuffer toByteBuffer()
    {
        return memory.asByteBuffer();
    }

    public ByteBuffer toByteBuffer(int index, int length)
    {
        return memory.asSlice(index, length).asByteBuffer();
    }

    @Override
    public String toString()
    {
        return "Slice{memory=" + memory + '}';
    }

    //
    // The following methods were forked from Guava primitives
    //

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
