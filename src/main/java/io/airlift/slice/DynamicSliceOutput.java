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
import java.nio.charset.Charset;

import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.instanceSize;

public class DynamicSliceOutput
        extends SliceOutput
{
    private static final int INSTANCE_SIZE = instanceSize(DynamicSliceOutput.class);

    private Slice slice;
    private int size;

    public DynamicSliceOutput(int estimatedSize)
    {
        this.slice = Slices.allocate(estimatedSize);
    }

    @Override
    public void reset()
    {
        size = 0;
    }

    @Override
    public void reset(int position)
    {
        checkArgument(position >= 0, "position is negative");
        checkArgument(position <= size, "position is larger than size");
        size = position;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public long getRetainedSize()
    {
        return slice.getRetainedSize() + INSTANCE_SIZE;
    }

    @Override
    public boolean isWritable()
    {
        return writableBytes() > 0;
    }

    @Override
    public int writableBytes()
    {
        return slice.length() - size;
    }

    /**
     * Returns the slice with room for {@code length} more bytes at {@code size}. The field is stored only
     * when the slice is replaced: an unconditional store pays a GC write barrier on every write.
     */
    private Slice writableSlice(int size, int length)
    {
        Slice slice = this.slice;
        int minSize = size + length;
        if (minSize < 0 || minSize > slice.length()) {
            slice = Slices.ensureSize(slice, minSize);
            this.slice = slice;
        }
        return slice;
    }

    @Override
    public void writeByte(int value)
    {
        int size = this.size;
        writableSlice(size, SIZE_OF_BYTE).setByteUnchecked(size, value);
        this.size = size + SIZE_OF_BYTE;
    }

    @Override
    public void writeShort(int value)
    {
        int size = this.size;
        writableSlice(size, SIZE_OF_SHORT).setShortUnchecked(size, value);
        this.size = size + SIZE_OF_SHORT;
    }

    @Override
    public void writeInt(int value)
    {
        int size = this.size;
        writableSlice(size, SIZE_OF_INT).setIntUnchecked(size, value);
        this.size = size + SIZE_OF_INT;
    }

    @Override
    public void writeLong(long value)
    {
        int size = this.size;
        writableSlice(size, SIZE_OF_LONG).setLongUnchecked(size, value);
        this.size = size + SIZE_OF_LONG;
    }

    @Override
    public void writeFloat(float value)
    {
        int size = this.size;
        writableSlice(size, SIZE_OF_FLOAT).setFloatUnchecked(size, value);
        this.size = size + SIZE_OF_FLOAT;
    }

    @Override
    public void writeDouble(double value)
    {
        int size = this.size;
        writableSlice(size, SIZE_OF_DOUBLE).setDoubleUnchecked(size, value);
        this.size = size + SIZE_OF_DOUBLE;
    }

    @Override
    public void writeBytes(byte[] source)
    {
        writeBytes(source, 0, source.length);
    }

    @Override
    public void writeBytes(byte[] source, int sourceIndex, int length)
    {
        int size = this.size;
        writableSlice(size, length).setBytes(size, source, sourceIndex, length);
        this.size = size + length;
    }

    @Override
    public void writeShorts(short[] source, int sourceIndex, int length)
    {
        int size = this.size;
        writableSlice(size, length * Short.BYTES).setShorts(size, source, sourceIndex, length);
        this.size = size + length * Short.BYTES;
    }

    @Override
    public void writeInts(int[] source, int sourceIndex, int length)
    {
        int size = this.size;
        writableSlice(size, length * Integer.BYTES).setInts(size, source, sourceIndex, length);
        this.size = size + length * Integer.BYTES;
    }

    @Override
    public void writeLongs(long[] source, int sourceIndex, int length)
    {
        int size = this.size;
        writableSlice(size, length * Long.BYTES).setLongs(size, source, sourceIndex, length);
        this.size = size + length * Long.BYTES;
    }

    @Override
    public void writeFloats(float[] source, int sourceIndex, int length)
    {
        int size = this.size;
        writableSlice(size, length * Float.BYTES).setFloats(size, source, sourceIndex, length);
        this.size = size + length * Float.BYTES;
    }

    @Override
    public void writeDoubles(double[] source, int sourceIndex, int length)
    {
        int size = this.size;
        writableSlice(size, length * Double.BYTES).setDoubles(size, source, sourceIndex, length);
        this.size = size + length * Double.BYTES;
    }

    @Override
    public void writeBytes(Slice source)
    {
        writeBytes(source, 0, source.length());
    }

    @Override
    public void writeBytes(Slice source, int sourceIndex, int length)
    {
        int size = this.size;
        writableSlice(size, length).setBytes(size, source, sourceIndex, length);
        this.size = size + length;
    }

    @Override
    public void writeBytes(InputStream in, int length)
            throws IOException
    {
        int size = this.size;
        writableSlice(size, length).setBytes(size, in, length);
        this.size = size + length;
    }

    @Override
    public void writeZero(int length)
    {
        Slice slice = writableSlice(size, length);
        if (length >= BULK_ZERO_FILL_THRESHOLD) {
            // Bulk zero-fill (Arrays.fill) for long runs; for shorter runs the writeLong/writeByte
            // loop is faster (Arrays.fill carries a fixed setup cost). See SliceOutput.writeZero.
            slice.clear(size, length);
            size += length;
        }
        else {
            super.writeZero(length);
        }
    }

    @Override
    public DynamicSliceOutput appendLong(long value)
    {
        writeLong(value);
        return this;
    }

    @Override
    public DynamicSliceOutput appendDouble(double value)
    {
        writeDouble(value);
        return this;
    }

    @Override
    public DynamicSliceOutput appendInt(int value)
    {
        writeInt(value);
        return this;
    }

    @Override
    public DynamicSliceOutput appendShort(int value)
    {
        writeShort(value);
        return this;
    }

    @Override
    public DynamicSliceOutput appendByte(int value)
    {
        writeByte(value);
        return this;
    }

    @Override
    public DynamicSliceOutput appendBytes(byte[] source, int sourceIndex, int length)
    {
        write(source, sourceIndex, length);
        return this;
    }

    @Override
    public DynamicSliceOutput appendBytes(byte[] source)
    {
        writeBytes(source);
        return this;
    }

    @Override
    public DynamicSliceOutput appendBytes(Slice slice)
    {
        writeBytes(slice);
        return this;
    }

    @Override
    public Slice slice()
    {
        return slice.slice(0, size);
    }

    public Slice copySlice()
    {
        Slice copy = Slices.allocate(size);
        slice.getBytes(0, copy);
        return copy;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    @Override
    public Slice getUnderlyingSlice()
    {
        return slice;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("BasicSliceOutput{");
        builder.append("size=").append(size);
        builder.append(", capacity=").append(slice.length());
        builder.append('}');
        return builder.toString();
    }

    @Override
    public String toString(Charset charset)
    {
        return slice.toString(0, size, charset);
    }
}
