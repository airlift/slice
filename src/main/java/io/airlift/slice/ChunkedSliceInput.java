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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static java.util.Objects.requireNonNull;

public final class ChunkedSliceInput
        extends FixedLengthSliceInput
{
    private final InternalLoader<?> loader;
    private final Slice buffer;

    private final long globalLength;
    private long globalPosition;
    private int bufferPosition;
    private int bufferLength;

    public ChunkedSliceInput(SliceLoader<?> loader, int bufferSize)
    {
        this.loader = new InternalLoader<>(requireNonNull(loader, "loader is null"), bufferSize);
        this.buffer = this.loader.getBufferSlice();
        this.globalLength = loader.getSize();
    }

    @Override
    public long length()
    {
        return globalLength;
    }

    @Override
    public long position()
    {
        return globalPosition + bufferPosition;
    }

    @Override
    public void setPosition(long position)
    {
        if (position < 0 || position > globalLength) {
            throw new IndexOutOfBoundsException("Invalid position " + position + " for slice with length " + globalLength);
        }
        if (position >= globalPosition && position - globalPosition < bufferLength) {
            // (position - globalPosition) is guaranteed to fit in int type here because of the above condition
            bufferPosition = (int) (position - globalPosition);
            return;
        }
        this.globalPosition = position;
        this.bufferLength = 0;
        this.bufferPosition = 0;
    }

    @Override
    public boolean isReadable()
    {
        return bufferPosition < bufferLength;
    }

    @Override
    public int available()
    {
        return bufferLength - bufferPosition;
    }

    public void ensureAvailable(int size)
    {
        if (available() >= size) {
            return;
        }

        checkArgument(size <= buffer.length(), "Size is larger than buffer");
        checkBound(position() + size, globalLength, "End of stream");

        // advance position
        globalPosition += bufferPosition;
        bufferPosition = 0;

        // this will reread unused data in the buffer
        long readSize = Math.min(buffer.length(), globalLength - globalPosition);
        if (readSize > Integer.MAX_VALUE) {
            readSize = Integer.MAX_VALUE;
        }
        bufferLength = (int) readSize;
        loader.load(globalPosition, bufferLength);
    }

    @Override
    public boolean readBoolean()
    {
        return readByte() != 0;
    }

    @Override
    public int read()
    {
        if (position() >= globalLength) {
            return -1;
        }
        ensureAvailable(SIZE_OF_BYTE);
        int result = buffer.getByte(bufferPosition) & 0xFF;
        bufferPosition++;
        return result;
    }

    @Override
    public byte readByte()
    {
        int value = read();
        if (value == -1) {
            throw new IndexOutOfBoundsException();
        }
        return (byte) value;
    }

    @Override
    public int readUnsignedByte()
    {
        return readByte() & 0xFF;
    }

    @Override
    public short readShort()
    {
        ensureAvailable(SIZE_OF_SHORT);
        short v = buffer.getShort(bufferPosition);
        bufferPosition += SIZE_OF_SHORT;
        return v;
    }

    @Override
    public int readUnsignedShort()
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public int readInt()
    {
        ensureAvailable(SIZE_OF_INT);
        int v = buffer.getInt(bufferPosition);
        bufferPosition += SIZE_OF_INT;
        return v;
    }

    @Override
    public long readLong()
    {
        ensureAvailable(SIZE_OF_LONG);
        long v = buffer.getLong(bufferPosition);
        bufferPosition += SIZE_OF_LONG;
        return v;
    }

    @Override
    public float readFloat()
    {
        ensureAvailable(SIZE_OF_FLOAT);
        float v = buffer.getFloat(bufferPosition);
        bufferPosition += SIZE_OF_FLOAT;
        return v;
    }

    @Override
    public double readDouble()
    {
        ensureAvailable(SIZE_OF_DOUBLE);
        double v = buffer.getDouble(bufferPosition);
        bufferPosition += SIZE_OF_DOUBLE;
        return v;
    }

    @Override
    public Slice readSlice(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        Slice slice = Slices.allocate(length);
        readBytes(slice);
        return slice;
    }

    @Override
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        checkBound(position() + length, globalLength, "End of stream");

        while (length > 0) {
            int bytesToRead = Math.min(available(), length);
            buffer.getBytes(bufferPosition, destination, destinationIndex, bytesToRead);

            bufferPosition += bytesToRead;
            length -= bytesToRead;
            destinationIndex += bytesToRead;

            ensureAvailable(Math.min(length, buffer.length()));
        }
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
    {
        if (length == 0) {
            return 0;
        }

        if (globalLength - position() == 0) {
            return -1;
        }

        // limit read to stream size
        length = (int) Math.min(length, globalLength - position());

        // do a full read of the available data
        readBytes(destination, destinationIndex, length);
        return length;
    }

    @Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        checkBound(position() + length, globalLength, "End of stream");

        while (length > 0) {
            int bytesToRead = Math.min(available(), length);
            buffer.getBytes(bufferPosition, destination, destinationIndex, bytesToRead);

            bufferPosition += bytesToRead;
            length -= bytesToRead;
            destinationIndex += bytesToRead;

            ensureAvailable(Math.min(length, buffer.length()));
        }
    }

    @Override
    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        checkBound(position() + length, globalLength, "End of stream");

        while (length > 0) {
            int bytesToRead = Math.min(available(), length);
            buffer.getBytes(bufferPosition, out, bytesToRead);

            bufferPosition += bytesToRead;
            length -= bytesToRead;

            ensureAvailable(Math.min(length, buffer.length()));
        }
    }

    @Override
    public long skip(long length)
    {
        // is skip within the current buffer?
        if (available() >= length) {
            bufferPosition += length;
            return length;
        }

        // drop current buffer
        globalPosition += bufferPosition;
        bufferPosition = 0;
        bufferLength = 0;

        // trim length to stream size
        length = Math.min(length, remaining());

        // skip
        globalPosition += length;

        return length;
    }

    @Override
    public int skipBytes(int length)
    {
        return (int) skip(length);
    }

    @Override
    public void close()
    {
        globalPosition = globalLength;
        bufferPosition = 0;
        bufferLength = 0;
        loader.close();
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("SliceStreamInput{");
        builder.append("globalLength=").append(globalLength);
        builder.append(", globalPosition=").append(globalPosition);
        builder.append(", bufferLength=").append(bufferLength);
        builder.append(", bufferPosition=").append(bufferPosition);
        builder.append('}');
        return builder.toString();
    }

    private static void checkBound(long index, long size, String message)
    {
        if (index > size) {
            throw new IndexOutOfBoundsException(message);
        }
    }

    public interface SliceLoader<B extends BufferReference>
            extends Closeable
    {
        B createBuffer(int bufferSize);

        long getSize();

        void load(long position, B bufferReference, int length);

        @Override
        void close();
    }

    public interface BufferReference
    {
        Slice getSlice();
    }

    private static class InternalLoader<T extends BufferReference>
    {
        private final SliceLoader<T> loader;
        private final T bufferReference;

        public InternalLoader(SliceLoader<T> loader, int bufferSize)
        {
            this.loader = loader;
            checkArgument(bufferSize >= 128, "Buffer size must be at least 128");
            this.bufferReference = loader.createBuffer(bufferSize);
        }

        public Slice getBufferSlice()
        {
            return bufferReference.getSlice();
        }

        public void load(long position, int length)
        {
            loader.load(position, bufferReference, length);
        }

        public void close()
        {
            loader.close();
        }
    }
}
