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

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

@SuppressWarnings("JavaDoc") // IDEA-81310
public abstract class SliceOutput
        extends OutputStream
        implements DataOutput
{
    /**
     * Resets this stream to the initial position.
     */
    public abstract void reset();

    /**
     * Resets this stream to the specified position.
     */
    public abstract void reset(int position);

    /**
     * Returns the {@code writerIndex} of this buffer.
     */
    public abstract int size();

    /**
     * Approximate number of bytes retained by this.
     */
    public abstract long getRetainedSize();

    /**
     * Returns the number of writable bytes which is equal to
     * {@code (this.capacity - this.writerIndex)}.
     */
    public abstract int writableBytes();

    /**
     * Returns {@code true}
     * if and only if {@code (this.capacity - this.writerIndex)} is greater
     * than {@code 0}.
     */
    public abstract boolean isWritable();

    @Override
    public final void writeBoolean(boolean value)
    {
        writeByte(value ? 1 : 0);
    }

    @Override
    public final void write(int value)
    {
        writeByte(value);
    }

    /**
     * Sets the specified byte at the current {@code writerIndex}
     * and increases the {@code writerIndex} by {@code 1} in this buffer.
     * The 24 high-order bits of the specified value are ignored.
     *
     * @throws IndexOutOfBoundsException if {@code this.writableBytes} is less than {@code 1}
     */
    @Override
    public abstract void writeByte(int value);

    /**
     * Sets the specified 16-bit short integer at the current
     * {@code writerIndex} and increases the {@code writerIndex} by {@code 2}
     * in this buffer.  The 16 high-order bits of the specified value are ignored.
     *
     * @throws IndexOutOfBoundsException if {@code this.writableBytes} is less than {@code 2}
     */
    @Override
    public abstract void writeShort(int value);

    /**
     * Sets the specified 32-bit integer at the current {@code writerIndex}
     * and increases the {@code writerIndex} by {@code 4} in this buffer.
     *
     * @throws IndexOutOfBoundsException if {@code this.writableBytes} is less than {@code 4}
     */
    @Override
    public abstract void writeInt(int value);

    /**
     * Sets the specified 64-bit long integer at the current
     * {@code writerIndex} and increases the {@code writerIndex} by {@code 8}
     * in this buffer.
     *
     * @throws IndexOutOfBoundsException if {@code this.writableBytes} is less than {@code 8}
     */
    @Override
    public abstract void writeLong(long value);

    /**
     * Sets the specified 32-bit float at the current
     * {@code writerIndex} and increases the {@code writerIndex} by {@code 4}
     * in this buffer.
     *
     * @throws IndexOutOfBoundsException if {@code this.writableBytes} is less than {@code 4}
     */
    @Override
    public abstract void writeFloat(float v);

    /**
     * Sets the specified 64-bit double at the current
     * {@code writerIndex} and increases the {@code writerIndex} by {@code 8}
     * in this buffer.
     *
     * @throws IndexOutOfBoundsException if {@code this.writableBytes} is less than {@code 8}
     */
    @Override
    public abstract void writeDouble(double value);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the current {@code writerIndex} until the source buffer becomes
     * unreadable, and increases the {@code writerIndex} by the number of
     * the transferred bytes.  This method is basically same with
     * {@link #writeBytes(Slice, int, int)}, except that this method
     * increases the {@code readerIndex} of the source buffer by the number of
     * the transferred bytes while {@link #writeBytes(Slice, int, int)}
     * does not.
     *
     * @throws IndexOutOfBoundsException if {@code source.readableBytes} is greater than {@code this.writableBytes}
     */
    public abstract void writeBytes(Slice source);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the current {@code writerIndex} and increases the {@code writerIndex}
     * by the number of the transferred bytes (= {@code length}).
     *
     * @param sourceIndex the first index of the source
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code sourceIndex + length} is greater than {@code source.capacity}, or
     * if {@code length} is greater than {@code this.writableBytes}
     */
    public abstract void writeBytes(Slice source, int sourceIndex, int length);

    @Override
    public final void write(byte[] source)
            throws IOException
    {
        writeBytes(source);
    }

    /**
     * Transfers the specified source array's data to this buffer starting at
     * the current {@code writerIndex} and increases the {@code writerIndex}
     * by the number of the transferred bytes (= {@code source.length}).
     *
     * @throws IndexOutOfBoundsException if {@code source.length} is greater than {@code this.writableBytes}
     */
    public abstract void writeBytes(byte[] source);

    @Override
    public final void write(byte[] source, int sourceIndex, int length)
    {
        writeBytes(source, sourceIndex, length);
    }

    /**
     * Transfers the specified source array's data to this buffer starting at
     * the current {@code writerIndex} and increases the {@code writerIndex}
     * by the number of the transferred bytes (= {@code length}).
     *
     * @param sourceIndex the first index of the source
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code sourceIndex + length} is greater than {@code source.length}, or
     * if {@code length} is greater than {@code this.writableBytes}
     */
    public abstract void writeBytes(byte[] source, int sourceIndex, int length);

    /**
     * Transfers the content of the specified stream to this buffer
     * starting at the current {@code writerIndex} and increases the
     * {@code writerIndex} by the number of the transferred bytes.
     *
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if {@code length} is greater than {@code this.writableBytes}
     * @throws java.io.IOException if the specified stream threw an exception during I/O
     */
    public abstract void writeBytes(InputStream in, int length)
            throws IOException;

    /**
     * Fills this buffer with <tt>NUL (0x00)</tt> starting at the current
     * {@code writerIndex} and increases the {@code writerIndex} by the
     * specified {@code length}.
     *
     * @param length the number of <tt>NUL</tt>s to write to the buffer
     * @throws IndexOutOfBoundsException if {@code length} is greater than {@code this.writableBytes}
     */
    public void writeZero(int length)
    {
        if (length == 0) {
            return;
        }
        if (length < 0) {
            throw new IllegalArgumentException(
                    "length must be 0 or greater than 0.");
        }
        int nLong = length >>> 3;
        int nBytes = length & 7;
        for (int i = nLong; i > 0; i--) {
            writeLong(0);
        }
        if (nBytes == 4) {
            writeInt(0);
        }
        else if (nBytes < 4) {
            for (int i = nBytes; i > 0; i--) {
                writeByte((byte) 0);
            }
        }
        else {
            writeInt(0);
            for (int i = nBytes - 4; i > 0; i--) {
                writeByte((byte) 0);
            }
        }
    }

    /**
     * Returns a slice of this buffer's readable bytes. Modifying the content
     * of the returned buffer or this buffer affects each other's content
     * while they maintain separate indexes and marks.  This method is
     * identical to {@code buf.slice(buf.readerIndex(), buf.readableBytes())}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     */
    public abstract Slice slice();

    /**
     * Returns the raw underlying slice of this output stream.  The slice may
     * be larger than the size of this stream.
     */
    public abstract Slice getUnderlyingSlice();

    /**
     * Decodes this buffer's readable bytes into a string with the specified
     * character set name.  This method is identical to
     * {@code buf.toString(buf.readerIndex(), buf.readableBytes(), charsetName)}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     */
    public abstract String toString(Charset charset);

    public abstract SliceOutput appendLong(long value);

    public abstract SliceOutput appendDouble(double value);

    public abstract SliceOutput appendInt(int value);

    public abstract SliceOutput appendShort(int value);

    public abstract SliceOutput appendByte(int value);

    public abstract SliceOutput appendBytes(byte[] source, int sourceIndex, int length);

    public abstract SliceOutput appendBytes(byte[] source);

    public abstract SliceOutput appendBytes(Slice slice);

    //
    // Unsupported operations
    //

    /**
     * Unsupported operation
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void writeChar(int value)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void writeChars(String s)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void writeUTF(String s)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void writeBytes(String s)
    {
        throw new UnsupportedOperationException();
    }
}
