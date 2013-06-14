package io.airlift.slice;

import com.google.common.base.Objects;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkPositionIndex;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;

@SuppressWarnings("JavaDoc") // IDEA-81310
public final class BasicSliceInput
        extends SliceInput
{
    private final Slice slice;
    private int position;

    public BasicSliceInput(Slice slice)
    {
        this.slice = slice;
    }

    @Override
    public int position()
    {
        return position;
    }

    @Override
    public void setPosition(int position)
    {
        checkPositionIndex(position, slice.length());
        this.position = position;
    }

    @Override
    public boolean isReadable()
    {
        return position < slice.length();
    }

    @Override
    public int available()
    {
        return slice.length() - position;
    }

    @Override
    public boolean readBoolean()
            throws IOException
    {
        return readByte() != 0;
    }

    @Override
    public int read()
    {
        if (position >= slice.length()) {
            return -1;
        }
        int result = slice.getByte(position) & 0xFF;
        position++;
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
        short v = slice.getShort(position);
        position += SIZE_OF_SHORT;
        return v;
    }

    @Override
    public int readUnsignedShort()
            throws IOException
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public int readInt()
    {
        int v = slice.getInt(position);
        position += SIZE_OF_INT;
        return v;
    }

    @Override
    public long readLong()
    {
        long v = slice.getLong(position);
        position += SIZE_OF_LONG;
        return v;
    }

    @Override
    public float readFloat()
    {
        float v = slice.getFloat(position);
        position += SIZE_OF_FLOAT;
        return v;
    }

    @Override
    public double readDouble()
    {
        double v = slice.getDouble(position);
        position += SIZE_OF_DOUBLE;
        return v;
    }

    @Override
    public Slice readSlice(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        Slice newSlice = slice.slice(position, length);
        position += length;
        return newSlice;
    }

    @Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        slice.getBytes(position, destination, destinationIndex, length);
        position += length;
    }

    @Override
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        slice.getBytes(position, destination, destinationIndex, length);
        position += length;
    }

    @Override
    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        slice.getBytes(position, out, length);
        position += length;
    }

    @Override
    public int skipBytes(int length)
    {
        length = Math.min(length, available());
        position += length;
        return length;
    }

    /**
     * Returns a slice of this buffer's readable bytes. Modifying the content
     * of the returned buffer or this buffer affects each other's content
     * while they maintain separate indexes and marks.  This method is
     * identical to {@code buf.slice(buf.position(), buf.available()())}.
     * This method does not modify {@code position} or {@code writerIndex} of
     * this buffer.
     */
    public Slice slice()
    {
        return slice.slice(position, slice.length() - position);
    }

    /**
     * Decodes this buffer's readable bytes into a string with the specified
     * character set name.  This method is identical to
     * {@code buf.toString(buf.position(), buf.available()(), charsetName)}.
     * This method does not modify {@code position} or {@code writerIndex} of
     * this buffer.
     */
    public String toString(Charset charset)
    {
        return slice.toString(position, available(), charset);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("position", position)
                .add("capacity", slice.length())
                .toString();
    }
}
