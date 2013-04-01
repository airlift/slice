package io.airlift.slice;

import com.google.common.base.Objects;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class OutputStreamSliceOutput
        extends SliceOutput
{
    private final CountingOutputStream countingOutputStream; // Used only to track byte usage
    private final LittleEndianDataOutputStream dataOutputStream;

    @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
    public OutputStreamSliceOutput(OutputStream outputStream)
    {
        countingOutputStream = new CountingOutputStream(outputStream);
        dataOutputStream = new LittleEndianDataOutputStream(countingOutputStream);
    }

    @Override
    public void flush()
            throws IOException
    {
        countingOutputStream.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        countingOutputStream.close();
    }

    @Override
    public void reset()
    {
        throw new UnsupportedOperationException("OutputStream can not be reset");
    }

    @Override
    public int size()
    {
        return Ints.checkedCast(countingOutputStream.getCount());
    }

    @Override
    public int writableBytes()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isWritable()
    {
        return true;
    }

    @Override
    public void writeByte(int value)
    {
        try {
            dataOutputStream.writeByte(value);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public void writeShort(int value)
    {
        try {
            dataOutputStream.writeShort(value);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public void writeInt(int value)
    {
        try {
            dataOutputStream.writeInt(value);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public void writeLong(long value)
    {
        try {
            dataOutputStream.writeLong(value);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public void writeDouble(double value)
    {
        try {
            dataOutputStream.writeDouble(value);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public void writeBytes(Slice source)
    {
        writeBytes(source, 0, source.length());
    }

    @Override
    public void writeBytes(Slice source, int sourceIndex, int length)
    {
        try {
            source.getBytes(sourceIndex, dataOutputStream, length);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public void writeBytes(byte[] source)
    {
        writeBytes(source, 0, source.length);
    }

    @Override
    public void writeBytes(byte[] source, int sourceIndex, int length)
    {
        try {
            dataOutputStream.write(source, sourceIndex, length);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public int writeBytes(InputStream in, int length)
            throws IOException
    {
        int bytesRead = 0;
        byte[] bytes = new byte[4096];
        while (bytesRead < length) {
            int newBytes = in.read(bytes);
            if (newBytes < 0) {
                break;
            }
            dataOutputStream.write(bytes, 0, Math.min(newBytes, length - bytesRead));
            bytesRead += newBytes;
        }
        return bytesRead;
    }

    @Override
    public SliceOutput appendLong(long value)
    {
        writeLong(value);
        return this;
    }

    @Override
    public SliceOutput appendDouble(double value)
    {
        writeDouble(value);
        return this;
    }

    @Override
    public SliceOutput appendInt(int value)
    {
        writeInt(value);
        return this;
    }

    @Override
    public SliceOutput appendShort(int value)
    {
        writeShort(value);
        return this;
    }

    @Override
    public SliceOutput appendBytes(byte[] source, int sourceIndex, int length)
    {
        writeBytes(source, sourceIndex, length);
        return this;
    }

    @Override
    public SliceOutput appendBytes(byte[] source)
    {
        writeBytes(source);
        return this;
    }

    @Override
    public SliceOutput appendBytes(Slice slice)
    {
        writeBytes(slice);
        return this;
    }

    @Override
    public Slice slice()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString(Charset charset)
    {
        return toString();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("countingOutputStream", countingOutputStream)
                .add("dataOutputStream", dataOutputStream)
                .toString();
    }
}
