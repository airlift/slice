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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;

import static io.airlift.slice.Preconditions.checkNotNull;

public final class InputStreamSliceInput
        extends SliceInput
{
    private final PushbackInputStream pushbackInputStream;
    private final CountingInputStream countingInputStream;
    private final LittleEndianDataInputStream dataInputStream;

    @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
    public InputStreamSliceInput(InputStream inputStream)
    {
        checkNotNull(inputStream, "inputStream is null");
        pushbackInputStream = new PushbackInputStream(inputStream);
        countingInputStream = new CountingInputStream(pushbackInputStream);
        dataInputStream = new LittleEndianDataInputStream(countingInputStream);
    }

    @Override
    public int position()
    {
        return (int) countingInputStream.getCount();
    }

    @Override
    public void setPosition(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadable()
    {
        try {
            int value = pushbackInputStream.read();
            if (value == -1) {
                return false;
            }
            pushbackInputStream.unread(value);
            return true;
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public int skipBytes(int n)
    {
        try {
            return dataInputStream.skipBytes(n);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public float readFloat()
    {
        try {
            return dataInputStream.readFloat();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public double readDouble()
    {
        try {
            return dataInputStream.readDouble();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public int readUnsignedByte()
    {
        try {
            return dataInputStream.readUnsignedByte();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public int readUnsignedShort()
    {
        try {
            return dataInputStream.readUnsignedShort();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public int readInt()
    {
        try {
            return dataInputStream.readInt();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public long readLong()
    {
        try {
            return dataInputStream.readLong();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public short readShort()
    {
        try {
            return dataInputStream.readShort();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public byte readByte()
    {
        try {
            return dataInputStream.readByte();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public boolean readBoolean()
    {
        try {
            return dataInputStream.readBoolean();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public int read()
    {
        try {
            return dataInputStream.read();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public int read(byte[] b)
    {
        try {
            return dataInputStream.read(b);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len)
    {
        try {
            return dataInputStream.read(b, off, len);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public long skip(long n)
    {
        try {
            return dataInputStream.skip(n);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public int available()
    {
        try {
            return countingInputStream.available();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public void close()
    {
        try {
            dataInputStream.close();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        try {
            dataInputStream.read(destination, destinationIndex, length);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public Slice readSlice(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        try {
            Slice newSlice = Slices.allocate(length);
            newSlice.setBytes(0, countingInputStream, length);
            return newSlice;
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        try {
            destination.setBytes(destinationIndex, countingInputStream, length);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        SliceStreamUtils.copyStream(this, out, length);
    }
}
