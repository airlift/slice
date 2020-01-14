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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.collect.Iterables.cycle;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static java.lang.Double.doubleToLongBits;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractSliceInputTest
{
    protected static final int BUFFER_SIZE = 129;

    private static final List<Integer> VARIABLE_READ_SIZES = ImmutableList.of(
            1,
            7,
            15,
            BUFFER_SIZE - 1,
            BUFFER_SIZE,
            BUFFER_SIZE + 1,
            BUFFER_SIZE + 13);

    protected abstract SliceInput createSliceInput(Slice slice);

    @Test
    public void testReadBoolean()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_BYTE)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                output.writeBoolean(valueIndex % 2 == 0);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                assertEquals(input.readBoolean(), valueIndex % 2 == 0);
            }
        });
    }

    @Test
    public void testReadByte()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_BYTE)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                output.appendByte((byte) valueIndex);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                assertEquals(input.readByte(), (byte) valueIndex);
            }
        });
    }

    @Test
    public void testRead()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_BYTE)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                output.appendByte((byte) valueIndex);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                assertEquals(input.read(), valueIndex & 0xFF);
            }

            @Override
            public void verifyReadOffEnd(SliceInput input)
            {
                assertEquals(input.read(), -1);
            }
        });
    }

    @Test
    public void testReadShort()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_SHORT)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                output.appendShort(valueIndex);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                assertEquals(input.readShort(), (short) valueIndex);
            }
        });
    }

    @Test
    public void testReadUnsignedShort()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_SHORT)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                output.appendShort(valueIndex);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                assertEquals(input.readUnsignedShort(), valueIndex & 0xFFF);
            }
        });
    }

    @Test
    public void testReadInt()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_INT)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                output.appendInt(valueIndex);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                assertEquals(input.readInt(), valueIndex);
            }
        });
    }

    @Test
    public void testUnsignedReadInt()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_INT)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                output.appendInt(valueIndex);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                assertEquals(input.readUnsignedInt(), valueIndex);
            }
        });
    }

    @Test
    public void testReadLong()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_LONG)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                output.appendLong(valueIndex);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                assertEquals(input.readLong(), valueIndex);
            }
        });
    }

    @Test
    public void testReadFloat()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_FLOAT)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                output.writeFloat(valueIndex + 0.12f);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                assertEquals(input.readFloat(), valueIndex + 0.12f);
            }
        });
    }

    @Test
    public void testReadDouble()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_DOUBLE)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                output.appendDouble(valueIndex + 0.12);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                assertEquals(input.readDouble(), valueIndex + 0.12);
            }
        });
    }

    @Test
    public void testReadDoubles()
    {
        testSliceInput(new DoublesSliceInputTester(1));
        testSliceInput(new DoublesSliceInputTester(4));
        testSliceInput(new DoublesSliceInputTester(13));
    }

    @Test
    public void testSkip()
    {
        for (int readSize : VARIABLE_READ_SIZES) {
            // skip without any reads
            testSliceInput(new SkipSliceInputTester(readSize)
            {
                @Override
                public void verifyValue(SliceInput input, int valueIndex)
                {
                    input.skip(valueSize());
                }

                @Override
                public void verifyReadOffEnd(SliceInput input)
                {
                    assertEquals(input.skip(valueSize()), valueSize() - 1);
                }
            });
            testSliceInput(new SkipSliceInputTester(readSize)
            {
                @Override
                public void verifyValue(SliceInput input, int valueIndex)
                {
                    input.skipBytes(valueSize());
                }

                @Override
                public void verifyReadOffEnd(SliceInput input)
                {
                    assertEquals(input.skip(valueSize()), valueSize() - 1);
                }
            });

            // read when no data available to force buffering
            testSliceInput(new SkipSliceInputTester(readSize)
            {
                @Override
                public void verifyValue(SliceInput input, int valueIndex)
                {
                    int length = valueSize();
                    while (length > 0) {
                        if (!input.isReadable()) {
                            input.readByte();
                            length--;
                        }
                        int skipSize = input.skipBytes(length);
                        length -= skipSize;
                    }
                    assertEquals(input.skip(0), 0);
                }
            });
            testSliceInput(new SkipSliceInputTester(readSize)
            {
                @Override
                public void verifyValue(SliceInput input, int valueIndex)
                {
                    long length = valueSize();
                    while (length > 0) {
                        if (!input.isReadable()) {
                            input.readByte();
                            length--;
                        }
                        long skipSize = input.skip(length);
                        length -= skipSize;
                    }
                    assertEquals(input.skip(0), 0);
                }
            });
        }
    }

    @Test
    public void testReadSlice()
    {
        for (int readSize : VARIABLE_READ_SIZES) {
            testSliceInput(new StringSliceInputTester(readSize)
            {
                @Override
                public String readActual(SliceInput input)
                {
                    return input.readSlice(valueSize()).toStringUtf8();
                }
            });
        }
    }

    @Test
    public void testReadBytes()
    {
        for (int readSize : VARIABLE_READ_SIZES) {
            testSliceInput(new StringSliceInputTester(readSize)
            {
                @Override
                public String readActual(SliceInput input)
                {
                    Slice slice = Slices.allocate(valueSize());
                    input.readBytes(slice);
                    return slice.toStringUtf8();
                }
            });
            testSliceInput(new StringSliceInputTester(readSize)
            {
                @Override
                public String readActual(SliceInput input)
                {
                    Slice slice = Slices.allocate(valueSize() + 5);
                    input.readBytes(slice, valueSize());
                    return slice.slice(0, valueSize()).toStringUtf8();
                }
            });
            testSliceInput(new StringSliceInputTester(readSize)
            {
                @Override
                public String readActual(SliceInput input)
                {
                    Slice slice = Slices.allocate(valueSize() + 10);
                    input.readBytes(slice, 5, valueSize());
                    return slice.slice(5, valueSize()).toStringUtf8();
                }
            });
            testSliceInput(new StringSliceInputTester(readSize)
            {
                @Override
                public String readActual(SliceInput input)
                {
                    byte[] bytes = new byte[valueSize()];
                    input.readBytes(bytes, 0, valueSize());
                    return new String(bytes, 0, valueSize(), UTF_8);
                }
            });
            testSliceInput(new StringSliceInputTester(readSize)
            {
                @Override
                public String readActual(SliceInput input)
                {
                    byte[] bytes = new byte[valueSize() + 10];
                    input.readBytes(bytes, 5, valueSize());
                    return new String(bytes, 5, valueSize(), UTF_8);
                }
            });
            testSliceInput(new StringSliceInputTester(readSize)
            {
                @Override
                public String readActual(SliceInput input)
                {
                    byte[] bytes = new byte[valueSize()];
                    int bytesRead = input.read(bytes);
                    if (bytesRead == -1) {
                        throw new IndexOutOfBoundsException();
                    }
                    assertTrue(bytesRead > 0, "Expected to read at least one byte");
                    input.readBytes(bytes, bytesRead, bytes.length - bytesRead);
                    return new String(bytes, 0, valueSize(), UTF_8);
                }
            });
            testSliceInput(new StringSliceInputTester(readSize)
            {
                @Override
                public String readActual(SliceInput input)
                {
                    try {
                        byte[] bytes = new byte[valueSize() + 10];
                        ByteStreams.readFully(input, bytes, 5, valueSize());
                        return new String(bytes, 5, valueSize(), UTF_8);
                    }
                    catch (EOFException e) {
                        throw new IndexOutOfBoundsException();
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            });
            testSliceInput(new StringSliceInputTester(readSize)
            {
                @Override
                public String readActual(SliceInput input)
                {
                    try {
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        input.readBytes(out, valueSize());
                        return new String(out.toByteArray(), UTF_8);
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            });
        }
    }

    protected void testSliceInput(SliceInputTester tester)
    {
        Slice slice = Slices.allocate((BUFFER_SIZE * 3) + 10);
        SliceOutput output = slice.getOutput();
        for (int i = 0; i < slice.length() / tester.valueSize(); i++) {
            tester.loadValue(output, i);
        }
        testReadForward(tester, slice);
        testReadReverse(tester, slice);
        testReadOffEnd(tester, slice);
    }

    protected void testReadReverse(SliceInputTester tester, Slice slice)
    {
        SliceInput input = createSliceInput(slice);
        for (int i = slice.length() / tester.valueSize() - 1; i >= 0; i--) {
            int position = i * tester.valueSize();
            input.setPosition(position);
            assertEquals(input.position(), position);
            tester.verifyValue(input, i);
        }
    }

    protected void testReadForward(SliceInputTester tester, Slice slice)
    {
        SliceInput input = createSliceInput(slice);
        for (int i = 0; i < slice.length() / tester.valueSize(); i++) {
            int position = i * tester.valueSize();
            assertEquals(input.position(), position);
            tester.verifyValue(input, i);
        }
    }

    protected void testReadOffEnd(SliceInputTester tester, Slice slice)
    {
        SliceInput input = createSliceInput(slice);
        try {
            ByteStreams.skipFully(input, slice.length() - tester.valueSize() + 1);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        tester.verifyReadOffEnd(input);
    }

    private static String getExpectedStringValue(int index, int size)
    {
        try {
            return ByteSource.concat(cycle(ByteSource.wrap(String.valueOf(index).getBytes(UTF_8)))).slice(0, size).asCharSource(UTF_8).read();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    protected abstract static class SliceInputTester
    {
        private final int size;

        public SliceInputTester(int size)
        {
            this.size = size;
        }

        public final int valueSize()
        {
            return size;
        }

        public abstract void loadValue(SliceOutput slice, int valueIndex);

        public abstract void verifyValue(SliceInput input, int valueIndex);

        public void verifyReadOffEnd(SliceInput input)
        {
            try {
                verifyValue(input, 1);
                fail("expected IndexOutOfBoundsException");
            }
            catch (IndexOutOfBoundsException expected) {
            }
        }
    }

    private abstract static class SkipSliceInputTester
            extends SliceInputTester
    {
        public SkipSliceInputTester(int size)
        {
            super(size);
        }

        @Override
        public void loadValue(SliceOutput output, int valueIndex)
        {
            output.writeBytes(new byte[valueSize()]);
        }
    }

    private abstract static class StringSliceInputTester
            extends SliceInputTester
    {
        public StringSliceInputTester(int size)
        {
            super(size);
        }

        @Override
        public final void loadValue(SliceOutput output, int valueIndex)
        {
            output.writeBytes(getExpectedStringValue(valueIndex, valueSize()).getBytes(UTF_8));
        }

        @Override
        public final void verifyValue(SliceInput input, int valueIndex)
        {
            String actual = readActual(input);
            String expected = getExpectedStringValue(valueIndex, valueSize());
            assertEquals(actual, expected);
        }

        protected abstract String readActual(SliceInput input);
    }

    private abstract static class ByteArraySliceInputTester
            extends SliceInputTester
    {
        protected final int valueBytes;
        protected final int arrayLength;

        public ByteArraySliceInputTester(int valueBytes, int arrayLength)
        {
            super(valueBytes * arrayLength);
            this.valueBytes = valueBytes;
            this.arrayLength = arrayLength;
        }

        @Override
        public final void loadValue(SliceOutput output, int valueIndex)
        {
            byte[] value = expectedByteArray(valueIndex);
            output.writeBytes(value);
        }

        @Override
        public final void verifyValue(SliceInput input, int valueIndex)
        {
            byte[] actual = readActual(input);
            byte[] expected = expectedByteArray(valueIndex);
            assertEquals(actual, expected);
        }

        protected abstract byte[] readActual(SliceInput input);

        protected abstract byte[] expectedByteArray(int valueIndex);
    }

    private static class DoublesSliceInputTester
            extends ByteArraySliceInputTester
    {
        public DoublesSliceInputTester(int arrayLength)
        {
            super(SIZE_OF_DOUBLE, arrayLength);
        }

        @Override
        protected byte[] readActual(SliceInput input)
        {
            double[] doubles = input.readDoubles(arrayLength);
            ByteBuffer buffer = ByteBuffer.allocate(arrayLength * valueBytes);
            for (int i = 0; i < doubles.length; i++) {
                buffer.put(doubleToBytes(doubles[i]));
            }
            return buffer.array();
        }

        @Override
        protected byte[] expectedByteArray(int valueIndex)
        {
            ByteBuffer buffer = ByteBuffer.allocate(arrayLength * valueBytes);
            for (int i = 0; i < arrayLength; i++) {
                byte[] bytes = doubleToBytes(valueIndex);
                buffer.put(bytes);
            }
            return buffer.array();
        }

        private byte[] doubleToBytes(double value)
        {
            long bits = doubleToLongBits(value);
            return new byte[] {
                    (byte) bits,
                    (byte) (bits >> 8),
                    (byte) (bits >> 16),
                    (byte) (bits >> 24),
                    (byte) (bits >> 32),
                    (byte) (bits >> 40),
                    (byte) (bits >> 48),
                    (byte) (bits >> 56),
            };
        }
    }
}
