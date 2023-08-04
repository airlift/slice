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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                assertThat(input.readBoolean()).isEqualTo(valueIndex % 2 == 0);
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
                assertThat(input.readByte()).isEqualTo((byte) valueIndex);
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
                assertThat(input.read()).isEqualTo(valueIndex & 0xFF);
            }

            @Override
            public void verifyReadOffEnd(SliceInput input)
            {
                assertThat(input.read()).isEqualTo(-1);
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
                assertThat(input.readShort()).isEqualTo((short) valueIndex);
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
                assertThat(input.readUnsignedShort()).isEqualTo(valueIndex & 0xFFF);
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
                assertThat(input.readInt()).isEqualTo(valueIndex);
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
                assertThat(input.readUnsignedInt()).isEqualTo(valueIndex);
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
                assertThat(input.readLong()).isEqualTo(valueIndex);
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
                assertThat(input.readFloat()).isEqualTo(valueIndex + 0.12f);
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
                assertThat(input.readDouble()).isEqualTo(valueIndex + 0.12);
            }
        });
    }

    @Test
    public void testReadShorts()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_SHORT * 17)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                short[] shorts = new short[23];
                for (int i = 0; i < shorts.length; i++) {
                    shorts[i] = (short) (i * 37 + valueIndex);
                }
                output.writeShorts(shorts, 3, 17);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                short[] shorts = new short[27];
                input.readShorts(shorts, 5, 17);
                for (int i = 0; i < 17; i++) {
                    assertThat(shorts[i + 5]).isEqualTo((short) ((i + 3) * 37 + valueIndex));
                }
            }
        });
    }

    @Test
    public void testReadInts()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_INT * 17)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                int[] ints = new int[23];
                for (int i = 0; i < ints.length; i++) {
                    ints[i] = i * 37 + valueIndex;
                }
                output.writeInts(ints, 3, 17);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                int[] ints = new int[27];
                input.readInts(ints, 5, 17);
                for (int i = 0; i < 17; i++) {
                    assertThat(ints[i + 5]).isEqualTo((i + 3) * 37 + valueIndex);
                }
            }
        });
    }

    @Test
    public void testReadLongs()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_LONG * 17)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                long[] longs = new long[23];
                for (int i = 0; i < longs.length; i++) {
                    longs[i] = i * 37 + valueIndex;
                }
                output.writeLongs(longs, 3, 17);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                long[] longs = new long[27];
                input.readLongs(longs, 5, 17);
                for (int i = 0; i < 17; i++) {
                    assertThat(longs[i + 5]).isEqualTo(((i + 3) * 37 + valueIndex));
                }
            }
        });
    }

    @Test
    public void testReadFloats()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_FLOAT * 17)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                float[] floats = new float[23];
                for (int i = 0; i < floats.length; i++) {
                    floats[i] = (float) (i * 37 + valueIndex);
                }
                output.writeFloats(floats, 3, 17);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                float[] floats = new float[27];
                input.readFloats(floats, 5, 17);
                for (int i = 0; i < 17; i++) {
                    assertThat(floats[i + 5]).isEqualTo(((i + 3) * 37 + valueIndex));
                }
            }
        });
    }

    @Test
    public void testReadDoubles()
    {
        testSliceInput(new SliceInputTester(SIZE_OF_DOUBLE * 17)
        {
            @Override
            public void loadValue(SliceOutput output, int valueIndex)
            {
                double[] doubles = new double[23];
                for (int i = 0; i < doubles.length; i++) {
                    doubles[i] = i * 37 + valueIndex;
                }
                output.writeDoubles(doubles, 3, 17);
            }

            @Override
            public void verifyValue(SliceInput input, int valueIndex)
            {
                double[] doubles = new double[27];
                input.readDoubles(doubles, 5, 17);
                for (int i = 0; i < 17; i++) {
                    assertThat(doubles[i + 5]).isEqualTo((i + 3) * 37 + valueIndex);
                }
            }
        });
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
                    assertThat(input.skip(valueSize())).isEqualTo(valueSize() - 1);
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
                    assertThat(input.skip(valueSize())).isEqualTo(valueSize() - 1);
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
                    assertThat(input.skip(0)).isZero();
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
                    assertThat(input.skip(0)).isZero();
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
                    assertThat(bytesRead).isGreaterThan(0);
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
                        int size = input.readNBytes(bytes, 5, valueSize());
                        if (size != valueSize()) {
                            throw new IndexOutOfBoundsException();
                        }
                        return new String(bytes, 5, valueSize(), UTF_8);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
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
                        return out.toString(UTF_8);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
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
            assertThat(input.position()).isEqualTo(position);
            tester.verifyValue(input, i);
        }
    }

    protected void testReadForward(SliceInputTester tester, Slice slice)
    {
        SliceInput input = createSliceInput(slice);
        for (int i = 0; i < slice.length() / tester.valueSize(); i++) {
            int position = i * tester.valueSize();
            assertThat(input.position()).isEqualTo(position);
            tester.verifyValue(input, i);
        }
    }

    protected void testReadOffEnd(SliceInputTester tester, Slice slice)
    {
        SliceInput input = createSliceInput(slice);
        try {
            input.skipNBytes(slice.length() - tester.valueSize() + 1);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        tester.verifyReadOffEnd(input);
    }

    private static String getExpectedStringValue(int index, int size)
    {
        String value = String.valueOf(index);
        return Strings.repeat(value, (size / value.length()) + 1).substring(0, size);
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
            assertThatThrownBy(() -> verifyValue(input, 1))
                    .isInstanceOf(IndexOutOfBoundsException.class);
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
            assertThat(actual).isEqualTo(expected);
        }

        protected abstract String readActual(SliceInput input);
    }
}
