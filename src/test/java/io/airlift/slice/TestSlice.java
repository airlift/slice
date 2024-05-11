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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSlice
{
    @Test
    public void testFillAndClear()
    {
        for (byte size = 0; size < 100; size++) {
            Slice slice = allocate(size);
            for (int i = 0; i < slice.length(); i++) {
                assertThat(slice.getByte(i)).isEqualTo((byte) 0);
            }
            slice.fill((byte) 0xA5);
            for (int i = 0; i < slice.length(); i++) {
                assertThat(slice.getByte(i)).isEqualTo((byte) 0xA5);
            }
            slice.clear();
            for (int i = 0; i < slice.length(); i++) {
                assertThat(slice.getByte(i)).isEqualTo((byte) 0);
            }
        }
    }

    @Test
    public void testSlicing()
    {
        Slice slice = Slices.utf8Slice("ala ma kota");

        Slice subSlice = slice.slice(4, slice.length() - 4);
        assertThat(subSlice).isEqualTo(utf8Slice("ma kota"));
        assertThat(subSlice.byteArray()).isEqualTo(slice.byteArray());

        Slice subSubSlice = subSlice.slice(3, subSlice.length() - 3);
        assertThat(subSubSlice).isEqualTo(utf8Slice("kota"));
        assertThat(subSubSlice.byteArray()).isEqualTo(subSlice.byteArray());

        Slice subSubSubSlice = subSubSlice.slice(3, subSubSlice.length() - 3);
        assertThat(subSubSubSlice).isEqualTo(utf8Slice("a"));
        assertThat(subSubSubSlice.byteArray()).isEqualTo(subSubSlice.byteArray());
    }

    @Test
    public void testEqualsHashCodeCompare()
    {
        for (int size = 0; size < 100; size++) {
            // self equals
            Slice slice = allocate(size);
            assertSlicesEquals(slice, slice);

            // equals other all zero
            Slice other = allocate(size);
            assertSlicesEquals(slice, other);

            // equals self fill pattern
            slice = allocate(size); // create a new slice since slices cache the hash code value
            slice.fill((byte) 0xA5);
            assertSlicesEquals(slice, slice);

            // equals other fill pattern
            other = allocate(size); // create a new slice since slices cache the hash code value
            other.fill((byte) 0xA5);
            assertSlicesEquals(slice, other);

            // different types
            assertThat(slice).isNotEqualTo(new Object());
            assertThat(new Object()).isNotEqualTo(slice);

            // different sizes
            Slice oneBigger = allocate(size + 1);
            oneBigger.fill((byte) 0xA5);
            assertThat(slice).isNotEqualTo(oneBigger);
            assertThat(oneBigger).isNotEqualTo(slice);
            assertThat(slice.compareTo(oneBigger) < 0).isTrue();
            assertThat(oneBigger.compareTo(slice) > 0).isTrue();
            assertThat(slice.equals(0, size, oneBigger, 0, size + 1)).isFalse();
            assertThat(oneBigger.equals(0, size + 1, slice, 0, size)).isFalse();
            assertThat(slice.compareTo(0, size, oneBigger, 0, size + 1) < 0).isTrue();
            assertThat(oneBigger.compareTo(0, size + 1, slice, 0, size) > 0).isTrue();

            // different in one byte
            for (int i = 1; i < slice.length(); i++) {
                slice.setByte(i - 1, 0xA5);
                assertThat(slice.equals(i - 1, size - i, other, i - 1, size - i)).isTrue();
                slice.setByte(i, 0xFF);
                assertThat(slice).isNotEqualTo(other);
                assertThat(slice.equals(i, size - i, other, i, size - i)).isFalse();
                assertThat(slice.compareTo(0, size, oneBigger, 0, size + 1) > 0).isTrue();
            }

            // compare with empty slice
            if (slice.length() > 0) {
                testCompareWithEmpty(size, slice);
            }
        }
    }

    private static void testCompareWithEmpty(int size, Slice slice)
    {
        assertThat(slice).isNotEqualTo(EMPTY_SLICE);
        assertThat(EMPTY_SLICE).isNotEqualTo(slice);

        assertThat(slice.equals(0, size, EMPTY_SLICE, 0, 0)).isFalse();
        assertThat(EMPTY_SLICE.equals(0, 0, slice, 0, size)).isFalse();

        assertThat(slice.compareTo(0, size, EMPTY_SLICE, 0, 0) > 0).isTrue();
        assertThat(EMPTY_SLICE.compareTo(0, 0, slice, 0, size) < 0).isTrue();

        assertThatThrownBy(() -> slice.equals(0, size, EMPTY_SLICE, 0, size))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> EMPTY_SLICE.equals(0, size, slice, 0, size))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> slice.compareTo(0, size, EMPTY_SLICE, 0, size))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> EMPTY_SLICE.compareTo(0, size, slice, 0, size))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testBackingByteArray()
    {
        byte[] bytes = new byte[Byte.MAX_VALUE];
        for (int i = 0; i < bytes.length; i++) {
            Slice slice = Slices.wrappedBuffer(bytes, i, bytes.length - i);
            assertThat(slice.byteArrayOffset()).isEqualTo(i);
            assertThat(slice.byteArray()).isSameAs(bytes);
            bytes[i] = (byte) i;
            assertThat(slice.getByte(0)).isEqualTo((byte) i);
        }
    }

    private static void assertSlicesEquals(Slice slice, Slice other)
    {
        int size = slice.length();

        assertThat(slice).isEqualTo(other);
        assertThat(slice.equals(0, size, other, 0, size)).isTrue();
        assertThat(slice.hashCode()).isEqualTo(other.hashCode());
        assertThat(slice.hashCode()).isEqualTo(other.hashCode(0, size));
        assertThat(slice.compareTo(other)).isEqualTo(0);
        assertThat(slice.compareTo(0, size, other, 0, size)).isEqualTo(0);
        for (int i = 0; i < slice.length(); i++) {
            assertThat(slice.equals(i, size - i, other, i, size - i)).isTrue();
            assertThat(slice.hashCode(i, size - i)).isEqualTo(other.hashCode(i, size - i));
            assertThat(slice.compareTo(i, size - i, other, i, size - i)).isEqualTo(0);
        }
        for (int i = 0; i < slice.length(); i++) {
            assertThat(slice.equals(0, size - i, other, 0, size - i)).isTrue();
            assertThat(slice.hashCode(0, size - i)).isEqualTo(other.hashCode(0, size - i));
            assertThat(slice.compareTo(0, size - i, other, 0, size - i)).isEqualTo(0);
        }
    }

    @Test
    public void testToString()
    {
        assertThat(Slices.copiedBuffer("apple", UTF_8).toString(UTF_8)).isEqualTo("apple");

        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < size; index++) {
                assertToStrings(allocate(size), index);
            }
        }
    }

    @Test
    public void testUtf8Conversion()
    {
        String s = "apple \u2603 snowman";
        Slice slice = Slices.copiedBuffer(s, UTF_8);

        assertThat(utf8Slice(s)).isEqualTo(slice);
        assertThat(slice.toStringUtf8()).isEqualTo(s);
        assertThat(utf8Slice(s).toStringUtf8()).isEqualTo(s);
    }

    @SuppressWarnings("CharUsedInArithmeticContext")
    private static void assertToStrings(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        // set and get the value
        char[] chars = new char[(slice.length() - index) / 2];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) ('a' + (i % 26));
        }
        String string = new String(chars);
        Slice value = Slices.copiedBuffer(string, UTF_8);
        slice.setBytes(index, value);
        assertThat(slice.toString(index, value.length(), UTF_8)).isEqualTo(string);

        for (int length = 0; length < value.length(); length++) {
            slice.fill((byte) 0xFF);
            slice.setBytes(index, value, 0, length);
            assertThat(slice.toString(index, length, UTF_8)).isEqualTo(string.substring(0, length));
        }
    }

    @Test
    public void testByte()
    {
        for (byte size = 0; size < 100; size++) {
            for (byte index = 0; index < (size - SIZE_OF_BYTE); index++) {
                assertByte(allocate(size), index);
            }
        }
    }

    private static void assertByte(Slice slice, byte index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        // set and get unsigned value
        slice.setByte(index, 0xA5);
        assertThat(slice.getUnsignedByte(index)).isEqualTo((short) 0x0000_00A5);

        // set and get the value
        slice.setByte(index, 0xA5);
        assertThat(slice.getByte(index)).isEqualTo((byte) 0xA5);

        assertThatThrownBy(() -> slice.getByte(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> slice.getByte((slice.length() - SIZE_OF_BYTE) + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> slice.getByte(slice.length()))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> slice.getByte(slice.length() + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testShort()
    {
        for (short size = 0; size < 100; size++) {
            for (short index = 0; index < (size - SIZE_OF_SHORT); index++) {
                assertShort(allocate(size), index);
            }
        }
    }

    private static void assertShort(Slice slice, short index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        // set and get the value
        slice.setShort(index, 0xAA55);
        assertThat(slice.getShort(index)).isEqualTo((short) 0xAA55);

        assertThatThrownBy(() -> slice.getShort(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getShort((slice.length() - SIZE_OF_SHORT) + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getShort(slice.length()))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getShort(slice.length() + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testInt()
    {
        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < (size - SIZE_OF_INT); index++) {
                assertInt(allocate(size), index);
            }
        }
    }

    @Test
    public void testMismatch()
    {
        Slice first = Slices.utf8Slice("ala ma kota");
        Slice second = Slices.utf8Slice("ala ma psa");

        int mismatch = first.mismatch(second);
        assertThat(mismatch).isEqualTo(7);

        assertThat(first.toStringUtf8().substring(0, mismatch))
                .isEqualTo(second.toStringUtf8().substring(0, mismatch));

        assertThat(first.toStringUtf8().substring(0, mismatch + 1))
                .isNotEqualTo(second.toStringUtf8().substring(0, mismatch + 1));

        // Different slices
        assertThat(first.mismatch(utf8Slice("pies i kot")))
                .isEqualTo(0);

        assertThat(first.mismatch(0, first.length(), second, 5, second.length() - 5))
                .isEqualTo(1);

        assertThat(first.mismatch(first))
                .isEqualTo(-1);

        assertThat(second.mismatch(second))
                .isEqualTo(-1);

        assertThat(first.slice(0, mismatch).mismatch(second.slice(0, mismatch)))
                .isEqualTo(-1);

        assertThat(first.mismatch(0, mismatch, second, 0, mismatch))
                .isEqualTo(-1);
    }

    private static void assertInt(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        // set and get the value
        slice.setInt(index, 0xAAAA_5555);
        assertThat(slice.getInt(index)).isEqualTo(0xAAAA_5555);

        assertThatThrownBy(() -> slice.getInt(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getInt((slice.length() - SIZE_OF_INT) + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getInt(slice.length()))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getInt(slice.length() + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testLong()
    {
        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < (size - SIZE_OF_LONG); index++) {
                assertLong(allocate(size), index);
            }
        }
    }

    private static void assertLong(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        // set and get the value
        slice.setLong(index, 0xAAAA_AAAA_5555_5555L);
        assertThat(slice.getLong(index)).isEqualTo(0xAAAA_AAAA_5555_5555L);

        assertThatThrownBy(() -> slice.getLong(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getLong((slice.length() - SIZE_OF_LONG) + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getLong(slice.length()))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getLong(slice.length() + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testFloat()
    {
        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < (size - SIZE_OF_FLOAT); index++) {
                assertFloat(allocate(size), index);
            }
        }
    }

    private static void assertFloat(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        // set and get the value
        slice.setFloat(index, intBitsToFloat(0xAAAA_5555));
        assertThat(floatToIntBits(slice.getFloat(index))).isEqualTo(0xAAAA_5555);

        assertThatThrownBy(() -> slice.getFloat(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getFloat((slice.length() - SIZE_OF_FLOAT) + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getFloat(slice.length()))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getFloat(slice.length() + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testDouble()
    {
        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < (size - SIZE_OF_DOUBLE); index++) {
                assertDouble(allocate(size), index);
            }
        }
    }

    private static void assertDouble(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        // set and get the value
        slice.setDouble(index, longBitsToDouble(0xAAAA_AAAA_5555_5555L));
        assertThat(doubleToLongBits(slice.getDouble(index))).isEqualTo(0xAAAA_AAAA_5555_5555L);

        assertThatThrownBy(() -> slice.getDouble(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getDouble((slice.length() - SIZE_OF_DOUBLE) + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getDouble(slice.length()))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getDouble(slice.length() + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testBytesArray()
    {
        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < size; index++) {
                assertBytesArray(allocate(size), index);
            }
        }
    }

    private static void assertBytesArray(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        byte[] value = new byte[slice.length()];
        Arrays.fill(value, (byte) 0xFF);
        assertThat(slice.getBytes()).isEqualTo(value);

        // set and get the value
        value = new byte[(slice.length() - index) / 2];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) i;
        }
        slice.setBytes(index, value);
        assertThat(slice.getBytes(index, value.length)).isEqualTo(value);

        for (int length = 0; length < value.length; length++) {
            slice.fill((byte) 0xFF);
            slice.setBytes(index, value, 0, length);
            assertThat(slice.getBytes(index, length)).isEqualTo(Arrays.copyOf(value, length));
        }

        assertThatThrownBy(() -> slice.setBytes(slice.length() - 1, new byte[10]))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.setBytes(slice.length() - 1, new byte[20], 1, 10))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testShortsArray()
    {
        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < size; index++) {
                assertShortsArray(allocate(size), index);
            }
        }
    }

    private static void assertShortsArray(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        short[] value = new short[(slice.length() - index) / 2];
        slice.getShorts(index, value);
        for (short v : value) {
            assertThat(v).isEqualTo((short) -1);
        }
        Arrays.fill(value, (short) -1);
        assertThat(slice.getShorts(index, value.length)).isEqualTo(value);

        // set and get the value
        value = new short[value.length / 2];
        for (int i = 0; i < value.length; i++) {
            value[i] = (short) i;
        }
        slice.setShorts(index, value);
        assertThat(slice.getShorts(index, value.length)).isEqualTo(value);

        for (int length = 0; length < value.length; length++) {
            slice.fill((byte) 0xFF);
            slice.setShorts(index, value, 0, length);
            assertThat(slice.getShorts(index, length)).isEqualTo(Arrays.copyOf(value, length));
        }

        assertThatThrownBy(() -> slice.setShorts(slice.length() - 1, new short[10]))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.setShorts(slice.length() - 1, new short[20], 1, 10))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testIntsArray()
    {
        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < size; index++) {
                assertIntsArray(allocate(size), index);
            }
        }
    }

    private static void assertIntsArray(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        int[] value = new int[(slice.length() - index) / 4];
        slice.getInts(index, value);
        for (int v : value) {
            assertThat(v).isEqualTo(-1);
        }
        Arrays.fill(value, -1);
        assertThat(slice.getInts(index, value.length)).isEqualTo(value);

        // set and get the value
        value = new int[value.length / 2];
        for (int i = 0; i < value.length; i++) {
            value[i] = i;
        }
        slice.setInts(index, value);
        assertThat(slice.getInts(index, value.length)).isEqualTo(value);

        for (int length = 0; length < value.length; length++) {
            slice.fill((byte) 0xFF);
            slice.setInts(index, value, 0, length);
            assertThat(slice.getInts(index, length)).isEqualTo(Arrays.copyOf(value, length));
        }

        assertThatThrownBy(() -> slice.setInts(slice.length() - 1, new int[10]))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.setInts(slice.length() - 1, new int[20], 1, 10))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testLongsArray()
    {
        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < size; index++) {
                assertLongsArray(allocate(size), index);
            }
        }
    }

    private static void assertLongsArray(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        long[] value = new long[(slice.length() - index) / 8];
        slice.getLongs(index, value);
        for (long v : value) {
            assertThat(v).isEqualTo(-1);
        }
        Arrays.fill(value, -1L);
        assertThat(slice.getLongs(index, value.length)).isEqualTo(value);

        // set and get the value
        value = new long[value.length / 2];
        for (int i = 0; i < value.length; i++) {
            value[i] = i;
        }
        slice.setLongs(index, value);
        assertThat(slice.getLongs(index, value.length)).isEqualTo(value);

        for (int length = 0; length < value.length; length++) {
            slice.fill((byte) 0xFF);
            slice.setLongs(index, value, 0, length);
            assertThat(slice.getLongs(index, length)).isEqualTo(Arrays.copyOf(value, length));
        }

        assertThatThrownBy(() -> slice.setLongs(slice.length() - 1, new long[10]))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.setLongs(slice.length() - 1, new long[20], 1, 10))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testFloatsArray()
    {
        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < size; index++) {
                assertFloatsArray(allocate(size), index);
            }
        }
    }

    private static void assertFloatsArray(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        float[] value = new float[(slice.length() - index) / 4];
        slice.getFloats(index, value);
        for (float v : value) {
            assertThat(v).isNaN();
        }
        Arrays.fill(value, intBitsToFloat(-1));
        assertThat(slice.getFloats(index, value.length)).isEqualTo(value);

        // set and get the value
        value = new float[value.length / 2];
        for (int i = 0; i < value.length; i++) {
            value[i] = i;
        }
        slice.setFloats(index, value);
        assertThat(slice.getFloats(index, value.length)).isEqualTo(value);

        for (int length = 0; length < value.length; length++) {
            slice.fill((byte) 0xFF);
            slice.setFloats(index, value, 0, length);
            assertThat(slice.getFloats(index, length)).isEqualTo(Arrays.copyOf(value, length));
        }

        assertThatThrownBy(() -> slice.setFloats(slice.length() - 1, new float[10]))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.setFloats(slice.length() - 1, new float[20], 1, 10))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testDoublesArray()
    {
        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < size; index++) {
                assertDoublesArray(allocate(size), index);
            }
        }
    }

    private static void assertDoublesArray(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        double[] value = new double[(slice.length() - index) / 8];
        slice.getDoubles(index, value);
        for (double v : value) {
            assertThat(v).isNaN();
        }
        Arrays.fill(value, longBitsToDouble(-1));
        assertThat(slice.getDoubles(index, value.length)).isEqualTo(value);

        // set and get the value
        value = new double[value.length / 2];
        for (int i = 0; i < value.length; i++) {
            value[i] = i;
        }
        slice.setDoubles(index, value);
        assertThat(slice.getDoubles(index, value.length)).isEqualTo(value);

        for (int length = 0; length < value.length; length++) {
            slice.fill((byte) 0xFF);
            slice.setDoubles(index, value, 0, length);
            assertThat(slice.getDoubles(index, length)).isEqualTo(Arrays.copyOf(value, length));
        }

        assertThatThrownBy(() -> slice.setDoubles(slice.length() - 1, new double[10]))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.setDoubles(slice.length() - 1, new double[20], 1, 10))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testBytesSlice()
    {
        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < size; index++) {
                assertBytesSlice(allocate(size), index);
            }
        }
    }

    private void assertBytesSlice(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        // compare to self slice
        assertThat(slice.slice(0, slice.length())).isEqualTo(slice);
        Slice value = allocate(slice.length());
        slice.getBytes(0, value, 0, slice.length());
        assertThat(value).isEqualTo(slice);

        // set and get the value
        value = allocate((slice.length() - index) / 2);
        for (int i = 0; i < value.length(); i++) {
            value.setByte(i, i);
        }

        // check by slicing out the region
        slice.setBytes(index, value);
        assertThat(value).isEqualTo(slice.slice(index, value.length()));

        // check by getting out the region
        Slice tempValue = allocate(value.length());
        slice.getBytes(index, tempValue, 0, tempValue.length());
        assertThat(tempValue).isEqualTo(slice.slice(index, tempValue.length()));
        assertThat(tempValue.equals(0, tempValue.length(), slice, index, tempValue.length())).isTrue();

        for (int length = 0; length < value.length(); length++) {
            slice.fill((byte) 0xFF);
            slice.setBytes(index, value, 0, length);

            // check by slicing out the region
            assertThat(value.slice(0, length)).isEqualTo(slice.slice(index, length));
            assertThat(value.equals(0, length, slice, index, length)).isTrue();

            // check by getting out the region
            tempValue = allocate(length);
            slice.getBytes(index, tempValue);
            assertThat(tempValue).isEqualTo(slice.slice(index, length));
            assertThat(tempValue.equals(0, length, slice, index, length)).isTrue();
        }

        assertThatThrownBy(() -> slice.getBytes(slice.length() - 1, 10))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getBytes(slice.length() - 1, new byte[20]))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> slice.getBytes(slice.length() - 1, new byte[20], 1, 10))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testBytesStreams()
            throws Exception
    {
        for (int size = 0; size < 100; size++) {
            for (int index = 0; index < size; index++) {
                assertBytesStreams(allocate(size), index);
            }
        }
        assertBytesStreams(allocate(16 * 1024), 3);
    }

    private static void assertBytesStreams(Slice slice, int index)
            throws Exception
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        byte[] value = new byte[slice.length()];
        Arrays.fill(value, (byte) 0xFF);
        assertThat(slice.getBytes()).isEqualTo(value);

        // set and get the value
        value = new byte[(slice.length() - index) / 2];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) i;
        }
        slice.setBytes(index, new ByteArrayInputStream(value), value.length);
        assertThat(slice.getBytes(index, value.length)).isEqualTo(value);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        slice.getBytes(index, out, value.length);
        assertThat(slice.getBytes(index, value.length)).isEqualTo(out.toByteArray());

        for (int length = 0; length < value.length; length++) {
            slice.fill((byte) 0xFF);
            slice.setBytes(index, new ByteArrayInputStream(value), length);
            assertThat(slice.getBytes(index, length)).isEqualTo(Arrays.copyOf(value, length));

            out = new ByteArrayOutputStream();
            slice.getBytes(index, out, length);
            assertThat(slice.getBytes(index, length)).isEqualTo(out.toByteArray());
        }
    }

    @Test
    public void testRetainedSize()
            throws Exception
    {
        MemorySegment heapAllocatedSegment = MemorySegment.ofArray(new byte[0]);
        int sliceInstanceSize = instanceSize(Slice.class) + instanceSize(heapAllocatedSegment.getClass());
        Slice slice = Slices.allocate(10);
        assertThat(slice.getRetainedSize()).isEqualTo(sizeOfByteArray(10) + sliceInstanceSize);
        assertThat(slice.length()).isEqualTo(10);
        Slice subSlice = slice.slice(0, 1);
        assertThat(subSlice.getRetainedSize()).isEqualTo(sizeOfByteArray(10) + sliceInstanceSize);
        assertThat(subSlice.length()).isEqualTo(1);
    }

    @Test
    public void testCopyOf()
            throws Exception
    {
        // slightly stronger guarantees for empty slice
        assertThat(EMPTY_SLICE.copy()).isSameAs(EMPTY_SLICE);
        assertThat(utf8Slice("hello world").copy(1, 0)).isSameAs(EMPTY_SLICE);

        Slice slice = utf8Slice("hello world");
        assertThat(slice.copy()).isEqualTo(slice);
        assertThat(slice.copy(1, 3)).isEqualTo(slice.slice(1, 3));

        // verify it's an actual copy
        Slice original = utf8Slice("hello world");
        Slice copy = original.copy();

        original.fill((byte) 0);
        assertThat(copy).isEqualTo(utf8Slice("hello world"));

        // read before beginning
        assertThatThrownBy(() -> slice.copy(-1, slice.length()))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // read after end
        assertThatThrownBy(() -> slice.copy(slice.length() + 1, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // start before but extend past end
        assertThatThrownBy(() -> slice.copy(1, slice.length()))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testIndexOf()
    {
        assertIndexOf(utf8Slice("no-match-bigger"), utf8Slice("test"));
        assertIndexOf(utf8Slice("no"), utf8Slice("test"));

        assertIndexOf(utf8Slice("test"), utf8Slice("test"));
        assertIndexOf(utf8Slice("test-start"), utf8Slice("test"));
        assertIndexOf(utf8Slice("end-test"), utf8Slice("test"));
        assertIndexOf(utf8Slice("a-test-middle"), utf8Slice("test"));
        assertIndexOf(utf8Slice("this-test-is-a-test"), utf8Slice("test"));

        assertIndexOf(utf8Slice("test"), EMPTY_SLICE, 0, 0);
        assertIndexOf(EMPTY_SLICE, utf8Slice("test"), 0, -1);

        assertIndexOf(utf8Slice("test"), utf8Slice("no"), 4, -1);
        assertIndexOf(utf8Slice("test"), utf8Slice("no"), 5, -1);
        assertIndexOf(utf8Slice("test"), utf8Slice("no"), -1, -1);
    }

    public static void assertIndexOf(Slice data, Slice pattern, int offset, int expected)
    {
        assertThat(data.indexOf(pattern, offset)).isEqualTo(expected);
        assertThat(data.indexOfBruteForce(pattern, offset)).isEqualTo(expected);
    }

    public static void assertIndexOf(Slice data, Slice pattern)
    {
        int index;

        List<Integer> bruteForce = new ArrayList<>();
        index = 0;
        while (index >= 0 && index < data.length()) {
            index = data.indexOfBruteForce(pattern, index);
            if (index >= 0) {
                bruteForce.add(index);
                index++;
            }
        }

        List<Integer> indexOf = new ArrayList<>();
        index = 0;
        while (index >= 0 && index < data.length()) {
            index = data.indexOf(pattern, index);
            if (index >= 0) {
                indexOf.add(index);
                index++;
            }
        }

        assertThat(bruteForce).isEqualTo(indexOf);
    }

    @Test
    public void testIndexOfByte()
    {
        Slice slice = utf8Slice("apple");

        assertThat(slice.indexOfByte((byte) 'a')).isEqualTo(0);
        assertThat(slice.indexOfByte((byte) 'p')).isEqualTo(1);
        assertThat(slice.indexOfByte((byte) 'e')).isEqualTo(4);
        assertThat(slice.indexOfByte((byte) 'x')).isEqualTo(-1);

        assertThat(slice.indexOfByte('a')).isEqualTo(0);
        assertThat(slice.indexOfByte('p')).isEqualTo(1);
        assertThat(slice.indexOfByte('e')).isEqualTo(4);
        assertThat(slice.indexOfByte('x')).isEqualTo(-1);

        assertThatThrownBy(() -> slice.indexOfByte(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> slice.indexOfByte(-123)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> slice.indexOfByte(256)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> slice.indexOfByte(500)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testToByteBuffer()
    {
        byte[] original = "hello world".getBytes(UTF_8);

        Slice slice = allocate(original.length);
        slice.setBytes(0, original);
        assertThat(slice.getBytes()).isEqualTo(original);

        assertThat(getBytes(slice.toByteBuffer())).isEqualTo(original);

        assertToByteBuffer(slice, original);
    }

    @Test
    public void testToByteBufferEmpty()
    {
        ByteBuffer buffer = allocate(0).toByteBuffer();
        assertThat(buffer.position()).isEqualTo(0);
        assertThat(buffer.remaining()).isEqualTo(0);
    }

    private static void assertToByteBuffer(Slice slice, byte[] original)
    {
        for (int index = 0; index < original.length; index++) {
            for (int length = 0; length < (original.length - index); length++) {
                byte[] actual = getBytes(slice.toByteBuffer(index, length));
                byte[] expected = Arrays.copyOfRange(original, index, index + length);
                assertThat(actual).isEqualTo(expected);
            }
            byte[] actual = getBytes(slice.toByteBuffer(index));
            byte[] expected = Arrays.copyOfRange(original, index, original.length);
            assertThat(actual).isEqualTo(expected);
        }
    }

    private static byte[] getBytes(ByteBuffer buffer)
    {
        assertThat(buffer.position()).isEqualTo(0);
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return data;
    }

    protected Slice allocate(int size)
    {
        return Slices.allocate(size);
    }
}
