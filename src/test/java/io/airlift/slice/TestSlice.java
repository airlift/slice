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

import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSlice
{
    @Test
    public void testFillAndClear()
    {
        for (byte size = 0; size < 100; size++) {
            Slice slice = allocate(size);
            for (int i = 0; i < slice.length(); i++) {
                assertEquals(slice.getByte(i), (byte) 0);
            }
            slice.fill((byte) 0xA5);
            for (int i = 0; i < slice.length(); i++) {
                assertEquals(slice.getByte(i), (byte) 0xA5);
            }
            slice.clear();
            for (int i = 0; i < slice.length(); i++) {
                assertEquals(slice.getByte(i), (byte) 0);
            }
        }
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
            assertNotEquals(slice, new Object());
            //noinspection MisorderedAssertEqualsArgumentsTestNG
            assertNotEquals(new Object(), slice);

            // different sizes
            Slice oneBigger = allocate(size + 1);
            oneBigger.fill((byte) 0xA5);
            assertNotEquals(slice, oneBigger);
            assertNotEquals(oneBigger, slice);
            assertTrue(slice.compareTo(oneBigger) < 0);
            assertTrue(oneBigger.compareTo(slice) > 0);
            assertFalse(slice.equals(0, size, oneBigger, 0, size + 1));
            assertFalse(oneBigger.equals(0, size + 1, slice, 0, size));
            assertTrue(slice.compareTo(0, size, oneBigger, 0, size + 1) < 0);
            assertTrue(oneBigger.compareTo(0, size + 1, slice, 0, size) > 0);

            // different in one byte
            for (int i = 1; i < slice.length(); i++) {
                slice.setByte(i - 1, 0xA5);
                assertTrue(slice.equals(i - 1, size - i, other, i - 1, size - i));
                slice.setByte(i, 0xFF);
                assertNotEquals(slice, other);
                assertFalse(slice.equals(i, size - i, other, i, size - i));
                assertTrue(slice.compareTo(0, size, oneBigger, 0, size + 1) > 0);
            }

            // compare with empty slice
            if (slice.length() > 0) {
                assertNotEquals(slice, EMPTY_SLICE);
                //noinspection MisorderedAssertEqualsArgumentsTestNG
                assertNotEquals(EMPTY_SLICE, slice);

                assertFalse(slice.equals(0, size, EMPTY_SLICE, 0, 0));
                assertFalse(EMPTY_SLICE.equals(0, 0, slice, 0, size));

                assertTrue(slice.compareTo(0, size, EMPTY_SLICE, 0, 0) > 0);
                assertTrue(EMPTY_SLICE.compareTo(0, 0, slice, 0, size) < 0);

                try {
                    //noinspection ResultOfMethodCallIgnored
                    slice.equals(0, size, EMPTY_SLICE, 0, size);
                    fail("expected IndexOutOfBoundsException");
                }
                catch (IndexOutOfBoundsException expected) {
                }
                try {
                    //noinspection ResultOfMethodCallIgnored
                    EMPTY_SLICE.equals(0, size, slice, 0, size);
                    fail("expected IndexOutOfBoundsException");
                }
                catch (IndexOutOfBoundsException expected) {
                }

                try {
                    slice.compareTo(0, size, EMPTY_SLICE, 0, size);
                    fail("expected IndexOutOfBoundsException");
                }
                catch (IndexOutOfBoundsException expected) {
                }
                try {
                    EMPTY_SLICE.compareTo(0, size, slice, 0, size);
                    fail("expected IndexOutOfBoundsException");
                }
                catch (IndexOutOfBoundsException expected) {
                }
            }
        }
    }

    private static void assertSlicesEquals(Slice slice, Slice other)
    {
        int size = slice.length();

        assertEquals(slice, other);
        assertTrue(slice.equals(0, size, other, 0, size));
        assertEquals(slice.hashCode(), other.hashCode());
        assertEquals(slice.hashCode(), other.hashCode(0, size));
        assertEquals(slice.compareTo(other), 0);
        assertEquals(slice.compareTo(0, size, other, 0, size), 0);
        for (int i = 0; i < slice.length(); i++) {
            assertTrue(slice.equals(i, size - i, other, i, size - i));
            assertEquals(slice.hashCode(i, size - i), other.hashCode(i, size - i));
            assertEquals(slice.compareTo(i, size - i, other, i, size - i), 0);
        }
        for (int i = 0; i < slice.length(); i++) {
            assertTrue(slice.equals(0, size - i, other, 0, size - i));
            assertEquals(slice.hashCode(0, size - i), other.hashCode(0, size - i));
            assertEquals(slice.compareTo(0, size - i, other, 0, size - i), 0);
        }
    }

    @Test
    public void testToString()
    {
        assertEquals(Slices.copiedBuffer("apple", UTF_8).toString(UTF_8), "apple");

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

        assertEquals(utf8Slice(s), slice);
        assertEquals(slice.toStringUtf8(), s);
        assertEquals(utf8Slice(s).toStringUtf8(), s);
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
        assertEquals(slice.toString(index, value.length(), UTF_8), string);

        for (int length = 0; length < value.length(); length++) {
            slice.fill((byte) 0xFF);
            slice.setBytes(index, value, 0, length);
            assertEquals(slice.toString(index, length, UTF_8), string.substring(0, length));
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
        assertEquals(slice.getUnsignedByte(index), 0x0000_00A5);

        // set and get the value
        slice.setByte(index, 0xA5);
        assertEquals(slice.getByte(index), (byte) 0xA5);

        try {
            slice.getByte(-1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getByte((slice.length() - SIZE_OF_BYTE) + 1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getByte(slice.length());
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getByte(slice.length() + 1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }
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
        assertEquals(slice.getShort(index), (short) 0xAA55);

        try {
            slice.getShort(-1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getShort((slice.length() - SIZE_OF_SHORT) + 1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getShort(slice.length());
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getShort(slice.length() + 1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }
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

    private static void assertInt(Slice slice, int index)
    {
        // fill slice with FF
        slice.fill((byte) 0xFF);

        // set and get the value
        slice.setInt(index, 0xAAAA_5555);
        assertEquals(slice.getInt(index), 0xAAAA_5555);

        try {
            slice.getInt(-1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getInt((slice.length() - SIZE_OF_INT) + 1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getInt(slice.length());
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getInt(slice.length() + 1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }
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
        assertEquals(slice.getLong(index), 0xAAAA_AAAA_5555_5555L);

        try {
            slice.getLong(-1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getLong((slice.length() - SIZE_OF_LONG) + 1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getLong(slice.length());
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getLong(slice.length() + 1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }
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
        assertEquals(floatToIntBits(slice.getFloat(index)), 0xAAAA_5555);

        try {
            slice.getFloat(-1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getFloat((slice.length() - SIZE_OF_FLOAT) + 1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getFloat(slice.length());
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getFloat(slice.length() + 1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }
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
        assertEquals(doubleToLongBits(slice.getDouble(index)), 0xAAAA_AAAA_5555_5555L);

        try {
            slice.getDouble(-1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getDouble((slice.length() - SIZE_OF_DOUBLE) + 1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getDouble(slice.length());
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }

        try {
            slice.getDouble(slice.length() + 1);
            fail("expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) {
        }
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
        assertEquals(slice.getBytes(), value);

        // set and get the value
        value = new byte[(slice.length() - index) / 2];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) i;
        }
        slice.setBytes(index, value);
        assertEquals(slice.getBytes(index, value.length), value);

        for (int length = 0; length < value.length; length++) {
            slice.fill((byte) 0xFF);
            slice.setBytes(index, value, 0, length);
            assertEquals(slice.getBytes(index, length), Arrays.copyOf(value, length));
        }
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
        assertEquals(slice.slice(0, slice.length()), slice);
        Slice value = allocate(slice.length());
        slice.getBytes(0, value, 0, slice.length());
        assertEquals(value, slice);

        // set and get the value
        value = allocate((slice.length() - index) / 2);
        for (int i = 0; i < value.length(); i++) {
            value.setByte(i, i);
        }

        // check by slicing out the region
        slice.setBytes(index, value);
        assertEquals(value, slice.slice(index, value.length()));

        // check by getting out the region
        Slice tempValue = allocate(value.length());
        slice.getBytes(index, tempValue, 0, tempValue.length());
        assertEquals(tempValue, slice.slice(index, tempValue.length()));
        assertTrue(tempValue.equals(0, tempValue.length(), slice, index, tempValue.length()));

        for (int length = 0; length < value.length(); length++) {
            slice.fill((byte) 0xFF);
            slice.setBytes(index, value, 0, length);

            // check by slicing out the region
            assertEquals(value.slice(0, length), slice.slice(index, length));
            assertTrue(value.equals(0, length, slice, index, length));

            // check by getting out the region
            tempValue = allocate(length);
            slice.getBytes(index, tempValue);
            assertEquals(tempValue, slice.slice(index, length));
            assertTrue(tempValue.equals(0, length, slice, index, length));
        }
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
        assertEquals(slice.getBytes(), value);

        // set and get the value
        value = new byte[(slice.length() - index) / 2];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) i;
        }
        slice.setBytes(index, new ByteArrayInputStream(value), value.length);
        assertEquals(slice.getBytes(index, value.length), value);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        slice.getBytes(index, out, value.length);
        assertEquals(slice.getBytes(index, value.length), out.toByteArray());

        for (int length = 0; length < value.length; length++) {
            slice.fill((byte) 0xFF);
            slice.setBytes(index, new ByteArrayInputStream(value), length);
            assertEquals(slice.getBytes(index, length), Arrays.copyOf(value, length));

            out = new ByteArrayOutputStream();
            slice.getBytes(index, out, length);
            assertEquals(slice.getBytes(index, length), out.toByteArray());
        }
    }

    @Test
    public void testMemoryMappedReads()
            throws IOException
    {
        Path path = Files.createTempFile("longs", null);
        List<Long> values = createRandomLongs(20000);

        Slice output = allocate(values.size() * (int) SIZE_OF_LONG);
        for (int i = 0; i < values.size(); i++) {
            output.setLong(i * SIZE_OF_LONG, values.get(i));
        }

        Files.write(path, output.getBytes());

        Slice slice = Slices.mapFileReadOnly(path.toFile());
        for (int i = 0; i < values.size(); i++) {
            long actual = slice.getLong(i * SIZE_OF_LONG);
            long expected = values.get(i);
            assertEquals(actual, expected);
        }

        assertEquals(slice.getBytes(), output.getBytes());
    }

    @Test
    public void testRetainedSize()
            throws Exception
    {
        int sliceInstanceSize = ClassLayout.parseClass(Slice.class).instanceSize();
        Slice slice = Slices.allocate(10);
        assertEquals(slice.getRetainedSize(), sizeOfByteArray(10) + sliceInstanceSize);
        assertEquals(slice.length(), 10);
        Slice subSlice = slice.slice(0, 1);
        assertEquals(subSlice.getRetainedSize(), sizeOfByteArray(10) + sliceInstanceSize);
        assertEquals(subSlice.length(), 1);
    }

    @Test
    public void testCopyOf()
            throws Exception
    {
        // slightly stronger guarantees for empty slice
        assertSame(Slices.copyOf(EMPTY_SLICE), EMPTY_SLICE);
        assertSame(Slices.copyOf(utf8Slice("hello world"), 1, 0), EMPTY_SLICE);

        Slice slice = utf8Slice("hello world");
        assertEquals(Slices.copyOf(slice), slice);
        assertEquals(Slices.copyOf(slice, 1, 3), slice.slice(1, 3));

        // verify it's an actual copy
        Slice original = utf8Slice("hello world");
        Slice copy = Slices.copyOf(original);

        original.fill((byte) 0);
        assertEquals(copy, utf8Slice("hello world"));

        // read before beginning
        try {
            Slices.copyOf(slice, -1, slice.length());
            fail();
        }
        catch (IndexOutOfBoundsException ignored) {
        }

        // read after end
        try {
            Slices.copyOf(slice, slice.length() + 1, 1);
            fail();
        }
        catch (IndexOutOfBoundsException ignored) {
        }

        // start before but extend past end
        try {
            Slices.copyOf(slice, 1, slice.length());
            fail();
        }
        catch (IndexOutOfBoundsException ignored) {
        }
    }

    @Test
    public void testIndexOf()
            throws Exception
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
        assertEquals(data.indexOf(pattern, offset), expected);
        assertEquals(data.indexOfBruteForce(pattern, offset), expected);
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

        assertEquals(bruteForce, indexOf);
    }

    private static List<Long> createRandomLongs(int count)
    {
        Random random = new Random();
        List<Long> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(random.nextLong());
        }
        return Collections.unmodifiableList(list);
    }

    protected Slice allocate(int size)
    {
        return Slices.allocate(size);
    }
}
