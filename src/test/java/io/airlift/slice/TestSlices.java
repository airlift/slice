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

import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.MAX_ARRAY_SIZE;
import static io.airlift.slice.Slices.SLICE_ALLOC_THRESHOLD;
import static io.airlift.slice.Slices.SLICE_ALLOW_SKEW;
import static io.airlift.slice.Slices.allocate;
import static io.airlift.slice.Slices.ensureSize;
import static io.airlift.slice.Slices.wrappedBooleanArray;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static io.airlift.slice.Slices.wrappedFloatArray;
import static io.airlift.slice.Slices.wrappedIntArray;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.airlift.slice.Slices.wrappedShortArray;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestSlices
{
    @Test
    public void testWrapDirectBuffer()
    {
        testWrapping(ByteBuffer.allocateDirect(50));
    }

    @Test
    public void testWrapHeapBuffer()
    {
        testWrapping(ByteBuffer.allocate(50));
    }

    @Test
    public void testWrapHeapBufferRetainedSize()
    {
        ByteBuffer heapByteBuffer = ByteBuffer.allocate(50);
        Slice slice = wrappedBuffer(heapByteBuffer);
        assertEquals(slice.getRetainedSize(), instanceSize(Slice.class) + sizeOf(heapByteBuffer.array()));
    }

    private static void testWrapping(ByteBuffer buffer)
    {
        // initialize buffer
        for (int i = 0; i < 50; i++) {
            buffer.put((byte) i);
        }

        // test full buffer
        buffer.rewind();
        Slice slice = wrappedBuffer(buffer);
        assertEquals(slice.length(), 50);
        for (int i = 0; i < 50; i++) {
            assertEquals(slice.getByte(i), i);
        }

        // test limited buffer
        buffer.position(10).limit(30);
        slice = wrappedBuffer(buffer);
        assertEquals(slice.length(), 20);
        for (int i = 0; i < 20; i++) {
            assertEquals(slice.getByte(i), i + 10);
        }

        // test limited buffer after slicing
        buffer = buffer.slice();
        slice = wrappedBuffer(buffer);
        assertEquals(slice.length(), 20);
        for (int i = 0; i < 20; i++) {
            assertEquals(slice.getByte(i), i + 10);
        }
    }

    @Test
    public void testWrapAllTypes()
    {
        boolean[] booleanArray = {true, false, false, true, false, true};
        assertEquals(wrappedBooleanArray(booleanArray).getByte(0) == 1, booleanArray[0]);
        assertEquals(wrappedBooleanArray(booleanArray, 1, 4).getByte(0) == 1, booleanArray[1]);
        assertEquals(wrappedBooleanArray(booleanArray, 1, 4).length(), 4 * SIZE_OF_BYTE);
        assertEquals(wrappedBooleanArray(booleanArray).getByte(5 * SIZE_OF_BYTE) == 1, booleanArray[5]);
        assertFalse(wrappedBooleanArray(booleanArray).hasByteArray());

        byte[] byteArray = {(byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5};
        assertEquals(wrappedBuffer(byteArray).getByte(0), byteArray[0]);
        assertEquals(wrappedBuffer(byteArray, 1, 4).getByte(0), byteArray[1]);
        assertEquals(wrappedBuffer(byteArray, 1, 4).length(), 4 * SIZE_OF_BYTE);
        assertEquals(wrappedBuffer(byteArray).getByte(5 * SIZE_OF_BYTE), byteArray[5]);
        assertTrue(wrappedBuffer(byteArray).hasByteArray());

        short[] shortArray = {(short) 0, (short) 1, (short) 2, (short) 3, (short) 4, (short) 5};
        assertEquals(wrappedShortArray(shortArray).getShort(0), shortArray[0]);
        assertEquals(wrappedShortArray(shortArray, 1, 4).getShort(0), shortArray[1]);
        assertEquals(wrappedShortArray(shortArray, 1, 4).length(), 4 * SIZE_OF_SHORT);
        assertEquals(wrappedShortArray(shortArray).getShort(5 * SIZE_OF_SHORT), shortArray[5]);
        assertFalse(wrappedShortArray(shortArray).hasByteArray());

        int[] intArray = {0, 1, 2, 3, 4, 5};
        assertEquals(wrappedIntArray(intArray).getInt(0), intArray[0]);
        assertEquals(wrappedIntArray(intArray, 1, 4).getInt(0), intArray[1]);
        assertEquals(wrappedIntArray(intArray, 1, 4).length(), 4 * SIZE_OF_INT);
        assertEquals(wrappedIntArray(intArray).getInt(5 * SIZE_OF_INT), intArray[5]);
        assertFalse(wrappedIntArray(intArray).hasByteArray());

        long[] longArray = {0L, 1L, 2L, 3L, 4L, 5L};
        assertEquals(wrappedLongArray(longArray).getLong(0), longArray[0]);
        assertEquals(wrappedLongArray(longArray, 1, 4).getLong(0), longArray[1]);
        assertEquals(wrappedLongArray(longArray, 1, 4).length(), 4 * SIZE_OF_LONG);
        assertEquals(wrappedLongArray(longArray).getLong(5 * SIZE_OF_LONG), longArray[5]);
        assertFalse(wrappedLongArray(longArray).hasByteArray());

        float[] floatArray = {0.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        assertEquals(wrappedFloatArray(floatArray).getFloat(0), floatArray[0]);
        assertEquals(wrappedFloatArray(floatArray, 1, 4).getFloat(0), floatArray[1]);
        assertEquals(wrappedFloatArray(floatArray, 1, 4).length(), 4 * SIZE_OF_FLOAT);
        assertEquals(wrappedFloatArray(floatArray).getFloat(5 * SIZE_OF_FLOAT), floatArray[5]);
        assertFalse(wrappedFloatArray(floatArray).hasByteArray());

        double[] doubleArray = {0.0, 1.0, 2.0, 3.0, 4.0, 5.0};
        assertEquals(wrappedDoubleArray(doubleArray).getDouble(0), doubleArray[0]);
        assertEquals(wrappedDoubleArray(doubleArray, 1, 4).getDouble(0), doubleArray[1]);
        assertEquals(wrappedDoubleArray(doubleArray, 1, 4).length(), 4 * SIZE_OF_DOUBLE);
        assertEquals(wrappedDoubleArray(doubleArray).getDouble(5 * SIZE_OF_DOUBLE), doubleArray[5]);
        assertFalse(wrappedDoubleArray(doubleArray).hasByteArray());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot allocate slice larger than 2147483639 bytes")
    public void testAllocationLimit()
    {
        allocate(Integer.MAX_VALUE - 1);
    }

    @Test
    public void testEnsureSize()
    {
        Slice fourBytes = wrappedBuffer(new byte[] {1, 2, 3, 4});

        assertEquals(ensureSize(null, 42).length(), 42);
        assertSame(ensureSize(fourBytes, 3), fourBytes);
        assertEquals(ensureSize(fourBytes, 8), wrappedBuffer(new byte[] {1, 2, 3, 4, 0, 0, 0, 0}));
        assertEquals(ensureSize(fourBytes, 5), wrappedBuffer(new byte[] {1, 2, 3, 4, 0, 0, 0, 0}));

        // Test that `ensureSize(s, slightly less than Integer.MAX_VALUE)` won't allocate a Slice beyond limit
        double initialSize = Integer.MAX_VALUE + 100L;
        while (initialSize > 50_000_000 && initialSize / SLICE_ALLOW_SKEW > SLICE_ALLOC_THRESHOLD) {
            initialSize /= SLICE_ALLOW_SKEW;
        }
        assertEquals(ensureSize(allocate((int) initialSize), Integer.MAX_VALUE - 50).length(), MAX_ARRAY_SIZE);
    }
}
