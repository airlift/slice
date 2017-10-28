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

import java.nio.ByteBuffer;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.wrappedBooleanArray;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static io.airlift.slice.Slices.wrappedFloatArray;
import static io.airlift.slice.Slices.wrappedIntArray;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.airlift.slice.Slices.wrappedShortArray;
import static org.testng.Assert.assertEquals;

public class TestSlices
{
    @Test
    public void testWrapDirectBuffer()
            throws Exception
    {
        testWrapping(ByteBuffer.allocateDirect(50));
    }

    @Test
    public void testWrapHeapBuffer()
            throws Exception
    {
        testWrapping(ByteBuffer.allocate(50));
    }

    @Test
    public void testWrapHeapBufferRetainedSize()
    {
        ByteBuffer heapByteBuffer = ByteBuffer.allocate(50);
        Slice slice = Slices.wrappedBuffer(heapByteBuffer);
        assertEquals(slice.getRetainedSize(), ClassLayout.parseClass(Slice.class).instanceSize() + sizeOf(heapByteBuffer.array()));
    }

    private static void testWrapping(ByteBuffer buffer)
    {
        // initialize buffer
        for (int i = 0; i < 50; i++) {
            buffer.put((byte) i);
        }

        // test full buffer
        buffer.rewind();
        Slice slice = Slices.wrappedBuffer(buffer);
        assertEquals(slice.length(), 50);
        for (int i = 0; i < 50; i++) {
            assertEquals(slice.getByte(i), i);
        }

        // test limited buffer
        buffer.position(10).limit(30);
        slice = Slices.wrappedBuffer(buffer);
        assertEquals(slice.length(), 20);
        for (int i = 0; i < 20; i++) {
            assertEquals(slice.getByte(i), i + 10);
        }

        // test limited buffer after slicing
        buffer = buffer.slice();
        slice = Slices.wrappedBuffer(buffer);
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

        byte[] byteArray = {(byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5};
        assertEquals(wrappedBuffer(byteArray).getByte(0), byteArray[0]);
        assertEquals(wrappedBuffer(byteArray, 1, 4).getByte(0), byteArray[1]);
        assertEquals(wrappedBuffer(byteArray, 1, 4).length(), 4 * SIZE_OF_BYTE);
        assertEquals(wrappedBuffer(byteArray).getByte(5 * SIZE_OF_BYTE), byteArray[5]);

        short[] shortArray = {(short) 0, (short) 1, (short) 2, (short) 3, (short) 4, (short) 5};
        assertEquals(wrappedShortArray(shortArray).getShort(0), shortArray[0]);
        assertEquals(wrappedShortArray(shortArray, 1, 4).getShort(0), shortArray[1]);
        assertEquals(wrappedShortArray(shortArray, 1, 4).length(), 4 * SIZE_OF_SHORT);
        assertEquals(wrappedShortArray(shortArray).getShort(5 * SIZE_OF_SHORT), shortArray[5]);

        int[] intArray = {0, 1, 2, 3, 4, 5};
        assertEquals(wrappedIntArray(intArray).getInt(0), intArray[0]);
        assertEquals(wrappedIntArray(intArray, 1, 4).getInt(0), intArray[1]);
        assertEquals(wrappedIntArray(intArray, 1, 4).length(), 4 * SIZE_OF_INT);
        assertEquals(wrappedIntArray(intArray).getInt(5 * SIZE_OF_INT), intArray[5]);

        long[] longArray = {0L, 1L, 2L, 3L, 4L, 5L};
        assertEquals(wrappedLongArray(longArray).getLong(0), longArray[0]);
        assertEquals(wrappedLongArray(longArray, 1, 4).getLong(0), longArray[1]);
        assertEquals(wrappedLongArray(longArray, 1, 4).length(), 4 * SIZE_OF_LONG);
        assertEquals(wrappedLongArray(longArray).getLong(5 * SIZE_OF_LONG), longArray[5]);

        float[] floatArray = {0f, 1f, 2f, 3f, 4f, 5f};
        assertEquals(wrappedFloatArray(floatArray).getFloat(0), floatArray[0]);
        assertEquals(wrappedFloatArray(floatArray, 1, 4).getFloat(0), floatArray[1]);
        assertEquals(wrappedFloatArray(floatArray, 1, 4).length(), 4 * SIZE_OF_FLOAT);
        assertEquals(wrappedFloatArray(floatArray).getFloat(5 * SIZE_OF_FLOAT), floatArray[5]);

        double[] doubleArray = {0.0, 1.0, 2.0, 3.0, 4.0, 5.0};
        assertEquals(wrappedDoubleArray(doubleArray).getDouble(0), doubleArray[0]);
        assertEquals(wrappedDoubleArray(doubleArray, 1, 4).getDouble(0), doubleArray[1]);
        assertEquals(wrappedDoubleArray(doubleArray, 1, 4).length(), 4 * SIZE_OF_DOUBLE);
        assertEquals(wrappedDoubleArray(doubleArray).getDouble(5 * SIZE_OF_DOUBLE), doubleArray[5]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot allocate slice larger than 2147483639 bytes")
    public void testAllocationLimit()
    {
        Slices.allocate(Integer.MAX_VALUE - 1);
    }
}
