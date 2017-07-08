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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestSliceCompactFlag
{
    @Test
    public void testSliceConstructors()
            throws Exception
    {
        assertCompact(new Slice());

        boolean[] booleanArray = {true, false, false, true, false, true};
        assertCompact(new Slice(booleanArray, 0, booleanArray.length));
        assertNotCompact(new Slice(booleanArray, 0, booleanArray.length - 1));
        assertNotCompact(new Slice(booleanArray, 1, booleanArray.length - 1));

        byte[] byteArray = {(byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5};
        assertCompact(new Slice(byteArray));
        assertCompact(new Slice(byteArray, 0, byteArray.length));
        assertNotCompact(new Slice(byteArray, 0, byteArray.length - 1));
        assertNotCompact(new Slice(byteArray, 1, byteArray.length - 1));

        short[] shortArray = {(short) 0, (short) 1, (short) 2, (short) 3, (short) 4, (short) 5};
        assertCompact(new Slice(shortArray, 0, shortArray.length));
        assertNotCompact(new Slice(shortArray, 0, shortArray.length - 1));
        assertNotCompact(new Slice(shortArray, 1, shortArray.length - 1));

        int[] intArray = {0, 1, 2, 3, 4, 5};
        assertCompact(new Slice(intArray, 0, intArray.length));
        assertNotCompact(new Slice(intArray, 0, intArray.length - 1));
        assertNotCompact(new Slice(intArray, 1, intArray.length - 1));

        long[] longArray = {0L, 1L, 2L, 3L, 4L, 5L};
        assertCompact(new Slice(longArray, 0, longArray.length));
        assertNotCompact(new Slice(longArray, 0, longArray.length - 1));
        assertNotCompact(new Slice(longArray, 1, longArray.length - 1));

        float[] floatArray = {0f, 1f, 2f, 3f, 4f, 5f};
        assertCompact(new Slice(floatArray, 0, floatArray.length));
        assertNotCompact(new Slice(floatArray, 0, floatArray.length - 1));
        assertNotCompact(new Slice(floatArray, 1, floatArray.length - 1));

        double[] doubleArray = {0.0, 1.0, 2.0, 3.0, 4.0, 5.0};
        assertCompact(new Slice(doubleArray, 0, doubleArray.length));
        assertNotCompact(new Slice(doubleArray, 0, doubleArray.length - 1));
        assertNotCompact(new Slice(doubleArray, 1, doubleArray.length - 1));
    }

    @Test
    public void testSubSliceAndCopy()
    {
        byte[] byteArray = {(byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5};
        Slice slice = new Slice(byteArray);

        assertCompact(slice);
        assertCompact(slice.slice(0, slice.length()));
        assertSame(slice.slice(0, slice.length()), slice);

        assertCompact(Slices.copyOf(slice));
        assertNotSame(Slices.copyOf(slice).getBase(), slice.getBase());
        assertCompact(Slices.copyOf(slice, 0, slice.length() - 1));
        assertCompact(Slices.copyOf(slice, 1, slice.length() - 1));

        Slice subSlice1 = slice.slice(0, slice.length() - 1);
        Slice subSlice2 = slice.slice(1, slice.length() - 1);
        assertNotCompact(subSlice1);
        assertNotCompact(subSlice2);
        assertCompact(Slices.copyOf(subSlice1));
        assertCompact(Slices.copyOf(subSlice2));
    }

    @Test
    public void testWrapDirectBuffer()
            throws Exception
    {
        // For DirectByteBuffer, the slice is always considered as not compacted
        // because there is no easy way to tell whether the DirectByteBuffer itself
        // is a view over a larger allocation.
        ByteBuffer buffer = ByteBuffer.allocateDirect(50);

        // initialize buffer
        for (int i = 0; i < 50; i++) {
            buffer.put((byte) i);
        }

        // test full buffer
        buffer.rewind();
        Slice slice = Slices.wrappedBuffer(buffer);
        assertNotCompact(slice);

        // test limited buffer
        buffer.position(10).limit(30);
        slice = Slices.wrappedBuffer(buffer);
        assertNotCompact(slice);

        // test limited buffer after slicing
        buffer = buffer.slice();
        slice = Slices.wrappedBuffer(buffer);
        assertNotCompact(slice);
    }

    @Test
    public void testWrapHeapBuffer()
            throws Exception
    {
        ByteBuffer buffer = ByteBuffer.allocate(50);

        // initialize buffer
        for (int i = 0; i < 50; i++) {
            buffer.put((byte) i);
        }

        // test full buffer
        buffer.rewind();
        Slice slice = Slices.wrappedBuffer(buffer);
        assertCompact(slice);

        // test limited buffer
        buffer.position(10).limit(30);
        slice = Slices.wrappedBuffer(buffer);
        assertNotCompact(slice);

        // test limited buffer after slicing
        buffer = buffer.slice();
        slice = Slices.wrappedBuffer(buffer);
        assertNotCompact(slice);
    }

    private static void assertCompact(Slice data)
    {
        assertTrue(data.isCompact());
    }

    private static void assertNotCompact(Slice data)
    {
        assertFalse(data.isCompact());
    }
}
