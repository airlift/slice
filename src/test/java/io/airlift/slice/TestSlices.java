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
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.MAX_ARRAY_SIZE;
import static io.airlift.slice.Slices.SLICE_ALLOC_THRESHOLD;
import static io.airlift.slice.Slices.SLICE_ALLOW_SKEW;
import static io.airlift.slice.Slices.allocate;
import static io.airlift.slice.Slices.ensureSize;
import static io.airlift.slice.Slices.wrappedBooleanArray;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static io.airlift.slice.Slices.wrappedFloatArray;
import static io.airlift.slice.Slices.wrappedHeapBuffer;
import static io.airlift.slice.Slices.wrappedIntArray;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.airlift.slice.Slices.wrappedShortArray;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSlices
{
    @Test
    public void testEmptySlice()
    {
        assertThat(EMPTY_SLICE.length()).isEqualTo(0);
        assertThat(EMPTY_SLICE.hasByteArray()).isTrue();
        assertThat(EMPTY_SLICE.byteArray().length).isEqualTo(0);
        assertThat(EMPTY_SLICE.byteArrayOffset()).isEqualTo(0);
    }

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
        Slice slice = wrappedHeapBuffer(heapByteBuffer);
        assertThat(slice.getRetainedSize()).isEqualTo(instanceSize(Slice.class) + sizeOf(heapByteBuffer.array()));
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
        assertThat(slice.length()).isEqualTo(50);
        for (int i = 0; i < 50; i++) {
            assertThat(slice.getByte(i)).isEqualTo((byte) i);
        }

        // test limited buffer
        buffer.position(10).limit(30);
        slice = wrappedBuffer(buffer);
        assertThat(slice.length()).isEqualTo(20);
        for (int i = 0; i < 20; i++) {
            assertThat(slice.getByte(i)).isEqualTo((byte) (i + 10));
        }

        // test limited buffer after slicing
        buffer = buffer.slice();
        slice = wrappedBuffer(buffer);
        assertThat(slice.length()).isEqualTo(20);
        for (int i = 0; i < 20; i++) {
            assertThat(slice.getByte(i)).isEqualTo((byte) (i + 10));
        }
    }

    @Test
    public void testWrapAllTypes()
    {
        boolean[] booleanArray = {true, false, false, true, false, true};
        assertThat(wrappedBooleanArray(booleanArray).getByte(0) == 1).isEqualTo(booleanArray[0]);
        assertThat(wrappedBooleanArray(booleanArray, 1, 4).getByte(0) == 1).isEqualTo(booleanArray[1]);
        assertThat(wrappedBooleanArray(booleanArray, 1, 4).length()).isEqualTo(4 * SIZE_OF_BYTE);
        assertThat(wrappedBooleanArray(booleanArray).getByte(5 * SIZE_OF_BYTE) == 1).isEqualTo(booleanArray[5]);
        assertThat(wrappedBooleanArray(booleanArray).hasByteArray()).isFalse();

        byte[] byteArray = {(byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5};
        assertThat(wrappedBuffer(byteArray).getByte(0)).isEqualTo(byteArray[0]);
        assertThat(wrappedBuffer(byteArray, 1, 4).getByte(0)).isEqualTo(byteArray[1]);
        assertThat(wrappedBuffer(byteArray, 1, 4).length()).isEqualTo(4 * SIZE_OF_BYTE);
        assertThat(wrappedBuffer(byteArray).getByte(5 * SIZE_OF_BYTE)).isEqualTo(byteArray[5]);
        assertThat(wrappedBuffer(byteArray).hasByteArray()).isTrue();

        short[] shortArray = {(short) 0, (short) 1, (short) 2, (short) 3, (short) 4, (short) 5};
        assertThat(wrappedShortArray(shortArray).getShort(0)).isEqualTo(shortArray[0]);
        assertThat(wrappedShortArray(shortArray, 1, 4).getShort(0)).isEqualTo(shortArray[1]);
        assertThat(wrappedShortArray(shortArray, 1, 4).length()).isEqualTo(4 * SIZE_OF_SHORT);
        assertThat(wrappedShortArray(shortArray).getShort(5 * SIZE_OF_SHORT)).isEqualTo(shortArray[5]);
        assertThat(wrappedShortArray(shortArray).hasByteArray()).isFalse();

        int[] intArray = {0, 1, 2, 3, 4, 5};
        assertThat(wrappedIntArray(intArray).getInt(0)).isEqualTo(intArray[0]);
        assertThat(wrappedIntArray(intArray, 1, 4).getInt(0)).isEqualTo(intArray[1]);
        assertThat(wrappedIntArray(intArray, 1, 4).length()).isEqualTo(4 * SIZE_OF_INT);
        assertThat(wrappedIntArray(intArray).getInt(5 * SIZE_OF_INT)).isEqualTo(intArray[5]);
        assertThat(wrappedIntArray(intArray).hasByteArray()).isFalse();

        long[] longArray = {0L, 1L, 2L, 3L, 4L, 5L};
        assertThat(wrappedLongArray(longArray).getLong(0)).isEqualTo(longArray[0]);
        assertThat(wrappedLongArray(longArray, 1, 4).getLong(0)).isEqualTo(longArray[1]);
        assertThat(wrappedLongArray(longArray, 1, 4).length()).isEqualTo(4 * SIZE_OF_LONG);
        assertThat(wrappedLongArray(longArray).getLong(5 * SIZE_OF_LONG)).isEqualTo(longArray[5]);
        assertThat(wrappedLongArray(longArray).hasByteArray()).isFalse();

        float[] floatArray = {0.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        assertThat(wrappedFloatArray(floatArray).getFloat(0)).isEqualTo(floatArray[0]);
        assertThat(wrappedFloatArray(floatArray, 1, 4).getFloat(0)).isEqualTo(floatArray[1]);
        assertThat(wrappedFloatArray(floatArray, 1, 4).length()).isEqualTo(4 * SIZE_OF_FLOAT);
        assertThat(wrappedFloatArray(floatArray).getFloat(5 * SIZE_OF_FLOAT)).isEqualTo(floatArray[5]);
        assertThat(wrappedFloatArray(floatArray).hasByteArray()).isFalse();

        double[] doubleArray = {0.0, 1.0, 2.0, 3.0, 4.0, 5.0};
        assertThat(wrappedDoubleArray(doubleArray).getDouble(0)).isEqualTo(doubleArray[0]);
        assertThat(wrappedDoubleArray(doubleArray, 1, 4).getDouble(0)).isEqualTo(doubleArray[1]);
        assertThat(wrappedDoubleArray(doubleArray, 1, 4).length()).isEqualTo(4 * SIZE_OF_DOUBLE);
        assertThat(wrappedDoubleArray(doubleArray).getDouble(5 * SIZE_OF_DOUBLE)).isEqualTo(doubleArray[5]);
        assertThat(wrappedDoubleArray(doubleArray).hasByteArray()).isFalse();
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

        assertThat(ensureSize(null, 42).length()).isEqualTo(42);
        assertThat(ensureSize(fourBytes, 3)).isSameAs(fourBytes);
        assertThat(ensureSize(fourBytes, 8)).isEqualTo(wrappedBuffer(new byte[] {1, 2, 3, 4, 0, 0, 0, 0}));
        assertThat(ensureSize(fourBytes, 5)).isEqualTo(wrappedBuffer(new byte[] {1, 2, 3, 4, 0, 0, 0, 0}));

        // Test that `ensureSize(s, slightly less than Integer.MAX_VALUE)` won't allocate a Slice beyond limit
        double initialSize = Integer.MAX_VALUE + 100L;
        while (initialSize > 50_000_000 && initialSize / SLICE_ALLOW_SKEW > SLICE_ALLOC_THRESHOLD) {
            initialSize /= SLICE_ALLOW_SKEW;
        }
        assertThat(ensureSize(allocate((int) initialSize), Integer.MAX_VALUE - 50).length()).isEqualTo(MAX_ARRAY_SIZE);
    }
}
