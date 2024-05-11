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

import java.nio.ByteBuffer;
import java.util.Random;

import static io.airlift.slice.SizeOf.MEMORY_SEGMENT_INSTANCE_SIZE;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.MAX_ARRAY_SIZE;
import static io.airlift.slice.Slices.SLICE_ALLOC_THRESHOLD;
import static io.airlift.slice.Slices.SLICE_ALLOW_SKEW;
import static io.airlift.slice.Slices.allocate;
import static io.airlift.slice.Slices.ensureSize;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.slice.Slices.wrappedHeapBuffer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSlices
{
    @Test
    public void testEmptySlice()
    {
        assertThat(EMPTY_SLICE.length()).isEqualTo(0);
        assertThat(EMPTY_SLICE.byteArray().length).isEqualTo(0);
        assertThat(EMPTY_SLICE.byteArrayOffset()).isEqualTo(0);
    }

    @Test
    public void testWrapHeapBuffer()
    {
        ByteBuffer buffer = ByteBuffer.allocate(50);
        // initialize buffer
        for (int i = 0; i < 50; i++) {
            buffer.put((byte) i);
        }

        // test empty buffer
        assertThat(wrappedHeapBuffer(buffer)).isSameAs(EMPTY_SLICE);

        // test full buffer
        buffer.rewind();
        Slice slice = wrappedHeapBuffer(buffer);
        assertThat(slice.length()).isEqualTo(50);
        for (int i = 0; i < 50; i++) {
            assertThat(slice.getByte(i)).isEqualTo((byte) i);
        }

        // test limited buffer
        buffer.position(10).limit(30);
        slice = wrappedHeapBuffer(buffer);
        assertThat(slice.length()).isEqualTo(20);
        for (int i = 0; i < 20; i++) {
            assertThat(slice.getByte(i)).isEqualTo((byte) (i + 10));
        }

        // test limited buffer after slicing
        buffer = buffer.slice();
        slice = wrappedHeapBuffer(buffer);
        assertThat(slice.length()).isEqualTo(20);
        for (int i = 0; i < 20; i++) {
            assertThat(slice.getByte(i)).isEqualTo((byte) (i + 10));
        }
    }

    @Test
    public void testWrapByteArray()
    {
        byte[] byteArray = {(byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5};
        assertThat(wrappedBuffer(byteArray).getByte(0)).isEqualTo(byteArray[0]);
        assertThat(wrappedBuffer(byteArray, 1, 4).getByte(0)).isEqualTo(byteArray[1]);
        assertThat(wrappedBuffer(byteArray, 1, 4).length()).isEqualTo(4 * SIZE_OF_BYTE);
        assertThat(wrappedBuffer(byteArray).getByte(5 * SIZE_OF_BYTE)).isEqualTo(byteArray[5]);
    }

    @Test
    public void testWrapHeapBufferRetainedSize()
    {
        ByteBuffer heapByteBuffer = ByteBuffer.allocate(50);
        Slice slice = wrappedHeapBuffer(heapByteBuffer);
        assertThat(slice.getRetainedSize()).isEqualTo(instanceSize(Slice.class) + MEMORY_SEGMENT_INSTANCE_SIZE + sizeOf(heapByteBuffer.array()));
    }

    @Test
    public void testAllocationLimit()
    {
        assertThatThrownBy(() -> allocate(Integer.MAX_VALUE - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot allocate slice larger than 2147483639 bytes");
    }

    @Test
    public void testRandom()
    {
        assertThat(Slices.random(0)).isSameAs(EMPTY_SLICE);

        Slice randomSlice = Slices.random(10);
        assertThat(randomSlice.byteArrayOffset()).isEqualTo(0);
        assertThat(randomSlice.length()).isEqualTo(10);

        randomSlice = Slices.random(10, new Random(0));
        assertThat(randomSlice.byteArrayOffset()).isEqualTo(0);
        assertThat(randomSlice.length()).isEqualTo(10);
        byte[] bytes = new byte[10];
        new Random(0).nextBytes(bytes);
        assertThat(randomSlice.byteArray()).isEqualTo(bytes);
    }

    @Test
    public void testEnsureSize()
    {
        Slice slice = Slices.utf8Slice("testValueAbc").slice(4, 5);
        assertThat(slice.byteArray()).hasSize(12);
        assertThat(slice.byteArrayOffset()).isEqualTo(4);
        assertThat(slice.length()).isEqualTo(5);
        assertThat(slice.toStringUtf8()).isEqualTo("Value");

        // grow to a size within the existing outer slice
        Slice newSlice = ensureSize(slice, 6);
        // new byte array is always allocated if size changes
        assertThat(newSlice.byteArray()).isNotSameAs(slice.byteArray());
        // size is doubled when small
        assertThat(newSlice.byteArray()).hasSize(10);
        // new Slice covers the entire byte array range
        assertThat(newSlice.byteArrayOffset()).isEqualTo(0);
        assertThat(newSlice.length()).isEqualTo(10);
        // the existing data outside the slice view is not copied
        assertThat(newSlice.toStringUtf8()).isEqualTo("Value\0\0\0\0\0");

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
