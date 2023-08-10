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

import static org.assertj.core.api.Assertions.assertThat;

public class TestSliceCompactFlag
{
    @Test
    public void testSliceConstructors()
    {
        assertCompact(new Slice());

        byte[] byteArray = {(byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5};
        assertCompact(new Slice(byteArray));
        assertCompact(new Slice(byteArray, 0, byteArray.length));
        assertNotCompact(new Slice(byteArray, 0, byteArray.length - 1));
        assertNotCompact(new Slice(byteArray, 1, byteArray.length - 1));
    }

    @Test
    public void testSubSliceAndCopy()
    {
        byte[] byteArray = {(byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5};
        Slice slice = new Slice(byteArray);

        assertCompact(slice);
        assertCompact(slice.slice(0, slice.length()));
        assertThat(slice.slice(0, slice.length())).isSameAs(slice);

        assertCompact(Slices.copyOf(slice));
        assertThat(Slices.copyOf(slice).byteArray()).isNotSameAs(slice.byteArray());
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
    public void testWrapHeapBuffer()
    {
        ByteBuffer buffer = ByteBuffer.allocate(50);

        // initialize buffer
        for (int i = 0; i < 50; i++) {
            buffer.put((byte) i);
        }

        // test full buffer
        buffer.rewind();
        Slice slice = Slices.wrappedHeapBuffer(buffer);
        assertCompact(slice);

        // test limited buffer
        buffer.position(10).limit(30);
        slice = Slices.wrappedHeapBuffer(buffer);
        assertNotCompact(slice);

        // test limited buffer after slicing
        buffer = buffer.slice();
        slice = Slices.wrappedHeapBuffer(buffer);
        assertNotCompact(slice);
    }

    private static void assertCompact(Slice data)
    {
        assertThat(data.isCompact()).isTrue();
    }

    private static void assertNotCompact(Slice data)
    {
        assertThat(data.isCompact()).isFalse();
    }
}
