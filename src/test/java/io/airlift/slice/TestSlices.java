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

    private static void testWrapping(ByteBuffer buffer)
    {
        // initialize buffer
        for (int i = 0; i < 50; i++) {
            buffer.put((byte) i);
        }

        // test full buffer
        Slice slice = Slices.wrappedBuffer(buffer);
        assertEquals(slice.length(), 50);
        for (int i = 0; i < 50; i++) {
            assertEquals(slice.getByte(i), i);
        }

        // test limited buffer
        buffer.position(20).limit(30);
        slice = Slices.wrappedBuffer(buffer);
        assertEquals(slice.length(), 50);
        for (int i = 0; i < 50; i++) {
            assertEquals(slice.getByte(i), i);
        }

        // test limited buffer after slicing
        buffer = buffer.slice();
        slice = Slices.wrappedBuffer(buffer);
        assertEquals(slice.length(), 10);
        for (int i = 0; i < 10; i++) {
            assertEquals(slice.getByte(i), i + 20);
        }
    }
}
