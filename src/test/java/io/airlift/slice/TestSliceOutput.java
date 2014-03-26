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

import static org.testng.Assert.assertEquals;

public class TestSliceOutput
{
    @Test
    public void testAppendByte()
            throws Exception
    {
        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
            Slice actual = new DynamicSliceOutput(1)
                    .appendByte(i)
                    .slice();

            Slice expected = Slices.wrappedBuffer(new byte[] {(byte) i});

            assertEquals(actual, expected);
        }
    }

    @Test
    public void testAppendUnsignedByte()
            throws Exception
    {
        for (int i = 0; i < 256; i++) {
            Slice actual = new DynamicSliceOutput(1)
                    .appendByte(i)
                    .slice();

            Slice expected = Slices.wrappedBuffer(new byte[] {(byte) i});

            assertEquals(actual, expected);
        }
    }

    @Test
    public void testAppendByteTruncation()
            throws Exception
    {
        for (int i = 256; i < 512; i++) {
            Slice actual = new DynamicSliceOutput(1)
                    .appendByte(i)
                    .slice();

            Slice expected = Slices.wrappedBuffer(new byte[] {(byte) i});

            assertEquals(actual, expected);
        }
    }

    @Test
    public void testAppendMultiple()
            throws Exception
    {
        Slice actual = new DynamicSliceOutput(1)
                .appendByte(0)
                .appendByte(1)
                .appendByte(2)
                .appendByte(3)
                .appendByte(4)
                .slice();

        Slice expected = Slices.wrappedBuffer(new byte[] { 0, 1, 2, 3, 4 });
        assertEquals(actual, expected);
    }
}
