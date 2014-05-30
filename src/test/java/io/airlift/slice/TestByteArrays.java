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

import java.nio.ByteOrder;

import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static org.testng.Assert.assertEquals;

public class TestByteArrays
{
    @Test
    public void testReading()
    {
        assertEquals(ByteOrder.nativeOrder(), ByteOrder.LITTLE_ENDIAN);

        byte[] bytes = new byte[10];
        Slice slice = Slices.wrappedBuffer(bytes);
        slice.setInt(0, 0xDEADBEEF);
        slice.setInt(4, 0xCAFEBABE);

        // little endian memory layout: EF BE AD DE BE BA FE CA 00 00
        assertEquals(slice.getBytes(), new byte[] {
                (byte) 0xEF, (byte) 0xBE, (byte) 0xAD, (byte) 0xDE,
                (byte) 0xBE, (byte) 0xBA, (byte) 0xFE, (byte) 0xCA,
                0x00, 0x00});

        assertEquals(ByteArrays.getShort(bytes, 0), (short) 0xBEEF);
        assertEquals(ByteArrays.getShort(bytes, 1), (short) 0xADBE);
        assertEquals(ByteArrays.getShort(bytes, 2), (short) 0xDEAD);
        assertEquals(ByteArrays.getShort(bytes, 3), (short) 0xBEDE);

        assertEquals(ByteArrays.getInt(bytes, 0), 0xDEADBEEF);
        assertEquals(ByteArrays.getInt(bytes, 1), 0xBEDEADBE);
        assertEquals(ByteArrays.getInt(bytes, 2), 0xBABEDEAD);
        assertEquals(ByteArrays.getInt(bytes, 3), 0xFEBABEDE);
        assertEquals(ByteArrays.getInt(bytes, 4), 0xCAFEBABE);

        assertEquals(ByteArrays.getLong(bytes, 0), 0xCAFEBABE_DEADBEEFL);
        assertEquals(ByteArrays.getLong(bytes, 1), 0x00CAFEBA_BEDEADBEL);
        assertEquals(ByteArrays.getLong(bytes, 2), 0x0000CAFE_BABEDEADL);

        assertEquals(ByteArrays.getFloat(bytes, 0), intBitsToFloat(0xDEADBEEF));
        assertEquals(ByteArrays.getFloat(bytes, 1), intBitsToFloat(0xBEDEADBE));
        assertEquals(ByteArrays.getFloat(bytes, 2), intBitsToFloat(0xBABEDEAD));
        assertEquals(ByteArrays.getFloat(bytes, 3), intBitsToFloat(0xFEBABEDE));
        assertEquals(ByteArrays.getFloat(bytes, 4), intBitsToFloat(0xCAFEBABE));

        assertEquals(ByteArrays.getDouble(bytes, 0), longBitsToDouble(0xCAFEBABE_DEADBEEFL));
        assertEquals(ByteArrays.getDouble(bytes, 1), longBitsToDouble(0x00CAFEBA_BEDEADBEL));
        assertEquals(ByteArrays.getDouble(bytes, 2), longBitsToDouble(0x0000CAFE_BABEDEADL));
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testReadingShortBounds()
    {
        ByteArrays.getShort(new byte[3], 2);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testReadingIntBounds()
    {
        ByteArrays.getInt(new byte[5], 2);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testReadingLongBounds()
    {
        ByteArrays.getLong(new byte[9], 2);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testReadingFloatBounds()
    {
        ByteArrays.getFloat(new byte[5], 2);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testReadingDoubleBounds()
    {
        ByteArrays.getDouble(new byte[9], 2);
    }
}
