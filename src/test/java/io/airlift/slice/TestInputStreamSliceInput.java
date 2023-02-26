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

import java.io.ByteArrayInputStream;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static org.testng.Assert.assertEquals;

public class TestInputStreamSliceInput
        extends AbstractSliceInputTest
{
    @Override
    protected SliceInput createSliceInput(Slice slice)
    {
        return buildSliceInput(slice.getBytes());
    }

    @Override
    protected void testReadReverse(SliceInputTester tester, Slice slice)
    {
    }

    @Test
    public void testEmptyInput()
    {
        SliceInput input = buildSliceInput(new byte[0]);
        assertEquals(input.position(), 0);
    }

    @Test
    public void testEmptyRead()
    {
        SliceInput input = buildSliceInput(new byte[0]);
        assertEquals(input.read(), -1);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testReadByteBeyondEnd()
    {
        SliceInput input = buildSliceInput(new byte[0]);
        input.readByte();
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testReadShortBeyondEnd()
    {
        SliceInput input = buildSliceInput(new byte[1]);
        input.readShort();
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testReadIntBeyondEnd()
    {
        SliceInput input = buildSliceInput(new byte[3]);
        input.readInt();
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testReadLongBeyondEnd()
    {
        SliceInput input = buildSliceInput(new byte[7]);
        input.readLong();
    }

    @Test
    public void testEncodingBoolean()
    {
        assertEquals(buildSliceInput(new byte[] {1}).readBoolean(), true);
        assertEquals(buildSliceInput(new byte[] {0}).readBoolean(), false);
    }

    @Test
    public void testEncodingByte()
    {
        assertEquals(buildSliceInput(new byte[] {92}).readByte(), 92);
        assertEquals(buildSliceInput(new byte[] {-100}).readByte(), -100);
        assertEquals(buildSliceInput(new byte[] {-17}).readByte(), -17);

        assertEquals(buildSliceInput(new byte[] {92}).readUnsignedByte(), 92);
        assertEquals(buildSliceInput(new byte[] {-100}).readUnsignedByte(), 156);
        assertEquals(buildSliceInput(new byte[] {-17}).readUnsignedByte(), 239);
    }

    @Test
    public void testEncodingShort()
    {
        assertEquals(buildSliceInput(new byte[] {109, 92}).readShort(), 23661);
        assertEquals(buildSliceInput(new byte[] {109, -100}).readShort(), -25491);
        assertEquals(buildSliceInput(new byte[] {-52, -107}).readShort(), -27188);

        assertEquals(buildSliceInput(new byte[] {109, -100}).readUnsignedShort(), 40045);
        assertEquals(buildSliceInput(new byte[] {-52, -107}).readUnsignedShort(), 38348);
    }

    @Test
    public void testEncodingInteger()
    {
        assertEquals(buildSliceInput(new byte[] {109, 92, 75, 58}).readInt(), 978017389);
        assertEquals(buildSliceInput(new byte[] {-16, -60, -120, -1}).readInt(), -7813904);
    }

    @Test
    public void testEncodingLong()
    {
        assertEquals(buildSliceInput(new byte[] {49, -114, -96, -23, -32, -96, -32, 127}).readLong(), 9214541725452766769L);
        assertEquals(buildSliceInput(new byte[] {109, 92, 75, 58, 18, 120, -112, -17}).readLong(), -1184314682315678611L);
    }

    @Test
    public void testEncodingDouble()
    {
        assertEquals(buildSliceInput(new byte[] {31, -123, -21, 81, -72, 30, 9, 64}).readDouble(), 3.14);
        assertEquals(buildSliceInput(new byte[] {0, 0, 0, 0, 0, 0, -8, 127}).readDouble(), Double.NaN);
        assertEquals(buildSliceInput(new byte[] {0, 0, 0, 0, 0, 0, -16, -1}).readDouble(), Double.NEGATIVE_INFINITY);
        assertEquals(buildSliceInput(new byte[] {0, 0, 0, 0, 0, 0, -16, 127}).readDouble(), Double.POSITIVE_INFINITY);
    }

    @Test
    public void testEncodingFloat()
    {
        assertEquals(buildSliceInput(new byte[] {-61, -11, 72, 64}).readFloat(), 3.14f);
        assertEquals(buildSliceInput(new byte[] {0, 0, -64, 127}).readFloat(), Float.NaN);
        assertEquals(buildSliceInput(new byte[] {0, 0, -128, -1}).readFloat(), Float.NEGATIVE_INFINITY);
        assertEquals(buildSliceInput(new byte[] {0, 0, -128, 127}).readFloat(), Float.POSITIVE_INFINITY);
    }

    @Test
    public void testRetainedSize()
    {
        int bufferSize = 1024;
        InputStreamSliceInput input = new InputStreamSliceInput(new ByteArrayInputStream(new byte[] {0, 1}), bufferSize);
        assertEquals(input.getRetainedSize(), instanceSize(InputStreamSliceInput.class) + sizeOfByteArray(bufferSize));
    }

    private SliceInput buildSliceInput(byte[] bytes)
    {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        return new InputStreamSliceInput(inputStream, 16 * 1024);
    }
}
