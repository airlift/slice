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

import java.io.ByteArrayInputStream;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("resource")
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
        assertThat(input.position()).isEqualTo(0);
    }

    @Test
    public void testEmptyRead()
    {
        SliceInput input = buildSliceInput(new byte[0]);
        assertThat(input.read()).isEqualTo(-1);
    }

    @Test
    public void testReadByteBeyondEnd()
    {
        assertThatThrownBy(() -> buildSliceInput(new byte[0]).readByte())
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testReadShortBeyondEnd()
    {
        assertThatThrownBy(() -> buildSliceInput(new byte[1]).readShort())
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testReadIntBeyondEnd()
    {
        assertThatThrownBy(() -> buildSliceInput(new byte[3]).readInt())
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testReadLongBeyondEnd()
    {
        assertThatThrownBy(() -> buildSliceInput(new byte[7]).readLong())
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testEncodingBoolean()
    {
        assertThat(buildSliceInput(new byte[] {1}).readBoolean()).isTrue();
        assertThat(buildSliceInput(new byte[] {0}).readBoolean()).isFalse();
    }

    @Test
    public void testEncodingByte()
    {
        assertThat(buildSliceInput(new byte[] {92}).readByte()).isEqualTo((byte) 92);
        assertThat(buildSliceInput(new byte[] {-100}).readByte()).isEqualTo((byte) -100);
        assertThat(buildSliceInput(new byte[] {-17}).readByte()).isEqualTo((byte) -17);

        assertThat(buildSliceInput(new byte[] {92}).readUnsignedByte()).isEqualTo((short) 92);
        assertThat(buildSliceInput(new byte[] {-100}).readUnsignedByte()).isEqualTo((short) 156);
        assertThat(buildSliceInput(new byte[] {-17}).readUnsignedByte()).isEqualTo((short) 239);
    }

    @Test
    public void testEncodingShort()
    {
        assertThat(buildSliceInput(new byte[] {109, 92}).readShort()).isEqualTo((short) 23661);
        assertThat(buildSliceInput(new byte[] {109, -100}).readShort()).isEqualTo((short) -25491);
        assertThat(buildSliceInput(new byte[] {-52, -107}).readShort()).isEqualTo((short) -27188);

        assertThat(buildSliceInput(new byte[] {109, -100}).readUnsignedShort()).isEqualTo(40045);
        assertThat(buildSliceInput(new byte[] {-52, -107}).readUnsignedShort()).isEqualTo(38348);
    }

    @Test
    public void testEncodingInteger()
    {
        assertThat(buildSliceInput(new byte[] {109, 92, 75, 58}).readInt()).isEqualTo(978017389);
        assertThat(buildSliceInput(new byte[] {-16, -60, -120, -1}).readInt()).isEqualTo(-7813904);
    }

    @Test
    public void testEncodingLong()
    {
        assertThat(buildSliceInput(new byte[] {49, -114, -96, -23, -32, -96, -32, 127}).readLong()).isEqualTo(9214541725452766769L);
        assertThat(buildSliceInput(new byte[] {109, 92, 75, 58, 18, 120, -112, -17}).readLong()).isEqualTo(-1184314682315678611L);
    }

    @Test
    public void testEncodingDouble()
    {
        assertThat(buildSliceInput(new byte[] {31, -123, -21, 81, -72, 30, 9, 64}).readDouble()).isEqualTo(3.14);
        assertThat(buildSliceInput(new byte[] {0, 0, 0, 0, 0, 0, -8, 127}).readDouble()).isNaN();
        assertThat(buildSliceInput(new byte[] {0, 0, 0, 0, 0, 0, -16, -1}).readDouble()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(buildSliceInput(new byte[] {0, 0, 0, 0, 0, 0, -16, 127}).readDouble()).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    public void testEncodingFloat()
    {
        assertThat(buildSliceInput(new byte[] {-61, -11, 72, 64}).readFloat()).isEqualTo(3.14f);
        assertThat(buildSliceInput(new byte[] {0, 0, -64, 127}).readFloat()).isNaN();
        assertThat(buildSliceInput(new byte[] {0, 0, -128, -1}).readFloat()).isEqualTo(Float.NEGATIVE_INFINITY);
        assertThat(buildSliceInput(new byte[] {0, 0, -128, 127}).readFloat()).isEqualTo(Float.POSITIVE_INFINITY);
    }

    @Test
    public void testRetainedSize()
    {
        int bufferSize = 1024;
        InputStreamSliceInput input = new InputStreamSliceInput(new ByteArrayInputStream(new byte[] {0, 1}), bufferSize);
        assertThat(input.getRetainedSize()).isEqualTo(instanceSize(InputStreamSliceInput.class) + sizeOfByteArray(bufferSize));
    }

    private SliceInput buildSliceInput(byte[] bytes)
    {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        return new InputStreamSliceInput(inputStream, 16 * 1024);
    }
}
