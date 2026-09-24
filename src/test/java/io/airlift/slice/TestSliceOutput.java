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

import static io.airlift.slice.SizeOf.instanceSize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSliceOutput
{
    @Test
    public void testAppendByte()
    {
        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
            Slice actual = new DynamicSliceOutput(1)
                    .appendByte(i)
                    .slice();

            Slice expected = Slices.wrappedBuffer((byte) i);

            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    public void testAppendUnsignedByte()
    {
        for (int i = 0; i < 256; i++) {
            Slice actual = new DynamicSliceOutput(1)
                    .appendByte(i)
                    .slice();

            Slice expected = Slices.wrappedBuffer((byte) i);

            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    public void testAppendByteTruncation()
    {
        for (int i = 256; i < 512; i++) {
            Slice actual = new DynamicSliceOutput(1)
                    .appendByte(i)
                    .slice();

            Slice expected = Slices.wrappedBuffer((byte) i);

            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    public void testAppendMultiple()
    {
        Slice actual = new DynamicSliceOutput(1)
                .appendByte(0)
                .appendByte(1)
                .appendByte(2)
                .appendByte(3)
                .appendByte(4)
                .slice();

        Slice expected = Slices.wrappedBuffer(new byte[] {0, 1, 2, 3, 4});
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testRetainedSize()
    {
        int sliceOutputInstanceSize = instanceSize(DynamicSliceOutput.class);
        DynamicSliceOutput output = new DynamicSliceOutput(10);

        long originalRetainedSize = output.getRetainedSize();
        assertThat(originalRetainedSize).isEqualTo(sliceOutputInstanceSize + output.getUnderlyingSlice().getRetainedSize());
        assertThat(output.size()).isZero();
        output.appendLong(0);
        output.appendShort(0);
        assertThat(output.getRetainedSize()).isEqualTo(originalRetainedSize);
        assertThat(output.size()).isEqualTo(10);
    }

    @Test
    public void testWriteZero()
    {
        // zeroing must overwrite stale buffer content left behind by reset
        DynamicSliceOutput output = new DynamicSliceOutput(16);
        output.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        output.reset();
        output.writeByte(42);
        output.writeZero(9);
        output.writeByte(43);
        byte[] expected = new byte[11];
        expected[0] = 42;
        expected[10] = 43;
        assertThat(output.slice()).isEqualTo(Slices.wrappedBuffer(expected));

        // zeroing across a growth boundary
        output = new DynamicSliceOutput(4);
        output.writeByte(7);
        output.writeZero(1000);
        output.writeByte(8);
        Slice result = output.slice();
        assertThat(result.length()).isEqualTo(1002);
        assertThat(result.getByte(0)).isEqualTo((byte) 7);
        assertThat(result.slice(1, 1000)).isEqualTo(Slices.allocate(1000));
        assertThat(result.getByte(1001)).isEqualTo((byte) 8);

        output = new DynamicSliceOutput(4);
        output.writeZero(0);
        assertThat(output.size()).isEqualTo(0);

        assertThatThrownBy(() -> new DynamicSliceOutput(4).writeZero(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testReset()
    {
        assertReset(new DynamicSliceOutput(1));
        assertReset(Slices.allocate(50).getOutput());
    }

    private static void assertReset(SliceOutput output)
    {
        output
                .appendByte(0)
                .appendByte(1)
                .appendByte(2)
                .appendByte(3)
                .appendByte(4);
        assertThat(output.slice()).isEqualTo(Slices.wrappedBuffer(new byte[] {0, 1, 2, 3, 4}));

        output.reset();
        assertThat(output.slice()).isEqualTo(Slices.EMPTY_SLICE);

        output
                .appendByte(2)
                .appendByte(4)
                .appendByte(6)
                .appendByte(8)
                .appendByte(10);
        assertThat(output.slice()).isEqualTo(Slices.wrappedBuffer(new byte[] {2, 4, 6, 8, 10}));

        output.reset(5);
        assertThat(output.slice()).isEqualTo(Slices.wrappedBuffer(new byte[] {2, 4, 6, 8, 10}));

        output.reset(3);
        assertThat(output.slice()).isEqualTo(Slices.wrappedBuffer(new byte[] {2, 4, 6}));

        output.reset(1);
        assertThat(output.slice()).isEqualTo(Slices.wrappedBuffer(new byte[] {2}));

        output.reset(0);
        assertThat(output.slice()).isEqualTo(Slices.EMPTY_SLICE);
    }
}
