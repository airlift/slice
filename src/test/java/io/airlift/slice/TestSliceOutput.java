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
    public void testWriteZeroBeyondCapacityThrowsIndexOutOfBounds()
    {
        // length >= BULK_ZERO_FILL_THRESHOLD exercises the bulk Arrays.fill path in BasicSliceOutput.
        int bulk = SliceOutput.BULK_ZERO_FILL_THRESHOLD;

        // Backing offset is nonzero, so (baseOffset + size + length) overflows to a negative end
        // index. Must still be reported as IndexOutOfBoundsException, not IllegalArgumentException.
        BasicSliceOutput subSliceOutput = (BasicSliceOutput) Slices.allocate(100).slice(1, 99).getOutput();
        assertThatThrownBy(() -> subSliceOutput.writeZero(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // Nonzero writer position (size) after prior writes also overflows the end index.
        BasicSliceOutput output = (BasicSliceOutput) Slices.allocate(100).getOutput();
        output.writeLong(0);
        Slice underlying = output.getUnderlyingSlice();
        underlying.setByte(50, 0x7F);
        assertThatThrownBy(() -> output.writeZero(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);
        // the failed capacity check must not advance the writer or modify the slice
        assertThat(output.size()).isEqualTo(8);
        assertThat(underlying.getByte(50)).isEqualTo((byte) 0x7F);

        // A non-overflowing length that still exceeds capacity throws IndexOutOfBoundsException too.
        BasicSliceOutput small = (BasicSliceOutput) Slices.allocate(100).getOutput();
        assertThatThrownBy(() -> small.writeZero(bulk))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // A large in-bounds run still works through the bulk path.
        BasicSliceOutput inBounds = (BasicSliceOutput) Slices.allocate(bulk + 16).getOutput();
        inBounds.writeShort(0x0102);
        inBounds.writeZero(bulk);
        assertThat(inBounds.size()).isEqualTo(2 + bulk);
        Slice result = inBounds.getUnderlyingSlice();
        for (int i = 2; i < 2 + bulk; i++) {
            assertThat(result.getByte(i)).isEqualTo((byte) 0);
        }
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
