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

import io.airlift.slice.ChunkedSliceInput.BufferReference;
import io.airlift.slice.ChunkedSliceInput.SliceLoader;

import static com.google.common.base.Preconditions.checkPositionIndex;

public class TestChunkedSliceInput
        extends AbstractSliceInputTest
{
    @Override
    protected SliceInput createSliceInput(Slice slice)
    {
        return new ChunkedSliceInput(new SliceSliceLoader(slice), BUFFER_SIZE);
    }

    private static class SliceSliceLoader
            implements SliceLoader<BufferReference>
    {
        private final Slice data;

        public SliceSliceLoader(Slice data)
        {
            this.data = data;
        }

        @Override
        public BufferReference createBuffer(int bufferSize)
        {
            final Slice slice = Slices.allocate(bufferSize);
            return new BufferReference() {
                @Override
                public Slice getSlice()
                {
                    return slice;
                }
            };
        }

        @Override
        public long getSize()
        {
            return data.length();
        }

        @Override
        public void load(long position, BufferReference buffer, int length)
        {
            checkPositionIndex((int) (position + length), (int) getSize());
            this.data.getBytes((int) position, buffer.getSlice(), 0, length);
        }

        @Override
        public void close()
        {
        }
    }
}
