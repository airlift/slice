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

import static io.airlift.slice.SizeOf.instanceSize;
import static org.testng.Assert.assertEquals;

public class TestBasicSliceInput
        extends AbstractSliceInputTest
{
    @Override
    protected SliceInput createSliceInput(Slice slice)
    {
        return new BasicSliceInput(slice);
    }

    @Test
    public void testRetainedSize()
    {
        Slice slice = Slices.allocate(1024);
        SliceInput input = new BasicSliceInput(slice);
        assertEquals(input.getRetainedSize(), instanceSize(BasicSliceInput.class) + slice.getRetainedSize());
    }
}
