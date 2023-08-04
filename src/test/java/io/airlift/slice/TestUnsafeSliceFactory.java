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

import com.google.common.primitives.Ints;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static io.airlift.slice.JvmUtils.bufferAddress;
import static io.airlift.slice.JvmUtils.unsafe;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUnsafeSliceFactory
{
    @Test
    public void testRawAddress()
    {
        UnsafeSliceFactory factory = UnsafeSliceFactory.getInstance();

        int size = 100;
        long address = unsafe.allocateMemory(size);
        try {
            Slice slice = factory.newSlice(address, size);
            for (int i = 0; i < size; i += Ints.BYTES) {
                slice.setInt(i, i);
            }
            for (int i = 0; i < size; i += Ints.BYTES) {
                assertThat(slice.getInt(i)).isEqualTo(i);
            }
        }
        finally {
            unsafe.freeMemory(address);
        }
    }

    @Test
    public void testRawAddressWithReference()
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(100);
        assertThat(buffer.isDirect()).isTrue();
        long address = bufferAddress(buffer);

        UnsafeSliceFactory factory = UnsafeSliceFactory.getInstance();

        Slice slice = factory.newSlice(address, buffer.capacity(), buffer);

        slice.setInt(32, 0xDEADBEEF);
        assertThat(slice.getInt(32)).isEqualTo(0xDEADBEEF);
    }
}
