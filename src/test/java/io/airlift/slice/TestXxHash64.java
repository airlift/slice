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

import static io.airlift.slice.XxHash64.hash;
import static org.testng.Assert.assertEquals;

public class TestXxHash64
{
    private static final long PRIME = 2654435761L;

    private final Slice buffer;

    public TestXxHash64()
    {
        buffer = Slices.allocate(101);

        long value = PRIME;
        for (int i = 0; i < buffer.length(); i++) {
            buffer.setByte(i, (byte) (value >> 24));
            value *= value;
        }
    }

    @Test
    public void testSanity()
            throws Exception
    {
        assertEquals(hash(0, buffer, 0, 1), 0x4FCE394CC88952D8L);
        assertEquals(hash(PRIME, buffer, 0, 1), 0x739840CB819FA723L);

        assertEquals(hash(0, buffer, 0, 4), 0x9256E58AA397AEF1L);
        assertEquals(hash(PRIME, buffer, 0, 4), 0x9D5FFDFB928AB4BL);

        assertEquals(hash(0, buffer, 0, 8), 0xF74CB1451B32B8CFL);
        assertEquals(hash(PRIME, buffer, 0, 8), 0x9C44B77FBCC302C5L);

        assertEquals(hash(0, buffer, 0, 14), 0xCFFA8DB881BC3A3DL);
        assertEquals(hash(PRIME, buffer, 0, 14), 0x5B9611585EFCC9CBL);

        assertEquals(hash(0, buffer, 0, 32), 0xAF5753D39159EDEEL);
        assertEquals(hash(PRIME, buffer, 0, 32), 0xDCAB9233B8CA7B0FL);

        assertEquals(hash(0, buffer), 0x0EAB543384F878ADL);
        assertEquals(hash(PRIME, buffer), 0xCAA65939306F1E21L);
    }

    @Test
    public void testEmpty()
            throws Exception
    {
        assertEquals(hash(0, Slices.EMPTY_SLICE), 0xEF46DB3751D8E999L);
    }

    @Test
    public void testHashLong()
            throws Exception
    {
        assertEquals(hash(buffer.getLong(0)), hash(buffer, 0, SizeOf.SIZE_OF_LONG));
    }
}
