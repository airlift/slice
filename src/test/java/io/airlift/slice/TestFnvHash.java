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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestFnvHash
{
    @Test
    public void testFNVHash32()
    {
        byte[][] testData = new byte[][] {
                "".getBytes(UTF_8),
                "foobar".getBytes(UTF_8),
                "foobar!\n".getBytes(UTF_8),
                "line 1\nline 2\nline 3".getBytes(UTF_8),
                "http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash".getBytes(UTF_8),
                {(byte) 128},
                "128".getBytes(UTF_8),
        };

        int[] expected = new int[] {
                0x811c9dc5,
                0xbf9cf968,
                0x24c0f933,
                0x97b4ea23,
                0xdd16ef45,
                0x4f3b29f,
                0x6b385816,
        };

        assertEquals(testData.length, expected.length);

        for (int i = 0; i < testData.length; ++i) {
            Slice data = Slices.wrappedBuffer(testData[i]);
            assertEquals(FnvHash.hash32(data), expected[i]);
        }
    }

    @Test
    public void testFNVHash64()
    {
        byte[][] testData = new byte[][] {
                "".getBytes(UTF_8),
                "foobar".getBytes(UTF_8),
                "foobar!\n".getBytes(UTF_8),
                "line 1\nline 2\nline 3".getBytes(UTF_8),
                "http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash".getBytes(UTF_8),
                {(byte) 128},
                "128".getBytes(UTF_8),
        };

        long[] expected = new long[] {
                0xcbf29ce484222325L,
                0x85944171f73967e8L,
                0x745f83eb4ecac933L,
                0x7829851fac17b143L,
                0xd9b957fb7fe794c5L,
                0x509c0cb379fdec5fL,
                0x456fbb181822b8f6L
        };

        assertEquals(testData.length, expected.length);

        for (int i = 0; i < testData.length; ++i) {
            Slice data = Slices.wrappedBuffer(testData[i]);
            assertEquals(FnvHash.hash64(data), expected[i]);
        }
    }
}
