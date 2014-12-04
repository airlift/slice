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

import com.google.common.hash.Hashing;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.testng.Assert.assertEquals;

public class TestMurmur3Hash32
{
    @Test(invocationCount = 100)
    public void testLessThan4Bytes()
            throws Exception
    {
        byte[] data = randomBytes(ThreadLocalRandom.current().nextInt(4));

        int expected = Hashing.murmur3_32().hashBytes(data).asInt();
        int actual = Murmur3Hash32.hash(Slices.wrappedBuffer(data));

        assertEquals(actual, expected);
    }

    @Test(invocationCount = 100)
    public void testMoreThan4Bytes()
            throws Exception
    {
        byte[] data = randomBytes(131);

        int expected = Hashing.murmur3_32().hashBytes(data).asInt();
        int actual = Murmur3Hash32.hash(Slices.wrappedBuffer(data));

        assertEquals(actual, expected);
    }

    @Test(invocationCount = 100)
    public void testOffsetAndLength()
            throws Exception
    {
        byte[] data = randomBytes(131);

        int offset = 13;
        int length = 55;

        int expected = Hashing.murmur3_32().hashBytes(data, offset, length).asInt();
        int actual = Murmur3Hash32.hash(Slices.wrappedBuffer(data), offset, length);

        assertEquals(actual, expected);
    }

    @Test(invocationCount = 100)
    public void testNonDefaultSeed()
            throws Exception
    {
        byte[] data = randomBytes(131);

        int seed = 123456789;

        int expected = Hashing.murmur3_32(seed).hashBytes(data).asInt();
        int actual = Murmur3Hash32.hash(seed, Slices.wrappedBuffer(data), 0, data.length);

        assertEquals(actual, expected);
    }

    @Test
    public void testTail()
            throws Exception
    {
        for (int i = 0; i < 4; i++) {
            byte[] data = randomBytes(50 + i);

            int expected = Hashing.murmur3_32().hashBytes(data).asInt();
            int actual = Murmur3Hash32.hash(Slices.wrappedBuffer(data));

            assertEquals(actual, expected);
        }
    }

    @Test(invocationCount = 100)
    public void testSingleInt()
            throws Exception
    {
        int value = ThreadLocalRandom.current().nextInt();

        Slice slice = Slices.allocate(4);
        slice.setInt(0, value);
        int expected = Murmur3Hash32.hash(slice);
        int actual = Murmur3Hash32.hash(value);
        assertEquals(actual, expected);
    }

    @Test(invocationCount = 100)
    public void testSingleLong()
            throws Exception
    {
        long value = ThreadLocalRandom.current().nextLong();

        Slice slice = Slices.allocate(8);
        slice.setLong(0, value);
        int expected = Murmur3Hash32.hash(slice);
        int actual = Murmur3Hash32.hash(value);
        assertEquals(actual, expected);
    }

    private static byte[] randomBytes(int length)
    {
        byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }
}
