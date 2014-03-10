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

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.testng.Assert.assertEquals;

public class TestMurmur3
{
    @Test(invocationCount = 100)
    public void testLessThan16Bytes()
            throws Exception
    {
        byte[] data = randomBytes(ThreadLocalRandom.current().nextInt(16));

        HashCode expected = Hashing.murmur3_128().hashBytes(data);
        Slice actual = Murmur3.hash(Slices.wrappedBuffer(data));

        assertEquals(actual.getBytes(), expected.asBytes());
    }

    @Test(invocationCount = 100)
    public void testMoreThan16Bytes()
            throws Exception
    {
        byte[] data = randomBytes(131);

        HashCode expected = Hashing.murmur3_128().hashBytes(data);
        Slice actual = Murmur3.hash(Slices.wrappedBuffer(data));

        assertEquals(actual.getBytes(), expected.asBytes());
    }

    @Test(invocationCount = 100)
    public void testOffsetAndLength()
            throws Exception
    {
        byte[] data = randomBytes(131);

        int offset = 13;
        int length = 55;

        HashCode expected = Hashing.murmur3_128().hashBytes(data, offset, length);
        Slice actual = Murmur3.hash(Slices.wrappedBuffer(data), offset, length);

        assertEquals(actual.getBytes(), expected.asBytes());
    }

    @Test(invocationCount = 100)
    public void testNonDefaultSeed()
            throws Exception
    {
        byte[] data = randomBytes(131);

        int seed = 123456789;

        HashCode expected = Hashing.murmur3_128(seed).hashBytes(data);
        Slice actual = Murmur3.hash(seed, Slices.wrappedBuffer(data), 0, data.length);

        assertEquals(actual.getBytes(), expected.asBytes());
    }

    @Test(invocationCount = 100)
    public void testLessThan16Bytes64()
            throws Exception
    {
        byte[] data = randomBytes(ThreadLocalRandom.current().nextInt(16));

        long expected = Murmur3.hash(Slices.wrappedBuffer(data)).getLong(0);
        long actual = Murmur3.hash64(Slices.wrappedBuffer(data));

        assertEquals(actual, expected);
    }

    @Test(invocationCount = 100)
    public void testMoreThan16Bytes64()
            throws Exception
    {
        byte[] data = randomBytes(131);

        long expected = Murmur3.hash(Slices.wrappedBuffer(data)).getLong(0);
        long actual = Murmur3.hash64(Slices.wrappedBuffer(data));

        assertEquals(actual, expected);
    }

    @Test(invocationCount = 100)
    public void testOffsetAndLength64()
            throws Exception
    {
        byte[] data = randomBytes(131);

        int offset = 13;
        int length = 55;

        long expected = Murmur3.hash(Slices.wrappedBuffer(data), offset, length).getLong(0);
        long actual = Murmur3.hash64(Slices.wrappedBuffer(data), offset, length);

        assertEquals(actual, expected);
    }

    @Test(invocationCount = 100)
    public void testNonDefaultSeed64()
            throws Exception
    {
        byte[] data = randomBytes(131);

        int seed = 123456789;

        long expected = Murmur3.hash(seed, Slices.wrappedBuffer(data), 0, data.length).getLong(0);
        long actual = Murmur3.hash64(seed, Slices.wrappedBuffer(data), 0, data.length);

        assertEquals(actual, expected);
    }

    @Test(invocationCount = 100)
    public void test64ReturnsMsb()
            throws Exception
    {
        byte[] data = randomBytes(ThreadLocalRandom.current().nextInt(200));

        long expected = Murmur3.hash(Slices.wrappedBuffer(data)).getLong(0);
        long actual = Murmur3.hash64(Slices.wrappedBuffer(data));

        assertEquals(actual, expected);
    }

    @Test(invocationCount = 100)
    public void testSingleLong()
            throws Exception
    {
        long value = ThreadLocalRandom.current().nextLong();

        Slice slice = Slices.allocate(8);
        slice.setLong(0, value);
        long expected = Murmur3.hash64(slice);
        long actual = Murmur3.hash64(value);
        assertEquals(actual, expected);
    }

    private static byte[] randomBytes(int length)
    {
        byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }
}
