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
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMurmur3Hash32
{
    @RepeatedTest(100)
    public void testLessThan4Bytes()
    {
        byte[] data = randomBytes(ThreadLocalRandom.current().nextInt(4));

        int expected = Hashing.murmur3_32_fixed().hashBytes(data).asInt();
        int actual = Murmur3Hash32.hash(Slices.wrappedBuffer(data));

        assertThat(actual).isEqualTo(expected);
    }

    @RepeatedTest(100)
    public void testMoreThan4Bytes()
    {
        byte[] data = randomBytes(131);

        int expected = Hashing.murmur3_32_fixed().hashBytes(data).asInt();
        int actual = Murmur3Hash32.hash(Slices.wrappedBuffer(data));

        assertThat(actual).isEqualTo(expected);
    }

    @RepeatedTest(100)
    public void testOffsetAndLength()
    {
        byte[] data = randomBytes(131);

        int offset = 13;
        int length = 55;

        int expected = Hashing.murmur3_32_fixed().hashBytes(data, offset, length).asInt();
        int actual = Murmur3Hash32.hash(Slices.wrappedBuffer(data), offset, length);

        assertThat(actual).isEqualTo(expected);
    }

    @RepeatedTest(100)
    public void testNonDefaultSeed()
    {
        byte[] data = randomBytes(131);

        int seed = 123456789;

        int expected = Hashing.murmur3_32_fixed(seed).hashBytes(data).asInt();
        int actual = Murmur3Hash32.hash(seed, Slices.wrappedBuffer(data), 0, data.length);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testTail()
    {
        for (int i = 0; i < 4; i++) {
            byte[] data = randomBytes(50 + i);

            int expected = Hashing.murmur3_32_fixed().hashBytes(data).asInt();
            int actual = Murmur3Hash32.hash(Slices.wrappedBuffer(data));

            assertThat(actual).isEqualTo(expected);
        }
    }

    @RepeatedTest(100)
    public void testSingleInt()
    {
        int value = ThreadLocalRandom.current().nextInt();

        Slice slice = Slices.allocate(4);
        slice.setInt(0, value);
        int expected = Murmur3Hash32.hash(slice);
        int actual = Murmur3Hash32.hash(value);
        assertThat(actual).isEqualTo(expected);
    }

    @RepeatedTest(100)
    public void testSingleLong()
    {
        long value = ThreadLocalRandom.current().nextLong();

        Slice slice = Slices.allocate(8);
        slice.setLong(0, value);
        int expected = Murmur3Hash32.hash(slice);
        int actual = Murmur3Hash32.hash(value);
        assertThat(actual).isEqualTo(expected);
    }

    private static byte[] randomBytes(int length)
    {
        byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }
}
