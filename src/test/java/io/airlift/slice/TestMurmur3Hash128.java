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
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMurmur3Hash128
{
    @RepeatedTest(100)
    public void testLessThan16Bytes()
    {
        int length = ThreadLocalRandom.current().nextInt(16);
        Slice data = Slices.random(length);

        HashCode expected = Hashing.murmur3_128().hashBytes(data.byteArray());
        Slice actual = Murmur3Hash128.hash(data);

        assertThat(actual.getBytes()).isEqualTo(expected.asBytes());
    }

    @RepeatedTest(100)
    public void testMoreThan16Bytes()
    {
        Slice data = Slices.random(131);

        HashCode expected = Hashing.murmur3_128().hashBytes(data.byteArray());
        Slice actual = Murmur3Hash128.hash(data);

        assertThat(actual.getBytes()).isEqualTo(expected.asBytes());
    }

    @RepeatedTest(100)
    public void testOffsetAndLength()
    {
        Slice data = Slices.random(131);

        int offset = 13;
        int length = 55;

        HashCode expected = Hashing.murmur3_128().hashBytes(data.byteArray(), offset, length);
        Slice actual = Murmur3Hash128.hash(data, offset, length);

        assertThat(actual.getBytes()).isEqualTo(expected.asBytes());
    }

    @RepeatedTest(100)
    public void testNonDefaultSeed()
    {
        Slice data = Slices.random(131);

        int seed = 123456789;

        HashCode expected = Hashing.murmur3_128(seed).hashBytes(data.byteArray());
        Slice actual = Murmur3Hash128.hash(seed, data, 0, data.length());

        assertThat(actual.getBytes()).isEqualTo(expected.asBytes());
    }

    @Test
    public void testTail()
    {
        for (int i = 0; i < 16; i++) {
            Slice data = Slices.random(50 + i);

            HashCode expected = Hashing.murmur3_128().hashBytes(data.byteArray());
            Slice actual = Murmur3Hash128.hash(data);

            assertThat(actual.getBytes()).isEqualTo(expected.asBytes());
        }
    }

    @RepeatedTest(100)
    public void testLessThan16Bytes64()
    {
        int length = ThreadLocalRandom.current().nextInt(16);
        Slice data = Slices.random(length);

        long expected = Murmur3Hash128.hash(data).getLong(0);
        long actual = Murmur3Hash128.hash64(data);

        assertThat(actual).isEqualTo(expected);
    }

    @RepeatedTest(100)
    public void testMoreThan16Bytes64()
    {
        Slice data = Slices.random(131);

        long expected = Murmur3Hash128.hash(data).getLong(0);
        long actual = Murmur3Hash128.hash64(data);

        assertThat(actual).isEqualTo(expected);
    }

    @RepeatedTest(100)
    public void testOffsetAndLength64()
    {
        Slice data = Slices.random(131);

        int offset = 13;
        int length = 55;

        long expected = Murmur3Hash128.hash(data, offset, length).getLong(0);
        long actual = Murmur3Hash128.hash64(data, offset, length);

        assertThat(actual).isEqualTo(expected);
    }

    @RepeatedTest(100)
    public void testNonDefaultSeed64()
    {
        Slice data = Slices.random(131);

        int seed = 123456789;

        long expected = Murmur3Hash128.hash(seed, data, 0, data.length()).getLong(0);
        long actual = Murmur3Hash128.hash64(seed, data, 0, data.length());

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testTail64()
    {
        for (int i = 0; i < 16; i++) {
            Slice data = Slices.random(50 + i);

            long expected = Murmur3Hash128.hash(data).getLong(0);
            long actual = Murmur3Hash128.hash64(data);

            assertThat(actual).isEqualTo(expected);
        }
    }

    @RepeatedTest(100)
    public void test64ReturnsMsb()
    {
        int length = ThreadLocalRandom.current().nextInt(200);
        Slice data = Slices.random(length);

        long expected = Murmur3Hash128.hash(data).getLong(0);
        long actual = Murmur3Hash128.hash64(data);

        assertThat(actual).isEqualTo(expected);
    }

    @RepeatedTest(100)
    public void testSingleLong()
    {
        long value = ThreadLocalRandom.current().nextLong();

        Slice slice = Slices.allocate(8);
        slice.setLong(0, value);
        long expected = Murmur3Hash128.hash64(slice);
        long actual = Murmur3Hash128.hash64(value);
        assertThat(actual).isEqualTo(expected);
    }
}
