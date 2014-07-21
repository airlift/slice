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

import static io.airlift.slice.JvmUtils.unsafe;
import static io.airlift.slice.Preconditions.checkPositionIndexes;
import static java.lang.Long.rotateLeft;

public class XxHash64
{
    private final static long PRIME64_1 = 0x9E3779B185EBCA87L;
    private final static long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
    private final static long PRIME64_3 = 0x165667B19E3779F9L;
    private final static long PRIME64_4 = 0x85EBCA77C2b2AE63L;
    private final static long PRIME64_5 = 0x27D4EB2F165667C5L;

    private final static long DEFAULT_SEED = 0;

    public static long hash(long value)
    {
        long hash = DEFAULT_SEED + PRIME64_5 + SizeOf.SIZE_OF_LONG;
        hash ^= mix(value);
        hash = rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;

        hash ^= hash >>> 33;
        hash *= PRIME64_2;
        hash ^= hash >>> 29;
        hash *= PRIME64_3;
        hash ^= hash >>> 32;

        return hash;
    }

    public static long hash(Slice data)
    {
        return hash(data, 0, data.length());
    }

    public static long hash(long seed, Slice data)
    {
        return hash(seed, data, 0, data.length());
    }

    public static long hash(Slice data, int offset, int length)
    {
        return hash(DEFAULT_SEED, data, offset, length);
    }

    public static long hash(long seed, Slice data, int offset, int length)
    {
        checkPositionIndexes(0, offset + length, data.length());

        Object base = data.getBase();
        long index = data.getAddress() + offset;
        long end = index + length;

        long hash;

        if (length >= 32) {
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed + 0;
            long v4 = seed - PRIME64_1;

            long limit = end - 32;
            do {
                v1 = readAndMix(base, index, v1);
                index += 8;

                v2 = readAndMix(base, index, v2);
                index += 8;

                v3 = readAndMix(base, index, v3);
                index += 8;

                v4 = readAndMix(base, index, v4);
                index += 8;
            }
            while (index <= limit);

            hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);

            v1 = mix(v1);
            hash ^= v1;
            hash = hash * PRIME64_1 + PRIME64_4;

            v2 = mix(v2);
            hash ^= v2;
            hash = hash * PRIME64_1 + PRIME64_4;

            v3 = mix(v3);
            hash ^= v3;
            hash = hash * PRIME64_1 + PRIME64_4;

            v4 = mix(v4);
            hash ^= v4;
            hash = hash * PRIME64_1 + PRIME64_4;
        }
        else {
            hash = seed + PRIME64_5;
        }

        hash += length;

        while (index <= end - 8) {
            long k1 = unsafe.getLong(base, index);
            k1 = mix(k1);
            hash ^= k1;
            hash = rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
            index += 8;
        }

        if (index <= end - 4) {
            hash ^= (unsafe.getInt(base, index) & 0xFFFF_FFFFL) * PRIME64_1;
            hash = rotateLeft(hash, 23) * PRIME64_2 + PRIME64_3;
            index += 4;
        }

        while (index < end) {
            hash ^= (unsafe.getByte(base, index) & 0xFF) * PRIME64_5;
            hash = rotateLeft(hash, 11) * PRIME64_1;
            index++;
        }

        hash ^= hash >>> 33;
        hash *= PRIME64_2;
        hash ^= hash >>> 29;
        hash *= PRIME64_3;
        hash ^= hash >>> 32;

        return hash;
    }

    private static long mix(long value)
    {
        value *= PRIME64_2;
        value = rotateLeft(value, 31);
        value *= PRIME64_1;
        return value;
    }

    private static long readAndMix(Object base, long index, long value)
    {
        value += unsafe.getLong(base, index) * PRIME64_2;
        value = rotateLeft(value, 31);
        value *= PRIME64_1;
        return value;
    }
}
