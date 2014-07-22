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
        hash = updateTail(hash, value);
        hash = finalShuffle(hash);

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
                v1 = mix(v1, unsafe.getLong(base, index));
                index += 8;

                v2 = mix(v2, unsafe.getLong(base, index));
                index += 8;

                v3 = mix(v3, unsafe.getLong(base, index));
                index += 8;

                v4 = mix(v4, unsafe.getLong(base, index));
                index += 8;
            }
            while (index <= limit);

            hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);

            v1 = mix(0, v1);
            hash = update(hash, v1);

            v2 = mix(0, v2);
            hash = update(hash, v2);

            v3 = mix(0, v3);
            hash = update(hash, v3);

            v4 = mix(0, v4);
            hash = update(hash, v4);
        }
        else {
            hash = seed + PRIME64_5;
        }

        hash += length;

        while (index <= end - 8) {
            hash = updateTail(hash, unsafe.getLong(base, index));
            index += 8;
        }

        if (index <= end - 4) {
            hash = updateTail(hash, unsafe.getInt(base, index));
            index += 4;
        }

        while (index < end) {
            hash = updateTail(hash, unsafe.getByte(base, index));
            index++;
        }

        hash = finalShuffle(hash);

        return hash;
    }

    private static long mix(long current, long value)
    {
        return rotateLeft(current + value * PRIME64_2, 31) * PRIME64_1;
    }

    private static long update(long hash, long value)
    {
        long temp = hash ^ value;
        return temp * PRIME64_1 + PRIME64_4;
    }

    private static long updateTail(long hash, long value)
    {
        long temp = hash ^ mix(0, value);
        return rotateLeft(temp, 27) * PRIME64_1 + PRIME64_4;
    }

    private static long updateTail(long hash, int value)
    {
        long unsigned = value & 0xFFFF_FFFFL;
        long temp = hash ^ (unsigned * PRIME64_1);
        return rotateLeft(temp, 23) * PRIME64_2 + PRIME64_3;
    }

    private static long updateTail(long hash, byte value)
    {
        int unsigned = value & 0xFF;
        long temp = hash ^ (unsigned * PRIME64_5);
        return rotateLeft(temp, 11) * PRIME64_1;
    }

    private static long finalShuffle(long hash)
    {
        hash ^= hash >>> 33;
        hash *= PRIME64_2;
        hash ^= hash >>> 29;
        hash *= PRIME64_3;
        hash ^= hash >>> 32;
        return hash;
    }
}
