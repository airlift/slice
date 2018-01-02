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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class Murmur3Hash32
{
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private static final int DEFAULT_SEED = 0;

    private Murmur3Hash32() {}

    public static int hash(Slice data)
    {
        return hash(data, 0, data.length());
    }

    public static int hash(Slice data, int offset, int length)
    {
        return hash(DEFAULT_SEED, data, offset, length);
    }

    @SuppressFBWarnings({"SF_SWITCH_NO_DEFAULT", "SF_SWITCH_FALLTHROUGH"})
    public static int hash(int seed, Slice data, int offset, int length)
    {
        final int fastLimit = offset + length - SizeOf.SIZE_OF_INT + 1;

        int h1 = seed;

        int current = offset;
        while (current < fastLimit) {
            int k1 = mixK1(data.getInt(current));
            current += SizeOf.SIZE_OF_INT;
            h1 = mixH1(h1, k1);
        }

        int k1 = 0;

        switch (length & 3) {
            case 3:
                k1 ^= ((int) data.getUnsignedByte(current + 2)) << 16;
            case 2:
                k1 ^= ((int) data.getUnsignedByte(current + 1)) << 8;
            case 1:
                k1 ^= ((int) data.getUnsignedByte(current + 0)) << 0;
        }

        h1 ^= mixK1(k1);

        return fmix(h1, length);
    }

    /**
     * Special-purpose version for hashing a single int value. Value is treated as little-endian
     */
    public static int hash(int input)
    {
        int k1 = mixK1(input);
        int h1 = mixH1(DEFAULT_SEED, k1);

        return fmix(h1, SizeOf.SIZE_OF_INT);
    }

    /**
     * Special-purpose version for hashing a single long value. Value is treated as little-endian
     */
    public static int hash(long input)
    {
        int low = (int) input;
        int high = (int) (input >>> 32);

        int k1 = mixK1(low);
        int h1 = mixH1(DEFAULT_SEED, k1);

        k1 = mixK1(high);
        h1 = mixH1(h1, k1);

        return fmix(h1, SizeOf.SIZE_OF_LONG);
    }

    private static int mixK1(int k1)
    {
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;
        return k1;
    }

    private static int mixH1(int h1, int k1)
    {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
        return h1;
    }

    private static int fmix(int h1, int length)
    {
        h1 ^= length;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }
}
