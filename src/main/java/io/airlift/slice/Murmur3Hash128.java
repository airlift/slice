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

public class Murmur3Hash128
{
    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    private final static long DEFAULT_SEED = 0;

    public static Slice hash(Slice data)
    {
        return hash(data, 0, data.length());
    }

    public static Slice hash(Slice data, int offset, int length)
    {
        return hash(DEFAULT_SEED, data, offset, length);
    }

    @SuppressFBWarnings("SF_SWITCH_NO_DEFAULT")
    public static Slice hash(long seed, Slice data, int offset, int length)
    {
        final int fastLimit = offset + length - (2 * SizeOf.SIZE_OF_LONG) + 1;

        long h1 = seed;
        long h2 = seed;

        int current = offset;
        while (current < fastLimit) {
            long k1 = data.getLong(current);
            current += SizeOf.SIZE_OF_LONG;

            long k2 = data.getLong(current);
            current += SizeOf.SIZE_OF_LONG;

            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            h1 ^= k1;

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729L;

            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            h2 ^= k2;

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5L;
        }

        long k1 = 0;
        long k2 = 0;

        switch (length & 15) {
            case 15:
                k2 ^= ((long) data.getUnsignedByte(current + 14)) << 48;
            case 14:
                k2 ^= ((long) data.getUnsignedByte(current + 13)) << 40;
            case 13:
                k2 ^= ((long) data.getUnsignedByte(current + 12)) << 32;
            case 12:
                k2 ^= ((long) data.getUnsignedByte(current + 11)) << 24;
            case 11:
                k2 ^= ((long) data.getUnsignedByte(current + 10)) << 16;
            case 10:
                k2 ^= ((long) data.getUnsignedByte(current + 9)) << 8;
            case 9:
                k2 ^= ((long) data.getUnsignedByte(current + 8)) << 0;

                k2 *= C2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= C1;
                h2 ^= k2;

            case 8:
                k1 ^= ((long) data.getUnsignedByte(current + 7)) << 56;
            case 7:
                k1 ^= ((long) data.getUnsignedByte(current + 6)) << 48;
            case 6:
                k1 ^= ((long) data.getUnsignedByte(current + 5)) << 40;
            case 5:
                k1 ^= ((long) data.getUnsignedByte(current + 4)) << 32;
            case 4:
                k1 ^= ((long) data.getUnsignedByte(current + 3)) << 24;
            case 3:
                k1 ^= ((long) data.getUnsignedByte(current + 2)) << 16;
            case 2:
                k1 ^= ((long) data.getUnsignedByte(current + 1)) << 8;
            case 1:
                k1 ^= ((long) data.getUnsignedByte(current + 0)) << 0;

                k1 *= C1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= C2;
                h1 ^= k1;
        }

        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = mix64(h1);
        h2 = mix64(h2);

        h1 += h2;
        h2 += h1;

        Slice result = Slices.allocate(16);
        result.setLong(0, h1);
        result.setLong(8, h2);

        return result;
    }

    /**
     * Returns the 64 most significant bits of the Murmur128 hash of the provided value
     */
    public static long hash64(Slice data)
    {
        return hash64(data, 0, data.length());
    }

    public static long hash64(Slice data, int offset, int length)
    {
        return hash64(DEFAULT_SEED, data, offset, length);
    }

    @SuppressFBWarnings({"SF_SWITCH_NO_DEFAULT", "SF_SWITCH_FALLTHROUGH"})
    public static long hash64(long seed, Slice data, int offset, int length)
    {
        final int fastLimit = offset + length - (2 * SizeOf.SIZE_OF_LONG) + 1;

        long h1 = seed;
        long h2 = seed;

        int current = offset;
        while (current < fastLimit) {
            long k1 = data.getLong(current);
            current += SizeOf.SIZE_OF_LONG;

            long k2 = data.getLong(current);
            current += SizeOf.SIZE_OF_LONG;

            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            h1 ^= k1;

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729L;

            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            h2 ^= k2;

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5L;
        }

        long k1 = 0;
        long k2 = 0;

        switch (length & 15) {
            case 15:
                k2 ^= ((long) data.getUnsignedByte(current + 14)) << 48;
            case 14:
                k2 ^= ((long) data.getUnsignedByte(current + 13)) << 40;
            case 13:
                k2 ^= ((long) data.getUnsignedByte(current + 12)) << 32;
            case 12:
                k2 ^= ((long) data.getUnsignedByte(current + 11)) << 24;
            case 11:
                k2 ^= ((long) data.getUnsignedByte(current + 10)) << 16;
            case 10:
                k2 ^= ((long) data.getUnsignedByte(current + 9)) << 8;
            case 9:
                k2 ^= ((long) data.getUnsignedByte(current + 8)) << 0;

                k2 *= C2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= C1;
                h2 ^= k2;

            case 8:
                k1 ^= ((long) data.getUnsignedByte(current + 7)) << 56;
            case 7:
                k1 ^= ((long) data.getUnsignedByte(current + 6)) << 48;
            case 6:
                k1 ^= ((long) data.getUnsignedByte(current + 5)) << 40;
            case 5:
                k1 ^= ((long) data.getUnsignedByte(current + 4)) << 32;
            case 4:
                k1 ^= ((long) data.getUnsignedByte(current + 3)) << 24;
            case 3:
                k1 ^= ((long) data.getUnsignedByte(current + 2)) << 16;
            case 2:
                k1 ^= ((long) data.getUnsignedByte(current + 1)) << 8;
            case 1:
                k1 ^= ((long) data.getUnsignedByte(current + 0)) << 0;

                k1 *= C1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= C2;
                h1 ^= k1;
        }

        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = mix64(h1);
        h2 = mix64(h2);

        return h1 + h2;
    }

    /**
     * Special-purpose version for hashing a single long value. Value is treated as little-endian
     */
    public static long hash64(long value)
    {
        long h2 = DEFAULT_SEED ^ SizeOf.SIZE_OF_LONG;
        long h1 = h2 + (h2 ^ (Long.rotateLeft(value * C1, 31) * C2));

        return mix64(h1) + mix64(h1 + h2);
    }

    private static long mix64(long k)
    {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;

        return k;
    }
}
