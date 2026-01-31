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

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static java.lang.Long.rotateLeft;
import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;

public final class XxHash64
{
    private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME64_3 = 0x165667B19E3779F9L;
    private static final long PRIME64_4 = 0x85EBCA77C2b2AE63L;
    private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

    private static final long DEFAULT_SEED = 0;

    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private final long seed;

    private final byte[] buffer = new byte[32];
    private int bufferSize;

    private long bodyLength;

    private long v1;
    private long v2;
    private long v3;
    private long v4;

    public XxHash64()
    {
        this(DEFAULT_SEED);
    }

    public XxHash64(long seed)
    {
        this.seed = seed;
        this.v1 = seed + PRIME64_1 + PRIME64_2;
        this.v2 = seed + PRIME64_2;
        this.v3 = seed;
        this.v4 = seed - PRIME64_1;
    }

    public XxHash64 update(byte[] data)
    {
        return update(data, 0, data.length);
    }

    public XxHash64 update(byte[] data, int offset, int length)
    {
        checkFromIndexSize(offset, length, data.length);
        updateHash(data, offset, length);
        return this;
    }

    public XxHash64 update(Slice data)
    {
        return update(data, 0, data.length());
    }

    public XxHash64 update(Slice data, int offset, int length)
    {
        checkFromIndexSize(offset, length, data.length());
        updateHash(data.byteArray(), data.byteArrayOffset() + offset, length);
        return this;
    }

    public long hash()
    {
        long hash;
        if (bodyLength > 0) {
            hash = computeBody();
        }
        else {
            hash = seed + PRIME64_5;
        }

        hash += bodyLength + bufferSize;

        return updateTail(hash, buffer, 0, bufferSize);
    }

    private long computeBody()
    {
        long hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);

        hash = update(hash, v1);
        hash = update(hash, v2);
        hash = update(hash, v3);
        hash = update(hash, v4);

        return hash;
    }

    private void updateHash(byte[] base, int offset, int length)
    {
        if (bufferSize > 0) {
            int available = min(32 - bufferSize, length);

            System.arraycopy(base, offset, buffer, bufferSize, available);

            bufferSize += available;
            offset += available;
            length -= available;

            if (bufferSize == 32) {
                updateBody(buffer, 0, bufferSize);
                bufferSize = 0;
            }
        }

        if (length >= 32) {
            int index = updateBody(base, offset, length);
            offset += index;
            length -= index;
        }

        if (length > 0) {
            System.arraycopy(base, offset, buffer, 0, length);
            bufferSize = length;
        }
    }

    private int updateBody(byte[] base, int offset, int length)
    {
        int remaining = length;
        while (remaining >= 32) {
            v1 = mix(v1, (long) LONG_HANDLE.get(base, offset));
            v2 = mix(v2, (long) LONG_HANDLE.get(base, offset + 8));
            v3 = mix(v3, (long) LONG_HANDLE.get(base, offset + 16));
            v4 = mix(v4, (long) LONG_HANDLE.get(base, offset + 24));

            offset += 32;
            remaining -= 32;
        }

        int index = length - remaining;
        bodyLength += index;
        return index;
    }

    public static long hash(long value)
    {
        return hash(DEFAULT_SEED, value);
    }

    public static long hash(long seed, long value)
    {
        long hash = seed + PRIME64_5 + SizeOf.SIZE_OF_LONG;
        hash = updateTail(hash, value);
        hash = finalShuffle(hash);

        return hash;
    }

    public static long hash(InputStream in)
            throws IOException
    {
        return hash(DEFAULT_SEED, in);
    }

    public static long hash(long seed, InputStream in)
            throws IOException
    {
        XxHash64 hash = new XxHash64(seed);
        byte[] buffer = new byte[8192];
        while (true) {
            int length = in.read(buffer);
            if (length == -1) {
                break;
            }
            hash.update(buffer, 0, length);
        }
        return hash.hash();
    }

    public static long hash(byte[] data)
    {
        return hash(DEFAULT_SEED, data, 0, data.length);
    }

    public static long hash(byte[] data, int offset, int length)
    {
        return hash(DEFAULT_SEED, data, offset, length);
    }

    public static long hash(long seed, byte[] data, int offset, int length)
    {
        checkFromIndexSize(offset, length, data.length);
        return hashBytes(seed, data, offset, length);
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
        checkFromIndexSize(offset, length, data.length());
        return hashBytes(seed, data.byteArray(), data.byteArrayOffset() + offset, length);
    }

    private static long hashBytes(long seed, byte[] base, int index, int length)
    {
        int end = index + length;
        long hash;

        if (length >= 32) {
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed;
            long v4 = seed - PRIME64_1;

            while (index <= end - 32) {
                v1 = mix(v1, (long) LONG_HANDLE.get(base, index));
                v2 = mix(v2, (long) LONG_HANDLE.get(base, index + 8));
                v3 = mix(v3, (long) LONG_HANDLE.get(base, index + 16));
                v4 = mix(v4, (long) LONG_HANDLE.get(base, index + 24));
                index += 32;
            }

            hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
            hash = update(hash, v1);
            hash = update(hash, v2);
            hash = update(hash, v3);
            hash = update(hash, v4);
        }
        else {
            hash = seed + PRIME64_5;
        }

        hash += length;

        // Process remaining bytes
        while (index <= end - 8) {
            hash = updateTail(hash, (long) LONG_HANDLE.get(base, index));
            index += 8;
        }

        if (index <= end - 4) {
            hash = updateTail(hash, (int) INT_HANDLE.get(base, index));
            index += 4;
        }

        while (index < end) {
            hash = updateTail(hash, base[index]);
            index++;
        }

        return finalShuffle(hash);
    }

    private static long updateTail(long hash, byte[] base, int index, int length)
    {
        int end = index + length;

        while (index <= end - 8) {
            hash = updateTail(hash, (long) LONG_HANDLE.get(base, index));
            index += 8;
        }

        if (index <= end - 4) {
            hash = updateTail(hash, (int) INT_HANDLE.get(base, index));
            index += 4;
        }

        while (index < end) {
            hash = updateTail(hash, base[index]);
            index++;
        }

        return finalShuffle(hash);
    }

    private static long mix(long current, long value)
    {
        return rotateLeft(current + value * PRIME64_2, 31) * PRIME64_1;
    }

    private static long update(long hash, long value)
    {
        long temp = hash ^ mix(0, value);
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
