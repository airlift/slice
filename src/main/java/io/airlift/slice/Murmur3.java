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

@Deprecated
public final class Murmur3
{
    private Murmur3() {}

    public static Slice hash(Slice data)
    {
        return Murmur3Hash128.hash(data);
    }

    public static Slice hash(Slice data, int offset, int length)
    {
        return Murmur3Hash128.hash(data, offset, length);
    }

    public static Slice hash(long seed, Slice data, int offset, int length)
    {
        return Murmur3Hash128.hash(seed, data, offset, length);
    }

    /**
     * Returns the 64 most significant bits of the Murmur128 hash of the provided value
     */
    public static long hash64(Slice data)
    {
        return Murmur3Hash128.hash64(data);
    }

    public static long hash64(Slice data, int offset, int length)
    {
        return Murmur3Hash128.hash64(data, offset, length);
    }

    public static long hash64(long seed, Slice data, int offset, int length)
    {
        return Murmur3Hash128.hash64(seed, data, offset, length);
    }

    /**
     * Special-purpose version for hashing a single long value. Value is treated as little-endian
     */
    public static long hash64(long value)
    {
        return Murmur3Hash128.hash64(value);
    }
}
