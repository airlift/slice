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
package io.airlift.slice.re2;

final class Bitmap256
{
    private final long[] words = new long[4];

    Bitmap256()
    {
        clear();
    }

    void clear()
    {
        for (int i = 0; i < words.length; i++) {
            words[i] = 0;
        }
    }

    boolean test(int c)
    {
        checkRange(c);
        return (words[c >>> 6] & (1L << (c & 63))) != 0;
    }

    void set(int c)
    {
        checkRange(c);
        words[c >>> 6] |= (1L << (c & 63));
    }

    int findNextSetBit(int c)
    {
        checkRange(c);

        int i = c >>> 6;
        long word = words[i] & (-1L << (c & 63));
        if (word != 0) {
            return (i << 6) + findLeastSignificantSetBit(word);
        }

        i++;
        switch (i) {
            case 1:
                if (words[1] != 0) {
                    return (1 << 6) + findLeastSignificantSetBit(words[1]);
                }
                // fall through
            case 2:
                if (words[2] != 0) {
                    return (2 << 6) + findLeastSignificantSetBit(words[2]);
                }
                // fall through
            case 3:
                if (words[3] != 0) {
                    return (3 << 6) + findLeastSignificantSetBit(words[3]);
                }
                // fall through
            default:
                return -1;
        }
    }

    private static int findLeastSignificantSetBit(long value)
    {
        return Long.numberOfTrailingZeros(value);
    }

    private static void checkRange(int byteValue)
    {
        if (byteValue < 0 || byteValue > 255) {
            throw new IllegalArgumentException("byteValue must be in [0,255]: " + byteValue);
        }
    }
}
