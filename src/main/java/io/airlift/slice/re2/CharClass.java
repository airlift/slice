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

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

record CharClass(boolean foldsAscii, int runeCount, RuneRange[] ranges)
{
    // util/utf.h: Runemax
    public static final int RUNEMAX = 0x10FFFF;

    public CharClass
    {
        ranges = Arrays.copyOf(requireNonNull(ranges, "ranges is null"), ranges.length);
    }

    @Override
    public RuneRange[] ranges()
    {
        return Arrays.copyOf(ranges, ranges.length);
    }

    public int rangeCount()
    {
        return ranges.length;
    }

    public RuneRange range(int index)
    {
        return ranges[index];
    }

    public boolean isEmpty()
    {
        return ranges.length == 0;
    }

    public boolean contains(int rune)
    {
        // Search by an upper-bound key so ranges with matching low endpoint stay on the left.
        int index = Arrays.binarySearch(ranges, new RuneRange(rune, Integer.MAX_VALUE));
        int candidateIndex = (index >= 0) ? index : (-index - 2);
        return candidateIndex >= 0 && ranges[candidateIndex].contains(rune);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof CharClass that)) {
            return false;
        }
        return foldsAscii == that.foldsAscii &&
                runeCount == that.runeCount &&
                Arrays.equals(ranges, that.ranges);
    }

    @Override
    public int hashCode()
    {
        int result = Boolean.hashCode(foldsAscii);
        result = 31 * result + Integer.hashCode(runeCount);
        result = 31 * result + Arrays.hashCode(ranges);
        return result;
    }
}
