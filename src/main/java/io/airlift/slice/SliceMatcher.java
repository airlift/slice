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

import java.util.Arrays;

import static java.lang.Integer.max;

public class SliceMatcher
{
    private static final int MIN_PATTERN_LENGTH = 20;
    private static final int MIN_DATA_LENGTH = 150;
    private final Slice pattern;
    private final int[] badCharacterTable;
    private final int[] goodCharacterTable;

    public static SliceMatcher sliceMatcher(Slice pattern)
    {
        // the default indexOf implementation is faster for small patterns
        if (pattern.length() < MIN_PATTERN_LENGTH) {
            return new SliceMatcher(pattern, null, null);
        }
        return createSliceMatcherInternal(pattern);
    }

    static SliceMatcher createSliceMatcherInternal(Slice pattern)
    {
        return new SliceMatcher(pattern, computeBadCharacterTable(pattern), computeGoodCharTable(pattern));
    }

    private SliceMatcher(Slice pattern, int[] badCharacterTable, int[] goodCharacterTable)
    {
        this.pattern = pattern;
        this.badCharacterTable = badCharacterTable;
        this.goodCharacterTable = goodCharacterTable;
    }

    public int length() {return pattern.length();}

    public int find(Slice string)
    {
        return find(string, 0);
    }

    public int find(Slice string, int offset)
    {
        // the default implementation is faster for smaller data
        if (pattern.length() < MIN_PATTERN_LENGTH || string.length() < MIN_DATA_LENGTH) {
            return string.indexOf(pattern, offset);
        }

        return findInternal(string, offset);
    }

    int findInternal(Slice string, int offset)
    {
        if (string.length() == 0 || offset >= string.length()) {
            return -1;
        }

        int position = offset;
        while (position <= string.length() - pattern.length()) {
            int matchRemaining;
            for (matchRemaining = pattern.length() - 1; pattern.getByteUnchecked(matchRemaining) == string.getByteUnchecked(position + matchRemaining); matchRemaining--) {
                if (matchRemaining == 0) {
                    return position;
                }
            }
            position += max(goodCharacterTable[matchRemaining + 1],
                    badCharacterTable[string.getByteUnchecked(position + matchRemaining) & 0xFF] - pattern.length() + matchRemaining + 1);
        }

        return -1;
    }

    private static int[] computeBadCharacterTable(Slice pattern)
    {
        int[] badCharacter = new int[256];

        // default jump size on mismatch is the size of the pattern
        Arrays.fill(badCharacter, pattern.length());

        // if the character is contained in the pattern, jump distance from
        // character to end of pattern
        for (int index = 0; index < pattern.length() - 1; index++) {
            badCharacter[pattern.getByteUnchecked(index) & 0xFF] = pattern.length() - index - 1;
        }
        return badCharacter;
    }

    private static int[] computeGoodCharTable(Slice pattern)
    {
        int[] result = new int[pattern.length() + 1];
        int[] f = new int[pattern.length() + 1];
        int j = pattern.length() + 1;

        f[pattern.length()] = j;

        for (int i = pattern.length(); i > 0; i--) {
            while (j <= pattern.length() && pattern.getByteUnchecked(i - 1) != pattern.getByteUnchecked(j - 1)) {
                if (result[j] == 0) {
                    result[j] = j - i;
                }
                j = f[j];
            }
            f[i - 1] = --j;
        }

        int p = f[0];
        for (j = 0; j <= pattern.length(); ++j) {
            if (result[j] == 0) {
                result[j] = p;
            }
            if (j == p) {
                p = f[p];
            }
        }
        return result;
    }
}
