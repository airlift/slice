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

import static io.airlift.slice.re2.CharClass.RUNEMAX;

/**
 * JVM-derived simple case-equivalence cycles used while compiling patterns.
 */
final class UnicodeCaseFold
{
    private UnicodeCaseFold() {}

    public static int cycleFoldRune(int rune)
    {
        if (rune < 0 || rune > RUNEMAX) {
            return rune;
        }
        return DataHolder.DATA.next()[rune];
    }

    public static int nextCaseFoldedRuneAtOrAfter(int rune)
    {
        int[] caseFoldedRunes = DataHolder.DATA.caseFoldedRunes();
        int index = Arrays.binarySearch(caseFoldedRunes, rune);
        if (index < 0) {
            index = -index - 1;
        }
        return index < caseFoldedRunes.length ? caseFoldedRunes[index] : -1;
    }

    private static Data build()
    {
        int[] first = new int[RUNEMAX + 1];
        int[] last = new int[RUNEMAX + 1];
        int[] next = new int[RUNEMAX + 1];
        Arrays.fill(first, -1);

        for (int codePoint = 0; codePoint <= RUNEMAX; codePoint++) {
            int canonicalCodePoint = canonicalCaseFold(codePoint);
            if (first[canonicalCodePoint] < 0) {
                first[canonicalCodePoint] = codePoint;
            }
            else {
                next[last[canonicalCodePoint]] = codePoint;
            }
            last[canonicalCodePoint] = codePoint;
        }

        for (int canonicalCodePoint = 0; canonicalCodePoint <= RUNEMAX; canonicalCodePoint++) {
            if (first[canonicalCodePoint] >= 0) {
                next[last[canonicalCodePoint]] = first[canonicalCodePoint];
            }
        }

        int caseFoldedRuneCount = 0;
        for (int codePoint = 0; codePoint <= RUNEMAX; codePoint++) {
            if (next[codePoint] != codePoint) {
                caseFoldedRuneCount++;
            }
        }
        int[] caseFoldedRunes = new int[caseFoldedRuneCount];
        int index = 0;
        for (int codePoint = 0; codePoint <= RUNEMAX; codePoint++) {
            if (next[codePoint] != codePoint) {
                caseFoldedRunes[index++] = codePoint;
            }
        }
        return new Data(next, caseFoldedRunes);
    }

    private static int canonicalCaseFold(int codePoint)
    {
        return Character.toLowerCase(Character.toUpperCase(codePoint));
    }

    private static final class DataHolder
    {
        private static final Data DATA = build();
    }

    private record Data(int[] next, int[] caseFoldedRunes) {}
}
