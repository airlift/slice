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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.airlift.slice.re2.CharClass.RUNEMAX;
import static java.lang.Math.max;
import static java.lang.Math.min;

@SuppressWarnings("CharUsedInArithmeticContext")
final class CharClassBuilder
{
    private static final int ALPHA_MASK = (1 << 26) - 1;

    private final List<RuneRange> ranges = new ArrayList<>();
    private int runeCount;

    // ASCII A-Z and a-z coverage bitmasks (bit 0 = 'A'/'a').
    private int upper;
    private int lower;

    public boolean isEmpty()
    {
        return ranges.isEmpty();
    }

    public int runeCount()
    {
        return runeCount;
    }

    public int rangeCount()
    {
        return ranges.size();
    }

    public RuneRange range(int index)
    {
        return ranges.get(index);
    }

    public boolean contains(int rune)
    {
        // Search by an upper-bound key so ranges with matching low endpoint stay on the left.
        int index = Collections.binarySearch(ranges, new RuneRange(rune, Integer.MAX_VALUE));
        int candidateIndex = (index >= 0) ? index : (-index - 2);
        return candidateIndex >= 0 && ranges.get(candidateIndex).contains(rune);
    }

    public void addRange(int low, int high)
    {
        if (high < low) {
            return;
        }

        if (low <= 'z' && high >= 'A') {
            updateAsciiBitmaps(low, high);
        }

        // Merge/insert into a sorted, non-overlapping, non-adjacent list.
        int insertAt = 0;
        while (insertAt < ranges.size() && ranges.get(insertAt).high() + 1 < low) {
            insertAt++;
        }

        int newLow = low;
        int newHigh = high;
        int removeFrom = insertAt;
        int removeTo = insertAt;
        while (removeTo < ranges.size() && ranges.get(removeTo).low() <= newHigh + 1) {
            RuneRange runeRange = ranges.get(removeTo);
            newLow = min(newLow, runeRange.low());
            newHigh = max(newHigh, runeRange.high());
            removeTo++;
        }

        // Update runeCount: remove any merged ranges, then add the new merged range.
        for (int removeIndex = removeFrom; removeIndex < removeTo; removeIndex++) {
            RuneRange runeRange = ranges.get(removeIndex);
            runeCount -= runeRange.high() - runeRange.low() + 1;
        }
        runeCount += newHigh - newLow + 1;

        if (removeFrom != removeTo) {
            ranges.subList(removeFrom, removeTo).clear();
        }
        ranges.add(removeFrom, new RuneRange(newLow, newHigh));
    }

    // Adds all ranges from another character class builder.
    public void addCharClass(CharClassBuilder other)
    {
        for (RuneRange runeRange : other.ranges) {
            addRange(runeRange.low(), runeRange.high());
        }
    }

    public CharClassBuilder copy()
    {
        CharClassBuilder copy = new CharClassBuilder();
        copy.ranges.addAll(ranges);
        copy.runeCount = runeCount;
        copy.upper = upper;
        copy.lower = lower;
        return copy;
    }

    public void addRangeFlags(int low, int high, int parseFlags)
    {
        // Exclude \n unless CLASS_NEWLINE is set, or NEVER_NEWLINE forces exclusion.
        boolean excludeNewline = ((parseFlags & Regexp.CLASS_NEWLINE) == 0) || ((parseFlags & Regexp.NEVER_NEWLINE) != 0);
        if (excludeNewline && low <= '\n' && '\n' <= high) {
            if (low < '\n') {
                addRangeFlags(low, '\n' - 1, parseFlags);
            }
            if (high > '\n') {
                addRangeFlags('\n' + 1, high, parseFlags);
            }
            return;
        }

        // If folding case, add fold-equivalent characters too.
        if ((parseFlags & Regexp.FOLD_CASE) != 0) {
            if ((parseFlags & Regexp.LATIN1) != 0) {
                addFoldedRangeLatin1(low, high);
            }
            else {
                addFoldedRange(low, high);
            }
            return;
        }

        addRange(low, high);
    }

    private void addFoldedRangeLatin1(int low, int high)
    {
        while (low <= high) {
            addRange(low, low);
            if ('A' <= low && low <= 'Z') {
                addRange(low - 'A' + 'a', low - 'A' + 'a');
            }
            if ('a' <= low && low <= 'z') {
                addRange(low - 'a' + 'A', low - 'a' + 'A');
            }
            low++;
        }
    }

    private void addFoldedRange(int low, int high)
    {
        addRange(low, high);

        int rune = UnicodeCaseFold.nextCaseFoldedRuneAtOrAfter(low);
        while (rune >= 0 && rune <= high) {
            int foldedRune = UnicodeCaseFold.cycleFoldRune(rune);
            while (foldedRune != rune) {
                addRange(foldedRune, foldedRune);
                foldedRune = UnicodeCaseFold.cycleFoldRune(foldedRune);
            }
            rune = rune == CharClass.RUNEMAX ? -1 : UnicodeCaseFold.nextCaseFoldedRuneAtOrAfter(rune + 1);
        }
    }

    public boolean foldsAscii()
    {
        return ((upper ^ lower) & ALPHA_MASK) == 0;
    }

    public void negate()
    {
        List<RuneRange> negatedRanges = new ArrayList<>();
        int next = 0;
        for (RuneRange runeRange : ranges) {
            if (next < runeRange.low()) {
                negatedRanges.add(new RuneRange(next, runeRange.low() - 1));
            }
            next = runeRange.high() + 1;
        }
        if (next <= RUNEMAX) {
            negatedRanges.add(new RuneRange(next, RUNEMAX));
        }

        ranges.clear();
        ranges.addAll(negatedRanges);

        runeCount = (RUNEMAX + 1) - runeCount;
        upper = ALPHA_MASK & ~upper;
        lower = ALPHA_MASK & ~lower;
    }

    public void removeAbove(int runeLimit)
    {
        if (ranges.isEmpty()) {
            return;
        }

        int rangeIndex = 0;
        while (rangeIndex < ranges.size()) {
            RuneRange runeRange = ranges.get(rangeIndex);
            if (runeRange.low() > runeLimit) {
                // remove whole suffix
                for (int removeIndex = rangeIndex; removeIndex < ranges.size(); removeIndex++) {
                    RuneRange removed = ranges.get(removeIndex);
                    runeCount -= removed.high() - removed.low() + 1;
                }
                ranges.subList(rangeIndex, ranges.size()).clear();
                return;
            }
            if (runeRange.high() > runeLimit) {
                // trim this range
                runeCount -= runeRange.high() - runeRange.low() + 1;
                RuneRange trimmed = new RuneRange(runeRange.low(), runeLimit);
                runeCount += trimmed.high() - trimmed.low() + 1;
                ranges.set(rangeIndex, trimmed);
                // remove rest
                for (int removeIndex = rangeIndex + 1; removeIndex < ranges.size(); removeIndex++) {
                    RuneRange removed = ranges.get(removeIndex);
                    runeCount -= removed.high() - removed.low() + 1;
                }
                ranges.subList(rangeIndex + 1, ranges.size()).clear();
                return;
            }
            rangeIndex++;
        }
    }

    public CharClass toCharClass()
    {
        return new CharClass(foldsAscii(), runeCount, ranges.toArray(RuneRange[]::new));
    }

    private void updateAsciiBitmaps(int low, int high)
    {
        int upperAlphaLow = max(low, 'A');
        int upperAlphaHigh = min(high, 'Z');
        if (upperAlphaLow <= upperAlphaHigh) {
            upper |= maskBits(upperAlphaLow - 'A', upperAlphaHigh - 'A');
        }
        int lowerAlphaLow = max(low, 'a');
        int lowerAlphaHigh = min(high, 'z');
        if (lowerAlphaLow <= lowerAlphaHigh) {
            lower |= maskBits(lowerAlphaLow - 'a', lowerAlphaHigh - 'a');
        }
    }

    private static int maskBits(int from, int to)
    {
        // Inclusive range [from..to] within [0..25].
        int width = to - from + 1;
        int mask = (1 << width) - 1;
        return mask << from;
    }
}
