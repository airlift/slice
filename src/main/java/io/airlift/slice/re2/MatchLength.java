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

import java.util.List;

final class MatchLength
        extends RegexpWalker<Integer>
{
    private static final int MAX_RECURSIVE_DEPTH = 256;

    // Non-negative values are exact byte lengths. A variable length is encoded as
    // -minimum - 1, which lets the recursive hot path propagate one primitive value.
    // Integer.MIN_VALUE therefore represents an impossible Java Slice match as well as
    // a variable minimum of Integer.MAX_VALUE; those cases have identical match behavior.
    private static final int IMPOSSIBLE = Integer.MIN_VALUE;

    record Analysis(int minimum, int fixed)
    {
        int encoded()
        {
            return fixed >= 0 ? fixed : variable(minimum);
        }
    }

    private MatchLength() {}

    public static Analysis analyze(Regexp regexp)
    {
        int encoded = analyzeEncoded(regexp, 0);
        return new Analysis(minimum(encoded), fixed(encoded));
    }

    static int minimum(int encoded)
    {
        return encoded >= 0 ? encoded : -encoded - 1;
    }

    static int fixed(int encoded)
    {
        return encoded >= 0 ? encoded : -1;
    }

    private static int analyzeEncoded(Regexp regexp, int depth)
    {
        if (depth >= MAX_RECURSIVE_DEPTH) {
            return new MatchLength().walk(regexp, 0);
        }

        return switch (regexp.op()) {
            case NO_MATCH -> IMPOSSIBLE;
            case EMPTY_MATCH, BEGIN_LINE, END_LINE, WORD_BOUNDARY, NO_WORD_BOUNDARY, BEGIN_TEXT, END_TEXT, HAVE_MATCH -> 0;
            case LITERAL -> literal(regexp.rune(), regexp.parseFlags());
            case LITERAL_STRING -> literalString(regexp.runes(), regexp.parseFlags());
            case CONCAT -> concatenate(regexp, depth + 1);
            case ALTERNATE -> alternate(regexp, depth + 1);
            case STAR, QUEST -> optional(analyzeEncoded(regexp.sub(0), depth + 1));
            case PLUS -> plus(analyzeEncoded(regexp.sub(0), depth + 1));
            case REPEAT -> repeat(analyzeEncoded(regexp.sub(0), depth + 1), regexp.min(), regexp.max());
            case CAPTURE -> analyzeEncoded(regexp.sub(0), depth + 1);
            case ANY_CHAR -> (regexp.parseFlags() & Regexp.LATIN1) != 0 ? 1 : variable(1);
            case ANY_BYTE -> 1;
            case CHAR_CLASS -> characterClass(regexp.charClass(), regexp.parseFlags());
        };
    }

    @Override
    protected PreVisitResult<Integer> preVisit(Regexp regexp, Integer parentArgument)
    {
        return new PreVisitResult<>(0, false);
    }

    @Override
    protected Integer postVisit(Regexp regexp, Integer parentArgument, Integer preArgument, List<Integer> childLengths)
    {
        return switch (regexp.op()) {
            case NO_MATCH -> IMPOSSIBLE;
            case EMPTY_MATCH, BEGIN_LINE, END_LINE, WORD_BOUNDARY, NO_WORD_BOUNDARY, BEGIN_TEXT, END_TEXT, HAVE_MATCH -> 0;
            case LITERAL -> literal(regexp.rune(), regexp.parseFlags());
            case LITERAL_STRING -> literalString(regexp.runes(), regexp.parseFlags());
            case CONCAT -> concatenate(childLengths);
            case ALTERNATE -> alternate(childLengths);
            case STAR, QUEST -> optional(childLengths.getFirst());
            case PLUS -> plus(childLengths.getFirst());
            case REPEAT -> repeat(childLengths.getFirst(), regexp.min(), regexp.max());
            case CAPTURE -> childLengths.getFirst();
            case ANY_CHAR -> (regexp.parseFlags() & Regexp.LATIN1) != 0 ? 1 : variable(1);
            case ANY_BYTE -> 1;
            case CHAR_CLASS -> characterClass(regexp.charClass(), regexp.parseFlags());
        };
    }

    @Override
    protected Integer shortVisit(Regexp regexp, Integer parentArgument)
    {
        throw new AssertionError("short visit is not used");
    }

    private static int variable(int minimum)
    {
        return -minimum - 1;
    }

    private static int literalString(int[] runes, int parseFlags)
    {
        int result = 0;
        for (int rune : runes) {
            result = concatenate(result, literal(rune, parseFlags));
        }
        return result;
    }

    private static int literal(int rune, int parseFlags)
    {
        if ((parseFlags & Regexp.LATIN1) != 0) {
            return 1;
        }

        int initialLength = utf8Length(rune);
        if ((parseFlags & Regexp.FOLD_CASE) == 0) {
            return initialLength;
        }

        int minimum = initialLength;
        boolean fixed = true;
        int foldedRune = UnicodeCaseFold.cycleFoldRune(rune);
        while (foldedRune != rune) {
            int foldedLength = utf8Length(foldedRune);
            minimum = Math.min(minimum, foldedLength);
            fixed &= foldedLength == initialLength;
            foldedRune = UnicodeCaseFold.cycleFoldRune(foldedRune);
        }
        return fixed ? minimum : variable(minimum);
    }

    private static int concatenate(List<Integer> childLengths)
    {
        int result = 0;
        for (int childLength : childLengths) {
            result = concatenate(result, childLength);
        }
        return result;
    }

    private static int concatenate(Regexp regexp, int depth)
    {
        int result = 0;
        for (int childIndex = 0; childIndex < regexp.subCount(); childIndex++) {
            result = concatenate(result, analyzeEncoded(regexp.sub(childIndex), depth));
        }
        return result;
    }

    private static int concatenate(int left, int right)
    {
        if (left == IMPOSSIBLE || right == IMPOSSIBLE) {
            return IMPOSSIBLE;
        }
        int minimum = addMinimum(minimum(left), minimum(right));
        if (left >= 0 && right >= 0 && minimum != Integer.MAX_VALUE) {
            return minimum;
        }
        return variable(minimum);
    }

    private static int alternate(List<Integer> childLengths)
    {
        int result = IMPOSSIBLE;
        for (int childLength : childLengths) {
            result = alternate(result, childLength);
        }
        return result;
    }

    private static int alternate(Regexp regexp, int depth)
    {
        int result = IMPOSSIBLE;
        for (int childIndex = 0; childIndex < regexp.subCount(); childIndex++) {
            result = alternate(result, analyzeEncoded(regexp.sub(childIndex), depth));
        }
        return result;
    }

    private static int alternate(int left, int right)
    {
        if (left == IMPOSSIBLE) {
            return right;
        }
        if (right == IMPOSSIBLE) {
            return left;
        }
        if (left >= 0 && left == right) {
            return left;
        }
        return variable(Math.min(minimum(left), minimum(right)));
    }

    private static int plus(int childLength)
    {
        if (childLength == IMPOSSIBLE || childLength == 0) {
            return childLength;
        }
        return variable(minimum(childLength));
    }

    private static int optional(int childLength)
    {
        return childLength == IMPOSSIBLE || childLength == 0 ? 0 : variable(0);
    }

    private static int repeat(int childLength, int minimumCount, int maximumCount)
    {
        if (maximumCount == 0 || childLength == 0) {
            return 0;
        }
        if (childLength == IMPOSSIBLE) {
            return minimumCount == 0 ? 0 : IMPOSSIBLE;
        }

        int minimum = multiplyMinimum(minimum(childLength), minimumCount);
        if (childLength >= 0 && minimumCount == maximumCount && minimum != Integer.MAX_VALUE) {
            return minimum;
        }
        return variable(minimum);
    }

    private static int characterClass(CharClass characterClass, int parseFlags)
    {
        if ((parseFlags & Regexp.LATIN1) != 0) {
            for (int rangeIndex = 0; rangeIndex < characterClass.rangeCount(); rangeIndex++) {
                if (characterClass.range(rangeIndex).low() <= 0xFF) {
                    return 1;
                }
            }
            return IMPOSSIBLE;
        }

        int minimum = Integer.MAX_VALUE;
        int fixed = 0;
        for (int rangeIndex = 0; rangeIndex < characterClass.rangeCount(); rangeIndex++) {
            RuneRange range = characterClass.range(rangeIndex);
            int lowLength = utf8Length(range.low());
            int highLength = utf8Length(range.high());
            minimum = Math.min(minimum, lowLength);
            if (lowLength != highLength || (fixed != 0 && fixed != lowLength)) {
                fixed = -1;
            }
            else if (fixed == 0) {
                fixed = lowLength;
            }
        }
        if (minimum == Integer.MAX_VALUE) {
            return IMPOSSIBLE;
        }
        return fixed >= 0 ? fixed : variable(minimum);
    }

    private static int addMinimum(int left, int right)
    {
        if (left > Integer.MAX_VALUE - right) {
            return Integer.MAX_VALUE;
        }
        return left + right;
    }

    private static int multiplyMinimum(int length, int count)
    {
        if (length == 0 || count == 0) {
            return 0;
        }
        if (length > Integer.MAX_VALUE / count) {
            return Integer.MAX_VALUE;
        }
        return length * count;
    }

    private static int utf8Length(int rune)
    {
        if (rune < 0x80) {
            return 1;
        }
        if (rune < 0x800) {
            return 2;
        }
        if (rune < 0x10000) {
            return 3;
        }
        return 4;
    }
}
