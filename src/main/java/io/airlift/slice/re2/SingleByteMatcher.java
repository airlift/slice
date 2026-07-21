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

import io.airlift.slice.Slice;

import java.util.ArrayDeque;
import java.util.List;

final class SingleByteMatcher
        extends RegexpWalker<SingleByteMatcher.Analysis>
{
    private static final int MAX_RECURSIVE_DEPTH = 256;
    private static final Analysis INVALID = new Analysis(-1, 0, 0, 0, 0);
    private static final Analysis EMPTY = new Analysis(0, 0, 0, 0, 0);
    private static final Analysis ALL_BYTES = new Analysis(1, -1, -1, -1, -1);
    private static final SingleByteMatcher UNSUPPORTED = new SingleByteMatcher();

    private final byte[] matches;

    private SingleByteMatcher()
    {
        matches = null;
    }

    private SingleByteMatcher(Analysis analysis)
    {
        matches = new byte[256];
        for (int value = 0; value < matches.length; value++) {
            if (analysis.matches(value)) {
                matches[value] = 1;
            }
        }
    }

    static SingleByteMatcher analyze(Regexp regexp)
    {
        Analysis analysis = analyzePattern(regexp);
        if (analysis.length() != 1 || analysis.isEmpty()) {
            return null;
        }
        return new SingleByteMatcher(analysis);
    }

    static boolean supports(Regexp regexp)
    {
        return supports(regexp, 0);
    }

    private static boolean supports(Regexp regexp, int depth)
    {
        if (!supportsNode(regexp)) {
            return false;
        }
        if (depth == MAX_RECURSIVE_DEPTH) {
            return supportsIteratively(regexp);
        }
        for (int childIndex = 0; childIndex < regexp.subCount(); childIndex++) {
            if (!supports(regexp.sub(childIndex), depth + 1)) {
                return false;
            }
        }
        return true;
    }

    private static boolean supportsIteratively(Regexp root)
    {
        ArrayDeque<Regexp> stack = new ArrayDeque<>();
        stack.add(root);
        while (!stack.isEmpty()) {
            Regexp regexp = stack.removeLast();
            if (!supportsNode(regexp)) {
                return false;
            }
            stack.addAll(regexp.subs());
        }
        return true;
    }

    private static boolean supportsNode(Regexp regexp)
    {
        return switch (regexp.op()) {
            case EMPTY_MATCH, CONCAT, ALTERNATE, CAPTURE, ANY_BYTE -> true;
            case LITERAL -> supportsLiteral(regexp.rune(), regexp.parseFlags());
            case LITERAL_STRING -> regexp.runes().length <= 1 &&
                    (regexp.runes().length == 0 || supportsLiteral(regexp.runes()[0], regexp.parseFlags()));
            case REPEAT -> regexp.max() == 0 || (regexp.min() == 1 && regexp.max() == 1);
            case ANY_CHAR -> (regexp.parseFlags() & Regexp.LATIN1) != 0;
            case CHAR_CLASS -> supportsCharacterClass(regexp.charClass(), regexp.parseFlags());
            case NO_MATCH, BEGIN_LINE, END_LINE, WORD_BOUNDARY, NO_WORD_BOUNDARY, BEGIN_TEXT, END_TEXT,
                    STAR, PLUS, QUEST, HAVE_MATCH -> false;
        };
    }

    private static boolean supportsLiteral(int rune, int parseFlags)
    {
        boolean latin1 = (parseFlags & Regexp.LATIN1) != 0;
        int currentRune = rune;
        do {
            if ((!latin1 && currentRune >= 0x80) || currentRune > 0xFF) {
                return false;
            }
            if ((parseFlags & Regexp.FOLD_CASE) == 0) {
                break;
            }
            currentRune = UnicodeCaseFold.cycleFoldRune(currentRune);
        }
        while (currentRune != rune);
        return true;
    }

    private static boolean supportsCharacterClass(CharClass characterClass, int parseFlags)
    {
        if (characterClass.isEmpty()) {
            return false;
        }
        int maximum = (parseFlags & Regexp.LATIN1) != 0 ? 0xFF : 0x7F;
        return characterClass.range(characterClass.rangeCount() - 1).high() <= maximum;
    }

    static Analysis analyzePattern(Regexp regexp)
    {
        return analyze(regexp, 0);
    }

    static SingleByteMatcher unsupported()
    {
        return UNSUPPORTED;
    }

    // PERFORMANCE-SENSITIVE HOT LOOPS: table access and loop shape directly affect the
    // containment and count fast paths. Do not apply readability-only changes without direct
    // path tests and focused Intel and Graviton benchmarks.

    long count(Slice input)
    {
        byte[] bytes = input.byteArray();
        int end = input.byteArrayOffset() + input.length();
        long count = 0;
        for (int position = input.byteArrayOffset(); position < end; position++) {
            count += matches[bytes[position] & 0xFF];
        }
        return count;
    }

    int find(Slice input, int start)
    {
        return find(input, start, input.length());
    }

    int find(Slice input, int start, int end)
    {
        byte[] bytes = input.byteArray();
        int offset = input.byteArrayOffset();
        int physicalEnd = offset + end;
        for (int position = offset + start; position < physicalEnd; position++) {
            if (matches[bytes[position] & 0xFF] != 0) {
                return position - offset;
            }
        }
        return -1;
    }

    private static Analysis analyze(Regexp regexp, int depth)
    {
        if (depth >= MAX_RECURSIVE_DEPTH) {
            return new SingleByteMatcher().walk(regexp, null);
        }

        return switch (regexp.op()) {
            case EMPTY_MATCH -> EMPTY;
            case LITERAL -> literal(regexp.rune(), regexp.parseFlags());
            case LITERAL_STRING -> literalString(regexp.runes(), regexp.parseFlags());
            case CONCAT -> concatenate(regexp, depth + 1);
            case ALTERNATE -> alternate(regexp, depth + 1);
            case REPEAT -> repeat(analyze(regexp.sub(0), depth + 1), regexp.min(), regexp.max());
            case CAPTURE -> analyze(regexp.sub(0), depth + 1);
            case ANY_CHAR -> (regexp.parseFlags() & Regexp.LATIN1) != 0 ? ALL_BYTES : INVALID;
            case ANY_BYTE -> ALL_BYTES;
            case CHAR_CLASS -> characterClass(regexp.charClass(), regexp.parseFlags());
            case NO_MATCH, BEGIN_LINE, END_LINE, WORD_BOUNDARY, NO_WORD_BOUNDARY, BEGIN_TEXT, END_TEXT,
                    STAR, PLUS, QUEST, HAVE_MATCH -> INVALID;
        };
    }

    @Override
    protected PreVisitResult<Analysis> preVisit(Regexp regexp, Analysis parentArgument)
    {
        return new PreVisitResult<>(null, false);
    }

    @Override
    protected Analysis postVisit(Regexp regexp, Analysis parentArgument, Analysis preArgument, List<Analysis> childAnalyses)
    {
        return switch (regexp.op()) {
            case EMPTY_MATCH -> EMPTY;
            case LITERAL -> literal(regexp.rune(), regexp.parseFlags());
            case LITERAL_STRING -> literalString(regexp.runes(), regexp.parseFlags());
            case CONCAT -> concatenate(childAnalyses);
            case ALTERNATE -> alternate(childAnalyses);
            case REPEAT -> repeat(childAnalyses.getFirst(), regexp.min(), regexp.max());
            case CAPTURE -> childAnalyses.getFirst();
            case ANY_CHAR -> (regexp.parseFlags() & Regexp.LATIN1) != 0 ? ALL_BYTES : INVALID;
            case ANY_BYTE -> ALL_BYTES;
            case CHAR_CLASS -> characterClass(regexp.charClass(), regexp.parseFlags());
            case NO_MATCH, BEGIN_LINE, END_LINE, WORD_BOUNDARY, NO_WORD_BOUNDARY, BEGIN_TEXT, END_TEXT,
                    STAR, PLUS, QUEST, HAVE_MATCH -> INVALID;
        };
    }

    @Override
    protected Analysis shortVisit(Regexp regexp, Analysis parentArgument)
    {
        throw new AssertionError("short visit is not used");
    }

    private static Analysis literalString(int[] runes, int parseFlags)
    {
        if (runes.length == 0) {
            return EMPTY;
        }
        if (runes.length != 1) {
            return INVALID;
        }
        return literal(runes[0], parseFlags);
    }

    private static Analysis literal(int rune, int parseFlags)
    {
        boolean latin1 = (parseFlags & Regexp.LATIN1) != 0;
        Analysis analysis = EMPTY;
        int currentRune = rune;
        do {
            if ((!latin1 && currentRune >= 0x80) || currentRune > 0xFF) {
                return INVALID;
            }
            analysis = analysis.withByte(currentRune);
            if ((parseFlags & Regexp.FOLD_CASE) == 0) {
                break;
            }
            currentRune = UnicodeCaseFold.cycleFoldRune(currentRune);
        }
        while (currentRune != rune);
        return analysis.withLength(1);
    }

    private static Analysis characterClass(CharClass characterClass, int parseFlags)
    {
        boolean latin1 = (parseFlags & Regexp.LATIN1) != 0;
        long[] bits = new long[4];
        for (int rangeIndex = 0; rangeIndex < characterClass.rangeCount(); rangeIndex++) {
            RuneRange range = characterClass.range(rangeIndex);
            if (!latin1 && range.high() >= 0x80) {
                return INVALID;
            }
            int low = Math.max(0, range.low());
            int high = Math.min(0xFF, range.high());
            for (int value = low; value <= high; value++) {
                bits[value >>> 6] |= 1L << value;
            }
        }
        return new Analysis(1, bits[0], bits[1], bits[2], bits[3]);
    }

    private static Analysis concatenate(Regexp regexp, int depth)
    {
        Analysis result = EMPTY;
        for (int childIndex = 0; childIndex < regexp.subCount(); childIndex++) {
            result = concatenate(result, analyze(regexp.sub(childIndex), depth));
            if (result == INVALID) {
                return INVALID;
            }
        }
        return result;
    }

    private static Analysis concatenate(List<Analysis> childAnalyses)
    {
        Analysis result = EMPTY;
        for (Analysis childAnalysis : childAnalyses) {
            result = concatenate(result, childAnalysis);
            if (result == INVALID) {
                return INVALID;
            }
        }
        return result;
    }

    private static Analysis concatenate(Analysis left, Analysis right)
    {
        if (left.length() < 0 || right.length() < 0 || left.length() + right.length() > 1) {
            return INVALID;
        }
        if (left.length() == 0) {
            return right;
        }
        if (right.length() == 0) {
            return left;
        }
        return INVALID;
    }

    private static Analysis alternate(Regexp regexp, int depth)
    {
        if (regexp.subCount() == 0) {
            return INVALID;
        }
        Analysis result = analyze(regexp.sub(0), depth);
        for (int childIndex = 1; childIndex < regexp.subCount(); childIndex++) {
            result = alternate(result, analyze(regexp.sub(childIndex), depth));
            if (result == INVALID) {
                return INVALID;
            }
        }
        return result;
    }

    private static Analysis alternate(List<Analysis> childAnalyses)
    {
        if (childAnalyses.isEmpty()) {
            return INVALID;
        }
        Analysis result = childAnalyses.getFirst();
        for (int childIndex = 1; childIndex < childAnalyses.size(); childIndex++) {
            result = alternate(result, childAnalyses.get(childIndex));
            if (result == INVALID) {
                return INVALID;
            }
        }
        return result;
    }

    private static Analysis alternate(Analysis left, Analysis right)
    {
        if (left.length() < 0 || left.length() != right.length()) {
            return INVALID;
        }
        return new Analysis(
                left.length(),
                left.first() | right.first(),
                left.second() | right.second(),
                left.third() | right.third(),
                left.fourth() | right.fourth());
    }

    private static Analysis repeat(Analysis child, int minimum, int maximum)
    {
        if (maximum == 0) {
            return EMPTY;
        }
        if (minimum == 1 && maximum == 1) {
            return child;
        }
        return INVALID;
    }

    record Analysis(int length, long first, long second, long third, long fourth)
    {
        boolean isEmpty()
        {
            return (first | second | third | fourth) == 0;
        }

        boolean matches(int value)
        {
            long bits = switch (value >>> 6) {
                case 0 -> first;
                case 1 -> second;
                case 2 -> third;
                case 3 -> fourth;
                default -> throw new IllegalArgumentException("not a byte: " + value);
            };
            return (bits & (1L << value)) != 0;
        }

        Analysis withByte(int value)
        {
            return switch (value >>> 6) {
                case 0 -> new Analysis(length, first | (1L << value), second, third, fourth);
                case 1 -> new Analysis(length, first, second | (1L << value), third, fourth);
                case 2 -> new Analysis(length, first, second, third | (1L << value), fourth);
                case 3 -> new Analysis(length, first, second, third, fourth | (1L << value));
                default -> throw new IllegalArgumentException("not a byte: " + value);
            };
        }

        Analysis withLength(int length)
        {
            return new Analysis(length, first, second, third, fourth);
        }
    }
}
