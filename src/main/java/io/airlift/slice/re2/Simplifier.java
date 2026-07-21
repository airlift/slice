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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("CharUsedInArithmeticContext")
final class Simplifier
{
    private static final int MAX_RECURSIVE_DEPTH = 256;

    private Simplifier() {}

    public static Regexp simplify(Regexp regexp)
    {
        requireNonNull(regexp, "regexp is null");
        // Upstream simplify.cc runs a coalescing pass before simplification.
        return simplifyInternal(Coalescer.coalesce(regexp), Mode.FULL);
    }

    /**
     * Parse-time canonicalization that intentionally preserves counted repetitions ({@code {n,m}}),
     * matching upstream RE2 parse.cc behavior (parse dumps still contain {@code rep{...}} nodes).
     * <p>
     * Compiler pipelines must call {@link #simplify(Regexp)} (FULL) to eliminate counted repeats.
     */
    public static Regexp simplifyForParse(Regexp regexp)
    {
        requireNonNull(regexp, "regexp is null");
        return simplifyInternal(regexp, Mode.PARSE);
    }

    private enum Mode { PARSE, FULL }

    private static Regexp simplifyInternal(Regexp regexp, Mode mode)
    {
        return simplifyRecursive(regexp, mode, 0);
    }

    private static Regexp simplifyRecursive(Regexp regexp, Mode mode, int depth)
    {
        if (depth == MAX_RECURSIVE_DEPTH) {
            return new SimplifyWalker(mode).walk(regexp, null);
        }

        // Do not short-circuit on regexp.simple(): Java leaf nodes are marked simple at
        // construction, but some still require canonicalization (for example, an empty class).
        int flags = regexp.parseFlags();

        return switch (regexp.op()) {
            case NO_MATCH,
                 EMPTY_MATCH,
                 ANY_CHAR,
                 ANY_BYTE,
                 BEGIN_LINE,
                 END_LINE,
                 WORD_BOUNDARY,
                 NO_WORD_BOUNDARY,
                 BEGIN_TEXT,
                 END_TEXT,
                 HAVE_MATCH,
                 LITERAL -> regexp;

            case LITERAL_STRING -> {
                int[] runes = regexp.runes();
                if (runes.length == 1) {
                    yield Regexp.literal(flags, runes[0]);
                }
                yield regexp;
            }

            case CAPTURE -> Regexp.capture(flags, simplifyRecursive(regexp.sub(0), mode, depth + 1), regexp.cap(), regexp.name());
            case STAR -> simplifyUnary(flags, simplifyRecursive(regexp.sub(0), mode, depth + 1), UnaryOp.STAR);
            case PLUS -> simplifyUnary(flags, simplifyRecursive(regexp.sub(0), mode, depth + 1), UnaryOp.PLUS);
            case QUEST -> simplifyUnary(flags, simplifyRecursive(regexp.sub(0), mode, depth + 1), UnaryOp.QUEST);
            case REPEAT -> simplifyRepeat(mode, flags, regexp, simplifyRecursive(regexp.sub(0), mode, depth + 1));
            case CONCAT -> simplifyConcatRecursive(mode, flags, regexp, depth);
            case ALTERNATE -> simplifyAlternateRecursive(mode, flags, regexp, depth);
            case CHAR_CLASS -> simplifyCharClass(mode, flags, regexp.charClass());
        };
    }

    private static final class SimplifyWalker
            extends RegexpWalker<Regexp>
    {
        private final Mode mode;

        private SimplifyWalker(Mode mode)
        {
            this.mode = mode;
        }

        @Override
        protected PreVisitResult<Regexp> preVisit(Regexp regexp, Regexp parentValue)
        {
            return new PreVisitResult<>(regexp, false);
        }

        @Override
        protected Regexp shortVisit(Regexp regexp, Regexp parentValue)
        {
            throw new IllegalStateException("Simplifier should not short-visit");
        }

        @Override
        protected Regexp postVisit(Regexp regexp, Regexp parentValue, Regexp preValue, List<Regexp> simplifiedChildren)
        {
            // Do not short-circuit on regexp.simple(): Java leaf nodes are marked simple at
            // construction, but some still require canonicalization (for example, an empty class).
            int flags = regexp.parseFlags();

            return switch (regexp.op()) {
                case NO_MATCH,
                     EMPTY_MATCH,
                     ANY_CHAR,
                     ANY_BYTE,
                     BEGIN_LINE,
                     END_LINE,
                     WORD_BOUNDARY,
                     NO_WORD_BOUNDARY,
                     BEGIN_TEXT,
                     END_TEXT,
                     HAVE_MATCH,
                     LITERAL -> regexp;

                case LITERAL_STRING -> {
                    int[] runes = regexp.runes();
                    if (runes.length == 1) {
                        yield Regexp.literal(flags, runes[0]);
                    }
                    yield regexp;
                }

                case CAPTURE -> Regexp.capture(flags, simplifiedChildren.getFirst(), regexp.cap(), regexp.name());
                case STAR -> simplifyUnary(flags, simplifiedChildren.getFirst(), UnaryOp.STAR);
                case PLUS -> simplifyUnary(flags, simplifiedChildren.getFirst(), UnaryOp.PLUS);
                case QUEST -> simplifyUnary(flags, simplifiedChildren.getFirst(), UnaryOp.QUEST);
                case REPEAT -> simplifyRepeat(mode, flags, regexp, simplifiedChildren.getFirst());
                case CONCAT -> simplifyConcat(flags, simplifiedChildren);
                case ALTERNATE -> simplifyAlternate(mode, flags, simplifiedChildren);
                case CHAR_CLASS -> simplifyCharClass(mode, flags, regexp.charClass());
            };
        }
    }

    private enum UnaryOp { STAR, PLUS, QUEST }

    private static Regexp simplifyUnary(int flags, Regexp childRegexp, UnaryOp operator)
    {
        // Simplify repetition over known-empty / known-impossible expressions.
        if (childRegexp.op() == RegexpOp.EMPTY_MATCH) {
            return Regexp.emptyMatch(flags);
        }
        if (childRegexp.op() == RegexpOp.NO_MATCH) {
            return switch (operator) {
                case STAR, QUEST -> Regexp.emptyMatch(flags);
                case PLUS -> Regexp.noMatch(flags);
            };
        }

        // Upstream simplify.cc only treats unary operators as idempotent when
        // the operator (and flags) are identical: **, ++, ??.
        if (childRegexp.op() == switch (operator) { case STAR -> RegexpOp.STAR; case PLUS -> RegexpOp.PLUS; case QUEST -> RegexpOp.QUEST; } &&
                childRegexp.parseFlags() == flags) {
            return childRegexp;
        }

        return switch (operator) {
            case STAR -> Regexp.rawUnary(RegexpOp.STAR, flags, childRegexp);
            case PLUS -> Regexp.rawUnary(RegexpOp.PLUS, flags, childRegexp);
            case QUEST -> Regexp.rawUnary(RegexpOp.QUEST, flags, childRegexp);
        };
    }

    private static Regexp simplifyRepeat(Mode mode, int flags, Regexp regexp, Regexp childRegexp)
    {
        int min = regexp.min();
        int max = regexp.max();

        if (mode == Mode.PARSE) {
            // Preserve counted repeats in parse results (upstream parse_test.cc expects rep{...}).
            return Regexp.repeat(flags, childRegexp, min, max);
        }
        // and SimplifyWalker::SimplifyRepeat().
        if (childRegexp.op() == RegexpOp.EMPTY_MATCH) {
            return childRegexp;
        }

        // For an empty-width op OR a concatenation or alternation of empty-width ops,
        // cap the repetition count at 1.
        if (isEmptyOp(childRegexp) || isConcatOrAlternateOfEmptyOps(childRegexp)) {
            min = Math.min(min, 1);
            max = Math.min(max, 1);
        }

        // x{n,} means at least n matches of x.
        if (max == -1) {
            if (min == 0) {
                return Regexp.star(flags, childRegexp);
            }
            if (min == 1) {
                return Regexp.plus(flags, childRegexp);
            }

            // General case: x{4,} is xxxx+
            List<Regexp> repeatedChildren = new ArrayList<>(min);
            for (int repeatedIndex = 0; repeatedIndex < min - 1; repeatedIndex++) {
                repeatedChildren.add(childRegexp);
            }
            repeatedChildren.add(Regexp.plus(flags, childRegexp));
            return Regexp.concat(flags, repeatedChildren);
        }

        // Special case: (x){0} matches only empty string.
        if (min == 0 && max == 0) {
            return Regexp.emptyMatch(flags);
        }

        // Special case: x{1} is just x.
        if (min == 1 && max == 1) {
            return childRegexp;
        }

        // General case: x{n,m} means n copies of x and m copies of x?.
        // Nest the final m copies:
        //   x{2,5} = xx(x(x(x)?)?)?
        Regexp result = null;
        if (min > 0) {
            List<Regexp> prefix = new ArrayList<>(min);
            for (int prefixIndex = 0; prefixIndex < min; prefixIndex++) {
                prefix.add(childRegexp);
            }
            result = (prefix.size() == 1) ? prefix.getFirst() : Regexp.concat(flags, prefix);
        }

        if (max > min) {
            Regexp suffix = Regexp.quest(flags, childRegexp);
            for (int repeatIndex = min + 1; repeatIndex < max; repeatIndex++) {
                suffix = Regexp.quest(flags, Regexp.concat(flags, List.of(childRegexp, suffix)));
            }
            result = (result == null) ? suffix : Regexp.concat(flags, List.of(result, suffix));
        }

        if (result == null) {
            // Degenerate cases (e.g., min > max) should be rejected by the parser.
            return Regexp.noMatch(flags);
        }
        return result;
    }

    private static boolean isEmptyOp(Regexp regexp)
    {
        return switch (regexp.op()) {
            case BEGIN_LINE, END_LINE, WORD_BOUNDARY, NO_WORD_BOUNDARY, BEGIN_TEXT, END_TEXT -> true;
            default -> false;
        };
    }

    private static boolean isConcatOrAlternateOfEmptyOps(Regexp regexp)
    {
        if (regexp.op() != RegexpOp.CONCAT && regexp.op() != RegexpOp.ALTERNATE) {
            return false;
        }
        for (Regexp childRegexp : regexp.subs()) {
            if (!isEmptyOp(childRegexp)) {
                return false;
            }
        }
        return true;
    }

    private static Regexp simplifyConcat(int flags, List<Regexp> children)
    {
        List<Regexp> simplifiedChildren = new ArrayList<>();
        for (Regexp simplifiedChild : children) {
            if (simplifiedChild.op() == RegexpOp.NO_MATCH) {
                return Regexp.noMatch(flags);
            }
            if (simplifiedChild.op() == RegexpOp.EMPTY_MATCH) {
                continue;
            }
            if (simplifiedChild.op() == RegexpOp.CONCAT) {
                simplifiedChildren.addAll(simplifiedChild.subs());
            }
            else {
                // Merge adjacent literal/literal-string nodes into a single literal string,
                // matching upstream parse_test.cc expectations like (?:ab)(?:cd) -> str{abcd}.
                if (!simplifiedChildren.isEmpty() && isLiteralOrLiteralString(simplifiedChildren.getLast()) && isLiteralOrLiteralString(simplifiedChild) &&
                        simplifiedChildren.getLast().parseFlags() == simplifiedChild.parseFlags()) {
                    Regexp merged = mergeAdjacentLiterals(simplifiedChildren.getLast(), simplifiedChild);
                    simplifiedChildren.set(simplifiedChildren.size() - 1, merged);
                }
                else {
                    simplifiedChildren.add(simplifiedChild);
                }
            }
        }

        if (simplifiedChildren.isEmpty()) {
            return Regexp.emptyMatch(flags);
        }
        if (simplifiedChildren.size() == 1) {
            return simplifiedChildren.getFirst();
        }
        return Regexp.concat(flags, simplifiedChildren);
    }

    private static Regexp simplifyConcatRecursive(Mode mode, int flags, Regexp regexp, int depth)
    {
        List<Regexp> simplifiedChildren = new ArrayList<>();
        for (Regexp child : regexp.subs()) {
            Regexp simplifiedChild = simplifyRecursive(child, mode, depth + 1);
            if (simplifiedChild.op() == RegexpOp.NO_MATCH) {
                return Regexp.noMatch(flags);
            }
            if (simplifiedChild.op() == RegexpOp.EMPTY_MATCH) {
                continue;
            }
            if (simplifiedChild.op() == RegexpOp.CONCAT) {
                simplifiedChildren.addAll(simplifiedChild.subs());
            }
            else if (!simplifiedChildren.isEmpty() && isLiteralOrLiteralString(simplifiedChildren.getLast()) && isLiteralOrLiteralString(simplifiedChild) &&
                    simplifiedChildren.getLast().parseFlags() == simplifiedChild.parseFlags()) {
                Regexp merged = mergeAdjacentLiterals(simplifiedChildren.getLast(), simplifiedChild);
                simplifiedChildren.set(simplifiedChildren.size() - 1, merged);
            }
            else {
                simplifiedChildren.add(simplifiedChild);
            }
        }

        if (simplifiedChildren.isEmpty()) {
            return Regexp.emptyMatch(flags);
        }
        if (simplifiedChildren.size() == 1) {
            return simplifiedChildren.getFirst();
        }
        return Regexp.concat(flags, simplifiedChildren);
    }

    private static boolean isLiteralOrLiteralString(Regexp regexp)
    {
        return regexp.op() == RegexpOp.LITERAL || regexp.op() == RegexpOp.LITERAL_STRING;
    }

    private static Regexp mergeAdjacentLiterals(Regexp left, Regexp right)
    {
        int[] leftRunes = (left.op() == RegexpOp.LITERAL) ? new int[] {left.rune()} : left.runes();
        int[] rightRunes = (right.op() == RegexpOp.LITERAL) ? new int[] {right.rune()} : right.runes();
        int[] merged = new int[leftRunes.length + rightRunes.length];
        System.arraycopy(leftRunes, 0, merged, 0, leftRunes.length);
        System.arraycopy(rightRunes, 0, merged, leftRunes.length, rightRunes.length);

        if (merged.length == 1) {
            return Regexp.literal(left.parseFlags(), merged[0]);
        }
        return Regexp.literalString(left.parseFlags(), merged);
    }

    private static Regexp simplifyAlternate(Mode mode, int flags, List<Regexp> children)
    {
        List<Regexp> simplifiedAlternatives = new ArrayList<>();
        for (Regexp simplifiedChild : children) {
            if (simplifiedChild.op() == RegexpOp.NO_MATCH) {
                continue;
            }
            if (simplifiedChild.op() == RegexpOp.ALTERNATE) {
                simplifiedAlternatives.addAll(simplifiedChild.subs());
            }
            else {
                simplifiedAlternatives.add(simplifiedChild);
            }
        }

        return simplifyAlternatives(mode, flags, simplifiedAlternatives);
    }

    private static Regexp simplifyAlternatives(Mode mode, int flags, List<Regexp> simplifiedAlternatives)
    {
        if (simplifiedAlternatives.isEmpty()) {
            return Regexp.noMatch(flags);
        }
        if (simplifiedAlternatives.size() == 1) {
            return simplifiedAlternatives.getFirst();
        }

        if (mode == Mode.PARSE) {
            // Dot/byte dominate alternations that are otherwise more specific.
            // This matches upstream parse_test.cc expectations like "a|." -> dot{} and ".|c" -> dot{}.
            // For now, we apply this only when the parse flags are consistent.
            boolean haveAnyByte = false;
            boolean haveAnyChar = false;
            for (Regexp alternative : simplifiedAlternatives) {
                if (alternative.parseFlags() != flags) {
                    haveAnyByte = false;
                    haveAnyChar = false;
                    break;
                }
                haveAnyByte |= alternative.op() == RegexpOp.ANY_BYTE;
                haveAnyChar |= alternative.op() == RegexpOp.ANY_CHAR;
            }
            if (haveAnyByte) {
                return Regexp.anyByte(flags);
            }
            if (haveAnyChar) {
                return Regexp.anyChar(flags);
            }

            List<Regexp> factored = factorAlternationForParse(simplifiedAlternatives, flags);
            if (factored.size() == 1) {
                return factored.getFirst();
            }
            return Regexp.alternate(flags, factored);
        }

        List<Regexp> merged = mergeAlternateCharClassesWherePossible(mode, flags, simplifiedAlternatives);
        if (merged.size() == 1) {
            return merged.getFirst();
        }

        return Regexp.alternate(flags, merged);
    }

    private static Regexp simplifyAlternateRecursive(Mode mode, int flags, Regexp regexp, int depth)
    {
        List<Regexp> simplifiedAlternatives = new ArrayList<>();
        for (Regexp child : regexp.subs()) {
            Regexp simplifiedChild = simplifyRecursive(child, mode, depth + 1);
            if (simplifiedChild.op() == RegexpOp.NO_MATCH) {
                continue;
            }
            if (simplifiedChild.op() == RegexpOp.ALTERNATE) {
                simplifiedAlternatives.addAll(simplifiedChild.subs());
            }
            else {
                simplifiedAlternatives.add(simplifiedChild);
            }
        }

        return simplifyAlternatives(mode, flags, simplifiedAlternatives);
    }

    private static Regexp simplifyCharClass(Mode mode, int flags, CharClass charClass)
    {
        if (charClass.isEmpty()) {
            return Regexp.noMatch(flags);
        }
        if (mode == Mode.FULL) {
            // A full character class can be represented more compactly as '.' (ANY_CHAR)
            // or '\C' (ANY_BYTE) depending on the rune range in effect.
            if (charClass.rangeCount() == 1) {
                RuneRange runeRange = charClass.range(0);
                int maxRune = Regexp.maxRune(flags);
                if (runeRange.low() == 0 && runeRange.high() == maxRune) {
                    if (maxRune == 0xFF) {
                        return Regexp.anyByte(flags);
                    }
                    return Regexp.anyChar(flags);
                }
            }
        }

        // Parse-time canonicalization: fold ASCII letter-pair classes like [Aa]
        // into a single FoldCase literal. This is required for features like
        // RequiredPrefixForAccel(), which only considers literals/strings.
        if (charClass.foldsAscii() && charClass.runeCount() == 2 && charClass.rangeCount() == 2) {
            RuneRange firstRange = charClass.range(0);
            RuneRange secondRange = charClass.range(1);
            if (firstRange.low() == firstRange.high() && secondRange.low() == secondRange.high()) {
                int firstRune = firstRange.low();
                int secondRune = secondRange.low();
                int lower = -1;
                int upper = -1;
                if ('a' <= firstRune && firstRune <= 'z' && 'A' <= secondRune && secondRune <= 'Z') {
                    lower = firstRune;
                    upper = secondRune;
                }
                else if ('a' <= secondRune && secondRune <= 'z' && 'A' <= firstRune && firstRune <= 'Z') {
                    lower = secondRune;
                    upper = firstRune;
                }
                if (lower >= 0 && (upper + ('a' - 'A')) == lower) {
                    return Regexp.literal(flags | Regexp.FOLD_CASE, lower);
                }
            }
        }

        if (charClass.runeCount() == 1 && charClass.rangeCount() == 1) {
            RuneRange runeRange = charClass.range(0);
            if (runeRange.low() == runeRange.high()) {
                return Regexp.literal(flags, runeRange.low());
            }
        }
        return Regexp.charClass(flags, charClass);
    }

    private static List<Regexp> mergeAlternateCharClassesWherePossible(Mode mode, int flags, List<Regexp> alternatives)
    {
        ArrayList<Regexp> mergedAlternatives = new ArrayList<>(alternatives.size());

        int flagsWithoutFoldCase = flags & ~Regexp.FOLD_CASE;
        int alternativeIndex = 0;
        while (alternativeIndex < alternatives.size()) {
            Regexp alternative = alternatives.get(alternativeIndex);

            if (!mergeableAlternateSub(alternative, flags, flagsWithoutFoldCase)) {
                mergedAlternatives.add(alternative);
                alternativeIndex++;
                continue;
            }

            CharClassBuilder mergedClassBuilder = new CharClassBuilder();
            int scanIndex = alternativeIndex;
            int count = 0;
            while (scanIndex < alternatives.size()) {
                Regexp scanAlternative = alternatives.get(scanIndex);
                if (!mergeableAlternateSub(scanAlternative, flags, flagsWithoutFoldCase)) {
                    break;
                }
                if (scanAlternative.op() == RegexpOp.LITERAL) {
                    mergedClassBuilder.addRangeFlags(scanAlternative.rune(), scanAlternative.rune(), flags);
                }
                else {
                    CharClass charClass = scanAlternative.charClass();
                    for (RuneRange runeRange : charClass.ranges()) {
                        mergedClassBuilder.addRangeFlags(runeRange.low(), runeRange.high(), flags);
                    }
                }
                count++;
                scanIndex++;
            }

            if (count >= 2) {
                mergedClassBuilder.removeAbove(Regexp.maxRune(flags));
                mergedAlternatives.add(simplifyCharClass(mode, flags, mergedClassBuilder.toCharClass()));
            }
            else {
                mergedAlternatives.add(alternative);
            }
            alternativeIndex = scanIndex;
        }

        return mergedAlternatives;
    }

    private static boolean mergeableAlternateSub(Regexp alternative, int flags, int flagsWithoutFoldCase)
    {
        if (alternative.op() == RegexpOp.LITERAL) {
            return alternative.parseFlags() == flags;
        }
        if (alternative.op() == RegexpOp.CHAR_CLASS) {
            return alternative.parseFlags() == flags || alternative.parseFlags() == flagsWithoutFoldCase;
        }
        return false;
    }

    private record LeadingString(int[] runes, int flags) {}

    private static List<Regexp> factorAlternationForParse(List<Regexp> subs, int flags)
    {
        List<Regexp> round1 = factorAlternationRound1(subs, flags);
        List<Regexp> round2 = factorAlternationRound2(round1, flags);
        return factorAlternationRound3(round2, flags);
    }

    private static List<Regexp> factorAlternationRound1(List<Regexp> alternatives, int flags)
    {
        ArrayList<Regexp> factoredAlternatives = new ArrayList<>(alternatives.size());
        int alternativeIndex = 0;
        while (alternativeIndex < alternatives.size()) {
            LeadingString base = leadingString(alternatives.get(alternativeIndex));
            if (base == null || base.runes.length == 0) {
                factoredAlternatives.add(alternatives.get(alternativeIndex));
                alternativeIndex++;
                continue;
            }

            int startIndex = alternativeIndex;
            int commonLen = base.runes.length;
            int endIndex = alternativeIndex + 1;
            for (; endIndex < alternatives.size(); endIndex++) {
                LeadingString next = leadingString(alternatives.get(endIndex));
                if (next == null || next.flags != base.flags) {
                    break;
                }
                int same = commonPrefixLength(base.runes, next.runes, commonLen);
                if (same <= 0) {
                    break;
                }
                commonLen = same;
            }

            if ((endIndex - startIndex) > 1 && commonLen > 0) {
                int[] prefixRunes = Arrays.copyOf(base.runes, commonLen);
                Regexp prefix = (prefixRunes.length == 1)
                        ? Regexp.literal(base.flags, prefixRunes[0])
                        : Regexp.literalString(base.flags, prefixRunes);

                ArrayList<Regexp> suffixes = new ArrayList<>(endIndex - startIndex);
                for (int suffixIndex = startIndex; suffixIndex < endIndex; suffixIndex++) {
                    suffixes.add(removeLeadingString(alternatives.get(suffixIndex), commonLen));
                }

                List<Regexp> factoredSuffixes = factorAlternationForParse(suffixes, flags);
                Regexp suffix = alternateNoFactor(flags, factoredSuffixes);
                factoredAlternatives.add(Regexp.concat(flags, List.of(prefix, suffix)));
            }
            else {
                for (int copyIndex = startIndex; copyIndex < endIndex; copyIndex++) {
                    factoredAlternatives.add(alternatives.get(copyIndex));
                }
            }

            alternativeIndex = endIndex;
        }

        return factoredAlternatives;
    }

    private static List<Regexp> factorAlternationRound2(List<Regexp> alternatives, int flags)
    {
        ArrayList<Regexp> factoredAlternatives = new ArrayList<>(alternatives.size());
        int alternativeIndex = 0;
        while (alternativeIndex < alternatives.size()) {
            Regexp firstPrefix = leadingRegexp(alternatives.get(alternativeIndex));
            if (firstPrefix == null || !isRound2PrefixCandidate(firstPrefix)) {
                factoredAlternatives.add(alternatives.get(alternativeIndex));
                alternativeIndex++;
                continue;
            }

            int startIndex = alternativeIndex;
            int endIndex = alternativeIndex + 1;
            for (; endIndex < alternatives.size(); endIndex++) {
                Regexp nextPrefix = leadingRegexp(alternatives.get(endIndex));
                if (nextPrefix == null || !isRound2PrefixCandidate(firstPrefix) || !Objects.equals(firstPrefix, nextPrefix)) {
                    break;
                }
            }

            if ((endIndex - startIndex) > 1) {
                ArrayList<Regexp> suffixes = new ArrayList<>(endIndex - startIndex);
                for (int suffixIndex = startIndex; suffixIndex < endIndex; suffixIndex++) {
                    suffixes.add(removeLeadingRegexp(alternatives.get(suffixIndex)));
                }

                List<Regexp> factoredSuffixes = factorAlternationForParse(suffixes, flags);
                Regexp suffix = alternateNoFactor(flags, factoredSuffixes);
                factoredAlternatives.add(Regexp.concat(flags, List.of(firstPrefix, suffix)));
            }
            else {
                factoredAlternatives.add(alternatives.get(startIndex));
            }

            alternativeIndex = endIndex;
        }

        return factoredAlternatives;
    }

    private static List<Regexp> factorAlternationRound3(List<Regexp> alternatives, int flags)
    {
        ArrayList<Regexp> factoredAlternatives = new ArrayList<>(alternatives.size());
        int alternativeIndex = 0;
        while (alternativeIndex < alternatives.size()) {
            Regexp alternative = alternatives.get(alternativeIndex);
            if (alternative.op() != RegexpOp.LITERAL && alternative.op() != RegexpOp.CHAR_CLASS) {
                factoredAlternatives.add(alternative);
                alternativeIndex++;
                continue;
            }

            int startIndex = alternativeIndex;
            int endIndex = alternativeIndex + 1;
            while (endIndex < alternatives.size()) {
                Regexp scanAlternative = alternatives.get(endIndex);
                if (scanAlternative.op() == RegexpOp.LITERAL || scanAlternative.op() == RegexpOp.CHAR_CLASS) {
                    endIndex++;
                }
                else {
                    break;
                }
            }

            if ((endIndex - startIndex) > 1) {
                CharClass charClass = mergeAlternativesIntoCharClass(alternatives, startIndex, endIndex);
                factoredAlternatives.add(Regexp.charClass(flags & ~Regexp.FOLD_CASE, charClass));
            }
            else {
                factoredAlternatives.add(alternative);
            }
            alternativeIndex = endIndex;
        }
        return factoredAlternatives;
    }

    private static CharClass mergeAlternativesIntoCharClass(List<Regexp> alternatives, int startIndex, int endIndex)
    {
        CharClassBuilder builder = new CharClassBuilder();
        for (int mergeIndex = startIndex; mergeIndex < endIndex; mergeIndex++) {
            Regexp mergeAlternative = alternatives.get(mergeIndex);
            if (mergeAlternative.op() == RegexpOp.CHAR_CLASS) {
                CharClass charClass = mergeAlternative.charClass();
                for (RuneRange runeRange : charClass.ranges()) {
                    builder.addRangeFlags(runeRange.low(), runeRange.high(), mergeAlternative.parseFlags());
                }
            }
            else if ((mergeAlternative.parseFlags() & Regexp.FOLD_CASE) != 0) {
                CharClassBuilder tempCharClassBuilder = new CharClassBuilder();
                tempCharClassBuilder.addRangeFlags(mergeAlternative.rune(), mergeAlternative.rune(), mergeAlternative.parseFlags());
                builder.addCharClass(tempCharClassBuilder);
            }
            else {
                builder.addRangeFlags(mergeAlternative.rune(), mergeAlternative.rune(), mergeAlternative.parseFlags());
            }
        }
        return builder.toCharClass();
    }

    private static Regexp alternateNoFactor(int flags, List<Regexp> alternatives)
    {
        if (alternatives.isEmpty()) {
            return Regexp.noMatch(flags);
        }
        if (alternatives.size() == 1) {
            return alternatives.getFirst();
        }
        return Regexp.alternate(flags, alternatives);
    }

    private static boolean isRound2PrefixCandidate(Regexp regexp)
    {
        return switch (regexp.op()) {
            case BEGIN_LINE, END_LINE, WORD_BOUNDARY, NO_WORD_BOUNDARY, BEGIN_TEXT, END_TEXT, CHAR_CLASS, ANY_CHAR, ANY_BYTE -> true;
            case REPEAT -> regexp.min() == regexp.max() &&
                    (regexp.sub(0).op() == RegexpOp.LITERAL ||
                            regexp.sub(0).op() == RegexpOp.CHAR_CLASS ||
                            regexp.sub(0).op() == RegexpOp.ANY_CHAR ||
                            regexp.sub(0).op() == RegexpOp.ANY_BYTE);
            default -> false;
        };
    }

    private static LeadingString leadingString(Regexp regexp)
    {
        Regexp cursor = regexp;
        while (cursor.op() == RegexpOp.CONCAT && cursor.subCount() > 0) {
            cursor = cursor.sub(0);
        }
        int runeFlags = cursor.parseFlags() & (Regexp.FOLD_CASE | Regexp.LATIN1);
        if (cursor.op() == RegexpOp.LITERAL) {
            return new LeadingString(new int[] {cursor.rune()}, runeFlags);
        }
        if (cursor.op() == RegexpOp.LITERAL_STRING) {
            return new LeadingString(cursor.runes(), runeFlags);
        }
        return null;
    }

    private static Regexp leadingRegexp(Regexp regexp)
    {
        if (regexp.op() == RegexpOp.EMPTY_MATCH) {
            return null;
        }
        if (regexp.op() == RegexpOp.CONCAT && regexp.subCount() >= 2) {
            Regexp first = regexp.sub(0);
            if (first.op() == RegexpOp.EMPTY_MATCH) {
                return null;
            }
            return first;
        }
        return regexp;
    }

    private static Regexp removeLeadingRegexp(Regexp regexp)
    {
        if (regexp.op() == RegexpOp.EMPTY_MATCH) {
            return regexp;
        }
        if (regexp.op() == RegexpOp.CONCAT && regexp.subCount() >= 2) {
            List<Regexp> children = regexp.subs();
            if (children.getFirst().op() == RegexpOp.EMPTY_MATCH) {
                return regexp;
            }
            List<Regexp> remaining = new ArrayList<>(children.size() - 1);
            for (int index = 1; index < children.size(); index++) {
                remaining.add(children.get(index));
            }
            if (remaining.size() == 1) {
                return remaining.getFirst();
            }
            return Regexp.concat(regexp.parseFlags(), remaining);
        }
        return Regexp.emptyMatch(regexp.parseFlags());
    }

    private static int commonPrefixLength(int[] left, int[] right, int max)
    {
        int limit = Math.min(Math.min(left.length, right.length), max);
        int same = 0;
        while (same < limit && left[same] == right[same]) {
            same++;
        }
        return same;
    }

    private static Regexp removeLeadingString(Regexp regexp, int prefixLength)
    {
        if (prefixLength <= 0) {
            return regexp;
        }

        return switch (regexp.op()) {
            case LITERAL -> Regexp.emptyMatch(regexp.parseFlags());
            case LITERAL_STRING -> {
                int[] runes = regexp.runes();
                if (prefixLength >= runes.length) {
                    yield Regexp.emptyMatch(regexp.parseFlags());
                }
                if (prefixLength == runes.length - 1) {
                    yield Regexp.literal(regexp.parseFlags(), runes[runes.length - 1]);
                }
                yield Regexp.literalString(regexp.parseFlags(), Arrays.copyOfRange(runes, prefixLength, runes.length));
            }
            case CONCAT -> {
                List<Regexp> children = regexp.subs();
                Regexp firstChild = removeLeadingString(children.getFirst(), prefixLength);
                ArrayList<Regexp> newChildren = new ArrayList<>(children.size());
                if (firstChild.op() != RegexpOp.EMPTY_MATCH) {
                    newChildren.add(firstChild);
                }
                for (int index = 1; index < children.size(); index++) {
                    newChildren.add(children.get(index));
                }
                if (newChildren.isEmpty()) {
                    yield Regexp.emptyMatch(regexp.parseFlags());
                }
                if (newChildren.size() == 1) {
                    yield newChildren.getFirst();
                }
                yield Regexp.concat(regexp.parseFlags(), newChildren);
            }
            default -> regexp;
        };
    }

    private static final class Coalescer
    {
        private Coalescer() {}

        public static Regexp coalesce(Regexp regexp)
        {
            return new CoalesceWalker().walk(regexp, null);
        }

        private static final class CoalesceWalker
                extends RegexpWalker<Regexp>
        {
            @Override
            protected PreVisitResult<Regexp> preVisit(Regexp regexp, Regexp parentValue)
            {
                return new PreVisitResult<>(regexp, false);
            }

            @Override
            protected Regexp shortVisit(Regexp regexp, Regexp parentValue)
            {
                throw new IllegalStateException("Coalescer should not short-visit");
            }

            @Override
            protected Regexp postVisit(Regexp regexp, Regexp parentValue, Regexp preValue, List<Regexp> childValues)
            {
                if (regexp.subCount() == 0) {
                    return regexp;
                }

                if (regexp.op() != RegexpOp.CONCAT) {
                    if (!childArgsChanged(regexp, childValues)) {
                        return regexp;
                    }
                    return rebuildNonConcat(regexp, childValues);
                }

                boolean foundCoalescingPair = false;
                for (int pairIndex = 0; pairIndex + 1 < childValues.size(); pairIndex++) {
                    if (canCoalesce(childValues.get(pairIndex), childValues.get(pairIndex + 1))) {
                        foundCoalescingPair = true;
                        break;
                    }
                }

                if (!foundCoalescingPair) {
                    if (!childArgsChanged(regexp, childValues)) {
                        return regexp;
                    }
                    return rebuildConcat(regexp, childValues);
                }

                // Perform a single left-to-right coalescing sweep while emitting only live nodes.
                ArrayList<Regexp> coalescedChildren = new ArrayList<>(childValues.size());
                Regexp current = childValues.getFirst();
                for (int childIndex = 1; childIndex < childValues.size(); childIndex++) {
                    Regexp next = childValues.get(childIndex);
                    if (canCoalesce(current, next)) {
                        current = coalescePair(current, next, coalescedChildren);
                    }
                    else {
                        if (current.op() != RegexpOp.EMPTY_MATCH) {
                            coalescedChildren.add(current);
                        }
                        current = next;
                    }
                }
                if (current.op() != RegexpOp.EMPTY_MATCH) {
                    coalescedChildren.add(current);
                }

                if (coalescedChildren.isEmpty()) {
                    return Regexp.emptyMatch(regexp.parseFlags());
                }
                if (coalescedChildren.size() == 1) {
                    return coalescedChildren.getFirst();
                }
                return Regexp.concat(regexp.parseFlags(), coalescedChildren);
            }

            private static boolean childArgsChanged(Regexp regexp, List<Regexp> childValues)
            {
                for (int childIndex = 0; childIndex < regexp.subCount(); childIndex++) {
                    if (regexp.sub(childIndex) != childValues.get(childIndex)) {
                        return true;
                    }
                }
                return false;
            }

            private static Regexp rebuildNonConcat(Regexp regexp, List<Regexp> childValues)
            {
                return switch (regexp.op()) {
                    case ALTERNATE -> {
                        if (childValues.size() == 1) {
                            yield childValues.getFirst();
                        }
                        yield Regexp.alternate(regexp.parseFlags(), childValues);
                    }
                    case CAPTURE -> Regexp.capture(regexp.parseFlags(), childValues.getFirst(), regexp.cap(), regexp.name());
                    case STAR -> Regexp.rawUnary(RegexpOp.STAR, regexp.parseFlags(), childValues.getFirst());
                    case PLUS -> Regexp.rawUnary(RegexpOp.PLUS, regexp.parseFlags(), childValues.getFirst());
                    case QUEST -> Regexp.rawUnary(RegexpOp.QUEST, regexp.parseFlags(), childValues.getFirst());
                    case REPEAT -> Regexp.repeat(regexp.parseFlags(), childValues.getFirst(), regexp.min(), regexp.max());
                    case CONCAT -> throw new IllegalStateException("handled elsewhere");
                    default -> throw new IllegalStateException("unhandled op: " + regexp.op());
                };
            }

            private static Regexp rebuildConcat(Regexp regexp, List<Regexp> childValues)
            {
                if (childValues.isEmpty()) {
                    return Regexp.emptyMatch(regexp.parseFlags());
                }
                if (childValues.size() == 1) {
                    return childValues.getFirst();
                }
                return Regexp.concat(regexp.parseFlags(), childValues);
            }

            private static boolean canCoalesce(Regexp repeatedExpression, Regexp followingExpression)
            {
                // The left expression must be repetition over a literal/class/any-char/any-byte atom.
                if ((repeatedExpression.op() == RegexpOp.STAR || repeatedExpression.op() == RegexpOp.PLUS ||
                        repeatedExpression.op() == RegexpOp.QUEST || repeatedExpression.op() == RegexpOp.REPEAT)) {
                    Regexp repeatedAtom = repeatedExpression.sub(0);
                    if (repeatedAtom.op() != RegexpOp.LITERAL && repeatedAtom.op() != RegexpOp.CHAR_CLASS &&
                            repeatedAtom.op() != RegexpOp.ANY_CHAR && repeatedAtom.op() != RegexpOp.ANY_BYTE) {
                        return false;
                    }

                    // ... followed by repetition over the same atom with matching greediness.
                    if ((followingExpression.op() == RegexpOp.STAR || followingExpression.op() == RegexpOp.PLUS ||
                            followingExpression.op() == RegexpOp.QUEST || followingExpression.op() == RegexpOp.REPEAT) &&
                            Objects.equals(repeatedAtom, followingExpression.sub(0)) &&
                            ((repeatedExpression.parseFlags() & Regexp.NON_GREEDY) == (followingExpression.parseFlags() & Regexp.NON_GREEDY))) {
                        return true;
                    }

                    // ... or one direct occurrence of the same atom.
                    if (Objects.equals(repeatedAtom, followingExpression)) {
                        return true;
                    }

                    // ... or a literal string with that literal as its first rune.
                    if (repeatedAtom.op() == RegexpOp.LITERAL &&
                            followingExpression.op() == RegexpOp.LITERAL_STRING &&
                            followingExpression.runes().length > 0 &&
                            followingExpression.runes()[0] == repeatedAtom.rune() &&
                            ((repeatedAtom.parseFlags() & Regexp.FOLD_CASE) == (followingExpression.parseFlags() & Regexp.FOLD_CASE))) {
                        return true;
                    }
                }
                return false;
            }

            private static Regexp coalescePair(Regexp repeatedExpression, Regexp followingExpression, ArrayList<Regexp> output)
            {
                Regexp repeatedAtom = repeatedExpression.sub(0);

                int minRepetitions;
                int maxRepetitions;
                switch (repeatedExpression.op()) {
                    case STAR -> {
                        minRepetitions = 0;
                        maxRepetitions = -1;
                    }
                    case PLUS -> {
                        minRepetitions = 1;
                        maxRepetitions = -1;
                    }
                    case QUEST -> {
                        minRepetitions = 0;
                        maxRepetitions = 1;
                    }
                    case REPEAT -> {
                        minRepetitions = repeatedExpression.min();
                        maxRepetitions = repeatedExpression.max();
                    }
                    default -> throw new IllegalStateException("unexpected repeatedExpression op: " + repeatedExpression.op());
                }

                if (followingExpression.op() == RegexpOp.STAR) {
                    return Regexp.repeat(repeatedExpression.parseFlags(), repeatedAtom, minRepetitions, -1);
                }
                if (followingExpression.op() == RegexpOp.PLUS) {
                    minRepetitions++;
                    return Regexp.repeat(repeatedExpression.parseFlags(), repeatedAtom, minRepetitions, -1);
                }
                if (followingExpression.op() == RegexpOp.QUEST) {
                    if (maxRepetitions != -1) {
                        maxRepetitions++;
                    }
                    return Regexp.repeat(repeatedExpression.parseFlags(), repeatedAtom, minRepetitions, maxRepetitions);
                }
                if (followingExpression.op() == RegexpOp.REPEAT) {
                    minRepetitions += followingExpression.min();
                    if (followingExpression.max() == -1) {
                        maxRepetitions = -1;
                    }
                    else if (maxRepetitions != -1) {
                        maxRepetitions += followingExpression.max();
                    }
                    return Regexp.repeat(repeatedExpression.parseFlags(), repeatedAtom, minRepetitions, maxRepetitions);
                }
                if (followingExpression.op() == RegexpOp.LITERAL || followingExpression.op() == RegexpOp.CHAR_CLASS ||
                        followingExpression.op() == RegexpOp.ANY_CHAR || followingExpression.op() == RegexpOp.ANY_BYTE) {
                    minRepetitions++;
                    if (maxRepetitions != -1) {
                        maxRepetitions++;
                    }
                    return Regexp.repeat(repeatedExpression.parseFlags(), repeatedAtom, minRepetitions, maxRepetitions);
                }

                if (followingExpression.op() == RegexpOp.LITERAL_STRING) {
                    int repeatedRune = repeatedAtom.rune();
                    int[] literalStringRunes = followingExpression.runes();
                    int repeatedPrefixLength = 1;
                    while (repeatedPrefixLength < literalStringRunes.length && literalStringRunes[repeatedPrefixLength] == repeatedRune) {
                        repeatedPrefixLength++;
                    }
                    minRepetitions += repeatedPrefixLength;
                    if (maxRepetitions != -1) {
                        maxRepetitions += repeatedPrefixLength;
                    }

                    Regexp coalescedRepeat = Regexp.repeat(repeatedExpression.parseFlags(), repeatedAtom, minRepetitions, maxRepetitions);
                    if (repeatedPrefixLength == literalStringRunes.length) {
                        return coalescedRepeat;
                    }

                    int[] literalStringTail = Arrays.copyOfRange(literalStringRunes, repeatedPrefixLength, literalStringRunes.length);
                    output.add(coalescedRepeat);
                    return Regexp.literalString(followingExpression.parseFlags(), literalStringTail);
                }

                throw new IllegalStateException("unexpected followingExpression op: " + followingExpression.op());
            }
        }
    }
}
