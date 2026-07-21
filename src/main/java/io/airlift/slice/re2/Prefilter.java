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
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import static io.airlift.slice.re2.Prefilter.PrefilterOp.ALL;
import static io.airlift.slice.re2.Prefilter.PrefilterOp.AND;
import static io.airlift.slice.re2.Prefilter.PrefilterOp.ATOM;
import static io.airlift.slice.re2.Prefilter.PrefilterOp.NONE;
import static io.airlift.slice.re2.Prefilter.PrefilterOp.OR;

/**
 * Extracts byte-string guards from a regular expression.
 */
final class Prefilter
{
    enum PrefilterOp
    {
        ALL,
        NONE,
        ATOM,
        AND,
        OR,
    }

    private PrefilterOp op;
    private final Slice atom;
    private final List<Prefilter> subs;

    private Prefilter(PrefilterOp op)
    {
        this(op, null);
    }

    private Prefilter(PrefilterOp op, Slice atom)
    {
        this.op = op;
        this.atom = atom;
        this.subs = new ArrayList<>();
    }

    PrefilterOp op()
    {
        return op;
    }

    Slice atom()
    {
        return atom;
    }

    List<Prefilter> subs()
    {
        return subs;
    }

    static Prefilter fromRe2(Re2 re2)
    {
        if (re2 == null || re2.regexp() == null) {
            return null;
        }

        Regexp regexp = Simplifier.simplify(re2.regexp());
        boolean latin1 = (regexp.parseFlags() & Regexp.LATIN1) != 0;
        return buildInfo(regexp, latin1).takeMatch();
    }

    private Prefilter simplify()
    {
        if (op != AND && op != OR) {
            return this;
        }
        if (subs.isEmpty()) {
            op = (op == AND) ? ALL : NONE;
            return this;
        }
        if (subs.size() == 1) {
            return subs.getFirst().simplify();
        }
        return this;
    }

    private static Prefilter and(Prefilter left, Prefilter right)
    {
        return andOr(AND, left, right);
    }

    private static Prefilter or(Prefilter left, Prefilter right)
    {
        return andOr(OR, left, right);
    }

    private static Prefilter andOr(PrefilterOp operation, Prefilter left, Prefilter right)
    {
        left = left.simplify();
        right = right.simplify();

        if (left.op.ordinal() > right.op.ordinal()) {
            Prefilter swap = left;
            left = right;
            right = swap;
        }

        if (left.op == ALL || left.op == NONE) {
            if ((left.op == ALL && operation == AND) || (left.op == NONE && operation == OR)) {
                return right;
            }
            return left;
        }

        if (left.op == operation && right.op == operation) {
            left.subs.addAll(right.subs);
            return left;
        }
        if (right.op == operation) {
            Prefilter swap = left;
            left = right;
            right = swap;
        }
        if (left.op == operation) {
            left.subs.add(right);
            return left;
        }

        Prefilter result = new Prefilter(operation);
        result.subs.add(left);
        result.subs.add(right);
        return result;
    }

    private static Prefilter fromString(Slice atom)
    {
        return new Prefilter(ATOM, atom);
    }

    private static Prefilter orStrings(NavigableSet<Slice> strings)
    {
        simplifyStringSet(strings);
        Prefilter result = new Prefilter(NONE);
        for (Slice string : strings) {
            result = or(result, fromString(string));
        }
        return result;
    }

    private static void simplifyStringSet(NavigableSet<Slice> strings)
    {
        List<Slice> ordered = new ArrayList<>(strings);
        for (int currentIndex = 0; currentIndex < ordered.size(); currentIndex++) {
            Slice current = ordered.get(currentIndex);
            if (current.length() == 0) {
                continue;
            }
            for (int candidateIndex = currentIndex + 1; candidateIndex < ordered.size(); candidateIndex++) {
                Slice candidate = ordered.get(candidateIndex);
                if (candidate.length() > current.length() && candidate.indexOf(current) >= 0) {
                    strings.remove(candidate);
                }
            }
        }
    }

    private static Info buildInfo(Regexp regexp, boolean latin1)
    {
        return switch (regexp.op()) {
            case NO_MATCH -> Info.noMatch();
            case EMPTY_MATCH, BEGIN_LINE, END_LINE, BEGIN_TEXT, END_TEXT, WORD_BOUNDARY, NO_WORD_BOUNDARY -> Info.emptyString();
            case LITERAL -> Info.literal(regexp.rune(), latin1);
            case LITERAL_STRING -> literalString(regexp.runes(), latin1);
            case CONCAT -> concat(regexp.subs(), latin1);
            case ALTERNATE -> alternate(regexp.subs(), latin1);
            case STAR -> Info.star(buildInfo(regexp.sub(0), latin1));
            case QUEST -> Info.quest(buildInfo(regexp.sub(0), latin1));
            case PLUS -> Info.plus(buildInfo(regexp.sub(0), latin1));
            case ANY_CHAR, ANY_BYTE -> Info.anyMatch();
            case CHAR_CLASS -> Info.characterClass(regexp.charClass(), latin1);
            case CAPTURE -> buildInfo(regexp.sub(0), latin1);
            case REPEAT, HAVE_MATCH -> Info.anyMatch();
        };
    }

    private static Info literalString(int[] runes, boolean latin1)
    {
        if (runes.length == 0) {
            return Info.noMatch();
        }

        Info result = Info.literal(runes[0], latin1);
        for (int runeIndex = 1; runeIndex < runes.length; runeIndex++) {
            result = Info.concat(result, Info.literal(runes[runeIndex], latin1));
        }
        return result;
    }

    private static Info concat(List<Regexp> children, boolean latin1)
    {
        Info result = null;
        Info exact = null;
        for (Regexp child : children) {
            Info childInfo = buildInfo(child, latin1);
            if (!childInfo.isExact || (exact != null && childInfo.exact.size() * exact.exact.size() > 16)) {
                result = Info.and(result, exact);
                exact = null;
                result = Info.and(result, childInfo);
            }
            else {
                exact = Info.concat(exact, childInfo);
            }
        }
        return Info.and(result, exact);
    }

    private static Info alternate(List<Regexp> children, boolean latin1)
    {
        Info result = buildInfo(children.getFirst(), latin1);
        for (int childIndex = 1; childIndex < children.size(); childIndex++) {
            result = Info.alternate(result, buildInfo(children.get(childIndex), latin1));
        }
        return result;
    }

    private static Slice concatSlices(Slice left, Slice right)
    {
        Slice result = Slices.allocate(left.length() + right.length());
        result.setBytes(0, left);
        result.setBytes(left.length(), right);
        return result;
    }

    private static Slice runeToSlice(int rune, boolean latin1)
    {
        if (latin1) {
            return Slices.wrappedBuffer(new byte[] {(byte) toLowerLatin1(rune)});
        }
        return Slices.utf8Slice(new String(Character.toChars(canonicalCaseFold(rune))));
    }

    private static int canonicalCaseFold(int rune)
    {
        // Lowercase alone does not canonicalize equivalence classes such as sigma/final sigma.
        return Character.toLowerCase(Character.toUpperCase(rune));
    }

    private static int toLowerLatin1(int rune)
    {
        if (rune >= 'A' && rune <= 'Z') {
            return rune + ('a' - 'A');
        }
        return rune;
    }

    private static NavigableSet<Slice> newStringSet()
    {
        return new TreeSet<>(Comparator.comparingInt(Slice::length).thenComparing(Slice::compareTo));
    }

    private static final class Info
    {
        private final NavigableSet<Slice> exact = newStringSet();
        private boolean isExact;
        private Prefilter match;

        private Prefilter takeMatch()
        {
            if (isExact) {
                match = orStrings(exact);
                isExact = false;
            }
            return match;
        }

        private static Info concat(Info left, Info right)
        {
            if (left == null) {
                return right;
            }

            Info result = new Info();
            for (Slice leftString : left.exact) {
                for (Slice rightString : right.exact) {
                    result.exact.add(concatSlices(leftString, rightString));
                }
            }
            result.isExact = true;
            return result;
        }

        private static Info and(Info left, Info right)
        {
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }

            Info result = new Info();
            result.match = Prefilter.and(left.takeMatch(), right.takeMatch());
            return result;
        }

        private static Info alternate(Info left, Info right)
        {
            Info result = new Info();
            if (left.isExact && right.isExact) {
                result.exact.addAll(left.exact);
                result.exact.addAll(right.exact);
                result.isExact = true;
            }
            else {
                result.match = Prefilter.or(left.takeMatch(), right.takeMatch());
            }
            return result;
        }

        private static Info quest(Info ignored)
        {
            return anyMatch();
        }

        private static Info star(Info info)
        {
            return quest(info);
        }

        private static Info plus(Info info)
        {
            Info result = new Info();
            result.match = info.takeMatch();
            return result;
        }

        private static Info literal(int rune, boolean latin1)
        {
            Info result = new Info();
            result.exact.add(runeToSlice(rune, latin1));
            result.isExact = true;
            return result;
        }

        private static Info characterClass(CharClass characterClass, boolean latin1)
        {
            if (characterClass.runeCount() > 10) {
                return anyMatch();
            }

            Info result = new Info();
            for (RuneRange range : characterClass.ranges()) {
                for (int rune = range.low(); rune <= range.high(); rune++) {
                    result.exact.add(runeToSlice(rune, latin1));
                }
            }
            result.isExact = true;
            return result;
        }

        private static Info noMatch()
        {
            Info result = new Info();
            result.match = new Prefilter(NONE);
            return result;
        }

        private static Info anyMatch()
        {
            Info result = new Info();
            result.match = new Prefilter(ALL);
            return result;
        }

        private static Info emptyString()
        {
            Info result = new Info();
            result.exact.add(Slices.EMPTY_SLICE);
            result.isExact = true;
            return result;
        }
    }
}
