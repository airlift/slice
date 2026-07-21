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

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static io.airlift.slice.re2.Regexp.MAX_REPEAT;
import static java.util.Objects.requireNonNull;

final class RegexpParser
{
    private RegexpParser() {}

    public static ParseResult parse(Slice pattern, int parseFlags)
    {
        requireNonNull(pattern, "pattern is null");
        if ((parseFlags & Regexp.LATIN1) == 0) {
            int invalidOffset = firstInvalidUtf8Offset(pattern);
            if (invalidOffset >= 0) {
                throw new RegexpParseException(RegexpStatusCode.BAD_UTF8, null, invalidOffset);
            }
        }

        Parser parser = new Parser(pattern, parseFlags);
        Regexp parsedRegexp = parser.parseRegexp();
        if (!parser.atEnd()) {
            // Unconsumed input at the top-level is typically a stray ')'.
            if (parser.peekByte() == ')') {
                throw parser.error(RegexpStatusCode.UNEXPECTED_PAREN, parser.errorArgAll());
            }
            throw parser.error(RegexpStatusCode.INTERNAL_ERROR, parser.errorArgAtCurrent());
        }
        // Upstream parse.cc canonicalizes, but it does not eliminate counted repetitions.
        // We keep compile-time simplify (repeat elimination, named class expansion) in the compiler pipeline.
        Regexp simplifiedRegexp = Simplifier.simplifyForParse(parsedRegexp);
        return new ParseResult(simplifiedRegexp, parser.capturingGroupCount);
    }

    @SuppressWarnings("CharUsedInArithmeticContext")
    private static final class Parser
    {
        // Capture names permit Lu, Ll, Lt, Lm, Lo, Nl, Mn, Mc, Nd, and Pc categories.
        private static final CharClass VALID_CAPTURE_NAME = buildValidCaptureNameCharClass();
        private static final CharClass PERL_DIGITS_CLASS = buildPerlDigitsCharClass();

        private final byte[] bytes;
        private final int start;
        private final int end;
        private int index;

        private int flags;
        private int capturingGroupCount;
        private boolean inQuote;

        Parser(Slice pattern, int flags)
        {
            this.bytes = pattern.byteArray();
            this.start = pattern.byteArrayOffset();
            this.end = start + pattern.length();
            this.index = start;
            this.flags = flags;

            if ((flags & ~Regexp.ALL_PARSE_FLAGS) != 0) {
                throw error(RegexpStatusCode.INTERNAL_ERROR, null);
            }
        }

        boolean atEnd()
        {
            return index >= end;
        }

        private Slice errorArgAtCurrent()
        {
            if (index >= end) {
                return null;
            }
            int width = runeByteWidthAt(index);
            if (width <= 0) {
                return null;
            }
            return Slices.wrappedBuffer(bytes, index, width);
        }

        private Slice errorArgAll()
        {
            return Slices.wrappedBuffer(bytes, start, end - start);
        }

        private int runeByteWidthAt(int offset)
        {
            if (offset >= end) {
                return 0;
            }
            if ((flags & Regexp.LATIN1) != 0) {
                return 1;
            }
            long decoded = Utf8.decode(bytes, offset, end);
            int width = Utf8.decodedWidth(decoded);
            return Math.max(width, 0);
        }

        private RegexpParseException error(RegexpStatusCode statusCode, Slice errorArgument)
        {
            return new RegexpParseException(statusCode, errorArgument, Math.max(index - start, 0));
        }

        Regexp parseRegexp()
        {
            if ((flags & Regexp.LITERAL) != 0) {
                return parseLiteralString();
            }
            return parseExpression();
        }

        private Regexp parseLiteralString()
        {
            if (atEnd()) {
                return Regexp.emptyMatch(flags);
            }
            List<Integer> runes = new ArrayList<>();
            while (!atEnd()) {
                runes.add(readRune(flags));
            }
            return makeLiteralString(flags, runes);
        }

        private Regexp parseExpression()
        {
            Deque<GroupFrame> groups = new ArrayDeque<>();
            groups.push(GroupFrame.root(flags));

            while (!atEnd()) {
                GroupFrame group = groups.peek();
                if (tryConsumeQuoteDirective()) {
                    continue;
                }
                int nextByte = peekByte();

                if (!inQuote && nextByte == '|') {
                    consumeByte('|');
                    group.expression().nextAlternative(flags);
                    continue;
                }

                if (!inQuote && nextByte == ')') {
                    if (groups.size() == 1) {
                        break;
                    }

                    consumeByte(')');
                    GroupFrame completed = groups.pop();
                    Regexp atom = completed.expression().build();
                    flags = completed.restoreFlags();
                    if (completed.capturing()) {
                        atom = Regexp.capture(completed.groupFlags(), atom, completed.captureIndex(), completed.name());
                    }
                    groups.peek().expression().add(parseRepeatSuffix(atom));
                    continue;
                }

                if (!inQuote && nextByte == '(') {
                    GroupFrame opened = parseGroupStart();
                    if (opened == null) {
                        group.expression().add(Regexp.emptyMatch(flags));
                    }
                    else {
                        groups.push(opened);
                    }
                    continue;
                }

                Regexp atom = parseRepeat();
                if (atom != null) {
                    group.expression().add(atom);
                }
            }

            if (groups.size() != 1) {
                throw error(RegexpStatusCode.MISSING_PAREN, errorArgAll());
            }
            return groups.pop().expression().build();
        }

        private static final class ExpressionBuilder
        {
            private final int expressionFlags;
            private int concatenationFlags;
            private final List<Regexp> alternatives = new ArrayList<>();
            private final List<Regexp> concatenation = new ArrayList<>();
            private final List<Integer> literalRunes = new ArrayList<>();
            private Integer literalFlags;

            ExpressionBuilder(int flags)
            {
                this.expressionFlags = flags;
                this.concatenationFlags = flags;
            }

            void add(Regexp atom)
            {
                if (atom.op() == RegexpOp.LITERAL) {
                    if (literalFlags != null && literalFlags != atom.parseFlags()) {
                        flushLiterals();
                    }
                    if (literalFlags == null) {
                        literalFlags = atom.parseFlags();
                    }
                    literalRunes.add(atom.rune());
                    return;
                }

                flushLiterals();
                concatenation.add(atom);
            }

            void nextAlternative(int flags)
            {
                alternatives.add(buildConcatenation());
                concatenationFlags = flags;
            }

            Regexp build()
            {
                alternatives.add(buildConcatenation());
                if (alternatives.size() == 1) {
                    return alternatives.getFirst();
                }
                return Regexp.alternate(expressionFlags, alternatives);
            }

            private Regexp buildConcatenation()
            {
                flushLiterals();
                Regexp result;
                if (concatenation.isEmpty()) {
                    result = Regexp.emptyMatch(concatenationFlags);
                }
                else if (concatenation.size() == 1) {
                    result = concatenation.getFirst();
                }
                else {
                    result = Regexp.concat(concatenationFlags, List.copyOf(concatenation));
                }
                concatenation.clear();
                return result;
            }

            private void flushLiterals()
            {
                if (literalRunes.isEmpty()) {
                    return;
                }
                concatenation.add(makeLiteralString(literalFlags == null ? concatenationFlags : literalFlags, literalRunes));
                literalRunes.clear();
                literalFlags = null;
            }
        }

        private record GroupFrame(
                ExpressionBuilder expression,
                int restoreFlags,
                int groupFlags,
                boolean capturing,
                int captureIndex,
                Slice name)
        {
            static GroupFrame root(int flags)
            {
                return new GroupFrame(new ExpressionBuilder(flags), flags, flags, false, -1, null);
            }
        }

        private static Regexp makeLiteralString(int flags, List<Integer> runes)
        {
            if (runes.size() == 1) {
                return Regexp.literal(flags, runes.getFirst());
            }
            int[] literalRunes = new int[runes.size()];
            for (int i = 0; i < runes.size(); i++) {
                literalRunes[i] = runes.get(i);
            }
            return Regexp.literalString(flags, literalRunes);
        }

        // repeat := atom (('*' | '+' | '?' | '{m,n}') '?'?)*
        private Regexp parseRepeat()
        {
            return parseRepeatSuffix(parseAtom());
        }

        private Regexp parseRepeatSuffix(Regexp atom)
        {
            boolean appliedRepeat = false;

            while (!atEnd()) {
                if (tryConsumeQuoteDirective()) {
                    // \Q and \E are syntax; they should not block repetition parsing.
                    continue;
                }
                if (inQuote) {
                    break;
                }
                int nextByte = peekByte();
                if (nextByte == '*' || nextByte == '+' || nextByte == '?' || nextByte == '{') {
                    if (nextByte == '{' && !isValidRepeatBrace()) {
                        break;
                    }

                    // Stacked repetition operators are a syntax error in Perl extensions mode.
                    // In non-Perl extensions mode, they are allowed and will be simplified later.
                    if (appliedRepeat && (flags & Regexp.PERL_EXTENSIONS) != 0) {
                        throw error(RegexpStatusCode.BAD_REPEAT_OP, errorArgAtCurrent());
                    }
                    atom = applyRepeat(atom);
                    appliedRepeat = true;
                    continue;
                }
                break;
            }

            return atom;
        }

        private boolean isValidRepeatBrace()
        {
            // Lookahead validation for repeat braces, matching C++ MaybeParseRepetition behavior.
            // Returns false if the syntax is invalid, causing '{' to be treated as literal.
            // C++ RE2 disallows leading zeros in repeat counts (e.g., {01} or {1,02}).
            int position = index + 1;

            // First number must exist and start with digit
            if (position >= end) {
                return false;
            }
            int firstByte = bytes[position] & 0xFF;
            if (firstByte < '0' || firstByte > '9') {
                return false;
            }

            // Check for leading zeros in first number
            if (firstByte == '0' && position + 1 < end) {
                int nextByte = bytes[position + 1] & 0xFF;
                if (nextByte >= '0' && nextByte <= '9') {
                    return false;
                }
            }

            // Skip first number digits
            while (position < end && (bytes[position] & 0xFF) >= '0' && (bytes[position] & 0xFF) <= '9') {
                position++;
            }

            if (position >= end) {
                return false;
            }

            int separatorByte = bytes[position] & 0xFF;
            if (separatorByte == ',') {
                position++;
                if (position >= end) {
                    return false;
                }
                int secondNumberByte = bytes[position] & 0xFF;
                separatorByte = secondNumberByte;
                if (secondNumberByte != '}') {
                    // Second number must exist and be valid
                    if (secondNumberByte < '0' || secondNumberByte > '9') {
                        return false;
                    }
                    // Check for leading zeros in second number
                    if (secondNumberByte == '0' && position + 1 < end) {
                        int nextByte = bytes[position + 1] & 0xFF;
                        if (nextByte >= '0' && nextByte <= '9') {
                            return false;
                        }
                    }
                    // Skip second number digits
                    while (position < end && (bytes[position] & 0xFF) >= '0' && (bytes[position] & 0xFF) <= '9') {
                        position++;
                    }
                    if (position >= end) {
                        return false;
                    }
                    separatorByte = bytes[position] & 0xFF;
                }
            }

            return separatorByte == '}';
        }

        private Regexp applyRepeat(Regexp atom)
        {
            if (atom == null) {
                throw error(RegexpStatusCode.REPEAT_ARGUMENT, errorArgAtCurrent());
            }

            int op = consumeByte();
            boolean greedyByDefault = (flags & Regexp.NON_GREEDY) == 0;

            int min = 0;
            int max = -1;
            RepetitionOperator repetitionOperator;

            if (op == '*') {
                repetitionOperator = RepetitionOperator.STAR;
            }
            else if (op == '+') {
                repetitionOperator = RepetitionOperator.PLUS;
                min = 1;
            }
            else if (op == '?') {
                repetitionOperator = RepetitionOperator.QUEST;
                max = 1;
            }
            else if (op == '{') {
                repetitionOperator = RepetitionOperator.REPEAT;
                int[] repetitionCounts = parseRepetitionCounts();
                min = repetitionCounts[0];
                max = repetitionCounts[1];
            }
            else {
                throw error(RegexpStatusCode.INTERNAL_ERROR, null);
            }

            boolean nonGreedy = !greedyByDefault;
            // Non-greedy operators (e.g. "*?") are only supported under Perl extensions.
            if ((flags & Regexp.PERL_EXTENSIONS) != 0 && !atEnd() && peekByte() == '?') {
                consumeByte('?');
                nonGreedy = greedyByDefault;
            }

            int nodeFlags = nonGreedy ? (flags | Regexp.NON_GREEDY) : (flags & ~Regexp.NON_GREEDY);

            return switch (repetitionOperator) {
                case STAR -> Regexp.star(nodeFlags, atom);
                case PLUS -> Regexp.plus(nodeFlags, atom);
                case QUEST -> Regexp.quest(nodeFlags, atom);
                case REPEAT -> {
                    // Match upstream: reject nested repetition whose worst-case expansion would exceed MAX_REPEAT.
                    // See re2/parse.cc PushRepetition() and RepetitionWalker.
                    if (wouldExceedRepeatLimit(atom, min, max)) {
                        throw error(RegexpStatusCode.REPEAT_SIZE, null);
                    }
                    yield Regexp.repeat(nodeFlags, atom, min, max);
                }
            };
        }

        private enum RepetitionOperator { STAR, PLUS, QUEST, REPEAT }

        private static boolean wouldExceedRepeatLimit(Regexp atom, int min, int max)
        {
            int maxRepetitionCount = (max < 0) ? min : max;
            if (maxRepetitionCount <= 1) {
                return false;
            }
            int budget = MAX_REPEAT / maxRepetitionCount;
            return repetitionBudget(atom, budget) == 0;
        }

        private static int repetitionBudget(Regexp regexp, int budget)
        {
            int minimumBudget = budget;
            Deque<RepetitionFrame> pending = new ArrayDeque<>();
            pending.push(new RepetitionFrame(regexp, budget));
            while (!pending.isEmpty()) {
                RepetitionFrame frame = pending.pop();
                int remainingBudget = frame.budget();
                Regexp current = frame.regexp();
                if (current.op() == RegexpOp.REPEAT) {
                    int maxRepetitionCount = (current.max() < 0) ? current.min() : current.max();
                    if (maxRepetitionCount > 0) {
                        remainingBudget /= maxRepetitionCount;
                        if (remainingBudget == 0) {
                            return 0;
                        }
                    }
                }

                minimumBudget = Math.min(minimumBudget, remainingBudget);
                for (Regexp child : current.subs()) {
                    pending.push(new RepetitionFrame(child, remainingBudget));
                }
            }
            return minimumBudget;
        }

        private record RepetitionFrame(Regexp regexp, int budget) {}

        private int[] parseRepetitionCounts()
        {
            int minRepetitions = parseDecimal();
            int maxRepetitions = minRepetitions;

            if (atEnd()) {
                throw error(RegexpStatusCode.MISSING_BRACKET, Slices.wrappedBuffer(bytes, index - 1, 1));
            }

            if (peekByte() == ',') {
                consumeByte(',');
                if (peekByte() == '}') {
                    maxRepetitions = -1;
                }
                else {
                    maxRepetitions = parseDecimal();
                }
            }

            if (atEnd() || consumeByte() != '}') {
                throw error(RegexpStatusCode.MISSING_BRACKET, errorArgAtCurrent());
            }

            if (minRepetitions < 0 || maxRepetitions < -1) {
                throw error(RegexpStatusCode.REPEAT_ARGUMENT, null);
            }
            if (maxRepetitions != -1 && maxRepetitions < minRepetitions) {
                throw error(RegexpStatusCode.REPEAT_ARGUMENT, null);
            }

            if (minRepetitions > MAX_REPEAT || maxRepetitions > MAX_REPEAT) {
                throw error(RegexpStatusCode.REPEAT_SIZE, null);
            }

            return new int[] {minRepetitions, maxRepetitions};
        }

        // atom := '(' ... ')' | '[' ... ']' | '.' | '^' | '$' | '\' escape | literal
        private Regexp parseAtom()
        {
            // Perl quoted literals: \Q...\E
            // These are syntax directives; they do not correspond to regexp operations themselves.
            do {
                if (atEnd()) {
                    return null;
                }
            }
            while (tryConsumeQuoteDirective());

            int nextByte = peekByte();
            if (inQuote) {
                return Regexp.literal(flags, readRune(flags));
            }

            // Repetition operators are not valid in atom position.
            // See upstream re2/parse.cc PushRepeatOp()/PushRepetition().
            if (nextByte == '*' || nextByte == '+' || nextByte == '?' || (nextByte == '{' && isValidRepeatBrace())) {
                throw error(RegexpStatusCode.REPEAT_ARGUMENT, Slices.wrappedBuffer(bytes, index, 1));
            }

            if (nextByte == '[') {
                return parseCharClass();
            }
            if (nextByte == '.') {
                consumeByte('.');
                if ((flags & Regexp.DOT_MATCHES_NEWLINE) != 0 && (flags & Regexp.NEVER_NEWLINE) == 0) {
                    return Regexp.anyChar(flags);
                }
                CharClassBuilder classBuilder = new CharClassBuilder();
                int maxRune = Regexp.maxRune(flags);
                if (maxRune < '\n') {
                    classBuilder.addRange(0, maxRune);
                }
                else {
                    if ('\n' > 0) {
                        classBuilder.addRange(0, '\n' - 1);
                    }
                    if ('\n' + 1 <= maxRune) {
                        classBuilder.addRange('\n' + 1, maxRune);
                    }
                }
                return Regexp.charClass(flags, classBuilder.toCharClass());
            }
            if (nextByte == '^') {
                consumeByte('^');
                return ((flags & Regexp.ONE_LINE) != 0) ? Regexp.beginText(flags) : Regexp.beginLine(flags);
            }
            if (nextByte == '$') {
                consumeByte('$');
                if ((flags & Regexp.ONE_LINE) != 0) {
                    return Regexp.endText(flags | Regexp.WAS_DOLLAR);
                }
                return Regexp.endLine(flags);
            }
            if (nextByte == '\\') {
                consumeByte('\\');
                return parseEscape();
            }

            return parseLiteral();
        }

        private boolean tryConsumeQuoteDirective()
        {
            if ((flags & Regexp.PERL_EXTENSIONS) == 0) {
                return false;
            }
            if (peekByte() != '\\' || index + 1 >= end) {
                return false;
            }
            int next = bytes[index + 1] & 0xFF;
            if (!inQuote && next == 'Q') {
                index += 2;
                inQuote = true;
                return true;
            }
            if (inQuote && next == 'E') {
                index += 2;
                inQuote = false;
                return true;
            }
            return false;
        }

        private GroupFrame parseGroupStart()
        {
            int groupFlags = flags;
            int groupStart = index;
            consumeByte('(');
            boolean capturing = (flags & Regexp.NEVER_CAPTURE) == 0;
            Slice name = null;
            int captureIndex = -1;

            if (!atEnd() && peekByte() == '?') {
                if ((flags & Regexp.PERL_EXTENSIONS) == 0) {
                    // Perl-style group syntax is only available under Perl extensions.
                    throw error(RegexpStatusCode.BAD_PERL_OP, Slices.wrappedBuffer(bytes, groupStart, 2));
                }
                consumeByte('?');
                // Look-around assertions are not supported. Match upstream error_arg formatting.
                // See upstream re2/parse.cc ParsePerlFlags() / parse_test.cc LookAround.ErrorArgs.
                if (!atEnd()) {
                    int c = peekByte();
                    if (c == '=' || c == '!') {
                        throw error(RegexpStatusCode.BAD_PERL_OP, Slices.wrappedBuffer(bytes, groupStart, 3));
                    }
                    if (c == '<' && index + 1 < end) {
                        int c2 = bytes[index + 1] & 0xFF;
                        if (c2 == '=' || c2 == '!') {
                            throw error(RegexpStatusCode.BAD_PERL_OP, Slices.wrappedBuffer(bytes, groupStart, 4));
                        }
                    }
                }
                if (!atEnd() && peekByte() == ':') {
                    consumeByte(':');
                    capturing = false;
                }
                else if (!atEnd() && peekByte() == 'P') {
                    // (?P<name>...)
                    consumeByte('P');
                    if (atEnd() || consumeByte() != '<') {
                        throw error(RegexpStatusCode.BAD_NAMED_CAPTURE, errorArgAtCurrent());
                    }
                    name = parseCaptureName(groupStart, '>');
                }
                else if (!atEnd() && peekByte() == '<') {
                    // (?<name>...)
                    consumeByte('<');
                    name = parseCaptureName(groupStart, '>');
                }
                else {
                    // Inline flags like (?m), (?-m), (?s:...), (?U:...), ...
                    int saved = flags;
                    flags = parseInlineFlags(saved, groupStart);

                    if (atEnd()) {
                        throw error(RegexpStatusCode.MISSING_PAREN, errorArgAll());
                    }
                    if (peekByte() == ')') {
                        // Directive: update flags for remainder of the current parse scope.
                        consumeByte(')');
                        return null;
                    }
                    if (peekByte() != ':') {
                        throw error(RegexpStatusCode.BAD_PERL_OP, errorArgAtCurrent());
                    }
                    consumeByte(':');
                    return new GroupFrame(new ExpressionBuilder(flags), saved, groupFlags, false, -1, null);
                }
            }

            if (capturing) {
                capturingGroupCount++;
                captureIndex = capturingGroupCount;
            }
            return new GroupFrame(new ExpressionBuilder(flags), groupFlags, groupFlags, capturing, captureIndex, name);
        }

        private int parseInlineFlags(int base, int groupStart)
        {
            int inlineFlags = base;
            boolean clearing = false;

            while (!atEnd()) {
                int c = peekByte();
                if (c == ')' || c == ':') {
                    break;
                }
                if (c == '-') {
                    consumeByte('-');
                    clearing = true;
                    continue;
                }
                int rune = readRune(flags);

                switch (rune) {
                    case 'i' -> inlineFlags = clearing ? (inlineFlags & ~Regexp.FOLD_CASE) : (inlineFlags | Regexp.FOLD_CASE);
                    case 's' -> inlineFlags = clearing ? (inlineFlags & ~Regexp.DOT_MATCHES_NEWLINE) : (inlineFlags | Regexp.DOT_MATCHES_NEWLINE);
                    case 'U' -> inlineFlags = clearing ? (inlineFlags & ~Regexp.NON_GREEDY) : (inlineFlags | Regexp.NON_GREEDY);
                    // RE2 uses "OneLine" with inverted sense: -m means OneLine, +m means !OneLine.
                    case 'm' -> inlineFlags = clearing ? (inlineFlags | Regexp.ONE_LINE) : (inlineFlags & ~Regexp.ONE_LINE);
                    default -> throw error(RegexpStatusCode.BAD_PERL_OP, Slices.wrappedBuffer(bytes, groupStart, index - groupStart));
                }
            }

            return inlineFlags;
        }

        private Slice parseCaptureName(int groupStart, int endDelimiter)
        {
            int nameStart = index;
            int nameEnd = index;
            while (nameEnd < end && (bytes[nameEnd] & 0xFF) != endDelimiter) {
                nameEnd++;
            }
            if (nameEnd >= end) {
                // Match upstream: error_arg is the remaining "(?P<name" / "(?<name" substring.
                throw error(RegexpStatusCode.BAD_NAMED_CAPTURE, Slices.wrappedBuffer(bytes, groupStart, end - groupStart));
            }

            int nameLength = nameEnd - nameStart;
            if (nameLength <= 0) {
                throw error(RegexpStatusCode.BAD_NAMED_CAPTURE, null);
            }

            // Validate name runes now that we know it's syntactically terminated by the delimiter.
            index = nameStart;
            while (index < nameEnd) {
                int rune = readUtf8RuneInName();
                if (!isValidCaptureNameRune(rune)) {
                    // Match upstream: error_arg is the full "(?P<...>" / "(?<...>" capture header.
                    throw error(RegexpStatusCode.BAD_NAMED_CAPTURE, Slices.wrappedBuffer(bytes, groupStart, (nameEnd - groupStart) + 1));
                }
            }
            // Consume the delimiter.
            index = nameEnd + 1;
            return Slices.wrappedBuffer(bytes, nameStart, nameLength);
        }

        private int readUtf8RuneInName()
        {
            long decoded = Utf8.decode(bytes, index, end);
            int width = Utf8.decodedWidth(decoded);
            if (width == 0) {
                throw error(RegexpStatusCode.BAD_UTF8, null);
            }
            int codePoint = Utf8.decodedCodePoint(decoded);
            if (width == 1 && codePoint == Utf8.RUNE_ERROR && (bytes[index] & 0xFF) >= 0x80) {
                throw error(RegexpStatusCode.BAD_UTF8, null);
            }
            index += width;
            return codePoint;
        }

        private static boolean isValidCaptureNameRune(int rune)
        {
            return VALID_CAPTURE_NAME.contains(rune);
        }

        private Regexp parseCharClass()
        {
            int nodeFlags = flags;
            int classStart = index;
            consumeByte('[');
            boolean negate = false;
            if (!atEnd() && peekByte() == '^') {
                consumeByte('^');
                negate = true;
            }

            CharClassBuilder characterClassBuilder = new CharClassBuilder();
            if (negate) {
                // Match upstream parse.cc: if NL can't match implicitly, pretend that
                // negated classes include a leading '\n' so that negation excludes it.
                boolean excludeNewline = ((nodeFlags & Regexp.CLASS_NEWLINE) == 0) || ((nodeFlags & Regexp.NEVER_NEWLINE) != 0);
                if (excludeNewline) {
                    characterClassBuilder.addRange('\n', '\n');
                }
            }
            boolean first = true;
            boolean closed = false;
            while (!atEnd()) {
                int nextByte = peekByte();
                if (nextByte == ']' && !first) {
                    consumeByte(']');
                    closed = true;
                    break;
                }
                // '-' is only okay unescaped as first or last in class, unless Perl extensions is enabled.
                // See upstream re2/parse.cc ParseCharClass() and parse_test.cc only_perl[].
                if (nextByte == '-' && !first && (flags & Regexp.PERL_EXTENSIONS) == 0 && !isAtClassEndAfterDash()) {
                    throw error(RegexpStatusCode.BAD_CHAR_RANGE, errorArgDashWithFollowingRune());
                }
                first = false;

                // Perl character classes inside [...]
                // Note: This is separate from parseEscape() since class escapes expand to ranges.
                if (nextByte == '\\' && index + 1 < end) {
                    int escapedByte = bytes[index + 1] & 0xFF;
                    CharClass characterClass = switch (escapedByte) {
                        case 'd' -> perlDigitsCharClass(nodeFlags, false);
                        case 'D' -> perlDigitsCharClass(nodeFlags, true);
                        case 's' -> perlSpacesCharClass(nodeFlags, false);
                        case 'S' -> perlSpacesCharClass(nodeFlags, true);
                        case 'w' -> perlWordCharClass(nodeFlags, false);
                        case 'W' -> perlWordCharClass(nodeFlags, true);
                        default -> null;
                    };
                    if (characterClass != null) {
                        if ((nodeFlags & Regexp.PERL_CLASSES) == 0) {
                            throw error(RegexpStatusCode.BAD_ESCAPE, null);
                        }
                        // consume '\' + escape char
                        index += 2;
                        for (RuneRange runeRange : characterClass.ranges()) {
                            characterClassBuilder.addRangeFlags(runeRange.low(), runeRange.high(), nodeFlags);
                        }
                        continue;
                    }
                }

                // POSIX character classes inside [...]
                // e.g. [[:lower:]] or [[:^lower:]].
                if (tryParsePosixCharClass(nodeFlags, characterClassBuilder)) {
                    continue;
                }

                // Unicode character groups inside [...], like \p{Han} or \P{Han}.
                if (nextByte == '\\' && index + 1 < end) {
                    int escapedByte = bytes[index + 1] & 0xFF;
                    if (escapedByte == 'p' || escapedByte == 'P') {
                        if ((nodeFlags & Regexp.UNICODE_GROUPS) == 0) {
                            throw error(RegexpStatusCode.BAD_ESCAPE, null);
                        }
                        // consume '\\' + 'p'/'P'
                        index += 2;
                        parseUnicodeGroupInto(characterClassBuilder, (escapedByte == 'P') ? -1 : 1, nodeFlags, index - 2);
                        continue;
                    }
                }

                int low = parseClassAtomRune(nodeFlags);
                int high = low;

                if (!atEnd() && peekByte() == '-' && lookaheadIsRangeEnd()) {
                    consumeByte('-');
                    high = parseClassAtomRune(nodeFlags);
                    if (high < low) {
                        throw error(RegexpStatusCode.BAD_CHAR_RANGE, null);
                    }
                }

                // For explicit singletons/ranges, do not implicitly filter '\n' out.
                characterClassBuilder.addRangeFlags(low, high, nodeFlags | Regexp.CLASS_NEWLINE);
            }

            if (!closed) {
                throw error(RegexpStatusCode.MISSING_BRACKET, Slices.wrappedBuffer(bytes, classStart, end - classStart));
            }

            if (negate) {
                characterClassBuilder.negate();
                characterClassBuilder.removeAbove(Regexp.maxRune(nodeFlags));
            }
            else {
                characterClassBuilder.removeAbove(Regexp.maxRune(nodeFlags));
            }

            CharClass characterClass = characterClassBuilder.toCharClass();
            return Regexp.charClass(nodeFlags & ~Regexp.FOLD_CASE, characterClass);
        }

        private boolean isAtClassEndAfterDash()
        {
            // index points at '-'. It's OK if '-' is the last element in class: "-]" or "-<end>".
            if (index + 1 >= end) {
                return true;
            }
            return (bytes[index + 1] & 0xFF) == ']';
        }

        private Slice errorArgDashWithFollowingRune()
        {
            // Match upstream: error_arg is "-<next rune>".
            int dashIndex = index;
            int nextIndex = dashIndex + 1;
            if (nextIndex >= end) {
                return Slices.wrappedBuffer(bytes, dashIndex, 1);
            }
            long decoded = Utf8.decode(bytes, nextIndex, end);
            int width = Utf8.decodedWidth(decoded);
            if (width <= 0) {
                return Slices.wrappedBuffer(bytes, dashIndex, 1);
            }
            return Slices.wrappedBuffer(bytes, dashIndex, 1 + width);
        }

        private static void addGroup(CharClassBuilder targetClassBuilder, CharClass group, int sign, int parseFlags)
        {
            if (sign == 1) {
                for (RuneRange runeRange : group.ranges()) {
                    targetClassBuilder.addRangeFlags(runeRange.low(), runeRange.high(), parseFlags);
                }
                return;
            }

            if ((parseFlags & Regexp.FOLD_CASE) != 0) {
                // Negating a case-folded group requires a two-step approach.
                CharClassBuilder temporaryBuilder = new CharClassBuilder();
                addGroup(temporaryBuilder, group, 1, parseFlags);
                boolean excludeNewline = ((parseFlags & Regexp.CLASS_NEWLINE) == 0) || ((parseFlags & Regexp.NEVER_NEWLINE) != 0);
                if (excludeNewline) {
                    temporaryBuilder.addRange('\n', '\n');
                }
                temporaryBuilder.negate();

                CharClass negatedClass = temporaryBuilder.toCharClass();
                for (RuneRange runeRange : negatedClass.ranges()) {
                    // Add the already-folded/negated result directly.
                    targetClassBuilder.addRange(runeRange.low(), runeRange.high());
                }
                return;
            }

            int next = 0;
            for (RuneRange runeRange : group.ranges()) {
                if (next < runeRange.low()) {
                    targetClassBuilder.addRangeFlags(next, runeRange.low() - 1, parseFlags);
                }
                next = runeRange.high() + 1;
            }
            if (next <= Regexp.RUNEMAX) {
                targetClassBuilder.addRangeFlags(next, Regexp.RUNEMAX, parseFlags);
            }
        }

        private static CharClass buildValidCaptureNameCharClass()
        {
            CharClassBuilder captureNameClassBuilder = new CharClassBuilder();
            // As in upstream, these are added with NoParseFlags (0).
            for (String group : new String[] {"Lu", "Ll", "Lt", "Lm", "Lo", "Nl", "Mn", "Mc", "Nd", "Pc"}) {
                CharClass characterClass = UnicodeGroups.lookup(group);
                if (characterClass == null) {
                    throw new IllegalStateException("missing Unicode group: " + group);
                }
                addGroup(captureNameClassBuilder, characterClass, 1, 0);
            }
            return captureNameClassBuilder.toCharClass();
        }

        private boolean tryParsePosixCharClass(int parseFlags, CharClassBuilder characterClassBuilder)
        {
            if (peekByte() != '[' || index + 1 >= end || bytes[index + 1] != ':') {
                return false;
            }

            int startIndex = index;
            index += 2; // "[:"

            boolean negate = false;
            if (!atEnd() && peekByte() == '^') {
                consumeByte('^');
                negate = true;
            }

            int nameStart = index;
            while (!atEnd()) {
                int c = peekByte();
                if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
                    consumeByte();
                    continue;
                }
                break;
            }
            int nameEnd = index;
            if (nameEnd == nameStart) {
                index = startIndex;
                return false;
            }

            if (atEnd() || peekByte() != ':' || index + 1 >= end || bytes[index + 1] != ']') {
                index = startIndex;
                return false;
            }

            consumeByte(':');
            consumeByte(']');

            String name = new String(bytes, nameStart, nameEnd - nameStart, StandardCharsets.US_ASCII);
            CharClass characterClass = posixCharClass(name);
            addGroup(characterClassBuilder, characterClass, negate ? -1 : 1, parseFlags);
            return true;
        }

        private static CharClass posixCharClass(String name)
        {
            CharClassBuilder characterClassBuilder = new CharClassBuilder();
            switch (name) {
                case "alnum" -> {
                    characterClassBuilder.addRange('0', '9');
                    characterClassBuilder.addRange('A', 'Z');
                    characterClassBuilder.addRange('a', 'z');
                }
                case "alpha" -> {
                    characterClassBuilder.addRange('A', 'Z');
                    characterClassBuilder.addRange('a', 'z');
                }
                case "blank" -> {
                    characterClassBuilder.addRange('\t', '\t');
                    characterClassBuilder.addRange(' ', ' ');
                }
                case "cntrl" -> {
                    characterClassBuilder.addRange(0x00, 0x1F);
                    characterClassBuilder.addRange(0x7F, 0x7F);
                }
                case "digit" -> characterClassBuilder.addRange('0', '9');
                case "graph" -> characterClassBuilder.addRange('!', '~');
                case "lower" -> characterClassBuilder.addRange('a', 'z');
                case "print" -> characterClassBuilder.addRange(' ', '~');
                case "punct" -> {
                    characterClassBuilder.addRange('!', '/');
                    characterClassBuilder.addRange(':', '@');
                    characterClassBuilder.addRange('[', '`');
                    characterClassBuilder.addRange('{', '~');
                }
                case "space" -> {
                    characterClassBuilder.addRange('\t', '\r'); // \t \n \v \f \r
                    characterClassBuilder.addRange(' ', ' ');
                }
                case "upper" -> characterClassBuilder.addRange('A', 'Z');
                case "xdigit" -> {
                    characterClassBuilder.addRange('0', '9');
                    characterClassBuilder.addRange('A', 'F');
                    characterClassBuilder.addRange('a', 'f');
                }
                case "ascii" -> characterClassBuilder.addRange(0x00, 0x7F);
                default -> throw new RegexpParseException(RegexpStatusCode.BAD_CHAR_CLASS, null, -1);
            }
            return characterClassBuilder.toCharClass();
        }

        private boolean lookaheadIsRangeEnd()
        {
            // Treat '-' as a range operator only if the following token is not ']' or end.
            if (index + 1 >= end) {
                return false;
            }
            int next = bytes[index + 1] & 0xFF;
            return next != ']';
        }

        private int parseClassAtomRune(int flags)
        {
            if (atEnd()) {
                throw error(RegexpStatusCode.BAD_CHAR_CLASS, null);
            }
            if (peekByte() == '\\') {
                int sequenceStart = index;
                consumeByte('\\');
                int escapedByte = consumeByte();
                // Mirror upstream ParseEscape() behavior: accept escapes for known sequences,
                // accept escaped non-alnum ASCII as itself, otherwise error.
                return switch (escapedByte) {
                    case 'n' -> '\n';
                    case 'r' -> '\r';
                    case 't' -> '\t';
                    case 'f' -> '\f';
                    case 'v' -> 0x0B;
                    case 'a' -> 0x07;
                    case 'x' -> parseHexEscape(flags, sequenceStart);
                    case '0', '1', '2', '3', '4', '5', '6', '7' -> parseOctalEscape(escapedByte, flags, sequenceStart);
                    default -> {
                        if (escapedByte < 0x80 && !Character.isLetterOrDigit((char) escapedByte)) {
                            yield escapedByte;
                        }
                        int width = runeByteWidthAt(index - 1);
                        int length = 1 + (width > 0 ? width : 1);
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, length));
                    }
                };
            }
            return readRune(flags);
        }

        private Regexp parseEscape()
        {
            if (atEnd()) {
                throw error(RegexpStatusCode.TRAILING_BACKSLASH, null);
            }
            int sequenceStart = index - 1; // the backslash was already consumed by the caller
            int escapedByte = consumeByte();
            return switch (escapedByte) {
                case 'A' -> {
                    if ((flags & Regexp.PERL_EXTENSIONS) == 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    yield Regexp.beginText(flags);
                }
                case 'z' -> {
                    if ((flags & Regexp.PERL_EXTENSIONS) == 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    yield Regexp.endText(flags);
                }
                case 'C' -> {
                    if ((flags & Regexp.PERL_EXTENSIONS) == 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    yield Regexp.anyByte(flags);
                }
                case 'b' -> {
                    if ((flags & Regexp.PERL_WORD_BOUNDARY) == 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    yield Regexp.wordBoundary(flags);
                }
                case 'B' -> {
                    if ((flags & Regexp.PERL_WORD_BOUNDARY) == 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    yield Regexp.noWordBoundary(flags);
                }
                case 'd' -> {
                    if ((flags & Regexp.PERL_CLASSES) == 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    yield perlDigits(flags, false);
                }
                case 'D' -> {
                    if ((flags & Regexp.PERL_CLASSES) == 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    yield perlDigits(flags, true);
                }
                case 's' -> {
                    if ((flags & Regexp.PERL_CLASSES) == 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    yield perlSpaces(flags, false);
                }
                case 'S' -> {
                    if ((flags & Regexp.PERL_CLASSES) == 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    yield perlSpaces(flags, true);
                }
                case 'w' -> {
                    if ((flags & Regexp.PERL_CLASSES) == 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    yield perlWord(flags, false);
                }
                case 'W' -> {
                    if ((flags & Regexp.PERL_CLASSES) == 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    yield perlWord(flags, true);
                }
                case 'p', 'P' -> {
                    if ((flags & Regexp.UNICODE_GROUPS) == 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    CharClassBuilder characterClassBuilder = new CharClassBuilder();
                    parseUnicodeGroupInto(characterClassBuilder, (escapedByte == 'P') ? -1 : 1, flags, index - 2);
                    characterClassBuilder.removeAbove(Regexp.maxRune(flags));
                    yield Regexp.charClass(flags & ~Regexp.FOLD_CASE, characterClassBuilder.toCharClass());
                }
                case 'Q', 'E' -> throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                case 'n' -> literalFromRune('\n');
                case 'r' -> literalFromRune('\r');
                case 't' -> literalFromRune('\t');
                case 'f' -> literalFromRune('\f');
                case 'v' -> literalFromRune(0x0B);
                case 'a' -> literalFromRune(0x07);
                case 'x' -> literalFromRune(parseHexEscape(flags, sequenceStart));
                case '0', '1', '2', '3', '4', '5', '6', '7' -> literalFromRune(parseOctalEscape(escapedByte, flags, sequenceStart));
                default -> {
                    if (escapedByte < 0x80 && !Character.isLetterOrDigit((char) escapedByte)) {
                        yield literalFromRune(escapedByte);
                    }
                    int width = runeByteWidthAt(index - 1);
                    int length = 1 + (width > 0 ? width : 1);
                    throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, length));
                }
            };
        }

        private void parseUnicodeGroupInto(CharClassBuilder characterClassBuilder, int sign, int parseFlags, int sequenceStart)
        {
            // Supports \pL and \p{...} (with optional leading '^' to invert).
            if (!atEnd() && peekByte() == '{') {
                consumeByte('{');
                int nameStart = index;
                while (!atEnd() && peekByte() != '}') {
                    consumeByte();
                }
                if (atEnd() || consumeByte() != '}') {
                    throw error(RegexpStatusCode.BAD_CHAR_RANGE, Slices.wrappedBuffer(bytes, sequenceStart, end - sequenceStart));
                }
                int nameEnd = index - 1;

                if (nameEnd > nameStart && (bytes[nameStart] & 0xFF) == '^') {
                    sign = -sign;
                    nameStart++;
                }

                String name = new String(bytes, nameStart, nameEnd - nameStart, StandardCharsets.UTF_8);
                CharClass group = UnicodeGroups.lookup(name);
                if (group == null) {
                    throw error(RegexpStatusCode.BAD_CHAR_RANGE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                }
                addGroup(characterClassBuilder, group, sign, parseFlags);
                return;
            }

            int nameStart = index;
            int rune = readRune(parseFlags);
            int nameEnd = index;
            if (rune == '^') {
                sign = -sign;
                nameStart = index;
                if (atEnd()) {
                    throw error(RegexpStatusCode.BAD_CHAR_RANGE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                }
                readRune(parseFlags);
                nameEnd = index;
            }

            String name = new String(bytes, nameStart, nameEnd - nameStart, StandardCharsets.UTF_8);
            CharClass group = UnicodeGroups.lookup(name);
            if (group == null) {
                throw error(RegexpStatusCode.BAD_CHAR_RANGE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
            }
            addGroup(characterClassBuilder, group, sign, parseFlags);
        }

        private static Regexp perlDigits(int flags, boolean negate)
        {
            if (!negate) {
                return Regexp.charClass(flags & ~Regexp.FOLD_CASE, PERL_DIGITS_CLASS);
            }
            return perlCharClass(flags, negate, perlDigitsCharClass(flags, false));
        }

        private static Regexp perlSpaces(int flags, boolean negate)
        {
            return perlCharClass(flags, negate, perlSpacesCharClass(flags, false));
        }

        private static Regexp perlWord(int flags, boolean negate)
        {
            return perlCharClass(flags, negate, perlWordCharClass(flags, false));
        }

        private static Regexp perlCharClass(int flags, boolean negate, CharClass group)
        {
            CharClassBuilder characterClassBuilder = new CharClassBuilder();
            addGroup(characterClassBuilder, group, negate ? -1 : 1, flags);
            characterClassBuilder.removeAbove(Regexp.maxRune(flags));
            return Regexp.charClass(flags & ~Regexp.FOLD_CASE, characterClassBuilder.toCharClass());
        }

        private static CharClass perlDigitsCharClass(int flags, boolean negate)
        {
            if (!negate) {
                return PERL_DIGITS_CLASS;
            }
            CharClassBuilder characterClassBuilder = new CharClassBuilder();
            characterClassBuilder.addRange('0', '9');
            if (negate) {
                characterClassBuilder.negate();
                characterClassBuilder.removeAbove(Regexp.maxRune(flags));
            }
            return characterClassBuilder.toCharClass();
        }

        private static CharClass buildPerlDigitsCharClass()
        {
            CharClassBuilder characterClassBuilder = new CharClassBuilder();
            characterClassBuilder.addRange('0', '9');
            return characterClassBuilder.toCharClass();
        }

        private static CharClass perlSpacesCharClass(int flags, boolean negate)
        {
            CharClassBuilder characterClassBuilder = new CharClassBuilder();
            characterClassBuilder.addRange(' ', ' ');
            characterClassBuilder.addRange('\t', '\t');
            characterClassBuilder.addRange('\n', '\n');
            characterClassBuilder.addRange('\r', '\r');
            characterClassBuilder.addRange('\f', '\f');
            if (negate) {
                characterClassBuilder.negate();
                characterClassBuilder.removeAbove(Regexp.maxRune(flags));
            }
            return characterClassBuilder.toCharClass();
        }

        private static CharClass perlWordCharClass(int flags, boolean negate)
        {
            CharClassBuilder characterClassBuilder = new CharClassBuilder();
            characterClassBuilder.addRange('0', '9');
            characterClassBuilder.addRange('A', 'Z');
            characterClassBuilder.addRange('a', 'z');
            characterClassBuilder.addRange('_', '_');
            if (negate) {
                characterClassBuilder.negate();
                characterClassBuilder.removeAbove(Regexp.maxRune(flags));
            }
            return characterClassBuilder.toCharClass();
        }

        private int parseHexEscape(int flags, int sequenceStart)
        {
            // Supports \xNN and \x{...} with any number of hex digits (>= 1).
            int runeMax = Regexp.maxRune(flags);

            if (!atEnd() && peekByte() == '{') {
                consumeByte('{');
                int value = 0;
                int digits = 0;
                while (!atEnd() && peekByte() != '}') {
                    int rune = readRune(flags);
                    int digitValue = hexDigit(rune);
                    if (digitValue < 0) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                    digits++;
                    value = (value << 4) | digitValue;
                    if (value > runeMax) {
                        throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                    }
                }
                if (digits == 0 || atEnd() || consumeByte() != '}') {
                    throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                }
                return value;
            }

            // \xNN
            if (atEnd()) {
                throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
            }
            int firstRune = readRune(flags);
            if (atEnd()) {
                throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
            }
            int secondRune = readRune(flags);
            int firstDigit = hexDigit(firstRune);
            int secondDigit = hexDigit(secondRune);
            if (firstDigit < 0 || secondDigit < 0) {
                throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
            }
            int value = (firstDigit << 4) | secondDigit;
            if (value > runeMax) {
                throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
            }
            return value;
        }

        private int parseOctalEscape(int firstDigit, int flags, int sequenceStart)
        {
            // A single non-zero octal digit would be a backreference (unsupported here),
            // so require at least one more octal digit for '1'..'7'.
            if (firstDigit != '0') {
                if (atEnd() || peekByte() < '0' || peekByte() > '7') {
                    throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
                }
            }

            int runeMax = Regexp.maxRune(flags);
            int code = firstDigit - '0';
            for (int i = 0; i < 2; i++) {
                if (atEnd()) {
                    break;
                }
                int c = peekByte();
                if (c < '0' || c > '7') {
                    break;
                }
                consumeByte();
                code = (code * 8) + (c - '0');
            }
            if (code > runeMax) {
                throw error(RegexpStatusCode.BAD_ESCAPE, Slices.wrappedBuffer(bytes, sequenceStart, index - sequenceStart));
            }
            return code;
        }

        private static int hexDigit(int value)
        {
            if (value >= '0' && value <= '9') {
                return value - '0';
            }
            if (value >= 'a' && value <= 'f') {
                return value - 'a' + 10;
            }
            if (value >= 'A' && value <= 'F') {
                return value - 'A' + 10;
            }
            return -1;
        }

        private Regexp parseLiteral()
        {
            int rune = readRune(flags);
            return literalFromRune(rune);
        }

        private Regexp literalFromRune(int rune)
        {
            // Under case folding, expand into a character class and then rely on
            // parse canonicalization (simplifyForParse) to rewrite [Aa] back
            // into a FoldCase literal where possible.
            if ((flags & Regexp.FOLD_CASE) != 0) {
                if ((flags & Regexp.LATIN1) != 0) {
                    if (('A' <= rune && rune <= 'Z') || ('a' <= rune && rune <= 'z')) {
                        CharClassBuilder characterClassBuilder = new CharClassBuilder();
                        addFoldedRangeLatin1(characterClassBuilder, rune, rune);
                        return Regexp.charClass(flags & ~Regexp.FOLD_CASE, characterClassBuilder.toCharClass());
                    }
                }
                else if (UnicodeCaseFold.cycleFoldRune(rune) != rune) {
                    CharClassBuilder characterClassBuilder = new CharClassBuilder();
                    int initialRune = rune;
                    do {
                        if ((flags & Regexp.NEVER_NEWLINE) == 0 || rune != '\n') {
                            characterClassBuilder.addRange(rune, rune);
                        }
                        rune = UnicodeCaseFold.cycleFoldRune(rune);
                    }
                    while (rune != initialRune);
                    return Regexp.charClass(flags & ~Regexp.FOLD_CASE, characterClassBuilder.toCharClass());
                }
            }

            // Exclude newline if applicable.
            if (((flags & Regexp.NEVER_NEWLINE) != 0) && rune == '\n') {
                return Regexp.noMatch(flags);
            }

            return Regexp.literal(flags, rune);
        }

        private static void addFoldedRangeLatin1(CharClassBuilder characterClassBuilder, int low, int high)
        {
            // Only folds ASCII letters A-Z and a-z (Latin-1 mode treats bytes).
            if (low > high) {
                return;
            }

            // Already clamped to Latin-1 in the parser when LATIN1 is set.
            for (int rune = low; rune <= high; rune++) {
                if ('A' <= rune && rune <= 'Z') {
                    characterClassBuilder.addRange(rune, rune);
                    characterClassBuilder.addRange(rune + ('a' - 'A'), rune + ('a' - 'A'));
                }
                else if ('a' <= rune && rune <= 'z') {
                    characterClassBuilder.addRange(rune, rune);
                    characterClassBuilder.addRange(rune - ('a' - 'A'), rune - ('a' - 'A'));
                }
                else {
                    characterClassBuilder.addRange(rune, rune);
                }
            }
        }

        private int readRune(int flags)
        {
            if ((flags & Regexp.LATIN1) != 0) {
                return consumeByte() & 0xFF;
            }

            long decoded = Utf8.decode(bytes, index, end);
            int width = Utf8.decodedWidth(decoded);
            if (width == 0) {
                throw error(RegexpStatusCode.BAD_UTF8, null);
            }
            int codePoint = Utf8.decodedCodePoint(decoded);
            if (width == 1 && codePoint == Utf8.RUNE_ERROR && (bytes[index] & 0xFF) >= 0x80) {
                throw error(RegexpStatusCode.BAD_UTF8, null);
            }
            index += width;
            return codePoint;
        }

        private int parseDecimal()
        {
            if (atEnd()) {
                throw error(RegexpStatusCode.REPEAT_ARGUMENT, null);
            }

            // Match upstream RE2: reject leading zeros (e.g., "01", "02")
            // but allow single "0"
            int firstDigit = peekByte();
            if (firstDigit < '0' || firstDigit > '9') {
                throw error(RegexpStatusCode.REPEAT_ARGUMENT, null);
            }
            if (firstDigit == '0') {
                consumeByte();
                // Check if there's another digit following - that would be a leading zero
                if (!atEnd()) {
                    int next = peekByte();
                    if (next >= '0' && next <= '9') {
                        throw error(RegexpStatusCode.REPEAT_ARGUMENT, null);
                    }
                }
                return 0;
            }

            int value = 0;
            boolean overflow = false;
            while (!atEnd()) {
                int nextByte = peekByte();
                if (nextByte < '0' || nextByte > '9') {
                    break;
                }
                consumeByte();
                if (!overflow) {
                    value = value * 10 + (nextByte - '0');
                    if (value > MAX_REPEAT) {
                        overflow = true;
                    }
                }
            }

            return value;
        }

        private int peekByte()
        {
            return bytes[index] & 0xFF;
        }

        private int consumeByte()
        {
            if (atEnd()) {
                return -1;
            }
            return bytes[index++] & 0xFF;
        }

        private void consumeByte(int expected)
        {
            int got = consumeByte();
            if (got != expected) {
                throw error(RegexpStatusCode.INTERNAL_ERROR, null);
            }
        }
    }

    private static int firstInvalidUtf8Offset(Slice pattern)
    {
        byte[] bytes = pattern.byteArray();
        int startOffset = pattern.byteArrayOffset();
        int byteOffset = startOffset;
        int endOffset = startOffset + pattern.length();

        // Fast path: pure ASCII input is always valid UTF-8.
        while (byteOffset < endOffset && (bytes[byteOffset] & 0x80) == 0) {
            byteOffset++;
        }
        if (byteOffset == endOffset) {
            return -1;
        }

        while (byteOffset < endOffset) {
            long decodedRune = Utf8.decode(bytes, byteOffset, endOffset);
            int decodedWidth = Utf8.decodedWidth(decodedRune);
            if (decodedWidth == 0) {
                return byteOffset - startOffset;
            }
            int decodedCodePoint = Utf8.decodedCodePoint(decodedRune);
            if (decodedWidth == 1 && decodedCodePoint == Utf8.RUNE_ERROR && (bytes[byteOffset] & 0xFF) >= 0x80) {
                return byteOffset - startOffset;
            }
            byteOffset += decodedWidth;
        }
        return -1;
    }
}
