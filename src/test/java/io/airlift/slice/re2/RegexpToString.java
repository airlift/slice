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

import static java.util.Objects.requireNonNull;

@SuppressWarnings("CharUsedInArithmeticContext")
public final class RegexpToString
{
    private RegexpToString() {}

    // Precedence levels used when emitting parentheses.
    private static final int PRECEDENCE_ATOM = 0;
    private static final int PRECEDENCE_UNARY = 1;
    private static final int PRECEDENCE_CONCAT = 2;
    private static final int PRECEDENCE_ALTERNATE = 3;
    private static final int PRECEDENCE_EMPTY = 4;
    private static final int PRECEDENCE_PAREN = 5;
    private static final int PRECEDENCE_TOP_LEVEL = 6;

    public static String toString(Regexp regexp)
    {
        requireNonNull(regexp, "regexp is null");

        StringBuilder output = new StringBuilder();
        ToStringWalker walker = new ToStringWalker(output);
        walker.walkExponential(regexp, PRECEDENCE_TOP_LEVEL, 100_000);
        if (walker.stoppedEarly) {
            output.append(" [truncated]");
        }
        return output.toString();
    }

    private static final class ToStringWalker
            extends RegexpWalker<Integer>
    {
        private final StringBuilder output;
        private boolean stoppedEarly;

        private ToStringWalker(StringBuilder output)
        {
            this.output = requireNonNull(output, "output is null");
        }

        @Override
        protected PreVisitResult<Integer> preVisit(Regexp regexp, Integer parentArg)
        {
            int parentPrecedence = parentArg;
            int childPrecedence = switch (regexp.op()) {
                case NO_MATCH, EMPTY_MATCH, LITERAL, ANY_CHAR, ANY_BYTE, BEGIN_LINE, END_LINE, BEGIN_TEXT,
                        END_TEXT, WORD_BOUNDARY, NO_WORD_BOUNDARY, CHAR_CLASS, HAVE_MATCH -> PRECEDENCE_ATOM;

                case CONCAT, LITERAL_STRING -> {
                    if (parentPrecedence < PRECEDENCE_CONCAT) {
                        output.append("(?:");
                    }
                    yield PRECEDENCE_CONCAT;
                }

                case ALTERNATE -> {
                    if (parentPrecedence < PRECEDENCE_ALTERNATE) {
                        output.append("(?:");
                    }
                    yield PRECEDENCE_ALTERNATE;
                }

                case CAPTURE -> {
                    output.append('(');
                    if (regexp.cap() == 0) {
                        throw new IllegalStateException("CAPTURE cap==0");
                    }
                    Slice name = regexp.name();
                    if (name != null) {
                        output.append("?P<")
                                .append(name.toStringUtf8())
                                .append(">");
                    }
                    yield PRECEDENCE_PAREN;
                }

                case STAR, PLUS, QUEST, REPEAT -> {
                    if (parentPrecedence < PRECEDENCE_UNARY) {
                        output.append("(?:");
                    }
                    // The subprecedence here is PRECEDENCE_ATOM instead of PRECEDENCE_UNARY because
                    // PCRE treats two unary ops in a row as a parse error.
                    yield PRECEDENCE_ATOM;
                }
            };

            return new PreVisitResult<>(childPrecedence, false);
        }

        @Override
        protected Integer shortVisit(Regexp regexp, Integer parentArg)
        {
            stoppedEarly = true;
            return 0;
        }

        @Override
        protected Integer postVisit(Regexp regexp, Integer parentArg, Integer preArg, java.util.List<Integer> childArgs)
        {
            int parentPrecedence = parentArg;
            switch (regexp.op()) {
                // There's no simple symbol for "no match", but [^0-Runemax] excludes everything.
                case NO_MATCH -> output.append("[^\\x00-\\x{10ffff}]");
                case EMPTY_MATCH -> {
                    // Append (?:) to make empty string visible, unless this is already being parenthesized.
                    if (parentPrecedence < PRECEDENCE_EMPTY) {
                        output.append("(?:)");
                    }
                }
                case LITERAL -> appendLiteral(output, regexp.rune(), (regexp.parseFlags() & Regexp.FOLD_CASE) != 0);
                case LITERAL_STRING -> {
                    for (int rune : regexp.runes()) {
                        appendLiteral(output, rune, (regexp.parseFlags() & Regexp.FOLD_CASE) != 0);
                    }
                    if (parentPrecedence < PRECEDENCE_CONCAT) {
                        output.append(')');
                    }
                }
                case CONCAT -> {
                    if (parentPrecedence < PRECEDENCE_CONCAT) {
                        output.append(')');
                    }
                }
                case ALTERNATE -> {
                    // Children all appended | at the end of their strings, so just remove the last one.
                    if (!output.isEmpty() && output.charAt(output.length() - 1) == '|') {
                        output.setLength(output.length() - 1);
                    }
                    else {
                        throw new IllegalStateException("alternate missing trailing |");
                    }
                    if (parentPrecedence < PRECEDENCE_ALTERNATE) {
                        output.append(')');
                    }
                }
                case STAR -> {
                    output.append('*');
                    if ((regexp.parseFlags() & Regexp.NON_GREEDY) != 0) {
                        output.append('?');
                    }
                    if (parentPrecedence < PRECEDENCE_UNARY) {
                        output.append(')');
                    }
                }
                case PLUS -> {
                    output.append('+');
                    if ((regexp.parseFlags() & Regexp.NON_GREEDY) != 0) {
                        output.append('?');
                    }
                    if (parentPrecedence < PRECEDENCE_UNARY) {
                        output.append(')');
                    }
                }
                case QUEST -> {
                    output.append('?');
                    if ((regexp.parseFlags() & Regexp.NON_GREEDY) != 0) {
                        output.append('?');
                    }
                    if (parentPrecedence < PRECEDENCE_UNARY) {
                        output.append(')');
                    }
                }
                case REPEAT -> {
                    if (regexp.max() == -1) {
                        output.append('{').append(regexp.min()).append(",}");
                    }
                    else if (regexp.min() == regexp.max()) {
                        output.append('{').append(regexp.min()).append('}');
                    }
                    else {
                        output.append('{').append(regexp.min()).append(',').append(regexp.max()).append('}');
                    }
                    if ((regexp.parseFlags() & Regexp.NON_GREEDY) != 0) {
                        output.append('?');
                    }
                    if (parentPrecedence < PRECEDENCE_UNARY) {
                        output.append(')');
                    }
                }
                case ANY_CHAR -> output.append('.');
                case ANY_BYTE -> output.append("\\C");
                case BEGIN_LINE -> output.append('^');
                case END_LINE -> output.append('$');
                case BEGIN_TEXT -> output.append("(?-m:^)");
                case END_TEXT -> {
                    if ((regexp.parseFlags() & Regexp.WAS_DOLLAR) != 0) {
                        output.append("(?-m:$)");
                    }
                    else {
                        output.append("\\z");
                    }
                }
                case WORD_BOUNDARY -> output.append("\\b");
                case NO_WORD_BOUNDARY -> output.append("\\B");
                case CHAR_CLASS -> appendCharClass(output, regexp.charClass());
                case CAPTURE -> output.append(')');
                // There's no syntax accepted by the parser to generate this node; make something readable.
                case HAVE_MATCH -> output.append("(?HaveMatch:").append(regexp.matchId()).append(')');
            }

            // If the parent is an alternation, append the | for it.
            if (parentPrecedence == PRECEDENCE_ALTERNATE) {
                output.append('|');
            }
            return 0;
        }
    }

    private static void appendLiteral(StringBuilder output, int rune, boolean foldCase)
    {
        if (rune != 0 && rune < 0x80 && "(){}[]*+?|.^$\\"
                .indexOf((char) rune) >= 0) {
            output.append('\\').append((char) rune);
            return;
        }

        if (foldCase && 'a' <= rune && rune <= 'z') {
            int upper = rune - ('a' - 'A');
            output.append('[')
                    .append((char) upper)
                    .append((char) (upper + ('a' - 'A')))
                    .append(']');
            return;
        }

        appendCcRange(output, rune, rune);
    }

    private static void appendCharClass(StringBuilder output, CharClass charClass)
    {
        if (charClass.rangeCount() == 0) {
            output.append("[^\\x00-\\x{10ffff}]");
            return;
        }

        output.append('[');

        // Heuristic: show class as negated if it contains the non-character 0xFFFE
        // and yet somehow isn't full.
        boolean full = (charClass.rangeCount() == 1 && charClass.range(0).low() == 0 && charClass.range(0).high() == Regexp.RUNEMAX);
        if (charClass.contains(0xFFFE) && !full) {
            CharClassBuilder charClassBuilder = new CharClassBuilder();
            for (RuneRange runeRange : charClass.ranges()) {
                charClassBuilder.addRange(runeRange.low(), runeRange.high());
            }
            charClassBuilder.negate();
            charClass = charClassBuilder.toCharClass();
            output.append('^');
        }

        for (RuneRange runeRange : charClass.ranges()) {
            appendCcRange(output, runeRange.low(), runeRange.high());
        }

        output.append(']');
    }

    private static void appendCcChar(StringBuilder output, int rune)
    {
        if (0x20 <= rune && rune <= 0x7E) {
            char c = (char) rune;
            if ("[]^-\\"
                    .indexOf(c) >= 0) {
                output.append('\\');
            }
            output.append(c);
            return;
        }

        switch (rune) {
            case '\r' -> {
                output.append("\\r");
                return;
            }
            case '\t' -> {
                output.append("\\t");
                return;
            }
            case '\n' -> {
                output.append("\\n");
                return;
            }
            case '\f' -> {
                output.append("\\f");
                return;
            }
            default -> {}
        }

        if (rune < 0x100) {
            output.append(String.format("\\x%02x", rune));
            return;
        }
        output.append("\\x{").append(Integer.toHexString(rune)).append('}');
    }

    private static void appendCcRange(StringBuilder output, int lowerBound, int upperBound)
    {
        if (lowerBound > upperBound) {
            return;
        }
        appendCcChar(output, lowerBound);
        if (lowerBound < upperBound) {
            output.append('-');
            appendCcChar(output, upperBound);
        }
    }
}
