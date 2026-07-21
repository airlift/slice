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

public final class RegexpDump
{
    private RegexpDump() {}

    public static String dump(Regexp regexp)
    {
        requireNonNull(regexp, "regexp is null");
        StringBuilder output = new StringBuilder();
        dumpInto(output, regexp);
        return output.toString();
    }

    private static void dumpInto(StringBuilder output, Regexp regexp)
    {
        output.append(opName(regexp));
        output.append('{');
        switch (regexp.op()) {
            case END_TEXT -> {
                if ((regexp.parseFlags() & Regexp.WAS_DOLLAR) == 0) {
                    output.append("\\z");
                }
            }
            case LITERAL -> appendRune(output, regexp.parseFlags(), regexp.rune());
            case LITERAL_STRING -> {
                for (int rune : regexp.runes()) {
                    appendRune(output, regexp.parseFlags(), rune);
                }
            }
            case CONCAT, ALTERNATE -> {
                for (int index = 0; index < regexp.subCount(); index++) {
                    dumpInto(output, regexp.sub(index));
                }
            }
            case STAR, PLUS, QUEST -> dumpInto(output, regexp.sub(0));
            case CAPTURE -> {
                if (regexp.cap() == 0) {
                    throw new IllegalStateException("CAPTURE cap==0");
                }
                Slice name = regexp.name();
                if (name != null) {
                    output.append(name.toStringUtf8());
                    output.append(':');
                }
                dumpInto(output, regexp.sub(0));
            }
            case REPEAT -> {
                output.append(regexp.min());
                output.append(',');
                output.append(regexp.max());
                output.append(' ');
                dumpInto(output, regexp.sub(0));
            }
            case CHAR_CLASS -> {
                String separator = "";
                CharClass charClass = regexp.charClass();
                for (RuneRange runeRange : charClass.ranges()) {
                    output.append(separator);
                    if (runeRange.low() == runeRange.high()) {
                        output.append(hex(runeRange.low()));
                    }
                    else {
                        output.append(hex(runeRange.low()));
                        output.append('-');
                        output.append(hex(runeRange.high()));
                    }
                    separator = " ";
                }
            }
            default -> {}
        }
        output.append('}');
    }

    private static String opName(Regexp regexp)
    {
        String operatorName = switch (regexp.op()) {
            case NO_MATCH -> "no";
            case EMPTY_MATCH -> "emp";
            case LITERAL -> "lit";
            case LITERAL_STRING -> "str";
            case CONCAT -> "cat";
            case ALTERNATE -> "alt";
            case STAR -> "star";
            case PLUS -> "plus";
            case QUEST -> "que";
            case REPEAT -> "rep";
            case CAPTURE -> "cap";
            case ANY_CHAR -> "dot";
            case ANY_BYTE -> "byte";
            case BEGIN_LINE -> "bol";
            case END_LINE -> "eol";
            case WORD_BOUNDARY -> "wb";
            case NO_WORD_BOUNDARY -> "nwb";
            case BEGIN_TEXT -> "bot";
            case END_TEXT -> "eot";
            case CHAR_CLASS -> "cc";
            case HAVE_MATCH -> "match";
        };

        switch (regexp.op()) {
            case STAR, PLUS, QUEST, REPEAT -> {
                if ((regexp.parseFlags() & Regexp.NON_GREEDY) != 0) {
                    operatorName = "n" + operatorName;
                }
            }
            default -> {}
        }

        if (regexp.op() == RegexpOp.LITERAL && (regexp.parseFlags() & Regexp.FOLD_CASE) != 0) {
            int rune = regexp.rune();
            if ('a' <= rune && rune <= 'z') {
                operatorName += "fold";
            }
        }
        if (regexp.op() == RegexpOp.LITERAL_STRING && (regexp.parseFlags() & Regexp.FOLD_CASE) != 0) {
            for (int rune : regexp.runes()) {
                if ('a' <= rune && rune <= 'z') {
                    operatorName += "fold";
                    break;
                }
            }
        }

        return operatorName;
    }

    private static void appendRune(StringBuilder output, int flags, int rune)
    {
        if ((flags & Regexp.LATIN1) != 0) {
            output.append((char) rune);
            return;
        }
        output.appendCodePoint(rune);
    }

    /**
     * Formats hex to match upstream RE2 dump.cc behavior.
     * RE2/C-style "%#x" output is "0" for zero and "0x..." otherwise.
     * Java "%#x" differs for zero ("0x0"), so this helper is intentionally custom.
     */
    private static String hex(int value)
    {
        if (value == 0) {
            return "0";
        }
        return "0x" + Integer.toHexString(value);
    }
}
