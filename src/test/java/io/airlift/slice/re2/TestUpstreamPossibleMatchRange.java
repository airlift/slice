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
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Ported from upstream RE2: re2/testing/possible_match_test.cc.
public class TestUpstreamPossibleMatchRange
{
    private record PrefixTest(String regexp, int maximumLength, String min, String max, boolean maximumUtf8)
    {
        private PrefixTest(String regexp, int maximumLength, String min, String max)
        {
            this(regexp, maximumLength, min, max, false);
        }
    }

    private static final List<PrefixTest> TESTS = List.of(
            new PrefixTest("", 10, "", ""),
            new PrefixTest("Abcdef", 10, "Abcdef", "Abcdef"),
            new PrefixTest("abc(def|ghi)", 10, "abcdef", "abcghi"),
            new PrefixTest("a+hello", 10, "aa", "ahello"),
            new PrefixTest("a*hello", 10, "a", "hello"),
            new PrefixTest("def|abc", 10, "abc", "def"),
            new PrefixTest("a(b)(c)[d]", 10, "abcd", "abcd"),
            new PrefixTest("ab(cab|cat)", 10, "abcab", "abcat"),
            new PrefixTest("ab(cab|ca)x", 10, "abcabx", "abcax"),
            new PrefixTest("(ab|x)(c|de)", 10, "abc", "xde"),
            new PrefixTest("(ab|x)?(c|z)?", 10, "", "z"),
            new PrefixTest("[^\\s\\S]", 10, "", ""),
            new PrefixTest("(abc)+", 5, "abc", "abcac"),
            new PrefixTest("(abc)+", 2, "ab", "ac"),
            new PrefixTest("(abc)+", 1, "a", "b"),
            new PrefixTest("[a\u00C3\u00A1]", 4, "a", "\u00C3\u00A1"),
            new PrefixTest("a*", 10, "", "ab"),

            new PrefixTest("(?i)Abcdef", 10, "ABCDEF", "abcdef"),
            new PrefixTest("(?i)abc(def|ghi)", 10, "ABCDEF", "abcgh\u0131", true),
            new PrefixTest("(?i)a+hello", 10, "AA", "ahello"),
            new PrefixTest("(?i)a*hello", 10, "A", "hello"),
            new PrefixTest("(?i)def|abc", 10, "ABC", "def"),
            new PrefixTest("(?i)a(b)(c)[d]", 10, "ABCD", "abcd"),
            new PrefixTest("(?i)ab(cab|cat)", 10, "ABCAB", "abcat"),
            new PrefixTest("(?i)ab(cab|ca)x", 10, "ABCABX", "abcax"),
            new PrefixTest("(?i)(ab|x)(c|de)", 10, "ABC", "xde"),
            new PrefixTest("(?i)(ab|x)?(c|z)?", 10, "", "z"),
            new PrefixTest("(?i)[^\\s\\S]", 10, "", ""),
            new PrefixTest("(?i)(abc)+", 5, "ABC", "abcac"),
            new PrefixTest("(?i)(abc)+", 2, "AB", "ac"),
            new PrefixTest("(?i)(abc)+", 1, "A", "b"),
            new PrefixTest("(?i)[a\u00C3\u00A1]", 4, "A", "\u00C3\u00A1"),
            new PrefixTest("(?i)a*", 10, "", "ab"),
            new PrefixTest("(?i)A*", 10, "", "ab"),

            new PrefixTest("\\AAbcdef", 10, "Abcdef", "Abcdef"),
            new PrefixTest("\\Aabc(def|ghi)", 10, "abcdef", "abcghi"),
            new PrefixTest("\\Aa+hello", 10, "aa", "ahello"),
            new PrefixTest("\\Aa*hello", 10, "a", "hello"),
            new PrefixTest("\\Adef|abc", 10, "abc", "def"),
            new PrefixTest("\\Aa(b)(c)[d]", 10, "abcd", "abcd"),
            new PrefixTest("\\Aab(cab|cat)", 10, "abcab", "abcat"),
            new PrefixTest("\\Aab(cab|ca)x", 10, "abcabx", "abcax"),
            new PrefixTest("\\A(ab|x)(c|de)", 10, "abc", "xde"),
            new PrefixTest("\\A(ab|x)?(c|z)?", 10, "", "z"),
            new PrefixTest("\\A[^\\s\\S]", 10, "", ""),
            new PrefixTest("\\A(abc)+", 5, "abc", "abcac"),
            new PrefixTest("\\A(abc)+", 2, "ab", "ac"),
            new PrefixTest("\\A(abc)+", 1, "a", "b"),
            new PrefixTest("\\A[a\u00C3\u00A1]", 4, "a", "\u00C3\u00A1"),
            new PrefixTest("\\Aa*", 10, "", "ab"),

            new PrefixTest("(?i)\\AAbcdef", 10, "ABCDEF", "abcdef"),
            new PrefixTest("(?i)\\Aabc(def|ghi)", 10, "ABCDEF", "abcgh\u0131", true),
            new PrefixTest("(?i)\\Aa+hello", 10, "AA", "ahello"),
            new PrefixTest("(?i)\\Aa*hello", 10, "A", "hello"),
            new PrefixTest("(?i)\\Adef|abc", 10, "ABC", "def"),
            new PrefixTest("(?i)\\Aa(b)(c)[d]", 10, "ABCD", "abcd"),
            new PrefixTest("(?i)\\Aab(cab|cat)", 10, "ABCAB", "abcat"),
            new PrefixTest("(?i)\\Aab(cab|ca)x", 10, "ABCABX", "abcax"),
            new PrefixTest("(?i)\\A(ab|x)(c|de)", 10, "ABC", "xde"),
            new PrefixTest("(?i)\\A(ab|x)?(c|z)?", 10, "", "z"),
            new PrefixTest("(?i)\\A[^\\s\\S]", 10, "", ""),
            new PrefixTest("(?i)\\A(abc)+", 5, "ABC", "abcac"),
            new PrefixTest("(?i)\\A(abc)+", 2, "AB", "ac"),
            new PrefixTest("(?i)\\A(abc)+", 1, "A", "b"),
            new PrefixTest("(?i)\\A[a\u00C3\u00A1]", 4, "A", "\u00C3\u00A1"),
            new PrefixTest("(?i)\\Aa*", 10, "", "ab"),
            new PrefixTest("(?i)\\AA*", 10, "", "ab"));

    @Test
    public void testPossibleMatchRangeHandWritten()
    {
        // From upstream re2/testing/possible_match_test.cc PossibleMatchRange.HandWritten.
        for (PrefixTest testCase : TESTS) {
            Prog program = compile(testCase.regexp, Regexp.LIKE_PERL);
            Prog.PossibleMatchRangeResult range = program.possibleMatchRange(testCase.maximumLength);
            assertThat(range).as("possibleMatchRange(%s)", escape(testCase.regexp)).isNotNull();

            assertThat(toByteArray(range.min())).as("min (%s)", escape(testCase.regexp)).isEqualTo(toBytesLatin1(testCase.min));
            byte[] expectedMaximum = testCase.maximumUtf8 ? testCase.max.getBytes(StandardCharsets.UTF_8) : toBytesLatin1(testCase.max);
            assertThat(toByteArray(range.max())).as("max (%s)", escape(testCase.regexp)).isEqualTo(expectedMaximum);
        }
    }

    @Test
    public void testPossibleMatchRangeNoMaxCases()
    {
        // These patterns should not have a maximum in byte-ordering; upstream returns false.
        List<String> noMaxLatin1 = List.of(
                "[\\s\\S]+",
                "[\\x00-\\xFF]+",
                ".+hello",
                ".*hello",
                ".*");

        for (String pattern : noMaxLatin1) {
            Prog program = compile(pattern, Regexp.LIKE_PERL | Regexp.LATIN1);
            assertThat(program.possibleMatchRange(10))
                    .as("possibleMatchRange (no max): %s", escape(pattern))
                    .isNull();
        }

        Prog program = compile("\\C*", Regexp.LIKE_PERL);
        assertThat(program.possibleMatchRange(10))
                .as("possibleMatchRange (no max): \\\\C*")
                .isNull();
    }

    @Test
    public void testPossibleMatchRangeMaximumLengthZero()
    {
        // Fails because no room to write max.
        Prog program = compile("abc", Regexp.LIKE_PERL);
        assertThat(program.possibleMatchRange(0))
                .as("possibleMatchRange with maximumLength=0")
                .isNull();
    }

    @Test
    public void testPossibleMatchRangeMalformedRegexp()
    {
        // Fails because it's a malformed regexp — "*hello" should not parse.
        byte[] patternBytes = "*hello".getBytes(StandardCharsets.ISO_8859_1);
        assertThatThrownBy(() -> RegexpParser.parse(Slices.wrappedBuffer(patternBytes), Regexp.LIKE_PERL))
                .isInstanceOf(RegexpParseException.class)
                .hasMessageContaining("REPEAT_ARGUMENT");
    }

    private static Prog compile(String regexp, int flags)
    {
        byte[] patternBytes = regexp.getBytes(StandardCharsets.ISO_8859_1);
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(patternBytes), flags);

        Prog program = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(program).as("compile: %s", escape(regexp)).isNotNull();
        return program;
    }

    private static byte[] toBytesLatin1(String value)
    {
        return value.getBytes(StandardCharsets.ISO_8859_1);
    }

    private static byte[] toByteArray(Slice slice)
    {
        byte[] out = new byte[slice.length()];
        System.arraycopy(slice.byteArray(), slice.byteArrayOffset(), out, 0, slice.length());
        return out;
    }

    private static String escape(String s)
    {
        // Minimal escaping for assertion messages.
        return s.replace("\\", "\\\\");
    }
}
