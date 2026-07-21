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

// Ported from upstream RE2: re2/testing/required_prefix_test.cc.
public class TestUpstreamRequiredPrefix
{
    private record PrefixTestCase(String regexp, boolean hasPrefix, String prefix, boolean foldCase, String suffix) {}

    @Test
    public void testRequiredPrefixSimpleTests()
    {
        // From upstream re2/testing/required_prefix_test.cc RequiredPrefix.SimpleTests.
        List<PrefixTestCase> tests = List.of(
                // Empty cases.
                new PrefixTestCase("", false, null, false, null),
                new PrefixTestCase("(?m)^", false, null, false, null),
                new PrefixTestCase("(?-m)^", false, null, false, null),

                // If the regexp has no ^, there's no required prefix.
                new PrefixTestCase("abc", false, null, false, null),

                // If the regexp immediately goes into something not a literal match, there's no required prefix.
                new PrefixTestCase("^a*", false, null, false, null),
                new PrefixTestCase("^(abc)", false, null, false, null),

                // Otherwise, it should work.
                new PrefixTestCase("^abc$", true, "abc", false, "(?-m:$)"),
                new PrefixTestCase("^abc", true, "abc", false, ""),
                new PrefixTestCase("^(?i)abc", true, "abc", true, ""),
                new PrefixTestCase("^abcd*", true, "abc", false, "d*"),
                new PrefixTestCase("^[Aa][Bb]cd*", true, "ab", true, "cd*"),
                new PrefixTestCase("^ab[Cc]d*", true, "ab", false, "[Cc]d*"),
                new PrefixTestCase("^\u263Aabc", true, "\u263Aabc", false, ""));

        for (PrefixTestCase t : tests) {
            for (boolean latin1 : List.of(true, false)) {
                int flags = Regexp.LIKE_PERL | (latin1 ? Regexp.LATIN1 : 0);
                ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(t.regexp().getBytes(StandardCharsets.UTF_8)), flags);

                Regexp.RequiredPrefixResult result = parsed.regexp().requiredPrefix();
                assertThat(result != null).as("requiredPrefix() present: %s (latin1=%s)", t.regexp, latin1).isEqualTo(t.hasPrefix);

                if (t.hasPrefix) {
                    assertThat(decodeUtf8(result.prefix())).as("prefix: %s (latin1=%s)", t.regexp, latin1).isEqualTo(t.prefix);
                    assertThat(result.foldCase()).as("foldCase: %s (latin1=%s)", t.regexp, latin1).isEqualTo(t.foldCase);
                    assertThat(RegexpToString.toString(result.suffix())).as("suffix: %s (latin1=%s)", t.regexp, latin1).isEqualTo(t.suffix);
                }
            }
        }
    }

    @Test
    public void testRequiredPrefixForAccelSimpleTests()
    {
        // From upstream re2/testing/required_prefix_test.cc RequiredPrefixForAccel.SimpleTests.
        List<PrefixTestCase> tests = List.of(
                // Empty cases.
                new PrefixTestCase("", false, null, false, null),
                new PrefixTestCase("(?m)^", false, null, false, null),
                new PrefixTestCase("(?-m)^", false, null, false, null),

                // If the regexp has a ^, there's no required prefix.
                new PrefixTestCase("^abc", false, null, false, null),

                // If the regexp immediately goes into something not a literal match, there's no required prefix.
                new PrefixTestCase("a*", false, null, false, null),

                // Unlike RequiredPrefix(), RequiredPrefixForAccel() can "see through"
                // capturing groups, but doesn't try to glue prefix fragments together.
                new PrefixTestCase("(a?)def", false, null, false, null),
                new PrefixTestCase("(ab?)def", true, "a", false, null),
                new PrefixTestCase("(abc?)def", true, "ab", false, null),
                new PrefixTestCase("(()a)def", false, null, false, null),
                new PrefixTestCase("((a)b)def", true, "a", false, null),
                new PrefixTestCase("((ab)c)def", true, "ab", false, null),

                // Otherwise, it should work.
                new PrefixTestCase("abc$", true, "abc", false, null),
                new PrefixTestCase("abc", true, "abc", false, null),
                new PrefixTestCase("(?i)abc", true, "abc", true, null),
                new PrefixTestCase("abcd*", true, "abc", false, null),
                new PrefixTestCase("[Aa][Bb]cd*", true, "ab", true, null),
                new PrefixTestCase("ab[Cc]d*", true, "ab", false, null),
                new PrefixTestCase("\u263Aabc", true, "\u263Aabc", false, null));

        for (PrefixTestCase t : tests) {
            for (boolean latin1 : List.of(true, false)) {
                int flags = Regexp.LIKE_PERL | (latin1 ? Regexp.LATIN1 : 0);
                ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(t.regexp().getBytes(StandardCharsets.UTF_8)), flags);

                Regexp.RequiredPrefixForAccelResult result = parsed.regexp().requiredPrefixForAccel();
                assertThat(result != null).as("requiredPrefixForAccel() present: %s (latin1=%s)", t.regexp, latin1).isEqualTo(t.hasPrefix);

                if (t.hasPrefix) {
                    assertThat(decodeUtf8(result.prefix())).as("prefix: %s (latin1=%s)", t.regexp, latin1).isEqualTo(t.prefix);
                    assertThat(result.foldCase()).as("foldCase: %s (latin1=%s)", t.regexp, latin1).isEqualTo(t.foldCase);
                }
            }
        }
    }

    @Test
    public void testRequiredPrefixForAccelCaseFoldingForKAndS()
    {
        // From upstream re2/testing/required_prefix_test.cc RequiredPrefixForAccel.CaseFoldingForKAndS.
        ParseResult parsed;
        Regexp.RequiredPrefixForAccelResult prefix;

        // With Latin-1 encoding, `(?i)` prefixes can include 'k' and 's'.
        parsed = RegexpParser.parse(Slices.wrappedBuffer("(?i)KLM".getBytes(StandardCharsets.UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1);
        prefix = parsed.regexp().requiredPrefixForAccel();
        assertThat(prefix).isNotNull();
        assertThat(decodeUtf8(prefix.prefix())).isEqualTo("klm");
        assertThat(prefix.foldCase()).isTrue();

        parsed = RegexpParser.parse(Slices.wrappedBuffer("(?i)STU".getBytes(StandardCharsets.UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1);
        prefix = parsed.regexp().requiredPrefixForAccel();
        assertThat(prefix).isNotNull();
        assertThat(decodeUtf8(prefix.prefix())).isEqualTo("stu");
        assertThat(prefix.foldCase()).isTrue();

        // With UTF-8 encoding, `(?i)` prefixes can't include 'k' and 's' because the
        // parser emits character classes instead of literals.
        parsed = RegexpParser.parse(Slices.wrappedBuffer("(?i)KLM".getBytes(StandardCharsets.UTF_8)), Regexp.LIKE_PERL);
        assertThat(parsed.regexp().requiredPrefixForAccel()).isNull();

        parsed = RegexpParser.parse(Slices.wrappedBuffer("(?i)STU".getBytes(StandardCharsets.UTF_8)), Regexp.LIKE_PERL);
        assertThat(parsed.regexp().requiredPrefixForAccel()).isNull();
    }

    private static String decodeUtf8(Slice bytes)
    {
        return new String(bytes.byteArray(), bytes.byteArrayOffset(), bytes.length(), StandardCharsets.UTF_8);
    }
}
