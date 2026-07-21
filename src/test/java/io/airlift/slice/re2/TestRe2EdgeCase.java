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

// Includes tests ported from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2EdgeCase
{
    /**
     * Ported from TEST(RE2, NullVsEmptyString).
     * Tests that empty Slice (zero length) matches .* pattern correctly.
     */
    @Test
    public void testEmptyStringMatch()
    {
        Re2 re = Re2.compile(utf8(".*"), Re2.Options.defaults());

        Slice empty = Slices.wrappedBuffer(new byte[0]);
        assertThat(re.fullMatch(empty)).isTrue();
    }

    /**
     * Ported from TEST(RE2, NullVsEmptyStringSubmatches).
     * Tests submatch behavior with empty input.
     */
    @Test
    public void testEmptyStringSubmatches()
    {
        Re2 re = Re2.compile(utf8("(foo)?bar"), Re2.Options.defaults());

        MatchResult bar = re.fullMatchResult(utf8("bar"));
        assertThat(bar).isNotNull();
        assertThat(bar.groupUtf8(1)).isNull();

        MatchResult foobar = re.fullMatchResult(utf8("foobar"));
        assertThat(foobar).isNotNull();
        assertThat(foobar.groupUtf8(1)).isEqualTo("foo");
    }

    @Test
    public void testUpstreamEmptyStringSubmatches()
    {
        Re2 re = Re2.compile(utf8("()|(foo)"), Re2.Options.defaults());

        MatchResult result = re.partialMatchResult(Slices.EMPTY_SLICE);
        assertThat(result).isNotNull();
        assertThat(result.groupSlice(0)).isNotNull();
        assertThat(result.groupSlice(0).length()).isZero();
        assertThat(result.groupSlice(1)).isNotNull();
        assertThat(result.groupSlice(1).length()).isZero();
        assertThat(result.groupSlice(2)).isNull();
    }

    /**
     * Extension of NullVsEmptyStringSubmatches.
     * Tests matching non-empty input via the empty group branch.
     */
    @Test
    public void testNonEmptyStringSubmatchesViaEmptyBranch()
    {
        Re2 re = Re2.compile(utf8("(foo|)bar"), Re2.Options.defaults());

        MatchResult result = re.fullMatchResult(utf8("bar"));
        assertThat(result).isNotNull();
        assertThat(result.groupUtf8(1)).isEqualTo("");
    }

    /**
     * Extension of NullVsEmptyStringSubmatches.
     * Tests leftmost-first matching semantics with alternation.
     */
    @Test
    public void testFooMatchSubmatches()
    {
        Re2 re = Re2.compile(utf8("(foo|)bar"), Re2.Options.defaults());

        MatchResult result = re.fullMatchResult(utf8("foobar"));
        assertThat(result).isNotNull();
        assertThat(result.groupUtf8(1)).isEqualTo("foo");
    }

    /**
     * Ported from TEST(RE2, RegexpToStringLossOfAnchor).
     * Tests anchor preservation in POSIX vs Perl mode.
     */
    @Test
    public void testRegexpToStringLossOfAnchor()
    {
        assertThat(format("^[a-c]at", Re2.Options.posix())).isEqualTo("^[a-c]at");
        assertThat(format("^[a-c]at", Re2.Options.defaults())).isEqualTo("(?-m:^)[a-c]at");
        assertThat(format("ca[t-z]$", Re2.Options.posix())).isEqualTo("ca[t-z]$");
        assertThat(format("ca[t-z]$", Re2.Options.defaults())).isEqualTo("ca[t-z](?-m:$)");
    }

    @Test
    public void testFullMatchEnd()
    {
        assertThat(Re2.compile(utf8("fo|foo")).fullMatch(utf8("fo"))).isTrue();
        assertThat(Re2.compile(utf8("fo|foo")).fullMatch(utf8("foo"))).isTrue();
        assertThat(Re2.compile(utf8("fo|foo$")).fullMatch(utf8("fo"))).isTrue();
        assertThat(Re2.compile(utf8("fo|foo$")).fullMatch(utf8("foo"))).isTrue();
        assertThat(Re2.compile(utf8("foo$")).fullMatch(utf8("foo"))).isTrue();
        assertThat(Re2.compile(utf8("foo\\$")).fullMatch(utf8("foo$bar"))).isFalse();
        assertThat(Re2.compile(utf8("fo|bar")).fullMatch(utf8("fox"))).isFalse();
    }

    @Test
    public void testEmptyCharsetWithCaptures()
    {
        for (String pattern : List.of(
                "((((()))))[^\\S\\s]?",
                "((((()))))([^\\S\\s])?",
                "((((()))))([^\\S\\s]|[^\\S\\s])?",
                "((((()))))(([^\\S\\s]|[^\\S\\s])|)")) {
            assertThat(Re2.compile(utf8(pattern)).partialMatchResult(Slices.EMPTY_SLICE))
                    .as("pattern %s", pattern)
                    .isNotNull();
        }
    }

    @Test
    public void testLargeInputsDoNotUseJavaRecursion()
    {
        Re2 re = Re2.compile(utf8("([a-zA-Z0-9]|-)+(\\.([a-zA-Z0-9]|-)+)*(\\.)?"));
        for (String unit : List.of(".", "a", "a.", "ab.", "abc.")) {
            StringBuilder input = new StringBuilder(15 * 1024);
            while (input.length() < 15 * 1024) {
                input.append(unit);
            }
            input.setLength(15 * 1024);
            re.fullMatch(utf8(input.toString()));
        }
    }

    /**
     * Adapted from InitNULL test.
     * Tests error handling for invalid patterns.
     */
    @Test
    public void testDefaultInitialization()
    {
        assertThatThrownBy(() -> Re2.compile(utf8("("), Re2.Options.defaults()))
                .isInstanceOf(IllegalArgumentException.class);

        Re2 reValid = Re2.compile(utf8("foo"), Re2.Options.defaults());
        assertThat(reValid).isNotNull();
    }

    /**
     * Adapted from LazyRE2 tests.
     * Tests that default options use UTF-8 encoding and Latin1 options work.
     */
    @Test
    public void testOptionsDefaults()
    {
        assertThat(Re2.Options.defaults().encoding()).isEqualTo(Re2.Options.Encoding.UTF8);
        assertThat(Re2.Options.latin1().encoding()).isEqualTo(Re2.Options.Encoding.LATIN1);
    }

    /**
     * Additional edge case test for zero-length matches at various positions.
     */
    @Test
    public void testZeroLengthMatchPositions()
    {
        Re2 re = Re2.compile(utf8("a*"), Re2.Options.defaults());

        assertThat(re.fullMatch(utf8(""))).isTrue();
        assertThat(re.fullMatch(utf8("a"))).isTrue();
        assertThat(re.fullMatch(utf8("aaa"))).isTrue();
        assertThat(re.fullMatch(utf8("b"))).isFalse();
        assertThat(re.partialMatch(utf8("b"))).isTrue();
    }

    /**
     * Additional edge case test for alternation patterns with empty branches.
     */
    @Test
    public void testAlternationWithEmptyBranches()
    {
        Re2 re = Re2.compile(utf8("(|a)b"), Re2.Options.defaults());

        assertThat(re.fullMatch(utf8("b"))).isTrue();
        assertThat(re.fullMatch(utf8("ab"))).isTrue();
        assertThat(re.fullMatch(utf8("aab"))).isFalse();
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String format(String pattern, Re2.Options options)
    {
        return RegexpToString.toString(Re2.compile(utf8(pattern), options).regexp());
    }
}
