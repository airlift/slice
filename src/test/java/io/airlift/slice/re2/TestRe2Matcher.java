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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Includes tests adapted from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2Matcher
{
    @Test
    public void testRepeatedFindReusesMatcher()
    {
        Re2Matcher matcher = Re2.compile(Slices.utf8Slice("(?<word>\\w+)"))
                .matcher(Slices.utf8Slice("one two"));

        assertThat(matcher.find()).isTrue();
        assertThat(matcher.start()).isEqualTo(0);
        assertThat(matcher.end()).isEqualTo(3);
        assertThat(matcher.group("word").toStringUtf8()).isEqualTo("one");

        assertThat(matcher.find()).isTrue();
        assertThat(matcher.start()).isEqualTo(4);
        assertThat(matcher.end()).isEqualTo(7);
        assertThat(matcher.group(1).toStringUtf8()).isEqualTo("two");
        assertThat(matcher.find()).isFalse();
    }

    @Test
    public void testRepeatedAnchoredMatchingOverRemainingInput()
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice("\\s*(\\w+)"));
        Slice remaining = Slices.utf8Slice("   aaa b!@#$@#$cccc");

        Re2Matcher matcher = pattern.matcher(remaining);
        assertThat(matcher.lookingAt()).isTrue();
        assertThat(matcher.group(1).toStringUtf8()).isEqualTo("aaa");
        remaining = remaining.slice(matcher.end(), remaining.length() - matcher.end());

        matcher.reset(remaining);
        assertThat(matcher.lookingAt()).isTrue();
        assertThat(matcher.group(1).toStringUtf8()).isEqualTo("b");
        remaining = remaining.slice(matcher.end(), remaining.length() - matcher.end());

        assertThat(matcher.reset(remaining).lookingAt()).isFalse();
    }

    @Test
    public void testRepeatedFindSkipsNonMatchingInput()
    {
        Re2Matcher matcher = Re2.compile(Slices.utf8Slice("(\\w+)"))
                .matcher(Slices.utf8Slice("   aaa b!@#$@#$cccc"));

        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group(1).toStringUtf8()).isEqualTo("aaa");
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group(1).toStringUtf8()).isEqualTo("b");
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group(1).toStringUtf8()).isEqualTo("cccc");
        assertThat(matcher.find()).isFalse();

        matcher = Re2.compile(Slices.utf8Slice("aaa")).matcher(Slices.utf8Slice("aaa"));
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.end()).isEqualTo(3);
        assertThat(matcher.find()).isFalse();
    }

    @Test
    public void testGroupZeroMatcherDoesNotExtractCaptures()
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice("(💰)"));
        Re2Matcher matcher = pattern.groupZeroMatcher(Slices.utf8Slice("a💰b💰"), null);

        assertThat(matcher.groupCount()).isZero();
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.start()).isEqualTo(1);
        assertThat(matcher.end()).isEqualTo(5);
        assertThat(matcher.group().toStringUtf8()).isEqualTo("💰");
        assertThatThrownBy(() -> matcher.group(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("group index out of range: 1");

        assertThat(matcher.find()).isTrue();
        assertThat(matcher.start()).isEqualTo(6);
        assertThat(matcher.end()).isEqualTo(10);
        assertThat(matcher.find()).isFalse();
    }

    @Test
    public void testPublicMatcherCanLimitCaptures()
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice("(?<first>a)(?<second>b)"));
        assertThat(pattern.isNamedCapturingGroupsComputed()).isFalse();

        Re2Matcher matcher = pattern.matcher(Slices.utf8Slice("zab"), 0);
        assertThat(matcher.groupCount()).isZero();
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.start()).isEqualTo(1);
        assertThat(matcher.end()).isEqualTo(3);
        assertThat(pattern.isNamedCapturingGroupsComputed()).isFalse();
        assertThatThrownBy(() -> matcher.group(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("group index out of range: 1");
    }

    @Test
    public void testGroupZeroMatcherDoesNotLoadNamedCaptures()
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice("(?<value>a)"));
        assertThat(pattern.isNamedCapturingGroupsComputed()).isFalse();

        assertThat(pattern.groupZeroMatcher(Slices.utf8Slice("a"), null).find()).isTrue();
        assertThat(pattern.isNamedCapturingGroupsComputed()).isFalse();

        assertThat(pattern.matcher(Slices.utf8Slice("a")).find()).isTrue();
        assertThat(pattern.isNamedCapturingGroupsComputed()).isTrue();
    }

    @Test
    public void testGroupZeroMatcherUsesSingleByteMatcher()
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice("([,;])"));
        SingleByteMatcher byteMatcher = pattern.createSingleByteMatcher();
        assertThat(byteMatcher).isNotNull();

        Re2Matcher matcher = pattern.groupZeroMatcher(Slices.utf8Slice("a,b;c"), byteMatcher);
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.start()).isEqualTo(1);
        assertThat(matcher.end()).isEqualTo(2);
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.start()).isEqualTo(3);
        assertThat(matcher.end()).isEqualTo(4);
        assertThat(matcher.find()).isFalse();
        assertThat(pattern.isReverseProgramComputed()).isFalse();
    }

    @Test
    public void testGroupZeroMatcherPreservesMatchBoundaries()
    {
        assertGroupZeroMatchesFull("(a+)", "zaaazaa");
        assertGroupZeroMatchesFull("((ab)|a)+", "zzabaabzaba");
        assertGroupZeroMatchesFull("(a*)", "baa");
        assertGroupZeroMatchesFull("(a+?)", "zaaazaa");
        assertGroupZeroMatchesFull("((a)?b)", "bbab");
        assertGroupZeroMatchesFull("^(a+)", "aaab");
        assertGroupZeroMatchesFull("(💰)", "a💰b💰");
        assertGroupZeroMatchesFull("(a|ab)", "zab");
        assertGroupZeroMatchesFull(
                "POSIX (a|aa)",
                Re2.compile(Slices.utf8Slice("(a|aa)"), Re2.Options.posix()),
                Slices.utf8Slice("zaa"));
        assertGroupZeroMatchesFull(
                "Latin1 (.)",
                Re2.compile(Slices.utf8Slice("(.)"), Re2.Options.latin1()),
                Slices.wrappedBuffer(new byte[] {(byte) 0xFF, 'a'}));
        assertGroupZeroMatchesFull(
                "nonzero Slice window",
                Re2.compile(Slices.utf8Slice("(a+)")),
                Slices.wrappedBuffer(Slices.utf8Slice("xxaaaxx").getBytes(), 2, 3));
    }

    private static void assertGroupZeroMatchesFull(String expression, String input)
    {
        assertGroupZeroMatchesFull(expression, Re2.compile(Slices.utf8Slice(expression)), Slices.utf8Slice(input));
    }

    private static void assertGroupZeroMatchesFull(String description, Re2 pattern, Slice input)
    {
        Re2Matcher fullMatcher = pattern.matcher(input);
        Re2Matcher groupZeroMatcher = pattern.groupZeroMatcher(input, null);

        while (fullMatcher.find()) {
            assertThat(groupZeroMatcher.find())
                    .as("group-zero match for %s", description)
                    .isTrue();
            assertThat(groupZeroMatcher.start())
                    .as("match start for %s", description)
                    .isEqualTo(fullMatcher.start());
            assertThat(groupZeroMatcher.end())
                    .as("match end for %s", description)
                    .isEqualTo(fullMatcher.end());
        }
        assertThat(groupZeroMatcher.find())
                .as("no extra group-zero match for %s", description)
                .isFalse();
    }

    @Test
    public void testEmptyMatchesAdvanceByUtf8CodePoint()
    {
        Re2Matcher matcher = Re2.compile(Slices.utf8Slice(""))
                .matcher(Slices.utf8Slice("a有💰"));
        List<Integer> starts = new ArrayList<>();

        while (matcher.find()) {
            starts.add(matcher.start());
            assertThat(matcher.end()).isEqualTo(matcher.start());
        }

        assertThat(starts).containsExactly(0, 1, 4, 8);
    }

    @Test
    public void testEmptyMatchesAdvanceByByteInLatin1()
    {
        Re2Matcher matcher = Re2.compile(
                        Slices.wrappedBuffer(new byte[0]),
                        Re2.Options.latin1())
                .matcher(Slices.wrappedBuffer(new byte[] {(byte) 0xC3, (byte) 0xA9}));
        List<Integer> starts = new ArrayList<>();

        while (matcher.find()) {
            starts.add(matcher.start());
        }

        assertThat(starts).containsExactly(0, 1, 2);
    }

    @Test
    public void testNullableStartByteFastPath()
    {
        Re2 star = Re2.compile(Slices.utf8Slice("x*"));
        assertThat(star.canReturnEmptyAtStart(Slices.utf8Slice("aaa"), 0)).isTrue();
        assertThat(star.canReturnEmptyAtStart(Slices.utf8Slice("xaa"), 0)).isFalse();
        assertThat(star.canReturnEmptyAtStart(Slices.utf8Slice("xaa"), 1)).isTrue();
        assertThat(star.canReturnEmptyAtStart(Slices.utf8Slice("xaa"), 3)).isTrue();

        assertThat(Re2.compile(Slices.utf8Slice("(x*)")).canReturnEmptyAtStart(Slices.utf8Slice("aaa"), 0)).isFalse();
        assertThat(Re2.compile(Slices.utf8Slice("^a")).canReturnEmptyAtStart(Slices.EMPTY_SLICE, 0)).isFalse();
        assertThat(Re2.compile(Slices.utf8Slice("x*$")).canReturnEmptyAtStart(Slices.utf8Slice("aaa"), 0)).isFalse();
        assertThat(Re2.compile(Slices.utf8Slice("\\b|x")).canReturnEmptyAtStart(Slices.utf8Slice("aaa"), 0)).isFalse();

        Re2Matcher matcher = star.matcher(Slices.utf8Slice("xxa"));
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.start()).isZero();
        assertThat(matcher.end()).isEqualTo(2);
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.start()).isEqualTo(2);
        assertThat(matcher.end()).isEqualTo(2);
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.start()).isEqualTo(3);
        assertThat(matcher.end()).isEqualTo(3);
        assertThat(matcher.find()).isFalse();
    }

    @Test
    public void testNullableStartByteFastPathPreservesMatchPriority()
    {
        assertMatchRanges(Re2.compile(Slices.utf8Slice("a|")), "ab", "0:1", "1:1", "2:2");
        assertMatchRanges(Re2.compile(Slices.utf8Slice("|a")), "a", "0:0", "1:1");
        assertMatchRanges(Re2.compile(Slices.utf8Slice("x*?")), "xaa", "0:0", "1:1", "2:2", "3:3");
        assertMatchRanges(Re2.compile(Slices.utf8Slice("(?:xy)*")), "xza", "0:0", "1:1", "2:2", "3:3");
        assertMatchRanges(
                Re2.compile(Slices.utf8Slice("|a"), Re2.Options.defaults().setLongestMatch(true)),
                "a",
                "0:1",
                "1:1");
    }

    private static void assertMatchRanges(Re2 pattern, String input, String... expectedRanges)
    {
        Re2Matcher matcher = pattern.matcher(Slices.utf8Slice(input));
        List<String> actualRanges = new ArrayList<>();
        while (matcher.find()) {
            actualRanges.add(matcher.start() + ":" + matcher.end());
        }
        assertThat(actualRanges).containsExactly(expectedRanges);
    }

    @Test
    public void testMatchModesAndReset()
    {
        Re2Matcher matcher = Re2.compile(Slices.utf8Slice("(abc)"))
                .matcher(Slices.utf8Slice("abc"));

        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group(1).toStringUtf8()).isEqualTo("abc");

        matcher.reset(Slices.utf8Slice("abc xyz"));
        assertThat(matcher.lookingAt()).isTrue();
        assertThat(matcher.end()).isEqualTo(3);

        matcher.reset(Slices.utf8Slice("xyz abc"));
        assertThat(matcher.find(4)).isTrue();
        assertThat(matcher.start()).isEqualTo(4);
    }

    @Test
    public void testLogicalRegionMatchesSliceView()
    {
        assertRegionMatchesSliceView("^abc$", Slices.utf8Slice("xabcx"), 1, 4);
        assertRegionMatchesSliceView("(?m)^abc$", Slices.utf8Slice("xabc\ny"), 1, 4);
        assertRegionMatchesSliceView("\\b(abc)\\b", Slices.utf8Slice("xabcx"), 1, 4);
        assertRegionMatchesSliceView("", Slices.utf8Slice("x💰y"), 1, 5);
        assertRegionMatchesSliceView("^$", Slices.utf8Slice("xy"), 1, 1);
        assertRegionMatchesSliceView("(abc)$", Slices.utf8Slice("xabc\r\ny"), 1, 4);

        byte[] malformedUtf8 = {'x', 'a', (byte) 0xFF, 'b', 'y'};
        assertRegionMatchesSliceView("a.b", Slices.wrappedBuffer(malformedUtf8), 1, 4);

        byte[] backing = Slices.utf8Slice("padding-xabcx-padding").getBytes();
        Slice nonzeroOffset = Slices.wrappedBuffer(backing, 8, 5);
        assertRegionMatchesSliceView("(abc)", nonzeroOffset, 1, 4);
    }

    @Test
    public void testLogicalRegionValidation()
    {
        Slice originalInput = Slices.utf8Slice("a");
        Re2Matcher matcher = Re2.compile(Slices.utf8Slice("a")).matcher(originalInput);
        assertThat(matcher.matches()).isTrue();

        assertThatThrownBy(() -> matcher.reset(Slices.utf8Slice("abc"), -1, 2))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("region out of bounds: [-1, 2)");
        assertThatThrownBy(() -> matcher.reset(Slices.utf8Slice("abc"), 2, 1))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("region out of bounds: [2, 1)");
        assertThatThrownBy(() -> matcher.reset(Slices.utf8Slice("zzz"), 0, 4))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("region out of bounds: [0, 4)");

        assertThat(matcher.group().toStringUtf8()).isEqualTo("a");
    }

    @Test
    public void testLogicalRegionMatchModesAndSnapshot()
    {
        Slice input = Slices.utf8Slice("xabcx");
        Re2Matcher matcher = Re2.compile(Slices.utf8Slice("(abc)"))
                .matcher(input)
                .reset(input, 1, 4);

        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.start()).isZero();
        assertThat(matcher.end()).isEqualTo(3);
        assertThat(matcher.group(1).toStringUtf8()).isEqualTo("abc");
        assertThat(matcher.toMatchResult().groupSlice(1).toStringUtf8()).isEqualTo("abc");

        assertThat(matcher.reset(input, 1, 5).lookingAt()).isTrue();
        assertThat(matcher.start()).isZero();
        assertThat(matcher.end()).isEqualTo(3);
    }

    @Test
    public void testLogicalRegionUsesSingleByteMatcher()
    {
        Slice input = Slices.utf8Slice("x,a,y");
        Re2 pattern = Re2.compile(Slices.utf8Slice("([,;])"));
        Re2Matcher matcher = pattern.groupZeroMatcher(input, pattern.createSingleByteMatcher())
                .reset(input, 2, 4);

        assertThat(matcher.find()).isTrue();
        assertThat(matcher.start()).isEqualTo(1);
        assertThat(matcher.end()).isEqualTo(2);
        assertThat(matcher.find()).isFalse();
    }

    @Test
    public void testLogicalRegionExhaustiveAgreement()
    {
        Slice input = Slices.utf8Slice(" a\nab💰x\n");
        List<String> expressions = List.of(
                "",
                "^",
                "$",
                "(?m)^a",
                "a$",
                "\\ba\\b",
                "(a|ab)+",
                "(.)",
                "a*?",
                "[^\\n]+",
                "💰?");

        for (String expression : expressions) {
            for (int regionStart = 0; regionStart <= input.length(); regionStart++) {
                for (int regionEnd = regionStart; regionEnd <= input.length(); regionEnd++) {
                    assertRegionMatchesSliceView(expression, input, regionStart, regionEnd);
                }
            }
        }
    }

    private static void assertRegionMatchesSliceView(String expression, Slice input, int regionStart, int regionEnd)
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice(expression));
        Re2Matcher regionMatcher = pattern.matcher(input).reset(input, regionStart, regionEnd);
        Re2Matcher viewMatcher = pattern.matcher(input.slice(regionStart, regionEnd - regionStart));

        while (viewMatcher.find()) {
            assertThat(regionMatcher.find()).as("region match for %s", expression).isTrue();
            assertThat(regionMatcher.start()).isEqualTo(viewMatcher.start());
            assertThat(regionMatcher.end()).isEqualTo(viewMatcher.end());
            assertThat(regionMatcher.groupCount()).isEqualTo(viewMatcher.groupCount());
            for (int group = 0; group <= viewMatcher.groupCount(); group++) {
                assertThat(regionMatcher.matched(group)).isEqualTo(viewMatcher.matched(group));
                assertThat(regionMatcher.start(group)).isEqualTo(viewMatcher.start(group));
                assertThat(regionMatcher.end(group)).isEqualTo(viewMatcher.end(group));
                assertThat(regionMatcher.group(group)).isEqualTo(viewMatcher.group(group));
            }
        }
        assertThat(regionMatcher.find()).as("no extra region match for %s", expression).isFalse();
    }

    @Test
    public void testSnapshotIsIndependentOfLaterFinds()
    {
        Re2Matcher matcher = Re2.compile(Slices.utf8Slice("(\\w+)"))
                .matcher(Slices.utf8Slice("one two"));

        assertThat(matcher.find()).isTrue();
        MatchResult first = matcher.toMatchResult();
        assertThat(matcher.find()).isTrue();

        assertThat(first.groupSlice(1).toStringUtf8()).isEqualTo("one");
        assertThat(matcher.group(1).toStringUtf8()).isEqualTo("two");
    }

    @Test
    public void testUnmatchedGroupAndMatchState()
    {
        Re2Matcher matcher = Re2.compile(Slices.utf8Slice("(a)|(b)"))
                .matcher(Slices.utf8Slice("a"));

        assertThatThrownBy(matcher::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("no successful match");

        assertThat(matcher.find()).isTrue();
        assertThat(matcher.matched(1)).isTrue();
        assertThat(matcher.matched(2)).isFalse();
        assertThat(matcher.group(2)).isNull();
    }

    @Test
    public void testResetInvalidatesWithoutClearingCaptureBuffer()
    {
        Re2Matcher matcher = Re2.compile(Slices.utf8Slice("(a)"))
                .matcher(Slices.utf8Slice("a"));

        assertThat(matcher.find()).isTrue();
        assertThat(matcher.groupOffsetForDiagnostics(0)).isZero();
        assertThat(matcher.groupOffsetForDiagnostics(1)).isEqualTo(1);

        matcher.reset(Slices.utf8Slice("b"));

        assertThatThrownBy(matcher::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("no successful match");
        assertThat(matcher.groupOffsetForDiagnostics(0)).isZero();
        assertThat(matcher.groupOffsetForDiagnostics(1)).isEqualTo(1);
    }
}
