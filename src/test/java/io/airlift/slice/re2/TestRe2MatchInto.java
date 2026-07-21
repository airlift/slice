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
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRe2MatchInto
{
    @Test
    public void testFullAndPartialMatchInto()
    {
        Re2 re = Re2.compile(utf8("(\\w+):(\\d+)"), Re2.Options.defaults());

        int[] groups = new int[6];
        assertThat(re.matchInto(utf8("host:9000"), Re2.Anchor.ANCHOR_BOTH, groups)).isTrue();
        assertThat(groups).containsExactly(0, 9, 0, 4, 5, 9);

        assertThat(re.matchInto(utf8("x host:9000 y"), Re2.Anchor.UNANCHORED, groups)).isTrue();
        assertThat(groups[0]).isEqualTo(2);
        assertThat(groups[1]).isEqualTo(11);
    }

    @Test
    public void testStartEndWindow()
    {
        Re2 re = Re2.compile(utf8("(\\d+)"), Re2.Options.defaults());
        Slice text = utf8("a11 b22");

        int[] groups = new int[4];
        assertThat(re.matchInto(text, 4, text.length(), Re2.Anchor.UNANCHORED, groups)).isTrue();
        assertThat(groups).containsExactly(5, 7, 5, 7);
    }

    @Test
    public void testEmptyWindowRetainsInputContext()
    {
        Slice text = utf8Slice("a ");
        Re2 wordBoundary = Re2.compile(utf8Slice("\\b"));
        Re2 notWordBoundary = Re2.compile(utf8Slice("\\B"));
        int[] groups = new int[2];

        assertThat(wordBoundary.matchInto(text, 0, 0, Re2.Anchor.ANCHOR_BOTH, groups)).isTrue();
        assertThat(groups).containsExactly(0, 0);
        assertThat(wordBoundary.matchInto(text, 1, 1, Re2.Anchor.ANCHOR_BOTH, groups)).isTrue();
        assertThat(groups).containsExactly(1, 1);
        assertThat(wordBoundary.matchInto(text, 2, 2, Re2.Anchor.ANCHOR_BOTH, groups)).isFalse();

        assertThat(notWordBoundary.matchInto(text, 0, 0, Re2.Anchor.ANCHOR_BOTH, groups)).isFalse();
        assertThat(notWordBoundary.matchInto(text, 1, 1, Re2.Anchor.ANCHOR_BOTH, groups)).isFalse();
        assertThat(notWordBoundary.matchInto(text, 2, 2, Re2.Anchor.ANCHOR_BOTH, groups)).isTrue();
        assertThat(groups).containsExactly(2, 2);
    }

    @Test
    public void testUnmatchedCapturesAreMinusOne()
    {
        Re2 re = Re2.compile(utf8("(foo)|(bar)"), Re2.Options.defaults());
        int[] groups = new int[6];

        assertThat(re.matchInto(utf8("foo"), Re2.Anchor.ANCHOR_BOTH, groups)).isTrue();
        assertThat(groups[2]).isEqualTo(0);
        assertThat(groups[3]).isEqualTo(3);
        assertThat(groups[4]).isEqualTo(-1);
        assertThat(groups[5]).isEqualTo(-1);
    }

    @Test
    public void testOddGroupsLengthThrows()
    {
        Re2 re = Re2.compile(utf8("(foo)"), Re2.Options.defaults());
        assertThatThrownBy(() -> re.matchInto(utf8("foo"), Re2.Anchor.ANCHOR_BOTH, new int[3]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("groups length must be even");
    }

    @Test
    public void testNullGroupsUsesBooleanPath()
    {
        Re2 re = Re2.compile(utf8("foo"), Re2.Options.defaults());
        assertThat(re.matchInto(utf8("foo"), Re2.Anchor.ANCHOR_BOTH, null)).isTrue();
        assertThat(re.matchInto(utf8("bar"), Re2.Anchor.ANCHOR_BOTH, null)).isFalse();
    }

    @Test
    public void testFullMatchUsesFullMatchSemantics()
    {
        assertThat(Re2.compile(utf8("a|ab")).fullMatch(utf8("ab"))).isTrue();
        assertThat(Re2.compile(utf8("a*?")).fullMatch(utf8("a"))).isTrue();
    }

    @Test
    public void testLongestMatchUsesLongestSemantics()
    {
        Re2 regexp = Re2.compile(utf8("a|ab"), Re2.Options.defaults().setLongestMatch(true));
        int[] groups = new int[2];

        assertThat(regexp.matchInto(utf8("ab"), Re2.Anchor.UNANCHORED, groups)).isTrue();
        assertThat(groups).containsExactly(0, 2);
    }

    @Test
    public void testExtendedOnePassCaptureLimit()
    {
        Re2 maximumSidecarPattern = Re2.compile(utf8("^" + "(a)".repeat(31) + "$"));
        int[] maximumSidecarGroups = new int[64];
        assertThat(maximumSidecarPattern.forwardProgramForDiagnostics().supportsOnePassCaptureSlots(maximumSidecarGroups.length)).isTrue();
        assertThat(maximumSidecarPattern.matchInto(utf8("a".repeat(31)), Re2.Anchor.ANCHOR_BOTH, maximumSidecarGroups)).isTrue();

        Re2 fallbackPattern = Re2.compile(utf8("^" + "(a)".repeat(32) + "$"));
        int[] fallbackGroups = new int[66];
        assertThat(fallbackPattern.forwardProgramForDiagnostics().isOnePass()).isTrue();
        assertThat(fallbackPattern.forwardProgramForDiagnostics().supportsOnePassCaptureSlots(fallbackGroups.length)).isFalse();
        assertThat(fallbackPattern.matchInto(utf8("a".repeat(32)), Re2.Anchor.ANCHOR_BOTH, fallbackGroups)).isTrue();
    }

    @Test
    public void testEmptyMatchAtSearchStartDoesNotCompileReverseProgram()
    {
        Re2 regexp = Re2.compile(utf8("x*"));
        int[] groups = new int[2];

        assertThat(regexp.matchInto(utf8("a"), Re2.Anchor.UNANCHORED, groups)).isTrue();
        assertThat(groups).containsExactly(0, 0);
        assertThat(regexp.isReverseProgramComputed()).isFalse();

        Re2 variableWidthRegexp = Re2.compile(utf8("a+"));
        assertThat(variableWidthRegexp.matchInto(utf8("a"), Re2.Anchor.UNANCHORED, groups)).isTrue();
        assertThat(variableWidthRegexp.isReverseProgramComputed()).isTrue();
    }

    @Test
    public void testFixedWidthMatchDoesNotCompileReverseProgram()
    {
        Re2 delimiter = Re2.compile(utf8("[,;]"));
        int[] match = new int[2];
        assertThat(delimiter.matchInto(utf8("aaaaa,"), Re2.Anchor.UNANCHORED, match)).isTrue();
        assertThat(match).containsExactly(5, 6);
        assertThat(delimiter.isReverseProgramComputed()).isFalse();

        Re2 capturedUnicode = Re2.compile(utf8("(\uD83D\uDCB0)"));
        int[] capturedMatch = new int[4];
        assertThat(capturedUnicode.matchInto(utf8("a\uD83D\uDCB0"), Re2.Anchor.UNANCHORED, capturedMatch)).isTrue();
        assertThat(capturedMatch).containsExactly(1, 5, 1, 5);
        assertThat(capturedUnicode.isReverseProgramComputed()).isFalse();

        Re2 variableWidth = Re2.compile(utf8("a+"));
        assertThat(variableWidth.matchInto(utf8("aa"), Re2.Anchor.UNANCHORED, match)).isTrue();
        assertThat(match).containsExactly(0, 2);
        assertThat(variableWidth.isReverseProgramComputed()).isTrue();
    }

    @Test
    public void testFixedWidthAnalysis()
    {
        assertFixedWidthMatch("a|b", "xb", 1, 2);
        assertFixedWidthMatch("ab|cd", "xcd", 1, 3);
        assertFixedWidthMatch("a{3}", "xaaa", 1, 4);
        assertFixedWidthMatch("[\uD83D\uDCB0-\uD83D\uDCB2]", "a\uD83D\uDCB1", 1, 5);

        assertVariableWidthMatch("a|bc", "xbc", 1, 3);
        assertVariableWidthMatch("a{2,3}", "xaaa", 1, 4);
        assertVariableWidthMatch(".", "\uD83D\uDCB0", 0, 4);
        assertVariableWidthMatch("[a\uD83D\uDCB0]", "\uD83D\uDCB0", 0, 4);

        Re2 latin1AnyCharacter = Re2.compile(utf8("."), Re2.Options.latin1());
        int[] latin1Match = new int[2];
        assertThat(latin1AnyCharacter.matchInto(Slices.wrappedBuffer(new byte[] {(byte) 0xFF}), Re2.Anchor.UNANCHORED, latin1Match)).isTrue();
        assertThat(latin1Match).containsExactly(0, 1);
        assertThat(latin1AnyCharacter.isReverseProgramComputed()).isFalse();
    }

    @Test
    public void testMinimumWidthRejectsShortInputBeforeReverseCompilation()
    {
        Re2 regexp = Re2.compile(utf8("a{32}$"));
        int[] match = new int[2];

        assertThat(regexp.matchInto(utf8("a".repeat(31)), Re2.Anchor.UNANCHORED, match)).isFalse();
        assertThat(regexp.isReverseProgramComputed()).isFalse();

        Re2 requiredPrefix = Re2.compile(utf8("^prefixa{32}$"));
        assertThat(requiredPrefix.matchInto(utf8("prefix" + "a".repeat(31)), Re2.Anchor.UNANCHORED, match)).isFalse();
        assertThat(requiredPrefix.isReverseProgramComputed()).isFalse();
    }

    @Test
    public void testFixedWidthAnalysisIsStackSafe()
            throws InterruptedException
    {
        int captureCount = 10_000;
        String pattern = "(".repeat(captureCount) + "a" + ")".repeat(captureCount) + "{2}";
        AtomicReference<int[]> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread matcherThread = new Thread(null, () -> {
            try {
                Re2 regexp = Re2.compile(utf8(pattern));
                int[] match = new int[2];
                assertThat(regexp.matchInto(utf8("aa"), Re2.Anchor.UNANCHORED, match)).isTrue();
                result.set(match);
            }
            catch (Throwable throwable) {
                failure.set(throwable);
            }
        }, "match-length-small-stack", 256 * 1024);

        matcherThread.start();
        matcherThread.join();

        assertThat(failure.get()).isNull();
        assertThat(result.get()).containsExactly(0, 2);
    }

    private static void assertFixedWidthMatch(String pattern, String text, int start, int end)
    {
        Re2 regexp = Re2.compile(utf8(pattern));
        int[] match = new int[2];
        assertThat(regexp.matchInto(utf8(text), Re2.Anchor.UNANCHORED, match)).isTrue();
        assertThat(match).containsExactly(start, end);
        assertThat(regexp.isReverseProgramComputed()).isFalse();
    }

    private static void assertVariableWidthMatch(String pattern, String text, int start, int end)
    {
        Re2 regexp = Re2.compile(utf8(pattern));
        int[] match = new int[2];
        assertThat(regexp.matchInto(utf8(text), Re2.Anchor.UNANCHORED, match)).isTrue();
        assertThat(match).containsExactly(start, end);
        assertThat(regexp.isReverseProgramComputed()).isTrue();
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
