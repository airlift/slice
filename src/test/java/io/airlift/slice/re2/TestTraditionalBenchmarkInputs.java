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

import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;

import static io.airlift.slice.re2.Re2.Anchor.UNANCHORED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTraditionalBenchmarkInputs
{
    @Test
    public void testPracticalInputsAndCaptureDemand()
    {
        BenchmarkRe2Practical.PracticalState state = new BenchmarkRe2Practical.PracticalState();
        state.setup();

        assertThat(state.simpleInput.toStringUtf8()).isEqualTo("abcdefg");

        BenchmarkRe2Practical benchmark = new BenchmarkRe2Practical();
        assertThat(benchmark.httpPartialMatch(state)).isTrue();
        assertThat(state.httpInput.slice(
                state.httpCaptureGroups[2],
                state.httpCaptureGroups[3] - state.httpCaptureGroups[2]).toStringUtf8())
                .isEqualTo("/asdfhjasdhfasdlfhasdflkjasdfkljasdhflaskdjhfalksdjfhasdlkfhasdlkjfhasdljkfhadsjklf");
    }

    @Test
    public void testFullMatchInputIncludesNativeSuffix()
    {
        BenchmarkRe2FullMatch.FullMatchState state = new BenchmarkRe2FullMatch.FullMatchState();
        state.textSize = 8;
        state.setup();

        assertThat(state.text.length()).isEqualTo(18);
        assertThat(state.text.slice(8, 10).toStringUtf8()).isEqualTo("ABCDEFGHIJ");
    }

    @Test
    public void testParseCaptureBuffersAreReused()
    {
        BenchmarkRe2Parse.ParseState state = new BenchmarkRe2Parse.ParseState();
        state.setup();
        int[] threeCaptureGroups = state.threeCaptureGroups;
        int[] oneCaptureGroup = state.oneCaptureGroup;

        BenchmarkRe2Parse benchmark = new BenchmarkRe2Parse();
        assertThat(benchmark.parse3DigitsRe2(state)).isTrue();
        assertThat(benchmark.parse1SplitRe2(state)).isTrue();
        assertThat(state.threeCaptureGroups).isSameAs(threeCaptureGroups);
        assertThat(state.oneCaptureGroup).isSameAs(oneCaptureGroup);
    }

    @Test
    public void testSplitBig2UsesDirectBitStateCapture()
    {
        BenchmarkRe2Parse.ParseState state = new BenchmarkRe2Parse.ParseState();
        state.setup();

        assertThat(state.progSplitHard.bitStateTextMaxSize())
                .isGreaterThanOrEqualTo(state.splitBig2Text.length());
        Re2Matcher matcher = state.re2SplitHard.matcher(state.splitBig2Text);
        assertThat(matcher.find()).isTrue();
        assertThat(state.re2SplitHard.isReverseProgramComputed()).isFalse();
        assertThat(matcher.start()).isZero();
        assertThat(matcher.end()).isEqualTo(100_008);
        assertThat(matcher.start(1)).isEqualTo(4);
        assertThat(matcher.end(1)).isEqualTo(100_008);
    }

    @Test
    public void testOnePassCapturesDoNotUseDirectBitState()
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice("([a-z]+)-([0-9]+)"));
        Slice input = Slices.utf8Slice(".".repeat(8 * 1024) + "abc-123");

        assertThat(pattern.forwardProgramForDiagnostics().isOnePass()).isTrue();
        assertThat(pattern.canUseDirectBitStateCapture(input.length(), 3)).isFalse();

        Re2Matcher matcher = pattern.matcher(input);
        assertThat(matcher.find()).isTrue();
        assertThat(pattern.isReverseProgramComputed()).isTrue();
        assertThat(matcher.group(1).toStringUtf8()).isEqualTo("abc");
        assertThat(matcher.group(2).toStringUtf8()).isEqualTo("123");
    }

    @Test
    public void testDirectBitStateCaptureAdjustsOffsetsAndPreservesUnmatchedGroups()
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice("((ab|a)+)(b)?"));
        Slice input = Slices.utf8Slice("x".repeat(4 * 1024) + "a");
        int[] groups = new int[8];

        assertThat(pattern.canUseDirectBitStateCapture(input.length() - 1, 4)).isTrue();

        assertThat(pattern.matchInto(
                input,
                1,
                input.length(),
                UNANCHORED,
                groups,
                new BitState.Workspace(),
                new Nfa.Workspace())).isTrue();
        assertThat(pattern.isReverseProgramComputed()).isFalse();
        assertThat(groups).containsExactly(
                4 * 1024, (4 * 1024) + 1,
                4 * 1024, (4 * 1024) + 1,
                4 * 1024, (4 * 1024) + 1,
                -1, -1);
    }

    @Test
    public void testDirectBitStateCaptureNoMatchDoesNotBuildReverseProgram()
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice("(a|ab)b"));
        int[] groups = new int[4];

        Slice input = Slices.utf8Slice("x".repeat(4 * 1024));
        assertThat(pattern.canUseDirectBitStateCapture(input.length(), 2)).isTrue();
        assertThat(pattern.matchInto(
                input,
                0,
                input.length(),
                UNANCHORED,
                groups,
                new BitState.Workspace(),
                new Nfa.Workspace())).isFalse();
        assertThat(pattern.isReverseProgramComputed()).isFalse();
        assertThat(groups).containsExactly(-1, -1, -1, -1);
    }

    @Test
    public void testDirectBitStateCaptureAgreesWithNfa()
    {
        List<String> patterns = List.of(
                "([ab]+)(b*)",
                "((ab|a)+)(b)?",
                "(a*)(a)",
                "(a|ab)b",
                "(a+)(a*)",
                "([0-9]+).(.*)");
        List<String> suffixes = List.of(
                "aabb",
                "aabab",
                "aaa",
                "abb",
                "aaa",
                "650-253-0000");

        for (int caseIndex = 0; caseIndex < patterns.size(); caseIndex++) {
            Re2 pattern = Re2.compile(Slices.utf8Slice(patterns.get(caseIndex)));
            Slice input = Slices.utf8Slice("x".repeat(4 * 1024) + suffixes.get(caseIndex));
            Prog program = pattern.forwardProgramForDiagnostics();
            int groupCount = pattern.capturingGroupCount() + 1;
            assertThat(pattern.canUseDirectBitStateCapture(input.length(), groupCount)).isTrue();

            int[] expected = new int[2 * groupCount];
            Arrays.fill(expected, -1);
            boolean expectedMatch = Nfa.search(
                    program,
                    input,
                    0,
                    input.length(),
                    false,
                    Prog.MatchKind.FIRST_MATCH,
                    expected);

            Re2Matcher matcher = pattern.matcher(input);
            assertThat(matcher.find()).isEqualTo(expectedMatch);
            if (expectedMatch) {
                int[] actual = new int[expected.length];
                for (int group = 0; group < groupCount; group++) {
                    actual[2 * group] = matcher.start(group);
                    actual[(2 * group) + 1] = matcher.end(group);
                }
                assertThat(actual).containsExactly(expected);
            }
            assertThat(pattern.isReverseProgramComputed()).isFalse();

            matcher.reset(input);
            assertThat(matcher.find()).isEqualTo(expectedMatch);
        }
    }

    @Test
    public void testParseBenchmarksUseFullMatch()
    {
        BenchmarkRe2Parse.ParseState state = new BenchmarkRe2Parse.ParseState();
        state.setup();
        state.phoneInput = Slices.utf8Slice("650-253-0001\n");

        BenchmarkRe2Parse benchmark = new BenchmarkRe2Parse();
        assertThat(benchmark.parse3DigitsNfa(state)).isFalse();
        assertThat(benchmark.parse3DigitsOnePass(state)).isFalse();
        assertThat(benchmark.parse3DigitsBitState(state)).isFalse();
        assertThat(benchmark.parse3DigitsBacktrack(state)).isFalse();
        assertThat(benchmark.parse3DigitsRe2(state)).isFalse();
        assertThat(benchmark.parse3DigitDsNfa(state)).isFalse();
        assertThat(benchmark.parse3DigitDsOnePass(state)).isFalse();
        assertThat(benchmark.parse3DigitDsBitState(state)).isFalse();
        assertThat(benchmark.parse3DigitDsBacktrack(state)).isFalse();
        assertThat(benchmark.parse3DigitDsRe2(state)).isFalse();
        assertThat(benchmark.parse1SplitNfa(state)).isFalse();
        assertThat(benchmark.parse1SplitOnePass(state)).isFalse();
        assertThat(benchmark.parse1SplitBitState(state)).isFalse();
        assertThat(benchmark.parse1SplitRe2(state)).isFalse();
        assertThat(benchmark.parseSplitHardNfa(state)).isFalse();
        assertThat(benchmark.parseSplitHardBitState(state)).isFalse();
        assertThat(benchmark.parseSplitHardBacktrack(state)).isFalse();
        assertThat(benchmark.parseSplitHardRe2(state)).isFalse();
    }

    @Test
    public void testParseBenchmarksExtractCaptures()
    {
        BenchmarkRe2Parse.ParseState state = new BenchmarkRe2Parse.ParseState();
        state.setup();

        assertThat(state.prog3Digits.isOnePass()).isTrue();
        assertThat(state.prog3DigitDs.isOnePass()).isTrue();
        assertThat(state.prog1Split.isOnePass()).isTrue();
        assertThat(state.progSplitHard.isOnePass()).isFalse();
        assertThat(state.prog3Digits.canBitState()).isTrue();
        assertThat(state.prog3DigitDs.canBitState()).isTrue();
        assertThat(state.prog1Split.canBitState()).isTrue();
        assertThat(state.progSplitHard.canBitState()).isTrue();

        BenchmarkRe2Parse benchmark = new BenchmarkRe2Parse();
        int[] threeCaptureGroups = {0, 12, 0, 3, 4, 7, 8, 12};
        int[] oneCaptureGroup = {0, 12, 4, 12};
        assertCaptures(() -> benchmark.parse3DigitsNfa(state), state.threeCaptureGroups, threeCaptureGroups);
        assertCaptures(() -> benchmark.parse3DigitsOnePass(state), state.threeCaptureGroups, threeCaptureGroups);
        assertCaptures(() -> benchmark.parse3DigitsBitState(state), state.threeCaptureGroups, threeCaptureGroups);
        assertCaptures(() -> benchmark.parse3DigitsBacktrack(state), state.threeCaptureGroups, threeCaptureGroups);
        assertCaptures(() -> benchmark.parse3DigitsRe2(state), state.threeCaptureGroups, threeCaptureGroups);
        assertCaptures(() -> benchmark.parse3DigitDsNfa(state), state.threeCaptureGroups, threeCaptureGroups);
        assertCaptures(() -> benchmark.parse3DigitDsOnePass(state), state.threeCaptureGroups, threeCaptureGroups);
        assertCaptures(() -> benchmark.parse3DigitDsBitState(state), state.threeCaptureGroups, threeCaptureGroups);
        assertCaptures(() -> benchmark.parse3DigitDsBacktrack(state), state.threeCaptureGroups, threeCaptureGroups);
        assertCaptures(() -> benchmark.parse3DigitDsRe2(state), state.threeCaptureGroups, threeCaptureGroups);
        assertCaptures(() -> benchmark.parse1SplitNfa(state), state.oneCaptureGroup, oneCaptureGroup);
        assertCaptures(() -> benchmark.parse1SplitOnePass(state), state.oneCaptureGroup, oneCaptureGroup);
        assertCaptures(() -> benchmark.parse1SplitBitState(state), state.oneCaptureGroup, oneCaptureGroup);
        assertCaptures(() -> benchmark.parse1SplitRe2(state), state.oneCaptureGroup, oneCaptureGroup);
        assertCaptures(() -> benchmark.parseSplitHardNfa(state), state.oneCaptureGroup, oneCaptureGroup);
        assertCaptures(() -> benchmark.parseSplitHardBitState(state), state.oneCaptureGroup, oneCaptureGroup);
        assertCaptures(() -> benchmark.parseSplitHardBacktrack(state), state.oneCaptureGroup, oneCaptureGroup);
        assertCaptures(() -> benchmark.parseSplitHardRe2(state), state.oneCaptureGroup, oneCaptureGroup);
    }

    private static void assertCaptures(BooleanSupplier invocation, int[] actual, int[] expected)
    {
        Arrays.fill(actual, Integer.MIN_VALUE);
        assertThat(invocation.getAsBoolean()).isTrue();
        assertThat(actual).containsExactly(expected);
    }

    @Test
    public void testPossibleMatchRangeMatchesUpstreamMaximumLength()
    {
        BenchmarkRe2Misc.PossibleMatchRangeState state = new BenchmarkRe2Misc.PossibleMatchRangeState();
        state.setup();

        BenchmarkRe2Misc benchmark = new BenchmarkRe2Misc();
        assertThat(benchmark.possibleMatchRangePrefix(state))
                .isEqualTo(state.progPrefix.possibleMatchRange(16));
    }
}
