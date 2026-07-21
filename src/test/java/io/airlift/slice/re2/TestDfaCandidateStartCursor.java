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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDfaCandidateStartCursor
{
    @Test
    public void testCandidateCursorReturnsBoundariesWithoutReverseProgram()
    {
        Re2 pattern = compile("a+z|b+");
        Re2Matcher matcher = pattern.groupZeroMatcher(utf8("xxaaazyybbb"), null);

        assertThat(matcher.candidateStartCursorEnabledForDiagnostics()).isTrue();
        assertThat(boundaries(matcher)).containsExactly(new Boundary(2, 6), new Boundary(8, 11));
        assertThat(matcher.candidateStartRouteCountForDiagnostics()).isEqualTo(2);
        assertThat(matcher.candidateStartFallbackCountForDiagnostics()).isZero();
        assertThat(pattern.isReverseProgramComputed()).isFalse();
    }

    @Test
    public void testCandidateCursorPreservesFirstMatchPriority()
    {
        assertCandidateAgrees("a|ab|b", "xab");
        assertCandidateAgrees("ab|a|b", "xab");
        assertCandidateAgrees("a+|b", "xaaay");
        assertCandidateAgrees("a+?|b", "xaaay");
        assertCandidateAgrees("ab|b", "acb");
        assertCandidateAgrees("a.*z|b", "acb");
    }

    @Test
    public void testCandidateCursorUsesCachedStateAtEndOfText()
    {
        Re2 pattern = compile("(a+|b+)");
        assertThat(boundaries(pattern.groupZeroMatcher(utf8("xxxxb"), null)))
                .containsExactly(new Boundary(4, 5));

        Re2Matcher matcher = pattern.groupZeroMatcher(utf8("xxxxxxxxxxxxxxb"), null);
        assertThat(matcher.find(14)).isTrue();
        assertThat(matcher.start()).isEqualTo(14);
        assertThat(matcher.end()).isEqualTo(15);
    }

    @Test
    public void testCandidateCursorBulkScansSparseCapturePattern()
    {
        TestingTrinoRegexpBenchmarkInputs.Input input = TestingTrinoRegexpBenchmarkInputs.create("captureSparse", 32_768);
        Re2 pattern = Re2.compile(input.pattern());
        Re2Matcher matcher = pattern.groupZeroMatcher(input.source(), null);

        assertThat(boundaries(matcher)).containsExactly(
                new Boundary(8_192, 8_199),
                new Boundary(16_384, 16_391),
                new Boundary(24_576, 24_583));
        assertThat(matcher.candidateStartRouteCountForDiagnostics()).isEqualTo(3);
        assertThat(matcher.candidateStartFallbackCountForDiagnostics()).isZero();
        assertThat(pattern.isReverseProgramComputed()).isFalse();
    }

    @Test
    public void testCandidateCursorBoundsBulkScanToRemainingWork()
    {
        Dfa.CandidateStartCursor cursor = compile("a+z|b+").createCandidateStartCursor();
        cursor.reset(4);

        assertThat(cursor.workBoundedScanLength(3)).isEqualTo(3);
        assertThat(cursor.workBoundedScanLength(100)).isEqualTo(12);
    }

    @Test
    public void testCandidateCursorPreservesRegionsAndUtf8()
    {
        Slice input = utf8("padding-x💰💰y-padding");
        Re2 pattern = compile("(💰+|y)");
        Re2Matcher candidate = pattern.groupZeroMatcher(input, null).reset(input, 9, 18);
        Re2Matcher control = pattern.matcher(input.slice(9, 9));

        assertThat(candidate.candidateStartCursorEnabledForDiagnostics()).isTrue();
        assertThat(boundaries(candidate)).isEqualTo(boundaries(control));

        byte[] malformed = {'x', 'a', (byte) 0xFF, 'b', 'y'};
        assertCandidateAgrees("a+|b+", Slices.wrappedBuffer(malformed));

        Slice multibyteStart = utf8("x💰a");
        Re2 insideCodePoint = compile("(a+|💰+)");
        Re2Matcher insideCodePointCandidate = insideCodePoint.groupZeroMatcher(multibyteStart, null);
        Re2Matcher insideCodePointControl = insideCodePoint.matcher(multibyteStart);
        assertThat(insideCodePointCandidate.find(2)).isEqualTo(insideCodePointControl.find(2));
        assertThat(insideCodePointCandidate.start()).isEqualTo(insideCodePointControl.start());
        assertThat(insideCodePointCandidate.end()).isEqualTo(insideCodePointControl.end());
    }

    @Test
    public void testCandidateCursorFallsBackWhenAttemptExceedsBound()
    {
        Re2 pattern = compile("(a.*z|b)");
        Slice input = utf8("a" + "x".repeat(1_000) + "b");
        Re2Matcher matcher = pattern.groupZeroMatcher(input, null);

        assertThat(boundaries(matcher)).containsExactly(new Boundary(1_001, 1_002));
        assertThat(matcher.candidateStartFallbackCountForDiagnostics()).isEqualTo(1);
        assertThat(pattern.isReverseProgramComputed()).isTrue();

        matcher.reset(utf8("xxaz"));
        assertThat(matcher.candidateStartCursorEnabledForDiagnostics()).isTrue();
        assertThat(boundaries(matcher)).containsExactly(new Boundary(2, 4));
        assertThat(matcher.candidateStartRouteCountForDiagnostics()).isEqualTo(1);
    }

    @Test
    public void testCandidateCursorRemainsCorrectWithConcurrentCacheResets()
            throws Exception
    {
        Re2 pattern = compile("a+z|b+");
        Dfa.DfaInstance dfa = pattern.forwardProgramForDiagnostics().getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        Slice input = utf8("xxaaazyybbb");
        List<Boundary> expected = List.of(new Boundary(2, 6), new Boundary(8, 11));
        CountDownLatch startGate = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> searches = executor.submit(() -> {
                await(startGate);
                for (int iteration = 0; iteration < 100; iteration++) {
                    assertThat(boundaries(pattern.groupZeroMatcher(input, null))).isEqualTo(expected);
                }
            });
            Future<?> resets = executor.submit(() -> {
                await(startGate);
                for (int iteration = 0; iteration < 100; iteration++) {
                    dfa.resetCacheExternal();
                }
            });

            startGate.countDown();
            searches.get(10, TimeUnit.SECONDS);
            resets.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testIneligiblePatternsUseExistingMatcher()
    {
        assertThat(compile("a*").groupZeroMatcher(utf8("aaa"), null).candidateStartCursorEnabledForDiagnostics()).isFalse();
        assertThat(compile("^a+").groupZeroMatcher(utf8("aaa"), null).candidateStartCursorEnabledForDiagnostics()).isFalse();
        assertThat(compile("a+$").groupZeroMatcher(utf8("aaa"), null).candidateStartCursorEnabledForDiagnostics()).isFalse();
        assertThat(compile("a{2}").groupZeroMatcher(utf8("aaa"), null).candidateStartCursorEnabledForDiagnostics()).isFalse();
        assertThat(compile("\\ba+").groupZeroMatcher(utf8("aaa"), null).candidateStartCursorEnabledForDiagnostics()).isFalse();
    }

    private static void assertCandidateAgrees(String expression, String input)
    {
        assertCandidateAgrees(expression, utf8(input));
    }

    private static void assertCandidateAgrees(String expression, Slice input)
    {
        Re2 pattern = compile("(" + expression + ")");
        Re2Matcher candidate = pattern.groupZeroMatcher(input, null);
        Re2Matcher control = pattern.matcher(input);

        assertThat(candidate.candidateStartCursorEnabledForDiagnostics()).isTrue();
        assertThat(boundaries(candidate)).isEqualTo(boundaries(control));
    }

    private static List<Boundary> boundaries(Re2Matcher matcher)
    {
        List<Boundary> boundaries = new ArrayList<>();
        while (matcher.find()) {
            boundaries.add(new Boundary(matcher.start(), matcher.end()));
        }
        return boundaries;
    }

    private static Re2 compile(String expression)
    {
        return Re2.compile(utf8(expression));
    }

    private static Slice utf8(String value)
    {
        return Slices.utf8Slice(value);
    }

    private static void await(CountDownLatch latch)
    {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private record Boundary(int start, int end) {}
}
