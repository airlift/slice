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

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.re2.Dfa.SEARCH_NO_MATCH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDfaStartByteAcceleration
{
    @Test
    public void testLeadingByteClassSelectsStartByteAcceleration()
    {
        Prog program = compile("([a-z]+)-([0-9]+)");
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        byte[] text = "...z...".getBytes(StandardCharsets.UTF_8);

        assertThat(dfa.canStartByteAcceleration()).isTrue();
        assertThat(dfa.findStartByteCandidate(text, 0, text.length)).isEqualTo(3);
        assertThat(dfa.findStartByteCandidate(text, 4, text.length - 4)).isEqualTo(-1);
        assertThat(Dfa.canUseStartByteAcceleration(dfa, false, true, 0)).isTrue();
        assertThat(Dfa.canUseStartByteAcceleration(dfa, true, true, 0)).isFalse();
        assertThat(Dfa.canUseStartByteAcceleration(dfa, false, false, 0)).isFalse();
        assertThat(Dfa.canUseStartByteAcceleration(dfa, false, true, 1)).isFalse();
    }

    @Test
    public void testOptionalLeadingByteAndUtf8Candidates()
    {
        Prog optional = compile("(?:[a-z])?[0-9]");
        Dfa.DfaInstance optionalDfa = optional.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        byte[] optionalText = "...7".getBytes(StandardCharsets.UTF_8);
        assertThat(optionalDfa.canStartByteAcceleration()).isTrue();
        assertThat(optionalDfa.findStartByteCandidate(optionalText, 0, optionalText.length)).isEqualTo(3);

        Prog utf8 = compile("[éê]x");
        Dfa.DfaInstance utf8Dfa = utf8.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        byte[] utf8Text = "...êx".getBytes(StandardCharsets.UTF_8);
        assertThat(utf8Dfa.canStartByteAcceleration()).isTrue();
        assertThat(utf8Dfa.findStartByteCandidate(utf8Text, 0, utf8Text.length)).isEqualTo(3);

        Dfa.DfaInstance foldedDfa = firstMatchDfa("(?i)[a-z]+-[0-9]+");
        byte[] foldedText = "...Z".getBytes(StandardCharsets.UTF_8);
        assertThat(foldedDfa.canStartByteAcceleration()).isTrue();
        assertThat(foldedDfa.findStartByteCandidate(foldedText, 0, foldedText.length)).isEqualTo(3);
    }

    @Test
    public void testDenseUtf8CandidatesFallBackToCompactSearch()
    {
        String pattern = "Шерлок Холмс|Джон Уотсон|Ирен Адлер|инспектор Лестрейд|профессор Мориарти";
        Prog program = compile(pattern);
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        Slice shortInput = Slices.utf8Slice("а".repeat(16 * 1024));
        Slice longInput = Slices.utf8Slice("а".repeat(64 * 1024));

        assertThat(dfa.canStartByteAcceleration()).isTrue();
        assertThat(dfa.canFixedDistanceByteAcceleration()).isFalse();
        assertThat(Dfa.search(program, shortInput, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(SEARCH_NO_MATCH);
        assertThat(dfa.byteScanFallbackCount()).isZero();
        assertThat(Dfa.search(program, longInput, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(SEARCH_NO_MATCH);
        assertThat(dfa.byteScanFallbackCount()).isPositive();
    }

    @Test
    public void testRejectedFixedDistanceScanUsesStartByteAcceleration()
    {
        Prog program = compile("[a-z]{8}-[0-9]{4}");
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        byte[] input = new byte[128 * 1024];
        Arrays.fill(input, (byte) '-');

        assertThat(dfa.canStartByteAcceleration()).isTrue();
        assertThat(dfa.canFixedDistanceByteAcceleration()).isTrue();
        assertThat(Dfa.search(program, Slices.wrappedBuffer(input), false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(SEARCH_NO_MATCH);
        assertThat(dfa.byteScanFallbackCount()).isZero();
    }

    @Test
    public void testUnsupportedPatternsDoNotConfigureAcceleration()
    {
        assertThat(firstMatchDfa("abc").canStartByteAcceleration()).isFalse();
        assertThat(firstMatchDfa("a*").canStartByteAcceleration()).isFalse();
        assertThat(firstMatchDfa("^[a-z]+").canStartByteAcceleration()).isFalse();
        assertThat(firstMatchDfa("\\b[a-z]+").canStartByteAcceleration()).isFalse();
        assertThat(firstMatchDfa("[^a]b").canStartByteAcceleration()).isFalse();
    }

    @Test
    public void testFixedDistanceByteSelectsMoreSelectiveCandidate()
    {
        Dfa.DfaInstance dfa = firstMatchDfa("[a-z]{8}-[0-9]{4}");
        byte[] text = "............abcdefgh-1234".getBytes(StandardCharsets.UTF_8);

        assertThat(dfa.stateBudget() - dfa.availableStateMemory())
                .isEqualTo(dfa.retainedStateMemory());
        assertThat(dfa.canFixedDistanceByteAcceleration()).isTrue();
        long memoryAfterCandidateAnalysis = dfa.availableStateMemory();
        assertThat(dfa.stateBudget() - memoryAfterCandidateAnalysis)
                .isEqualTo(dfa.retainedStateMemory() + 304);
        assertThat(dfa.fixedDistanceByteOffset()).isEqualTo(8);
        assertThat(dfa.findFixedDistanceByteCandidate(text, 0, text.length)).isEqualTo(12);
        byte[] offsetText = "....-.......abcdefgh-1234".getBytes(StandardCharsets.UTF_8);
        assertThat(dfa.findFixedDistanceByteCandidate(offsetText, 4, offsetText.length - 4)).isEqualTo(12);
        assertThat(Dfa.canUseFixedDistanceByteAcceleration(false, true, 0, 8 * 1024)).isTrue();
        assertThat(Dfa.canUseFixedDistanceByteAcceleration(false, true, 0, 4096)).isTrue();
        assertThat(Dfa.canUseFixedDistanceByteAcceleration(false, true, 0, 4095)).isFalse();
        assertThat(Dfa.canUseFixedDistanceByteAcceleration(false, true, 0, 1024)).isFalse();
        assertThat(Dfa.canUseFixedDistanceByteAcceleration(true, true, 0, 8 * 1024)).isFalse();
        assertThat(Dfa.canUseFixedDistanceByteAcceleration(false, false, 0, 8 * 1024)).isFalse();
        assertThat(Dfa.canUseFixedDistanceByteAcceleration(false, true, 1, 8 * 1024)).isFalse();

        dfa.resetCacheExternal();
        assertThat(dfa.availableStateMemory()).isEqualTo(memoryAfterCandidateAnalysis);
    }

    @Test
    public void testFixedDistanceByteCandidateRetriesAfterCacheReset()
            throws Exception
    {
        Dfa.DfaInstance dfa = firstMatchDfa("[a-z]{8}-[0-9]{4}");
        Field availableStateMemory = Dfa.DfaInstance.class.getDeclaredField("availableStateMemory");
        availableStateMemory.setAccessible(true);
        availableStateMemory.setLong(dfa, 303);

        assertThat(dfa.canFixedDistanceByteAcceleration()).isFalse();
        assertThat(dfa.fixedDistanceByteCandidatesInitialized()).isTrue();

        dfa.resetCacheExternal();
        assertThat(dfa.fixedDistanceByteCandidatesInitialized()).isFalse();
        assertThat(dfa.canFixedDistanceByteAcceleration()).isTrue();
        assertThat(dfa.stateBudget() - dfa.availableStateMemory())
                .isEqualTo(dfa.retainedStateMemory() + 304);
    }

    @Test
    public void testFixedDistanceByteAccelerationRequiresOneOffsetOnEveryPath()
    {
        assertThat(firstMatchDfa("(?:abc|xyz)-[0-9]").canFixedDistanceByteAcceleration()).isTrue();
        assertThat(firstMatchDfa("[a-z]:[0-9]").fixedDistanceByteOffset()).isEqualTo(1);
        assertThat(firstMatchDfa("[a-z]{16}-[0-9]").fixedDistanceByteOffset()).isEqualTo(16);
        assertThat(firstMatchDfa("[a-z]+-[0-9]").canFixedDistanceByteAcceleration()).isFalse();
        // Unicode case folding adds multi-byte equivalents, so ':' is not at one byte offset.
        assertThat(firstMatchDfa("(?i)[a-z]{4}:[0-9]").canFixedDistanceByteAcceleration()).isFalse();
        assertThat(firstMatchDfa("a*").canFixedDistanceByteAcceleration()).isFalse();
        assertThat(firstMatchDfa("\\b[a-z]{8}-[0-9]").canFixedDistanceByteAcceleration()).isFalse();
        assertThat(firstMatchDfa("abc").canFixedDistanceByteAcceleration()).isFalse();
    }

    @Test
    public void testFixedDistanceByteSearchMatchesPinnedUpstreamBoundary()
    {
        Prog program = compile("[a-z]{8}-[0-9]{4}");
        byte[] text = new byte[8 * 1024];
        Arrays.fill(text, (byte) 'a');
        byte[] match = "abcdefgh-1234".getBytes(StandardCharsets.UTF_8);
        int matchStart = 6 * 1024;
        System.arraycopy(match, 0, text, matchStart, match.length);

        assertThat(Dfa.search(
                program,
                Slices.wrappedBuffer(text),
                0,
                text.length,
                false,
                Prog.MatchKind.FIRST_MATCH,
                true))
                .isEqualTo(matchStart + match.length);
    }

    @Test
    public void testFixedDistanceByteSearchResetsAfterCacheExhaustion()
            throws Exception
    {
        for (Prog.MatchKind matchKind : List.of(Prog.MatchKind.FIRST_MATCH, Prog.MatchKind.LONGEST_MATCH)) {
            String pattern = "[a-z]{8}-[0-9]{4}";
            Prog program = compile(pattern);
            Dfa.DfaInstance.Kind dfaKind = matchKind == Prog.MatchKind.FIRST_MATCH
                    ? Dfa.DfaInstance.Kind.FIRST_MATCH
                    : Dfa.DfaInstance.Kind.LONGEST_MATCH;
            Dfa.DfaInstance dfa = program.getCachedDfa(dfaKind);

            assertThat(Dfa.search(program, Slices.EMPTY_SLICE, false, matchKind, true)).isEqualTo(SEARCH_NO_MATCH);
            assertThat(dfa.canFixedDistanceByteAcceleration()).isTrue();

            Field availableStateMemory = Dfa.DfaInstance.class.getDeclaredField("availableStateMemory");
            availableStateMemory.setAccessible(true);
            availableStateMemory.setLong(dfa, 0);

            byte[] text = new byte[8 * 1024];
            Arrays.fill(text, (byte) 'a');
            byte[] match = "abcdefgh-1234".getBytes(StandardCharsets.UTF_8);
            int matchStart = 6 * 1024;
            System.arraycopy(match, 0, text, matchStart, match.length);
            Slice context = Slices.wrappedBuffer(text);
            int resetCount = dfa.resetCount();

            assertDfaMatchesNfa(program, context, 0, context.length(), matchKind, pattern);
            assertThat(dfa.resetCount()).isEqualTo(resetCount + 1);
            assertThat(dfa.canFixedDistanceByteAcceleration()).isTrue();
        }
    }

    @Test
    public void testFixedDistanceByteSearchSupportsCandidateSets()
    {
        Dfa.DfaInstance byteClass = firstMatchDfa("[a-z]{8}[01][0-9]{4}");
        assertThat(byteClass.fixedDistanceByteOffset()).isEqualTo(8);
        assertThat(byteClass.findFixedDistanceByteCandidate("........0....".getBytes(StandardCharsets.UTF_8), 0, 13)).isZero();
        assertThat(byteClass.findFixedDistanceByteCandidate("........1....".getBytes(StandardCharsets.UTF_8), 0, 13)).isZero();
        assertThat(byteClass.findFixedDistanceByteCandidate("........2....".getBytes(StandardCharsets.UTF_8), 0, 13)).isEqualTo(-1);

        Dfa.DfaInstance foldedLiteral = firstMatchDfa("[a-z]{8}(?i:x)[0-9]{4}");
        assertThat(foldedLiteral.fixedDistanceByteOffset()).isEqualTo(8);
        assertThat(foldedLiteral.findFixedDistanceByteCandidate("........x....".getBytes(StandardCharsets.UTF_8), 0, 13)).isZero();
        assertThat(foldedLiteral.findFixedDistanceByteCandidate("........X....".getBytes(StandardCharsets.UTF_8), 0, 13)).isZero();
        assertThat(foldedLiteral.findFixedDistanceByteCandidate("........y....".getBytes(StandardCharsets.UTF_8), 0, 13)).isEqualTo(-1);
    }

    @Test
    public void testFixedDistanceByteSearchMatchesNfa()
    {
        for (FixedDistanceCase testCase : fixedDistanceCases()) {
            Prog program = compile(testCase.pattern());
            assertThat(program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH).canFixedDistanceByteAcceleration())
                    .as("pattern %s", testCase.pattern())
                    .isTrue();

            byte[] text = new byte[8 * 1024];
            Arrays.fill(text, (byte) 'a');
            byte[] match = testCase.match().getBytes(StandardCharsets.UTF_8);
            int matchStart = 6 * 1024;
            System.arraycopy(match, 0, text, matchStart, match.length);
            Slice context = Slices.wrappedBuffer(text);

            assertDfaMatchesNfa(program, context, 100, text.length - 100, Prog.MatchKind.FIRST_MATCH, testCase.pattern());
            assertDfaMatchesNfa(program, context, 100, text.length - 100, Prog.MatchKind.LONGEST_MATCH, testCase.pattern());
        }
    }

    @Test
    public void testFixedDistanceByteSearchRandomizedDifferential()
    {
        Random random = new Random(0);
        for (FixedDistanceCase testCase : fixedDistanceCases()) {
            Prog program = compile(testCase.pattern());
            assertThat(program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH).canFixedDistanceByteAcceleration())
                    .as("pattern %s", testCase.pattern())
                    .isTrue();

            for (int trial = 0; trial < 20; trial++) {
                byte[] text = new byte[8 * 1024];
                random.nextBytes(text);
                int textStart = random.nextInt(128);
                int textEnd = text.length - random.nextInt(128);
                if ((trial & 1) == 0) {
                    byte[] match = testCase.match().getBytes(StandardCharsets.UTF_8);
                    int matchStart = textStart + random.nextInt(textEnd - textStart - match.length);
                    System.arraycopy(match, 0, text, matchStart, match.length);
                }

                Slice context = Slices.wrappedBuffer(text);
                assertDfaMatchesNfa(program, context, textStart, textEnd, Prog.MatchKind.FIRST_MATCH, testCase.pattern());
                assertDfaMatchesNfa(program, context, textStart, textEnd, Prog.MatchKind.LONGEST_MATCH, testCase.pattern());

                int[] nfaGroups = new int[2];
                boolean nfaMatched = Nfa.search(program, context, textStart, textEnd, false, Prog.MatchKind.LONGEST_MATCH, nfaGroups);
                assertThat(Dfa.search(program, context, textStart, textEnd, false, Prog.MatchKind.FIRST_MATCH, false) >= 0)
                        .as("boundary-free match presence for pattern %s", testCase.pattern())
                        .isEqualTo(nfaMatched);
            }
        }
    }

    @Test
    public void testFixedDistanceByteSearchSemanticBoundaries()
    {
        String pattern = "[a-z]{4}:(?:[0-9]|[0-9]{3})";
        Prog program = compile(pattern);
        byte[] backing = new byte[(8 * 1024) + 34];
        Arrays.fill(backing, (byte) 'a');
        Slice context = Slices.wrappedBuffer(backing, 17, 8 * 1024);
        byte[] firstMatch = "abcd:123".getBytes(StandardCharsets.UTF_8);
        byte[] secondMatch = "wxyz:456".getBytes(StandardCharsets.UTF_8);
        int firstMatchStart = 256;
        int secondMatchStart = context.length() - secondMatch.length;
        System.arraycopy(firstMatch, 0, backing, context.byteArrayOffset() + firstMatchStart, firstMatch.length);
        System.arraycopy(secondMatch, 0, backing, context.byteArrayOffset() + secondMatchStart, secondMatch.length);

        assertThat(program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH).canFixedDistanceByteAcceleration()).isTrue();
        assertDfaMatchesNfa(program, context, 0, context.length(), Prog.MatchKind.FIRST_MATCH, pattern);
        assertDfaMatchesNfa(program, context, 0, context.length(), Prog.MatchKind.LONGEST_MATCH, pattern);
        assertThat(Dfa.search(program, context, 0, context.length(), false, Prog.MatchKind.FIRST_MATCH, true))
                .isEqualTo(firstMatchStart + "abcd:1".length());
        assertThat(Dfa.search(program, context, 0, context.length(), false, Prog.MatchKind.LONGEST_MATCH, true))
                .isEqualTo(firstMatchStart + firstMatch.length);
        assertDfaMatchesNfa(program, context, firstMatchStart + firstMatch.length, context.length(), Prog.MatchKind.FIRST_MATCH, pattern);

        byte[] denseFalsePositives = new byte[8 * 1024];
        Arrays.fill(denseFalsePositives, (byte) 'a');
        for (int position = 4; position < denseFalsePositives.length; position += 7) {
            denseFalsePositives[position] = ':';
        }
        Slice denseContext = Slices.wrappedBuffer(denseFalsePositives);
        assertDfaMatchesNfa(program, denseContext, 0, denseContext.length(), Prog.MatchKind.FIRST_MATCH, pattern);
        assertDfaMatchesNfa(program, denseContext, 0, denseContext.length(), Prog.MatchKind.LONGEST_MATCH, pattern);
    }

    @Test
    public void testFixedDistanceByteSearchLatin1()
    {
        String pattern = "[\\x80\\x81]{4}:[0-9]";
        Prog program = compile(pattern, Regexp.LIKE_PERL | Regexp.LATIN1);
        byte[] text = new byte[8 * 1024];
        Arrays.fill(text, (byte) 0x80);
        byte[] match = {(byte) 0x80, (byte) 0x81, (byte) 0x80, (byte) 0x81, ':', '7'};
        int matchStart = 6 * 1024;
        System.arraycopy(match, 0, text, matchStart, match.length);
        Slice context = Slices.wrappedBuffer(text);

        assertThat(program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH).canFixedDistanceByteAcceleration()).isTrue();
        assertDfaMatchesNfa(program, context, 0, context.length(), Prog.MatchKind.FIRST_MATCH, pattern);
        assertDfaMatchesNfa(program, context, 0, context.length(), Prog.MatchKind.LONGEST_MATCH, pattern);
    }

    @Test
    public void testAnchoredSearchDoesNotBuildFixedDistanceCandidates()
    {
        Prog program = compile("[a-z]{8}-[0-9]{4}");
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        byte[] text = new byte[8 * 1024];
        Arrays.fill(text, (byte) 'a');
        Slice context = Slices.wrappedBuffer(text);

        assertThat(dfa.fixedDistanceByteCandidatesInitialized()).isFalse();
        Dfa.search(program, context, 0, text.length, true, Prog.MatchKind.FIRST_MATCH, true);

        assertThat(dfa.fixedDistanceByteCandidatesInitialized()).isFalse();
    }

    @Test
    public void testFixedDistanceSearchRemainsCorrectWithConcurrentResets()
            throws Exception
    {
        Prog program = compile("[a-z]{8}-[0-9]{4}");
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        byte[] text = new byte[8 * 1024];
        Arrays.fill(text, (byte) 'a');
        byte[] match = "abcdefgh-1234".getBytes(StandardCharsets.UTF_8);
        int matchStart = 6 * 1024;
        System.arraycopy(match, 0, text, matchStart, match.length);
        Slice context = Slices.wrappedBuffer(text);
        CountDownLatch startGate = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> searches = executor.submit(() -> {
                await(startGate);
                for (int iteration = 0; iteration < 100; iteration++) {
                    assertThat(Dfa.search(program, context, 0, text.length, false, Prog.MatchKind.FIRST_MATCH, true))
                            .isEqualTo(matchStart + match.length);
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

    private static void assertDfaMatchesNfa(
            Prog program,
            Slice context,
            int start,
            int end,
            Prog.MatchKind matchKind,
            String pattern)
    {
        int[] nfaGroups = new int[2];
        boolean nfaMatched = Nfa.search(program, context, start, end, false, matchKind, nfaGroups);
        long dfaMatchEnd = Dfa.search(program, context, start, end, false, matchKind, true);

        assertThat(dfaMatchEnd >= 0)
                .as("match presence for pattern %s and kind %s", pattern, matchKind)
                .isEqualTo(nfaMatched);
        if (nfaMatched) {
            assertThat(dfaMatchEnd)
                    .as("match end for pattern %s and kind %s", pattern, matchKind)
                    .isEqualTo(nfaGroups[1]);
        }
    }

    @Test
    public void testNullableStartByteEarlyMatch()
    {
        Dfa.DfaInstance star = firstMatchDfa("x*");
        byte[] miss = "aaa".getBytes(StandardCharsets.UTF_8);
        byte[] hit = "xaa".getBytes(StandardCharsets.UTF_8);

        assertThat(star.canStartByteAcceleration()).isFalse();
        assertThat(star.canUseNullableStartByteCheck()).isTrue();
        assertThat(Dfa.canReturnEmptyAtStart(star, miss, 0, miss.length, true, false)).isTrue();
        assertThat(Dfa.canReturnEmptyAtStart(star, hit, 0, hit.length, true, false)).isFalse();
        assertThat(Dfa.canReturnEmptyAtStart(star, miss, 0, miss.length, false, false)).isFalse();
        assertThat(Dfa.canReturnEmptyAtStart(star, miss, 0, miss.length, true, true)).isFalse();

        Dfa.DfaInstance byteFirst = firstMatchDfa("a|");
        assertThat(Dfa.canReturnEmptyAtStart(byteFirst, miss, 0, miss.length, true, false)).isFalse();
        assertThat(Dfa.canReturnEmptyAtStart(byteFirst, "bbb".getBytes(StandardCharsets.UTF_8), 0, 3, true, false)).isTrue();

        Dfa.DfaInstance utf8 = firstMatchDfa("é*");
        assertThat(Dfa.canReturnEmptyAtStart(utf8, miss, 0, miss.length, true, false)).isTrue();

        assertThat(firstMatchDfa("\\b|x").canUseNullableStartByteCheck()).isFalse();
    }

    @Test
    public void testNullableSearchMatchesPinnedUpstreamBoundaries()
    {
        assertThat(Dfa.search(compile("x*"), Slices.utf8Slice("aaa"), 0, 3, false, Prog.MatchKind.FIRST_MATCH, true)).isZero();
        assertThat(Dfa.search(compile("x*"), Slices.utf8Slice("xxa"), 0, 3, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(2);
        assertThat(Dfa.search(compile("x*"), Slices.utf8Slice("xxa"), 2, 3, false, Prog.MatchKind.FIRST_MATCH, true)).isZero();
        assertThat(Dfa.search(compile("|a"), Slices.utf8Slice("a"), 0, 1, false, Prog.MatchKind.FIRST_MATCH, true)).isZero();
        assertThat(Dfa.search(compile("a|"), Slices.utf8Slice("a"), 0, 1, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(1);
    }

    @Test
    public void testSearchMatchesPinnedUpstreamBoundaries()
    {
        Prog program = compile("([a-z]+)-([0-9]+)");
        Slice context = Slices.wrappedBuffer(new byte[] {(byte) 0xFF, '.', '.', 'a', 'b', 'c', '-', '1', '2', '.', (byte) 0xFE});

        // Pinned re2_golden reports groups 3:9,3:6,7:9 for this exact window.
        assertThat(Dfa.search(program, context, 1, 10, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(8);
        assertThat(Dfa.search(program, context, 1, 3, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(SEARCH_NO_MATCH);
    }

    private static Prog compile(String pattern)
    {
        return compile(pattern, Regexp.LIKE_PERL);
    }

    private static Prog compile(String pattern, int parseFlags)
    {
        Regexp regexp = RegexpParser.parse(Slices.utf8Slice(pattern), parseFlags).regexp();
        return Compiler.compile(regexp, false, 0);
    }

    private static Dfa.DfaInstance firstMatchDfa(String pattern)
    {
        return compile(pattern).getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
    }

    private static List<FixedDistanceCase> fixedDistanceCases()
    {
        return List.of(
                new FixedDistanceCase("[a-z]{8}-[0-9]{4}", "abcdefgh-1234"),
                new FixedDistanceCase("(?:abc|xyz)-[0-9]", "xyz-7"),
                new FixedDistanceCase("(?i)(?:ab|cd){2}:[0-9]", "AbCd:7"),
                new FixedDistanceCase("[éĀ]{2}:[0-9]", "éĀ:7"),
                new FixedDistanceCase("([a-z]{4}):([0-9]+)", "abcd:123"),
                new FixedDistanceCase("(?:ab|cd){2}:[0-9]", "abcd:7"));
    }

    private static void await(CountDownLatch latch)
    {
        try {
            latch.await();
        }
        catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interruptedException);
        }
    }

    private record FixedDistanceCase(String pattern, String match) {}
}
