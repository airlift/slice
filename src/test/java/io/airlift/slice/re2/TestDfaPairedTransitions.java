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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDfaPairedTransitions
{
    @Test
    public void testSelfLoopSamplingRequiresLongEndConstrainedSearch()
    {
        assertThat(Dfa.shouldSampleSelfLoopTransitions(true, 32 * 1024, false)).isFalse();
        assertThat(Dfa.shouldSampleSelfLoopTransitions(true, 64 * 1024, false)).isTrue();
        assertThat(Dfa.shouldSampleSelfLoopTransitions(false, 64 * 1024, false)).isFalse();
        assertThat(Dfa.shouldSampleSelfLoopTransitions(true, 64 * 1024, true)).isFalse();
    }

    @Test
    public void testShortSearchDoesNotBuildPairedTransitions()
    {
        Prog program = compileProg("[ -~]*ABC$");
        Slice input = utf8Slice("xxxxABC");
        assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(input.length());

        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        assertThat(dfa.pairedTransitionMemory()).isZero();
    }

    @Test
    public void testPairedSearchAndFallback()
    {
        Prog program = compileProg("[ -~]*ABC$");
        Slice matchingInput = utf8Slice("x".repeat(300) + "ABC");

        assertThat(Dfa.search(program, matchingInput, false, Prog.MatchKind.FIRST_MATCH, true))
                .isEqualTo(matchingInput.length());

        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        assertThat(dfa.pairedTransitionMemory()).isBetween(1L, 64L * 1024);
        assertThat(dfa.pairedTransitionRowCount()).isEqualTo(dfa.stateCount);
        assertThat(dfa.pairedTransitionCount()).isPositive();

        int textBegin = matchingInput.byteArrayOffset();
        int textEnd = textBegin + matchingInput.length();
        Dfa.StateData start = dfa.analyzeStart(matchingInput, textBegin, textEnd, false, true);
        assertThat(Dfa.searchForwardPairs(
                dfa,
                matchingInput.byteArray(),
                textBegin,
                textEnd,
                textEnd,
                start.offset(),
                true,
                false))
                .isEqualTo(matchingInput.length());

        assertThat(Dfa.search(program, matchingInput, false, Prog.MatchKind.FIRST_MATCH, true))
                .isEqualTo(matchingInput.length());
        assertThat(Dfa.search(program, utf8Slice("x".repeat(300) + "ABD"), false, Prog.MatchKind.FIRST_MATCH, true))
                .isEqualTo(Dfa.SEARCH_NO_MATCH);
    }

    @Test
    public void testRepeatedShortMatchesDisablePairedTransitions()
    {
        Prog program = compileProg("^x*(ab|a)");
        Slice warmingInput = utf8Slice("x".repeat(300) + "abz");
        Slice input = utf8Slice("ab" + "z".repeat(300));

        assertThat(Dfa.search(program, warmingInput, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(302);
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        assertThat(dfa.pairedTransitionMemory()).isPositive();
        for (int search = 1; search < 16; search++) {
            assertThat(Dfa.search(program, input, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(2);
            assertThat(dfa.pairedTransitionsDisabled())
                    .as("paired transitions after short search %s", search)
                    .isFalse();
        }

        long pairedTransitionMemory = dfa.pairedTransitionMemory();
        long absolutePointerMemory = dfa.absolutePointerTransitionMemory();
        long availableStateMemory = dfa.availableStateMemory();
        assertThat(Dfa.search(program, input, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(2);
        assertThat(dfa.pairedTransitionsDisabled()).isTrue();
        assertThat(dfa.pairedTransitionMemory()).isZero();
        long allocatedAbsolutePointerMemory = dfa.absolutePointerTransitionMemory() - absolutePointerMemory;
        assertThat(dfa.availableStateMemory())
                .isEqualTo(availableStateMemory + pairedTransitionMemory - allocatedAbsolutePointerMemory);

        dfa.resetCacheExternal();
        boolean absolutePointersActive = dfa.absolutePointerTransitionMemory() > 0;
        assertThat(dfa.pairedTransitionsDisabled()).isEqualTo(absolutePointersActive);
        assertThat(Dfa.search(program, warmingInput, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(302);
        if (absolutePointersActive) {
            assertThat(dfa.pairedTransitionMemory()).isZero();
            assertThat(dfa.absolutePointerTransitionCount()).isPositive();
        }
        else {
            assertThat(dfa.pairedTransitionMemory()).isPositive();
        }
        assertThat(Dfa.search(program, warmingInput, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(302);
    }

    @Test
    public void testResetRebuildsPairedTransitions()
    {
        Prog program = compileProg("[ -~]*ABC$");
        Slice input = utf8Slice("x".repeat(300) + "ABC");
        assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(input.length());

        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        assertThat(dfa.pairedTransitionMemory()).isPositive();
        dfa.resetCacheExternal();
        assertThat(dfa.pairedTransitionMemory()).isZero();

        assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(input.length());
        assertThat(dfa.pairedTransitionMemory()).isPositive();
    }

    @Test
    public void testFallbackBeforeEitherInputByte()
    {
        for (int padding : List.of(299, 300)) {
            Prog program = compileProg("[ -~]*A");
            Slice input = utf8Slice("x".repeat(padding) + "Az");

            assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(padding + 1);
            Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
            assertThat(dfa.pairedTransitionMemory()).isPositive();
            assertThat(dfa.pairedMatchContinuationCount()).isPositive();

            // The transition to a match is abnormal in a different half of the
            // pair for odd and even padding. The paired decoder must preserve
            // the exact boundary before continuing the search.
            assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(padding + 1);
            Slice multipleMatches = utf8Slice("x".repeat(padding) + "AxxA");
            assertThat(Dfa.search(program, multipleMatches, false, Prog.MatchKind.FIRST_MATCH, true))
                    .isEqualTo(multipleMatches.length());
        }
    }

    @Test
    public void testDeadTransitionFallsBackWithoutConsumingInput()
    {
        Prog program = compileProg("^[xy]*A");
        Slice input = utf8Slice("x".repeat(300) + "zq");

        assertThat(Dfa.search(program, input, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(Dfa.SEARCH_NO_MATCH);
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        assertThat(dfa.pairedTransitionMemory()).isPositive();
        assertThat(Dfa.search(program, input, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(Dfa.SEARCH_NO_MATCH);
    }

    @Test
    public void testPairedMatchContinuationPreservesPriority()
    {
        Prog program = compileProg("^x*(ab|a)");
        Slice input = utf8Slice("x".repeat(300) + "abz");

        assertThat(Dfa.search(program, input, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(302);
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        assertThat(dfa.pairedTransitionMemory()).isPositive();
        assertThat(dfa.pairedMatchContinuationCount()).isPositive();
        assertThat(Dfa.search(program, input, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(302);
    }

    @Test
    public void testPairedMatchContinuationAtSearchBoundary()
    {
        Prog program = compileProg("^x*(ab|a)");
        Slice endOfTextInput = utf8Slice("x".repeat(300) + "ab");

        assertThat(Dfa.search(program, endOfTextInput, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(302);
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        assertThat(dfa.pairedTransitionMemory()).isPositive();
        assertThat(dfa.pairedMatchContinuationCount()).isPositive();
        assertThat(Dfa.search(program, endOfTextInput, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(302);

        // The byte following textEnd is context for empty-width assertions, not input.
        Slice contextSentinelInput = utf8Slice("x".repeat(300) + "abz");
        assertThat(Dfa.search(program, contextSentinelInput, 0, 302, true, Prog.MatchKind.FIRST_MATCH, true))
                .isEqualTo(302);

        Prog shorterAlternativeFirst = compileProg("^x*(a|ab)");
        assertThat(Dfa.search(shorterAlternativeFirst, endOfTextInput, true, Prog.MatchKind.FIRST_MATCH, true))
                .isEqualTo(301);
    }

    @Test
    public void testMatchingSelfLoopUsesDirectScan()
    {
        Prog program = compileProg(".*.*=.*");
        Slice input = utf8Slice("key=" + "x".repeat(10_000));

        assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(input.length());

        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        assertThat(dfa.pairedTransitionMemory()).isPositive();
        assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(input.length());
        assertThat(dfa.matchingSelfLoopScanCount()).isPositive();

        Slice newline = utf8Slice("key=" + "x".repeat(300) + "\nignored");
        assertThat(Dfa.search(program, newline, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(304);
    }

    @Test
    public void testPairedMatchContinuationFallsBackFromPartialTable()
    {
        Prog program = compileProg("[ -~]*(ABCDEFGHIJKLMNOPQRST|A)");
        Slice input = utf8Slice("x".repeat(32 * 1024) + "ABCDEFGHIJKLMNOPQRSTz");

        assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(32_788);
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        assertThat(dfa.estimatedPairedTransitionMemory()).isGreaterThan(Dfa.MAX_PAIRED_TRANSITION_MEMORY);
        assertThat(dfa.pairedTransitionRowCount()).isEqualTo(2);
        assertThat(dfa.pairedMatchContinuationCount()).isEqualTo(1);
        assertThat(dfa.pairedMatchContinuationToUnpairedCount()).isEqualTo(1);
        assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(32_788);
    }

    @Test
    public void testPairedMatchContinuationSurvivesCacheExhaustion()
    {
        Prog program = compileProg("^x*(a[01]{200}|a)");
        program.setDfaMemory(64 * 1024);
        Slice warmingInput = utf8Slice("x".repeat(300) + "a0");
        Slice input = utf8Slice("x".repeat(300) + "a" + "0".repeat(200) + 'q');

        assertThat(Dfa.search(program, warmingInput, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(301);
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        assertThat(dfa.pairedTransitionMemory()).isPositive();
        assertThat(dfa.pairedMatchContinuationCount()).isPositive();
        int resetCount = dfa.resetCount();

        assertThat(Dfa.search(program, input, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(501);
        assertThat(dfa.resetCount()).isGreaterThan(resetCount);
    }

    @Test
    public void testPartialPairingForHotSelfLoop()
    {
        Prog program = compileProg("[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$");
        Slice input = utf8Slice("x".repeat(32 * 1024) + "ABCDEFGHIJKLMNOPQRSTUVWXYZ");

        assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(input.length());
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        assertThat(dfa.estimatedPairedTransitionMemory()).isGreaterThan(Dfa.MAX_PAIRED_TRANSITION_MEMORY);

        // The unanchored start state consumes nearly all input through a self-loop.
        // Pairing the entry and self-loop rows preserves the fast path without
        // expanding the 28 suffix states that are visited once.
        assertThat(dfa.pairedTransitionMemory()).isBetween(1L, 16L * 1024);
        assertThat(dfa.pairedTransitionRowCount()).isEqualTo(2);
        assertThat(dfa.pairedTransitionCount()).isPositive();
        assertThat(dfa.selfLoopTransitionCount()).isPositive();
        assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(input.length());
        assertThat(Dfa.search(
                program,
                utf8Slice("x".repeat(32 * 1024) + "ABCDEFGHIJKLMNOPQRSTUVWXY_"),
                false,
                Prog.MatchKind.FIRST_MATCH,
                true))
                .isEqualTo(Dfa.SEARCH_NO_MATCH);
    }

    @Test
    public void testPairedTransitionsUseAndReleaseDfaBudget()
    {
        long dfaBudget = 34 * 1024;
        Prog program = compileProg("[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$");
        // Forward programs divide their DFA memory equally between first- and
        // longest-match instances.
        program.setDfaMemory(dfaBudget * 2);
        Slice input = utf8Slice("x".repeat(32 * 1024) + "ABCDEFGHIJKLMNOPQRSTUVWXYZ");

        assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(input.length());
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        assertThat(dfa.stateBudget()).isLessThan(dfaBudget);
        assertThat(dfa.pairedTransitionMemory()).isPositive();
        assertThat(dfa.availableStateMemory()).isNotNegative();
        assertThat(dfa.availableStateMemory() + dfa.pairedTransitionMemory()).isLessThan(dfa.stateBudget());

        dfa.resetCacheExternal();
        assertThat(dfa.pairedTransitionMemory()).isZero();
        assertThat(dfa.availableStateMemory() + dfa.retainedStateMemory())
                .isEqualTo(dfa.stateBudget());
    }

    @Test
    public void testPairedTransitionMemoryIsChargedExactly()
    {
        Prog program = compileProg("[ -~]*ABC$");
        Slice shortInput = utf8Slice("x".repeat(200) + "ABC");
        Slice longInput = utf8Slice("x".repeat(300) + "ABC");

        assertThat(Dfa.search(program, shortInput, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(shortInput.length());
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        assertThat(dfa.pairedTransitionMemory()).isZero();
        long memoryBeforePairing = dfa.availableStateMemory();

        assertThat(Dfa.search(program, longInput, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(longInput.length());
        assertThat(dfa.pairedTransitionMemory()).isPositive();
        assertThat(memoryBeforePairing - dfa.availableStateMemory()).isEqualTo(dfa.pairedTransitionMemory());
    }

    @Test
    public void testPartialPairingRejectsMultiRowGraph()
    {
        Prog program = compileProg(".{0,2}(Tom|Sawyer|Huckleberry|Finn)");
        StringBuilder builder = new StringBuilder(70_000);
        int pseudoRandom = 1;
        while (builder.length() < 64 * 1024) {
            pseudoRandom = pseudoRandom * 1_664_525 + 1_013_904_223;
            builder.append((char) (' ' + Integer.remainderUnsigned(pseudoRandom, 95)));
            if (builder.length() % 4_096 == 0) {
                builder.append(switch ((builder.length() / 4_096) & 3) {
                    case 0 -> "Tom";
                    case 1 -> "Sawyer";
                    case 2 -> "Huckleberry";
                    default -> "Finn";
                });
            }
        }
        Slice input = utf8Slice(builder.toString());

        int start = 0;
        int matchCount = 0;
        while (start <= input.length()) {
            long matchEnd = Dfa.search(program, input, start, input.length(), false, Prog.MatchKind.FIRST_MATCH, true);
            if (matchEnd == Dfa.SEARCH_NO_MATCH) {
                break;
            }
            assertThat(matchEnd).isPositive();
            start += (int) matchEnd;
            matchCount++;
        }
        assertThat(matchCount).isEqualTo(16);

        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        assertThat(dfa.estimatedPairedTransitionMemory()).isGreaterThan(Dfa.MAX_PAIRED_TRANSITION_MEMORY);
        assertThat(dfa.pairedTransitionCount()).isZero();
        assertThat(dfa.pairedTransitionMemory()).isZero();
    }

    @Test
    public void testPartialPairingRejectsLargeRows()
    {
        String suffix = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        Prog program = compileProg("[ -~]*" + suffix + '$');
        Slice input = utf8Slice("0".repeat(32 * 1024) + suffix);

        assertThat(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(input.length());
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        assertThat(dfa.estimatedPairedTransitionMemory()).isGreaterThan(Dfa.MAX_PAIRED_TRANSITION_MEMORY);
        assertThat(dfa.pairedRowSelectionCount()).isZero();
        assertThat(dfa.pairedTransitionMemory()).isZero();
    }

    @Test
    public void testConcurrentSearchAndResetWithPairedTransitions()
            throws Exception
    {
        Prog program = compileProg("[ -~]*ABC$");
        Slice matchingInput = utf8Slice("x".repeat(300) + "ABC");
        Slice nonMatchingInput = utf8Slice("x".repeat(300) + "ABD");
        assertThat(Dfa.search(program, matchingInput, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(matchingInput.length());

        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        assertThat(dfa.pairedTransitionMemory()).isPositive();

        try (ExecutorService executor = Executors.newFixedThreadPool(5)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < 4; thread++) {
                futures.add(executor.submit(() -> {
                    for (int iteration = 0; iteration < 100; iteration++) {
                        assertThat(Dfa.search(program, matchingInput, false, Prog.MatchKind.FIRST_MATCH, true))
                                .isEqualTo(matchingInput.length());
                        assertThat(Dfa.search(program, nonMatchingInput, false, Prog.MatchKind.FIRST_MATCH, true))
                                .isEqualTo(Dfa.SEARCH_NO_MATCH);
                    }
                }));
            }
            futures.add(executor.submit(() -> {
                for (int iteration = 0; iteration < 100; iteration++) {
                    dfa.resetCacheExternal();
                }
            }));

            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        }

        assertThat(Dfa.search(program, matchingInput, false, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(matchingInput.length());
        assertThat(dfa.pairedTransitionMemory()).isPositive();
    }

    @Test
    public void testConcurrentSearchAndResetWithPairedMatchContinuation()
            throws Exception
    {
        Prog program = compileProg("^x*(ab|a)");
        Slice matchingInput = utf8Slice("x".repeat(300) + "abz");
        Slice shorterMatchingInput = utf8Slice("x".repeat(300) + "aq");
        assertThat(Dfa.search(program, matchingInput, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(302);

        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        assertThat(dfa.pairedTransitionMemory()).isPositive();
        assertThat(dfa.pairedMatchContinuationCount()).isPositive();

        try (ExecutorService executor = Executors.newFixedThreadPool(5)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < 4; thread++) {
                futures.add(executor.submit(() -> {
                    for (int iteration = 0; iteration < 100; iteration++) {
                        assertThat(Dfa.search(program, matchingInput, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(302);
                        assertThat(Dfa.search(program, shorterMatchingInput, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(301);
                    }
                }));
            }
            futures.add(executor.submit(() -> {
                for (int iteration = 0; iteration < 100; iteration++) {
                    dfa.resetCacheExternal();
                }
            }));

            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        }

        assertThat(Dfa.search(program, matchingInput, true, Prog.MatchKind.FIRST_MATCH, true)).isEqualTo(302);
        assertThat(dfa.pairedTransitionMemory()).isPositive();
        assertThat(dfa.pairedMatchContinuationCount()).isPositive();
    }
}
