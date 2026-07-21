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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/dfa_test.cc.
public class TestDfaMultithreaded
{
    private static final int REPEAT = 20;
    private static final int THREADS = 8;

    /**
     * Test that multithreaded searching produces correct results.
     * Ported from TEST(Multithreaded, SearchDFA) in dfa_test.cc.
     *
     * The regular expression 0[01]{n}$ matches a binary string of 0s and 1s
     * only if the (n+1)th-to-last character is a 0. Matching this in a single
     * forward pass (as done by the DFA) requires keeping one bit for each of
     * the last n+1 characters (whether each was a 0), or 2^(n+1) possible states.
     */
    @Test
    public void testMultithreadedSearchDfa()
            throws InterruptedException, ExecutionException
    {
        // Use n=8 for reasonable test duration while still exercising the DFA.
        // Upstream uses n=18 but we use smaller for faster unit tests.
        int n = 8;

        String pattern = "0[01]{" + n + "}$";
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), Regexp.LIKE_PERL);

        // The De Bruijn string for n ends with a 1 followed by n 0s in a row,
        // which is not a match for 0[01]{n}$. Adding one more 0 is a match.
        String noMatch = deBruijnString(n);
        String match = noMatch + "0";

        Slice matchSlice = Slices.wrappedBuffer(match.getBytes(UTF_8));
        Slice noMatchSlice = Slices.wrappedBuffer(noMatch.getBytes(UTF_8));

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).as("compile failed for %s", pattern).isNotNull();
        doSearch(prog, matchSlice, noMatchSlice);

        // Run searches simultaneously against the same compiled program.
        for (int i = 0; i < REPEAT; i++) {
            ExecutorService executor = Executors.newFixedThreadPool(THREADS);
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int j = 0; j < THREADS; j++) {
                    futures.add(executor.submit(() -> {
                        doSearch(prog, matchSlice, noMatchSlice);
                    }));
                }

                // Wait for all threads to complete
                for (Future<?> future : futures) {
                    future.get();
                }
            }
            finally {
                executor.shutdown();
            }
        }
    }

    /**
     * Test that multithreaded DFA searching works correctly for patterns
     * that build large DFAs.
     * Ported from TEST(Multithreaded, BuildEntireDFA) in dfa_test.cc.
     */
    @Test
    public void testMultithreadedBuildEntireDfa()
            throws InterruptedException, ExecutionException
    {
        // Create regexp with 2^size states in DFA.
        // Using size=4 to keep tests fast while still exercising multithreaded access.
        int size = 4;
        StringBuilder builder = new StringBuilder("a");
        for (int i = 0; i < size; i++) {
            builder.append("[ab]");
        }
        builder.append("b");
        String pattern = builder.toString();

        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), Regexp.LIKE_PERL);

        // Test text that should match
        String matchingText = "a" + "a".repeat(size) + "b";
        Slice matchSlice = Slices.wrappedBuffer(matchingText.getBytes(UTF_8));

        // Test text that should not match
        String nonMatchingText = "b" + "a".repeat(size) + "a";
        Slice nonMatchSlice = Slices.wrappedBuffer(nonMatchingText.getBytes(UTF_8));

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).as("compile failed for %s", pattern).isNotNull();
        Dfa.DfaInstance dfa = prog.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);

        // Build the shared DFA simultaneously in multiple threads.
        for (int i = 0; i < REPEAT; i++) {
            dfa.resetCacheExternal();
            long cacheVersion = dfa.cacheVersion();
            ExecutorService executor = Executors.newFixedThreadPool(THREADS);
            try {
                CountDownLatch ready = new CountDownLatch(THREADS);
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                for (int j = 0; j < THREADS; j++) {
                    futures.add(executor.submit(() -> {
                        ready.countDown();
                        start.await();
                        long result = Dfa.search(prog, matchSlice, false, Prog.MatchKind.FIRST_MATCH, true);
                        assertThat(result >= 0).as("should match").isTrue();
                        return null;
                    }));
                }
                ready.await();
                start.countDown();

                // Wait for all threads to complete
                for (Future<?> future : futures) {
                    future.get();
                }
                assertThat(dfa.cacheVersion()).isEqualTo(cacheVersion + 1);
                doBuildSearch(prog, matchSlice, nonMatchSlice);
            }
            finally {
                executor.shutdown();
            }
        }
    }

    /**
     * Test concurrent DFA access with multiple different patterns.
     * This tests a more realistic scenario where multiple patterns are searched concurrently.
     * Each pattern has one Prog instance shared by all searches.
     */
    @Test
    public void testMultithreadedMultiplePatterns()
            throws InterruptedException, ExecutionException
    {
        String[] patterns = {
                "hello",
                "world",
                "[a-z]+",
                "\\d+",
                "(foo|bar)+",
                "a*b+c?"
        };

        Prog[] programs = new Prog[patterns.length];
        for (int i = 0; i < patterns.length; i++) {
            ParseResult parsed = RegexpParser.parse(
                    Slices.wrappedBuffer(patterns[i].getBytes(UTF_8)),
                    Regexp.LIKE_PERL);
            programs[i] = Compiler.compile(parsed.regexp(), false, 0);
        }

        String[] texts = {"hello world", "foobarfoo", "abc123xyz", "aaabbc"};
        Slice[] textSlices = new Slice[texts.length];
        for (int i = 0; i < texts.length; i++) {
            textSlices[i] = Slices.wrappedBuffer(texts[i].getBytes(UTF_8));
        }

        AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(THREADS * 2);
        try {
            List<Future<?>> futures = new ArrayList<>();

            // Submit many concurrent searches
            for (int i = 0; i < 100; i++) {
                int patternIndex = i % patterns.length;
                int textIndex = i % textSlices.length;
                Prog prog = programs[patternIndex];
                Slice text = textSlices[textIndex];

                futures.add(executor.submit(() -> {
                    Dfa.search(prog, text, false, Prog.MatchKind.FIRST_MATCH, true);
                    successCount.incrementAndGet();
                }));
            }

            // Wait for all to complete
            for (Future<?> future : futures) {
                future.get();
            }

            assertThat(successCount.get()).isEqualTo(100);
        }
        finally {
            executor.shutdown();
        }
    }

    @Test
    public void testConcurrentCacheReset()
            throws Exception
    {
        String pattern = "0[01]{8}$";
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), Regexp.LIKE_PERL);
        Prog prog = Compiler.compile(parsed.regexp(), false, 0);

        Slice match = Slices.wrappedBuffer((deBruijnString(8) + "0").getBytes(UTF_8));
        Slice noMatch = Slices.wrappedBuffer(deBruijnString(8).getBytes(UTF_8));
        doSearch(prog, match, noMatch);

        Dfa.DfaInstance dfa = prog.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        ExecutorService executor = Executors.newFixedThreadPool(THREADS + 1);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < THREADS; thread++) {
                futures.add(executor.submit(() -> {
                    for (int iteration = 0; iteration < 100; iteration++) {
                        doSearch(prog, match, noMatch);
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
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testVirtualThreadSearchesDuringCacheReset()
            throws Exception
    {
        String pattern = "0[01]{8}$";
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), Regexp.LIKE_PERL);
        Prog prog = Compiler.compile(parsed.regexp(), false, 0);

        Slice match = Slices.wrappedBuffer((deBruijnString(8) + "0").getBytes(UTF_8));
        Slice noMatch = Slices.wrappedBuffer(deBruijnString(8).getBytes(UTF_8));
        doSearch(prog, match, noMatch);

        Dfa.DfaInstance dfa = prog.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        int initialResetCount = dfa.resetCount();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>();
            for (int task = 0; task < 5_000; task++) {
                if (task % 100 == 0) {
                    futures.add(executor.submit(dfa::resetCacheExternal));
                    continue;
                }

                boolean shouldMatch = (task & 1) == 0;
                Slice text = shouldMatch ? match : noMatch;
                futures.add(executor.submit(() -> {
                    boolean matched = Dfa.search(prog, text, false, Prog.MatchKind.FIRST_MATCH, true) >= 0;
                    if (matched != shouldMatch) {
                        throw new AssertionError("unexpected virtual-thread search result");
                    }
                }));
            }

            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        }
        assertThat(dfa.resetCount()).isGreaterThan(initialResetCount);
    }

    private static void doSearch(Prog prog, Slice match, Slice noMatch)
    {
        for (int i = 0; i < 2; i++) {
            long matchResult = Dfa.search(prog, match, false, Prog.MatchKind.FIRST_MATCH, true);
            assertThat(matchResult >= 0).as("should match").isTrue();

            long noMatchResult = Dfa.search(prog, noMatch, false, Prog.MatchKind.FIRST_MATCH, true);
            assertThat(noMatchResult >= 0).as("should not match").isFalse();
        }
    }

    private static void doBuildSearch(Prog prog, Slice match, Slice noMatch)
    {
        // Search which triggers DFA state building
        long matchResult = Dfa.search(prog, match, false, Prog.MatchKind.FIRST_MATCH, true);
        assertThat(matchResult >= 0).as("should match").isTrue();

        long noMatchResult = Dfa.search(prog, noMatch, false, Prog.MatchKind.FIRST_MATCH, true);
        assertThat(noMatchResult >= 0).as("should not match").isFalse();
    }

    /**
     * Generates a De Bruijn string for the binary alphabet {0, 1}.
     * The De Bruijn string B(2,n) contains every n-bit binary string as a substring exactly once.
     *
     * Ported from RE2 re2/testing/string_generator.cc DeBruijnString().
     */
    private static String deBruijnString(int n)
    {
        if (n < 1 || n > 29) {
            throw new IllegalArgumentException("n must be between 1 and 29");
        }

        int size = 1 << n;
        int mask = size - 1;
        boolean[] did = new boolean[size];

        StringBuilder s = new StringBuilder(n + size);

        // Start with n-1 zeros
        for (int i = 0; i < n - 1; i++) {
            s.append('0');
        }

        int bits = 0;
        for (int i = 0; i < size; i++) {
            bits <<= 1;
            bits &= mask;
            if (!did[bits | 1]) {
                bits |= 1;
                s.append('1');
            }
            else {
                s.append('0');
            }
            if (did[bits]) {
                throw new AssertionError("De Bruijn invariant violated");
            }
            did[bits] = true;
        }

        if (s.length() != (n - 1) + size) {
            throw new AssertionError("De Bruijn string length mismatch");
        }

        return s.toString();
    }
}
