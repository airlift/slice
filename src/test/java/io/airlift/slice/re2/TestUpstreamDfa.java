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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/dfa_test.cc.
public class TestUpstreamDfa
{
    private static final int END_TEXT = 256;
    private static final int MATCH_FLAG = 0x0100;

    /**
     * Test data for reverse DFA matching.
     * Ported from RE2 dfa_test.cc ReverseTest structure.
     */
    private record ReverseTest(String regexp, String text, boolean match) {}

    private record CallbackTest(String regexp, String dump, boolean hasExactJavaTopology)
    {
        private CallbackTest(String regexp, String dump)
        {
            this(regexp, dump, true);
        }
    }

    private record BuildResult(int stateCount, boolean exhausted, String dump) {}

    // Test that reverse DFA handles anchored/unanchored correctly.
    // It's in the DFA interface but not used by RE2.
    private static final ReverseTest[] REVERSE_TESTS = {
            new ReverseTest("\\A(a|b)", "abc", true),
            new ReverseTest("(a|b)\\z", "cba", true),
            new ReverseTest("\\A(a|b)", "cba", false),
            new ReverseTest("(a|b)\\z", "abc", false),
    };

    private static final CallbackTest[] CALLBACK_TESTS = {
            new CallbackTest("\\Aa\\z", "[-1,1,-1] [-1,-1,2] [[-1,-1,-1]]"),
            new CallbackTest("\\Aab\\z", "[-1,1,-1,-1] [-1,-1,2,-1] [-1,-1,-1,3] [[-1,-1,-1,-1]]"),
            new CallbackTest("\\Aa*b\\z", "[-1,0,1,-1] [-1,-1,-1,2] [[-1,-1,-1,-1]]"),
            new CallbackTest("\\Aa+b\\z", "[-1,1,-1,-1] [-1,1,2,-1] [-1,-1,-1,3] [[-1,-1,-1,-1]]"),
            new CallbackTest("\\Aa?b\\z", "[-1,1,2,-1] [-1,-1,2,-1] [-1,-1,-1,3] [[-1,-1,-1,-1]]"),
            new CallbackTest("\\Aa\\C*\\z", "[-1,1,-1] [1,1,2] [[-1,-1,-1]]"),
            // Java collapses these accepting loops through its internal full-match sentinel.
            new CallbackTest("\\Aa\\C*", "[-1,1,-1] [2,2,3] [[2,2,2]] [[-1,-1,-1]]", false),
            new CallbackTest("a\\C*", "[0,1,-1] [2,2,3] [[2,2,2]] [[-1,-1,-1]]", false),
            new CallbackTest("\\C*", "[1,2] [[1,1]] [[-1,-1]]", false),
            new CallbackTest("a", "[0,1,-1] [2,2,2] [[-1,-1,-1]]"),
    };

    @Test
    public void testReverseMatch()
    {
        int nfail = 0;
        for (ReverseTest t : REVERSE_TESTS) {
            ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(t.regexp().getBytes(UTF_8)), Regexp.LIKE_PERL);

            Prog prog = Compiler.compile(parsed.regexp(), true, 0);
            assertThat(prog).as("compile failed for %s", t.regexp()).isNotNull();

            Slice text = Slices.wrappedBuffer(t.text().getBytes(UTF_8));
            long result = Dfa.search(prog, text, false, Prog.MatchKind.FIRST_MATCH, true);

            if ((result >= 0) != t.match()) {
                System.err.println(t.regexp() + " on " + t.text() + ": want " + t.match() + ", got " + (result >= 0));
                nfail++;
            }
        }
        assertThat(nfail).isEqualTo(0);
    }

    @Test
    public void testBuildEntireDfaHonorsMemoryLimits()
    {
        String pattern = "a[ab]{30}b";
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), Regexp.LIKE_PERL);

        for (int exponent = 17; exponent < 24; exponent++) {
            long limit = 1L << exponent;
            Prog prog = Compiler.compile(parsed.regexp(), false, limit);
            assertThat(prog).as("compile failed for limit %s", limit).isNotNull();

            BuildResult firstMatch = buildEntireDfa(prog, Dfa.DfaInstance.Kind.FIRST_MATCH, false);
            assertThat(firstMatch.stateCount()).as("first-match states for limit %s", limit).isPositive();
            assertThat(firstMatch.exhausted()).as("first-match budget for limit %s", limit).isTrue();

            BuildResult longestMatch = buildEntireDfa(prog, Dfa.DfaInstance.Kind.LONGEST_MATCH, false);
            assertThat(longestMatch.stateCount()).as("longest-match states for limit %s", limit).isPositive();
            assertThat(longestMatch.exhausted()).as("longest-match budget for limit %s", limit).isTrue();
        }
    }

    @Test
    public void testMultithreadedBuildEntireDfa()
            throws Exception
    {
        String pattern = "a[ab]{8}b";

        Prog singleThreadedProg = compile(pattern);
        assertConcurrentBuilds(singleThreadedProg, 1);

        for (int repetition = 0; repetition < 2; repetition++) {
            Prog prog = compile(pattern);
            assertConcurrentBuilds(prog, 4);

            BuildResult finalBuild = buildEntireDfa(prog, Dfa.DfaInstance.Kind.FIRST_MATCH, false);
            assertThat(finalBuild.exhausted()).as("final build %s", repetition).isFalse();
            assertThat(finalBuild.stateCount()).as("final build states %s", repetition).isPositive();
        }
    }

    @Test
    public void testBuildEntireDfaCallbacks()
    {
        for (CallbackTest test : CALLBACK_TESTS) {
            ParseResult parsed = RegexpParser.parse(Slices.utf8Slice(test.regexp()), Regexp.LIKE_PERL);
            Prog prog = Compiler.compile(parsed.regexp(), false, 0);
            assertThat(prog).as("compile failed for %s", test.regexp()).isNotNull();

            BuildResult result = buildEntireDfa(prog, Dfa.DfaInstance.Kind.LONGEST_MATCH, true);
            assertThat(result.exhausted()).as("unexpected cache exhaustion for %s", test.regexp()).isFalse();
            assertThat(result.stateCount()).as("DFA state count for %s", test.regexp()).isEqualTo(countDumpStates(test.dump()));
            if (test.hasExactJavaTopology()) {
                assertThat(result.dump()).as("DFA callback dump for %s", test.regexp()).isEqualTo(test.dump());
            }
        }
    }

    @Test
    public void testCallbackCasesPreserveLongestMatchBoundaries()
    {
        assertLongestMatchEndAfterFullBuild("\\Aa\\C*", "abc", 3);
        assertLongestMatchEndAfterFullBuild("a\\C*", "zabc", 4);
        assertLongestMatchEndAfterFullBuild("\\C*", "abc", 3);
    }

    @Test
    public void testMultithreadedSearchDfa()
            throws Exception
    {
        int n = 18;
        String pattern = "0[01]{" + n + "}$";
        Slice noMatch = Slices.utf8Slice(deBruijnString(n));
        Slice match = Slices.utf8Slice(deBruijnString(n) + "0");

        Dfa.dfaShouldBailWhenSlow = false;
        try {
            Prog singleThreadedProg = compile(pattern);
            singleThreadedProg.setDfaMemory(1 << n);
            assertConcurrentSearches(singleThreadedProg, match, noMatch, 1);

            for (int repetition = 0; repetition < 2; repetition++) {
                Prog prog = compile(pattern);
                prog.setDfaMemory(1 << n);
                assertConcurrentSearches(prog, match, noMatch, 4);
            }
        }
        finally {
            Dfa.dfaShouldBailWhenSlow = true;
        }
    }

    /**
     * Test basic DFA search functionality with simple patterns.
     */
    @Test
    public void testBasicDfaSearch()
    {
        // Test simple literal match
        assertSearch("hello", "hello world", true);
        assertSearch("hello", "goodbye", false);

        // Test character class
        assertSearch("[a-z]+", "hello", true);
        assertSearch("[a-z]+", "12345", false);

        // Test anchored patterns
        assertSearch("^hello", "hello world", true);
        assertSearch("^hello", "say hello", false);

        assertSearch("world$", "hello world", true);
        assertSearch("world$", "world hello", false);

        // Test alternation
        assertSearch("cat|dog", "I have a cat", true);
        assertSearch("cat|dog", "I have a dog", true);
        assertSearch("cat|dog", "I have a bird", false);

        // Test quantifiers
        assertSearch("a*", "", true);
        assertSearch("a+", "aaa", true);
        assertSearch("a+", "", false);
        assertSearch("a?", "", true);
        assertSearch("a{2,4}", "aa", true);
        assertSearch("a{2,4}", "a", false);
    }

    /**
     * Test DFA with anchored search mode.
     */
    @Test
    public void testAnchoredSearch()
    {
        String pattern = "abc";
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), Regexp.LIKE_PERL);

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).isNotNull();

        // Anchored search should match at the beginning
        Slice text1 = Slices.wrappedBuffer("abcdef".getBytes(UTF_8));
        long result1 = Dfa.search(prog, text1, true, Prog.MatchKind.FIRST_MATCH, true);
        assertThat(result1 >= 0).as("anchored should match at start").isTrue();

        // Anchored search should not match in the middle
        Slice text2 = Slices.wrappedBuffer("xyzabc".getBytes(UTF_8));
        long result2 = Dfa.search(prog, text2, true, Prog.MatchKind.FIRST_MATCH, true);
        assertThat(result2 >= 0).as("anchored should not match in middle").isFalse();

        // Unanchored search should find match anywhere
        long result3 = Dfa.search(prog, text2, false, Prog.MatchKind.FIRST_MATCH, true);
        assertThat(result3 >= 0).as("unanchored should find match").isTrue();
    }

    /**
     * Test DFA with earliestMatch mode.
     * The earliestMatch flag causes the DFA to return as soon as any match is detected.
     * Note: match detection is delayed by one byte in the DFA implementation (per upstream re2/dfa.cc),
     * so the exact boundary position may not be the absolute minimum possible.
     */
    @Test
    public void testEarliestMatchSearch()
    {
        String pattern = "a+";
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), Regexp.LIKE_PERL);

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).isNotNull();

        Slice text = Slices.wrappedBuffer("aaaa".getBytes(UTF_8));

        // Without earliestMatch, should match as much as possible (all 4 'a's)
        long result1 = Dfa.search(prog, text, true, Prog.MatchKind.FIRST_MATCH, true);
        assertThat(result1 >= 0).isTrue();
        assertThat((int) result1).isEqualTo(4);

        // With earliestMatch, returns as soon as a match is detected.
        // Due to the one-byte delay in match detection, this may not be the absolute minimum.
        long result2 = Dfa.search(prog, text, true, Prog.MatchKind.FIRST_MATCH, false);
        assertThat(result2 >= 0).isTrue();
        // Just verify it returns something less than the full match
        assertThat((int) result2).isLessThan(4);
    }

    /**
     * Test DFA with text/context separation.
     */
    @Test
    public void testTextContextSeparation()
    {
        String pattern = "^abc";
        int flags = Regexp.LIKE_PERL & ~Regexp.ONE_LINE; // Multi-line mode
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), flags);

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).isNotNull();

        // Context has newline before text
        byte[] contextBytes = "x\nabc".getBytes(UTF_8);
        Slice context = Slices.wrappedBuffer(contextBytes);
        Slice text = Slices.wrappedBuffer(contextBytes, 2, 3); // Just "abc"

        long result = Dfa.search(prog, context, 2, 5, true, Prog.MatchKind.FIRST_MATCH, true);
        assertThat(result >= 0).as("should match after newline in context").isTrue();
    }

    /**
     * Test that DFA produces correct results even when cache is exhausted and must be reset.
     * Ported from RE2 re2/testing/dfa_test.cc SingleThreaded.SearchDFA n=18 test.
     *
     * The pattern 0[01]{18}$ requires ~2^19 DFA states. With a constrained memory budget,
     * the DFA must reset its cache multiple times during the search. Like the upstream test,
     * the thrashing bailout is disabled so the DFA keeps going despite the high reset rate.
     */
    @Test
    public void testCacheExhaustionWithDeBruijnString()
    {
        // Like upstream: disable thrashing bailout so DFA trudges along
        Dfa.dfaShouldBailWhenSlow = false;
        try {
            int n = 18;
            String pattern = "0[01]{" + n + "}$";
            ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), Regexp.LIKE_PERL);

            Prog prog = Compiler.compile(parsed.regexp(), false, 0);
            assertThat(prog).as("compile failed for %s", pattern).isNotNull();

            // Set DFA budget to match upstream CompileToProg(1<<n).
            // The $ anchor is stripped by the compiler (anchorEnd=true), so the search
            // uses LONGEST_MATCH kind with endmatch=true. Budget must be small enough
            // to force cache resets for 2^19 states.
            prog.setDfaMemory(1 << n);

            String noMatch = deBruijnString(n);
            String match = noMatch + "0";

            Slice matchSlice = Slices.wrappedBuffer(match.getBytes(UTF_8));
            Slice noMatchSlice = Slices.wrappedBuffer(noMatch.getBytes(UTF_8));
            for (int i = 0; i < 10; i++) {
                long matchResult = Dfa.search(prog, matchSlice, false, Prog.MatchKind.FIRST_MATCH, true);
                assertThat(matchResult).as("matching search %s", i).isGreaterThanOrEqualTo(0);

                long noMatchResult = Dfa.search(prog, noMatchSlice, false, Prog.MatchKind.FIRST_MATCH, true);
                assertThat(noMatchResult).as("non-matching search %s", i).isEqualTo(Dfa.SEARCH_NO_MATCH);
            }

            // The $ anchor causes anchorEnd=true, so the search uses LONGEST_MATCH DFA.
            Dfa.DfaInstance dfa = prog.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
            assertThat(dfa).isNotNull();
            assertThat(dfa.resetCount()).as("cache should have been reset at least once").isGreaterThan(0);
        }
        finally {
            Dfa.dfaShouldBailWhenSlow = true;
        }
    }

    /**
     * Test that when the DFA cache thrashes (resets without progress), the search returns
     * SEARCH_FAILED and the Re2 caller falls back to NFA, still producing correct results.
     * Tested through the Re2 API so the full fallback path is exercised.
     */
    @Test
    public void testCacheThrashingFallsBackToNfa()
    {
        int n = 18;
        String pattern = "0[01]{" + n + "}$";

        // Use a small but compilable memory budget. The Re2 pipeline splits:
        // maxMemory -> forwardMemory (2/3) -> dfaMemory (1/2 of forwardMemory).
        // maxMemory=256K -> dfaMemory ~= 85K. The DFA needs ~10MB for this pattern,
        // so it will thrash (reset without making enough progress).
        Re2 re2 = Re2.compile(Slices.utf8Slice(pattern), Re2.Options.defaults().setMaxMemory(256 * 1024));

        String match = deBruijnString(n) + "0";
        Slice matchSlice = Slices.utf8Slice(match);

        // Re2 should still find the match via NFA fallback
        assertThat(re2.partialMatch(matchSlice)).isTrue();
    }

    private static void assertSearch(String pattern, String text, boolean expectMatch)
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), Regexp.LIKE_PERL);

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).as("compile failed for %s", pattern).isNotNull();

        Slice textSlice = Slices.wrappedBuffer(text.getBytes(UTF_8));
        long result = Dfa.search(prog, textSlice, false, Prog.MatchKind.FIRST_MATCH, true);

        assertThat(result >= 0).as("pattern '%s' on text '%s'", pattern, text).isEqualTo(expectMatch);
    }

    private static void assertLongestMatchEndAfterFullBuild(String pattern, String text, int expectedEnd)
    {
        ParseResult parsed = RegexpParser.parse(Slices.utf8Slice(pattern), Regexp.LIKE_PERL);
        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).as("compile failed for %s", pattern).isNotNull();
        BuildResult build = buildEntireDfa(prog, Dfa.DfaInstance.Kind.LONGEST_MATCH, false);
        assertThat(build.exhausted()).isFalse();

        long result = Dfa.search(prog, Slices.utf8Slice(text), false, Prog.MatchKind.LONGEST_MATCH, true);
        assertThat(result).as("longest match for %s on %s", pattern, text).isEqualTo(expectedEnd);
    }

    private static Prog compile(String pattern)
    {
        ParseResult parsed = RegexpParser.parse(Slices.utf8Slice(pattern), Regexp.LIKE_PERL);
        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).as("compile failed for %s", pattern).isNotNull();
        return prog;
    }

    private static void assertConcurrentBuilds(Prog prog, int threadCount)
            throws Exception
    {
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            List<Future<BuildResult>> builds = new ArrayList<>();
            for (int thread = 0; thread < threadCount; thread++) {
                builds.add(executor.submit(() -> buildEntireDfa(prog, Dfa.DfaInstance.Kind.FIRST_MATCH, false)));
            }
            for (Future<BuildResult> build : builds) {
                BuildResult result = build.get();
                assertThat(result.exhausted()).isFalse();
                assertThat(result.stateCount()).isPositive();
            }
        }
    }

    private static void assertConcurrentSearches(Prog prog, Slice match, Slice noMatch, int threadCount)
            throws Exception
    {
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            List<Future<?>> searches = new ArrayList<>();
            for (int thread = 0; thread < threadCount; thread++) {
                searches.add(executor.submit(() -> {
                    assertSearchPairTwice(prog, match, noMatch);
                    return null;
                }));
            }
            for (Future<?> search : searches) {
                search.get();
            }
        }

        Dfa.DfaInstance dfa = prog.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        assertThat(dfa).isNotNull();
        assertThat(dfa.resetCount()).isPositive();
    }

    private static void assertSearchPairTwice(Prog prog, Slice match, Slice noMatch)
    {
        for (int repetition = 0; repetition < 2; repetition++) {
            assertThat(Dfa.search(prog, match, false, Prog.MatchKind.FIRST_MATCH, true))
                    .as("matching search %s", repetition)
                    .isGreaterThanOrEqualTo(0);
            assertThat(Dfa.search(prog, noMatch, false, Prog.MatchKind.FIRST_MATCH, true))
                    .as("non-matching search %s", repetition)
                    .isEqualTo(Dfa.SEARCH_NO_MATCH);
        }
    }

    private static BuildResult buildEntireDfa(Prog prog, Dfa.DfaInstance.Kind kind, boolean collectDump)
    {
        Dfa.DfaInstance dfa = prog.getCachedDfa(kind);
        assertThat(dfa).as("DFA for %s", kind).isNotNull();

        Map<Integer, Integer> stateNumbers = new LinkedHashMap<>();
        ArrayDeque<Integer> states = new ArrayDeque<>();
        StringJoiner dump = new StringJoiner(" ");
        boolean exhausted = false;

        dfa.beginSearch(true);
        try {
            Dfa.StateData start = dfa.analyzeStart(Slices.EMPTY_SLICE, 0, 0, false, true);
            if (start.offset() == Dfa.T_DEAD) {
                return new BuildResult(0, false, "");
            }
            stateNumbers.put(start.offset(), 0);
            states.add(start.offset());

            int[] inputByClass = inputByClass(prog);
            while (!states.isEmpty()) {
                int stateOffset = states.remove();
                int[] output = new int[inputByClass.length];

                for (int byteClass = 0; byteClass < inputByClass.length; byteClass++) {
                    int transition;
                    if (stateOffset == Dfa.T_FULL_MATCH) {
                        transition = Dfa.T_DEAD;
                    }
                    else {
                        transition = dfa.computeTransition(stateOffset, inputByClass[byteClass]);
                    }

                    if (transition == Integer.MIN_VALUE) {
                        exhausted = true;
                        break;
                    }
                    if (transition == Dfa.T_DEAD) {
                        output[byteClass] = -1;
                        continue;
                    }

                    int nextStateOffset = transition == Dfa.T_FULL_MATCH ? transition : transition & ~Dfa.T_MATCH_BIT;
                    Integer nextStateNumber = stateNumbers.get(nextStateOffset);
                    if (nextStateNumber == null) {
                        nextStateNumber = stateNumbers.size();
                        stateNumbers.put(nextStateOffset, nextStateNumber);
                        states.add(nextStateOffset);
                    }
                    output[byteClass] = nextStateNumber;
                }

                if (exhausted) {
                    break;
                }
                if (collectDump) {
                    appendStateDump(dump, output, isMatchingState(dfa, stateOffset));
                }
            }
        }
        finally {
            dfa.endSearch(true, null);
        }
        return new BuildResult(stateNumbers.size(), exhausted, dump.toString());
    }

    private static int[] inputByClass(Prog prog)
    {
        int[] input = new int[prog.bytemapRange() + 1];
        for (int value = 0; value < 256; value++) {
            input[prog.bytemap(value)] = value;
        }
        input[prog.bytemapRange()] = END_TEXT;
        return input;
    }

    private static boolean isMatchingState(Dfa.DfaInstance dfa, int stateOffset)
    {
        if (stateOffset == Dfa.T_FULL_MATCH) {
            return true;
        }
        return (dfa.stateData[stateOffset / dfa.nextSize].flag() & MATCH_FLAG) != 0;
    }

    private static void appendStateDump(StringJoiner dump, int[] transitions, boolean match)
    {
        StringJoiner state = new StringJoiner(",", match ? "[[" : "[", match ? "]]" : "]");
        for (int transition : transitions) {
            state.add(Integer.toString(transition));
        }
        dump.add(state.toString());
    }

    private static int countDumpStates(String dump)
    {
        return dump.isEmpty() ? 0 : dump.split(" ").length;
    }

    /**
     * Generates a De Bruijn string for the binary alphabet {0, 1}.
     * The De Bruijn string B(2,n) contains every n-bit binary string as a substring exactly once.
     *
     * Ported from RE2 re2/testing/string_generator.cc DeBruijnString().
     *
     * @param n the length of substrings to cover (must be between 1 and 29)
     * @return the De Bruijn string
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

    /**
     * Test the De Bruijn string generator itself.
     */
    @Test
    public void testDeBruijnString()
    {
        // Test small n values
        String b2 = deBruijnString(2);
        assertThat(b2).hasSize(1 + (1 << 2)); // n-1 + 2^n = 1 + 4 = 5
        assertThat(b2).isEqualTo("01100"); // B(2,2)

        String b3 = deBruijnString(3);
        assertThat(b3).hasSize(2 + (1 << 3)); // n-1 + 2^n = 2 + 8 = 10
        // Verify all 3-bit substrings appear exactly once
        java.util.Set<String> substrings = new java.util.HashSet<>();
        for (int i = 0; i <= b3.length() - 3; i++) {
            String sub = b3.substring(i, i + 3);
            assertThat(substrings.add(sub)).as("duplicate substring: %s", sub).isTrue();
        }
        assertThat(substrings).hasSize(8); // 2^3 = 8 unique 3-bit strings
    }
}
