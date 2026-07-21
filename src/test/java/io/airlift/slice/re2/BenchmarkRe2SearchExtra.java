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
import io.airlift.slice.re2.Re2.Anchor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static io.airlift.slice.re2.Re2BenchmarkRunner.compileRe2;
import static io.airlift.slice.re2.Re2BenchmarkRunner.randomText;

/**
 * Additional search benchmarks from C++ upstream.
 * Includes case-insensitive, Unicode fanout, anchored matches, and submatch extraction.
 */
@SuppressWarnings("MethodMayBeStatic")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkRe2SearchExtra
{
    // Case-insensitive version of EASY0
    private static final String EASY2 = "(?i)ABCDEFGHIJKLMNOPQRSTUVWXYZ$";
    // Unicode fanout (high NFA load)
    private static final String FANOUT = "(?:[\\x{80}-\\x{10FFFF}]?){100}[\\x{80}-\\x{10FFFF}]";
    // Anchored patterns
    private static final String SUCCESS = ".*$";
    private static final String SUCCESS1 = ".*\\C$";
    private static final String ALT_MATCH = "\\C*";
    // Digits pattern with capturing groups
    private static final String DIGITS = "([0-9]+)-([0-9]+)-([0-9]+)";

    @State(Scope.Thread)
    public static class SearchState
    {
        @Param({"8", "64", "512", "4096", "32768", "262144", "2097152", "16777216"})
        int textSize;

        Slice text;

        Prog progEasy2;
        Prog progFanout;
        Prog progSuccess;
        Prog progSuccess1;
        Prog progAltMatch;

        Re2 re2Easy2;
        Re2 re2Fanout;
        Re2 re2Success;
        Re2 re2Success1;
        Re2 re2AltMatch;

        @Setup(Level.Trial)
        public void setup()
        {
            text = Slices.wrappedBuffer(randomText(textSize));

            progEasy2 = compileProg(EASY2);
            progFanout = compileProg(FANOUT);
            progSuccess = compileProg(SUCCESS);
            progSuccess1 = compileProg(SUCCESS1);
            progAltMatch = compileProg(ALT_MATCH);

            re2Easy2 = compileRe2(EASY2);
            re2Fanout = compileRe2(FANOUT);
            re2Success = compileRe2(SUCCESS);
            re2Success1 = compileRe2(SUCCESS1);
            re2AltMatch = compileRe2(ALT_MATCH);
        }
    }

    @State(Scope.Thread)
    public static class BigFixedState
    {
        @Param({"8", "64", "512", "4096", "32768", "262144", "1048576"})
        int textSize;

        Slice text;
        Prog progBigFixed;
        Re2 re2BigFixed;

        @Setup(Level.Trial)
        public void setup()
        {
            int half = textSize / 2;
            // Pattern: ^x...x.*$ (half x's)
            StringBuilder builder = new StringBuilder("^");
            for (int i = 0; i < half; i++) {
                builder.append('x');
            }
            builder.append(".*$");
            String pattern = builder.toString();
            progBigFixed = compileProg(pattern);
            re2BigFixed = compileRe2(pattern);

            // Text: half 'x', half random
            byte[] textBytes = new byte[textSize];
            Arrays.fill(textBytes, 0, half, (byte) 'x');
            byte[] random = randomText(textSize - half);
            System.arraycopy(random, 0, textBytes, half, textSize - half);
            text = Slices.wrappedBuffer(textBytes);
        }
    }

    @State(Scope.Thread)
    public static class DigitsState
    {
        Slice text;
        Prog progDigits;
        Re2 re2Digits;
        int[] submatch;

        @Setup(Level.Trial)
        public void setup()
        {
            text = Slices.wrappedBuffer("650-253-0001".getBytes(StandardCharsets.UTF_8));
            progDigits = compileProg(DIGITS);
            re2Digits = compileRe2(DIGITS);
            // 4 groups: full match + 3 captures, each needs start and end
            submatch = new int[8];
        }
    }

    // ========== Easy2 (case-insensitive) ==========

    @Benchmark
    public Object searchEasy2Dfa(SearchState state)
    {
        return Dfa.search(state.progEasy2, state.text, false, Prog.MatchKind.FIRST_MATCH, true);
    }

    @Benchmark
    public boolean searchEasy2Re2(SearchState state)
    {
        return state.re2Easy2.partialMatch(state.text);
    }

    // ========== Fanout (Unicode) ==========

    @Benchmark
    public Object searchFanoutDfa(SearchState state)
    {
        return Dfa.search(state.progFanout, state.text, false, Prog.MatchKind.FIRST_MATCH, true);
    }

    @Benchmark
    public boolean searchFanoutRe2(SearchState state)
    {
        return state.re2Fanout.partialMatch(state.text);
    }

    // ========== BigFixed (dynamic pattern) ==========

    @Benchmark
    public Object searchBigFixedDfa(BigFixedState state)
    {
        return Dfa.search(state.progBigFixed, state.text, false, Prog.MatchKind.FIRST_MATCH, true);
    }

    @Benchmark
    public boolean searchBigFixedRe2(BigFixedState state)
    {
        return state.re2BigFixed.partialMatch(state.text);
    }

    // ========== Success (anchored .*$) ==========

    @Benchmark
    public Object searchSuccessDfa(SearchState state)
    {
        return Dfa.search(state.progSuccess, state.text, true, Prog.MatchKind.FIRST_MATCH, true);
    }

    @Benchmark
    public boolean searchSuccessRe2(SearchState state)
    {
        return state.re2Success.matchInto(state.text, Anchor.ANCHOR_START, null);
    }

    @Benchmark
    public boolean searchSuccessOnePass(SearchState state)
    {
        return OnePass.search(state.progSuccess, state.text, true, Prog.MatchKind.FIRST_MATCH, null);
    }

    // ========== Success1 (anchored .*\C$) ==========

    @Benchmark
    public Object searchSuccess1Dfa(SearchState state)
    {
        return Dfa.search(state.progSuccess1, state.text, true, Prog.MatchKind.FIRST_MATCH, true);
    }

    @Benchmark
    public boolean searchSuccess1Re2(SearchState state)
    {
        return state.re2Success1.matchInto(state.text, Anchor.ANCHOR_START, null);
    }

    @Benchmark
    public boolean searchSuccess1BitState(SearchState state)
    {
        return BitState.search(state.progSuccess1, state.text, true, Prog.MatchKind.FIRST_MATCH, null);
    }

    // ========== AltMatch (anchored \C*) ==========

    @Benchmark
    public Object searchAltMatchDfa(SearchState state)
    {
        return Dfa.search(state.progAltMatch, state.text, true, Prog.MatchKind.FIRST_MATCH, true);
    }

    @Benchmark
    public boolean searchAltMatchRe2(SearchState state)
    {
        return state.re2AltMatch.matchInto(state.text, Anchor.ANCHOR_START, null);
    }

    @Benchmark
    public boolean searchAltMatchOnePass(SearchState state)
    {
        return OnePass.search(state.progAltMatch, state.text, true, Prog.MatchKind.FIRST_MATCH, null);
    }

    @Benchmark
    public boolean searchAltMatchBitState(SearchState state)
    {
        return BitState.search(state.progAltMatch, state.text, true, Prog.MatchKind.FIRST_MATCH, null);
    }

    // ========== Digits (fixed input with captures) ==========

    @Benchmark
    public Object searchDigitsDfa(DigitsState state)
    {
        return Dfa.search(state.progDigits, state.text, true, Prog.MatchKind.FIRST_MATCH, true);
    }

    @Benchmark
    public boolean searchDigitsNfa(DigitsState state)
    {
        return Nfa.search(state.progDigits, state.text, true, Prog.MatchKind.FIRST_MATCH, state.submatch);
    }

    @Benchmark
    public boolean searchDigitsOnePass(DigitsState state)
    {
        return OnePass.search(state.progDigits, state.text, true, Prog.MatchKind.FIRST_MATCH, state.submatch);
    }

    @Benchmark
    public boolean searchDigitsRe2(DigitsState state)
    {
        return state.re2Digits.matchInto(state.text, Anchor.ANCHOR_START, null);
    }

    @Benchmark
    public boolean searchDigitsBitState(DigitsState state)
    {
        return BitState.search(state.progDigits, state.text, true, Prog.MatchKind.FIRST_MATCH, state.submatch);
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = Re2BenchmarkRunner.buildOptions(BenchmarkRe2SearchExtra.class, args);
        new Runner(options).run();
    }
}
