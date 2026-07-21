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
 * Cached parse/extract benchmarks corresponding to upstream's anchored full-match rows.
 * The fixed-size cases contain 18 methods across NFA, OnePass, BitState, Backtrack, and Re2.
 */
@SuppressWarnings("MethodMayBeStatic")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkRe2Parse
{
    private static final String PARSE_3_DIGITS = "([0-9]+)-([0-9]+)-([0-9]+)";
    private static final String PARSE_3_DIGIT_DS = "(\\d+)-(\\d+)-(\\d+)";
    private static final String PARSE_1_SPLIT = "[0-9]+-(.*)";
    private static final String PARSE_SPLIT_HARD = "[0-9]+.(.*)";  // dot, not dash - ambiguous
    private static final String PHONE_INPUT = "650-253-0001";
    private static final String PHONE_SEARCH_PATTERN = "(\\d{3}-|\\(\\d{3}\\)\\s+)(\\d{3}-\\d{4})";
    private static final String PHONE_SEARCH_SUFFIX = "(650) 253-0001";

    @State(Scope.Thread)
    public static class ParseState
    {
        Slice phoneInput;

        Prog prog3Digits;
        Prog prog3DigitDs;
        Prog prog1Split;
        Prog progSplitHard;

        Re2 re2Parse3;
        Re2 re2Parse3Ds;
        Re2 re2Split;
        Re2 re2SplitHard;

        int[] threeCaptureGroups;
        int[] oneCaptureGroup;

        Slice splitBig1Text;  // 100K 'x' + phone
        Slice splitBig2Text;  // "650-253-" + 100K '0'

        @Setup(Level.Trial)
        public void setup()
        {
            phoneInput = Slices.wrappedBuffer(PHONE_INPUT.getBytes(StandardCharsets.UTF_8));

            prog3Digits = compileProg(PARSE_3_DIGITS);
            prog3DigitDs = compileProg(PARSE_3_DIGIT_DS);
            prog1Split = compileProg(PARSE_1_SPLIT);
            progSplitHard = compileProg(PARSE_SPLIT_HARD);

            re2Parse3 = compileRe2(PARSE_3_DIGITS);
            re2Parse3Ds = compileRe2(PARSE_3_DIGIT_DS);
            re2Split = compileRe2(PARSE_1_SPLIT);
            re2SplitHard = compileRe2(PARSE_SPLIT_HARD);

            threeCaptureGroups = new int[8];
            oneCaptureGroup = new int[4];

            // Big text 1: 100K 'x' + phone
            byte[] big1 = new byte[100_000 + PHONE_INPUT.length()];
            Arrays.fill(big1, 0, 100_000, (byte) 'x');
            System.arraycopy(PHONE_INPUT.getBytes(StandardCharsets.UTF_8), 0, big1, 100_000, PHONE_INPUT.length());
            splitBig1Text = Slices.wrappedBuffer(big1);

            // Big text 2: "650-253-" + 100K '0'
            String prefix = "650-253-";
            byte[] big2 = new byte[prefix.length() + 100_000];
            System.arraycopy(prefix.getBytes(StandardCharsets.UTF_8), 0, big2, 0, prefix.length());
            Arrays.fill(big2, prefix.length(), big2.length, (byte) '0');
            splitBig2Text = Slices.wrappedBuffer(big2);
        }
    }

    // Parse3 (3 capture groups)

    @Benchmark
    public boolean parse3DigitsNfa(ParseState state)
    {
        return Nfa.search(state.prog3Digits, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.threeCaptureGroups);
    }

    @Benchmark
    public boolean parse3DigitsOnePass(ParseState state)
    {
        return OnePass.search(state.prog3Digits, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.threeCaptureGroups);
    }

    @Benchmark
    public boolean parse3DigitsBitState(ParseState state)
    {
        return BitState.search(state.prog3Digits, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.threeCaptureGroups);
    }

    @Benchmark
    public boolean parse3DigitsBacktrack(ParseState state)
    {
        return Backtrack.search(state.prog3Digits, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.threeCaptureGroups);
    }

    @Benchmark
    public boolean parse3DigitsRe2(ParseState state)
    {
        return state.re2Parse3.matchInto(state.phoneInput, Anchor.ANCHOR_BOTH, state.threeCaptureGroups);
    }

    // Parse3 DigitDs (3 capture groups, \d shorthand)

    @Benchmark
    public boolean parse3DigitDsNfa(ParseState state)
    {
        return Nfa.search(state.prog3DigitDs, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.threeCaptureGroups);
    }

    @Benchmark
    public boolean parse3DigitDsOnePass(ParseState state)
    {
        return OnePass.search(state.prog3DigitDs, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.threeCaptureGroups);
    }

    @Benchmark
    public boolean parse3DigitDsBitState(ParseState state)
    {
        return BitState.search(state.prog3DigitDs, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.threeCaptureGroups);
    }

    @Benchmark
    public boolean parse3DigitDsBacktrack(ParseState state)
    {
        return Backtrack.search(state.prog3DigitDs, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.threeCaptureGroups);
    }

    @Benchmark
    public boolean parse3DigitDsRe2(ParseState state)
    {
        return state.re2Parse3Ds.matchInto(state.phoneInput, Anchor.ANCHOR_BOTH, state.threeCaptureGroups);
    }

    // Parse1 Split (1 capture group with greedy .*)

    @Benchmark
    public boolean parse1SplitNfa(ParseState state)
    {
        return Nfa.search(state.prog1Split, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.oneCaptureGroup);
    }

    @Benchmark
    public boolean parse1SplitOnePass(ParseState state)
    {
        return OnePass.search(state.prog1Split, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.oneCaptureGroup);
    }

    @Benchmark
    public boolean parse1SplitBitState(ParseState state)
    {
        return BitState.search(state.prog1Split, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.oneCaptureGroup);
    }

    @Benchmark
    public boolean parse1SplitRe2(ParseState state)
    {
        return state.re2Split.matchInto(state.phoneInput, Anchor.ANCHOR_BOTH, state.oneCaptureGroup);
    }

    // Parse SplitHard (ambiguous dot instead of dash)

    @Benchmark
    public boolean parseSplitHardNfa(ParseState state)
    {
        return Nfa.search(state.progSplitHard, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.oneCaptureGroup);
    }

    @Benchmark
    public boolean parseSplitHardBitState(ParseState state)
    {
        return BitState.search(state.progSplitHard, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.oneCaptureGroup);
    }

    @Benchmark
    public boolean parseSplitHardBacktrack(ParseState state)
    {
        return Backtrack.search(state.progSplitHard, state.phoneInput, true, Prog.MatchKind.FULL_MATCH, state.oneCaptureGroup);
    }

    @Benchmark
    public boolean parseSplitHardRe2(ParseState state)
    {
        return state.re2SplitHard.matchInto(state.phoneInput, Anchor.ANCHOR_BOTH, state.oneCaptureGroup);
    }

    // Parse SplitBig1 (100K 'x' + phone)

    @Benchmark
    public boolean parseSplitBig1Re2(ParseState state)
    {
        return state.re2SplitHard.matchInto(state.splitBig1Text, Anchor.UNANCHORED, state.oneCaptureGroup);
    }

    // Parse SplitBig2 ("650-253-" + 100K '0')

    @Benchmark
    public boolean parseSplitBig2Re2(ParseState state)
    {
        return state.re2SplitHard.matchInto(state.splitBig2Text, Anchor.UNANCHORED, state.oneCaptureGroup);
    }

    // SearchPhone (parameterized by text size)

    @State(Scope.Thread)
    public static class PhoneSearchState
    {
        @Param({"8", "64", "512", "4096", "32768", "262144", "2097152", "16777216"})
        int textSize;

        Slice text;
        Re2 re2PhoneSearch;

        @Setup(Level.Trial)
        public void setup()
        {
            // Random text + phone suffix
            byte[] random = randomText(textSize);
            byte[] suffix = PHONE_SEARCH_SUFFIX.getBytes(StandardCharsets.UTF_8);
            byte[] combined = new byte[random.length + suffix.length];
            System.arraycopy(random, 0, combined, 0, random.length);
            System.arraycopy(suffix, 0, combined, random.length, suffix.length);
            text = Slices.wrappedBuffer(combined);

            re2PhoneSearch = compileRe2(PHONE_SEARCH_PATTERN);
        }
    }

    @Benchmark
    public boolean searchPhoneRe2(PhoneSearchState state)
    {
        return state.re2PhoneSearch.partialMatch(state.text);
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = Re2BenchmarkRunner.buildOptions(BenchmarkRe2Parse.class, args);
        new Runner(options).run();
    }
}
