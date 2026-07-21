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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static io.airlift.slice.re2.Re2BenchmarkRunner.compileRe2;

/**
 * Miscellaneous RE2 benchmarks (edge cases and special patterns).
 */
@SuppressWarnings("MethodMayBeStatic")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkRe2Misc
{
    private static final String EMPTY_PATTERN = "";
    private static final String DOT_MATCH_PATTERN = "(?-s)^(.+)";
    private static final String ASCII_MATCH_PATTERN = "(?-s)^([ -~]+)";

    // HTTP text (102 bytes) - same as C++ benchmark
    private static final String HTTP_TEXT =
            "GET /asdfhjasdhfasdlfhasdflkjasdfkljasdhflaskdjhf" +
            "alksdjfhasdlkfhasdlkjfhasdljkfhadsjklf HTTP/1.1";

    // PossibleMatchRange patterns
    private static final String PMR_TRIVIAL_PATTERN = ".*";
    private static final String PMR_COMPLEX_PATTERN = "^abc[def]?[gh]{1,2}.*";
    private static final String PMR_PREFIX_PATTERN = "^some_random_prefix.*";
    private static final String PMR_NOPROG_PATTERN = "^some_random_string$";
    private static final int POSSIBLE_MATCH_RANGE_MAXIMUM_LENGTH = 16;

    @State(Scope.Thread)
    public static class MiscState
    {
        Slice emptyText;
        Slice httpText;

        Re2 re2Empty;
        Re2 re2DotMatch;
        Re2 re2AsciiMatch;

        @Setup(Level.Trial)
        public void setup()
        {
            emptyText = Slices.wrappedBuffer(new byte[0]);
            httpText = Slices.wrappedBuffer(HTTP_TEXT.getBytes(StandardCharsets.UTF_8));

            re2Empty = compileRe2(EMPTY_PATTERN);
            re2DotMatch = compileRe2(DOT_MATCH_PATTERN);
            re2AsciiMatch = compileRe2(ASCII_MATCH_PATTERN);
        }
    }

    @State(Scope.Thread)
    public static class PossibleMatchRangeState
    {
        Prog progTrivial;
        Prog progComplex;
        Prog progPrefix;
        Prog progNoProg;

        @Setup(Level.Trial)
        public void setup()
        {
            progTrivial = compileProg(PMR_TRIVIAL_PATTERN);
            progComplex = compileProg(PMR_COMPLEX_PATTERN);
            progPrefix = compileProg(PMR_PREFIX_PATTERN);
            progNoProg = compileProg(PMR_NOPROG_PATTERN);
        }
    }

    // EmptyPartialMatch: empty pattern on empty input

    @Benchmark
    public boolean emptyPartialMatch(MiscState state)
    {
        return state.re2Empty.partialMatch(state.emptyText);
    }

    // DotMatch: (?-s)^(.+) on HTTP text (dot without DOTALL)

    @Benchmark
    public boolean dotMatch(MiscState state)
    {
        return state.re2DotMatch.partialMatch(state.httpText);
    }

    // ASCIIMatch: (?-s)^([ -~]+) on HTTP text (printable ASCII range)

    @Benchmark
    public boolean asciiMatch(MiscState state)
    {
        return state.re2AsciiMatch.partialMatch(state.httpText);
    }

    // PossibleMatchRange benchmarks

    @Benchmark
    public Prog.PossibleMatchRangeResult possibleMatchRangeTrivial(PossibleMatchRangeState state)
    {
        return state.progTrivial.possibleMatchRange(POSSIBLE_MATCH_RANGE_MAXIMUM_LENGTH);
    }

    @Benchmark
    public Prog.PossibleMatchRangeResult possibleMatchRangeComplex(PossibleMatchRangeState state)
    {
        return state.progComplex.possibleMatchRange(POSSIBLE_MATCH_RANGE_MAXIMUM_LENGTH);
    }

    @Benchmark
    public Prog.PossibleMatchRangeResult possibleMatchRangePrefix(PossibleMatchRangeState state)
    {
        return state.progPrefix.possibleMatchRange(POSSIBLE_MATCH_RANGE_MAXIMUM_LENGTH);
    }

    @Benchmark
    public Prog.PossibleMatchRangeResult possibleMatchRangeNoProg(PossibleMatchRangeState state)
    {
        return state.progNoProg.possibleMatchRange(POSSIBLE_MATCH_RANGE_MAXIMUM_LENGTH);
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = Re2BenchmarkRunner.buildOptions(BenchmarkRe2Misc.class, args);
        new Runner(options).run();
    }
}
