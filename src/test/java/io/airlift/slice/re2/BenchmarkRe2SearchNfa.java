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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;

import java.util.concurrent.TimeUnit;

import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static io.airlift.slice.re2.Re2BenchmarkRunner.randomText;

/**
 * NFA search benchmarks (failed match on random text).
 * 5 patterns x 1 engine x 6 sizes = 30 benchmark points.
 */
@SuppressWarnings("MethodMayBeStatic")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkRe2SearchNfa
{
    private static final String EASY0 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ$";
    private static final String EASY1 = "A[AB]B[BC]C[CD]D[DE]E[EF]F[FG]G[GH]H[HI]I[IJ]J$";
    private static final String MEDIUM = "[XYZ]ABCDEFGHIJKLMNOPQRSTUVWXYZ$";
    private static final String HARD = "[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$";
    private static final String PARENS = "([ -~])*(A)(B)(C)(D)(E)(F)(G)(H)(I)(J)(K)(L)(M)(N)(O)(P)(Q)(R)(S)(T)(U)(V)(W)(X)(Y)(Z)$";

    @State(Scope.Thread)
    public static class NfaSearchState
    {
        @Param({"8", "64", "512", "4096", "32768", "262144"})
        int textSize;

        Slice text;

        Prog progEasy0;
        Prog progEasy1;
        Prog progMedium;
        Prog progHard;
        Prog progParens;

        @Setup(Level.Trial)
        public void setup()
        {
            text = Slices.wrappedBuffer(randomText(textSize));

            progEasy0 = compileProg(EASY0);
            progEasy1 = compileProg(EASY1);
            progMedium = compileProg(MEDIUM);
            progHard = compileProg(HARD);
            progParens = compileProg(PARENS);
        }
    }

    @Benchmark
    public boolean searchEasy0Nfa(NfaSearchState state)
    {
        return Nfa.search(state.progEasy0, state.text, false, Prog.MatchKind.FIRST_MATCH, null);
    }

    @Benchmark
    public boolean searchEasy1Nfa(NfaSearchState state)
    {
        return Nfa.search(state.progEasy1, state.text, false, Prog.MatchKind.FIRST_MATCH, null);
    }

    @Benchmark
    public boolean searchMediumNfa(NfaSearchState state)
    {
        return Nfa.search(state.progMedium, state.text, false, Prog.MatchKind.FIRST_MATCH, null);
    }

    @Benchmark
    public boolean searchHardNfa(NfaSearchState state)
    {
        return Nfa.search(state.progHard, state.text, false, Prog.MatchKind.FIRST_MATCH, null);
    }

    @Benchmark
    public boolean searchParensNfa(NfaSearchState state)
    {
        return Nfa.search(state.progParens, state.text, false, Prog.MatchKind.FIRST_MATCH, null);
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = Re2BenchmarkRunner.buildOptions(BenchmarkRe2SearchNfa.class, args);
        new Runner(options).run();
    }
}
