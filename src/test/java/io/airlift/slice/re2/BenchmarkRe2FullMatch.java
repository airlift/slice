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

import static io.airlift.slice.re2.Re2BenchmarkRunner.compileRe2;
import static io.airlift.slice.re2.Re2BenchmarkRunner.compileRe2Latin1;
import static io.airlift.slice.re2.Re2BenchmarkRunner.randomText;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * Full-match scaling benchmarks for UTF-8 and Latin1 execution.
 */
@SuppressWarnings("MethodMayBeStatic")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkRe2FullMatch
{
    private static final String FULLMATCH_DOTSTAR = "(?s).*";
    private static final String FULLMATCH_DOTSTAR_DOLLAR = "(?s).*$";
    private static final String FULLMATCH_DOTSTAR_CAPTURE = "(?s)((.*)()()($))";

    @State(Scope.Thread)
    public static class FullMatchState
    {
        @Param({"8", "64", "512", "4096", "32768", "262144", "2097152"})
        int textSize;

        Slice text;

        // UTF-8 mode validates the input while matching.
        Re2 re2DotStar;
        Re2 re2DotStarDollar;
        Re2 re2DotStarCapture;

        // Latin1 dot-all accepts every byte sequence.
        Re2 re2DotStarLatin1;
        Re2 re2DotStarDollarLatin1;
        Re2 re2DotStarCaptureLatin1;

        @Setup(Level.Trial)
        public void setup()
        {
            byte[] randomText = randomText(textSize);
            byte[] suffix = "ABCDEFGHIJ".getBytes(US_ASCII);
            byte[] input = new byte[randomText.length + suffix.length];
            System.arraycopy(randomText, 0, input, 0, randomText.length);
            System.arraycopy(suffix, 0, input, randomText.length, suffix.length);
            text = Slices.wrappedBuffer(input);

            // UTF-8 mode
            re2DotStar = compileRe2(FULLMATCH_DOTSTAR);
            re2DotStarDollar = compileRe2(FULLMATCH_DOTSTAR_DOLLAR);
            re2DotStarCapture = compileRe2(FULLMATCH_DOTSTAR_CAPTURE);

            // LATIN1 mode
            re2DotStarLatin1 = compileRe2Latin1(FULLMATCH_DOTSTAR);
            re2DotStarDollarLatin1 = compileRe2Latin1(FULLMATCH_DOTSTAR_DOLLAR);
            re2DotStarCaptureLatin1 = compileRe2Latin1(FULLMATCH_DOTSTAR_CAPTURE);
        }
    }

    // UTF-8 mode

    @Benchmark
    public boolean fullMatchDotStar(FullMatchState state)
    {
        return state.re2DotStar.fullMatch(state.text);
    }

    @Benchmark
    public boolean fullMatchDotStarDollar(FullMatchState state)
    {
        return state.re2DotStarDollar.fullMatch(state.text);
    }

    @Benchmark
    public boolean fullMatchDotStarCapture(FullMatchState state)
    {
        return state.re2DotStarCapture.fullMatch(state.text);
    }

    // Latin1 mode

    @Benchmark
    public boolean fullMatchDotStarLatin1(FullMatchState state)
    {
        return state.re2DotStarLatin1.fullMatch(state.text);
    }

    @Benchmark
    public boolean fullMatchDotStarDollarLatin1(FullMatchState state)
    {
        return state.re2DotStarDollarLatin1.fullMatch(state.text);
    }

    @Benchmark
    public boolean fullMatchDotStarCaptureLatin1(FullMatchState state)
    {
        return state.re2DotStarCaptureLatin1.fullMatch(state.text);
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = Re2BenchmarkRunner.buildOptions(BenchmarkRe2FullMatch.class, args);
        new Runner(options).run();
    }
}
