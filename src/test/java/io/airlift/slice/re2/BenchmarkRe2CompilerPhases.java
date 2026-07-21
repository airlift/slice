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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("MethodMayBeStatic")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkRe2CompilerPhases
{
    private static final String PARENS = "([ -~])*(A)(B)(C)(D)(E)(F)(G)(H)(I)(J)(K)(L)(M)(N)(O)(P)(Q)(R)(S)(T)(U)(V)(W)(X)(Y)(Z)$";

    @State(Scope.Thread)
    public static class CompilePatternState
    {
        @Param({
                "(.*)-(\\d+)-of-(\\d+)",
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ$",
                "[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$",
                PARENS,
                "[0-9]+.(.*)",
                "(?i)ABCDEFGHIJKLMNOPQRSTUVWXYZ$",
        })
        String pattern;

        Regexp preParsed;

        @Setup(Level.Trial)
        public void setup()
        {
            byte[] patternBytes = pattern.getBytes(StandardCharsets.UTF_8);
            ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(patternBytes), Regexp.LIKE_PERL);
            preParsed = parsed.regexp();
        }
    }

    @Benchmark
    public Object compileToRawProg(CompilePatternState state)
    {
        return Compiler.compileForBenchmark(state.preParsed, false, 0, Compiler.CompileStage.RAW);
    }

    @Benchmark
    public Object compileToOptimizedProg(CompilePatternState state)
    {
        return Compiler.compileForBenchmark(state.preParsed, false, 0, Compiler.CompileStage.OPTIMIZED);
    }

    @Benchmark
    public Object compileToFlattenedProg(CompilePatternState state)
    {
        return Compiler.compileForBenchmark(state.preParsed, false, 0, Compiler.CompileStage.FLATTENED);
    }

    @Benchmark
    public Object compileToByteMapProg(CompilePatternState state)
    {
        return Compiler.compileForBenchmark(state.preParsed, false, 0, Compiler.CompileStage.BYTEMAP);
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = Re2BenchmarkRunner.buildOptions(BenchmarkRe2CompilerPhases.class, args);
        new Runner(options).run();
    }
}
