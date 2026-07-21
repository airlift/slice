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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("MethodMayBeStatic")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkRe2CompileFocused
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
                "([a-z]+)-([0-9]+)",
                "[a-z]{8}-[0-9]{4}",
        })
        String pattern;

        byte[] patternBytes;
        Slice matchingText;
        Regexp preParsed;

        @Setup(Level.Trial)
        public void setup()
        {
            patternBytes = pattern.getBytes(StandardCharsets.UTF_8);
            ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(patternBytes), Regexp.LIKE_PERL);
            preParsed = parsed.regexp();
            matchingText = switch (pattern) {
                case "(.*)-(\\d+)-of-(\\d+)" -> Slices.utf8Slice("x-1-of-2");
                case "[0-9]+.(.*)" -> Slices.utf8Slice("1.x");
                case "([a-z]+)-([0-9]+)" -> Slices.utf8Slice("abc-123");
                case "[a-z]{8}-[0-9]{4}" -> Slices.utf8Slice("abcdefgh-1234");
                default -> Slices.utf8Slice("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
            };
        }
    }

    @State(Scope.Thread)
    public static class TrinoCompilePatternState
    {
        @Param({
                ";",
                "[,;]",
                "([a-z]+)-([0-9]+)",
                "x$",
                "\\bX",
        })
        String pattern;

        Slice patternSlice;

        @Setup(Level.Trial)
        public void setup()
        {
            patternSlice = Slices.utf8Slice(pattern);
        }
    }

    @Benchmark
    public Object parseOnly(CompilePatternState state)
    {
        return RegexpParser.parse(Slices.wrappedBuffer(state.patternBytes), Regexp.LIKE_PERL);
    }

    @Benchmark
    public Object simplifyOnly(CompilePatternState state)
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(state.patternBytes), Regexp.LIKE_PERL);
        return Simplifier.simplify(parsed.regexp());
    }

    @Benchmark
    public Object compileToProgFromParsed(CompilePatternState state)
    {
        return Compiler.compile(state.preParsed, false, 0);
    }

    @Benchmark
    public Object compileToProgAndConstructFirstMatchDfa(CompilePatternState state)
    {
        Prog program = Compiler.compile(state.preParsed, false, 0);
        return program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
    }

    @Benchmark
    public Object re2CompileTotal(CompilePatternState state)
    {
        return Re2.compile(Slices.wrappedBuffer(state.patternBytes));
    }

    @Benchmark
    public Object trinoRegexpCompileTotal(TrinoCompilePatternState state)
    {
        return TrinoRegexp.compile(state.patternSlice);
    }

    @Benchmark
    public boolean re2CompileAndFullMatch(CompilePatternState state)
    {
        return Re2.compile(Slices.wrappedBuffer(state.patternBytes)).fullMatch(state.matchingText);
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = Re2BenchmarkRunner.buildOptions(BenchmarkRe2CompileFocused.class, args);
        new Runner(options).run();
    }
}
