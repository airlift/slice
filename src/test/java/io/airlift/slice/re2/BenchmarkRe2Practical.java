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

/**
 * Practical pattern benchmarks + compile cost benchmarks.
 * 5 practical + 5 compile phases + 6 per-pattern compile = 16 benchmark points.
 */
@SuppressWarnings("MethodMayBeStatic")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkRe2Practical
{
    private static final String EASY0 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ$";
    private static final String HARD = "[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$";
    private static final String PARENS = "([ -~])*(A)(B)(C)(D)(E)(F)(G)(H)(I)(J)(K)(L)(M)(N)(O)(P)(Q)(R)(S)(T)(U)(V)(W)(X)(Y)(Z)$";

    private static final String HTTP_PATTERN = "(?-s)^(?:GET|POST) +([^ ]+) HTTP";
    private static final String HTTP_INPUT = "GET /asdfhjasdhfasdlfhasdflkjasdfkljasdhflaskdjhfalksdjfhasdlkfhasdlkjfhasdljkfhadsjklf HTTP/1.1";
    private static final String SMALL_HTTP_INPUT = "GET /abc HTTP/1.1";

    // C++ compile benchmark default pattern: (.*)-(\\d+)-of-(\\d+)
    private static final String COMPILE_PATTERN = "(.*)-(\\d+)-of-(\\d+)";

    // Practical state

    @State(Scope.Thread)
    public static class PracticalState
    {
        Slice httpInput;
        Slice smallHttpInput;
        Slice simpleInput;
        Slice emptyInput;
        Slice phoneSearchInput;

        Re2 re2Http;
        Re2 re2Simple;
        Re2 re2Empty;
        Re2 re2Phone;
        int[] httpCaptureGroups;

        @Setup(Level.Trial)
        public void setup()
        {
            httpInput = Slices.wrappedBuffer(HTTP_INPUT.getBytes(StandardCharsets.UTF_8));
            smallHttpInput = Slices.wrappedBuffer(SMALL_HTTP_INPUT.getBytes(StandardCharsets.UTF_8));
            simpleInput = Slices.wrappedBuffer("abcdefg".getBytes(StandardCharsets.UTF_8));
            emptyInput = Slices.wrappedBuffer(new byte[0]);
            phoneSearchInput = Slices.wrappedBuffer("the number is 650-253-0001 ok".getBytes(StandardCharsets.UTF_8));

            re2Http = Re2BenchmarkRunner.compileRe2(HTTP_PATTERN);
            re2Simple = Re2BenchmarkRunner.compileRe2("abcdefg");
            re2Empty = Re2BenchmarkRunner.compileRe2("");
            re2Phone = Re2BenchmarkRunner.compileRe2("[0-9]{3}-[0-9]{3}-[0-9]{4}");
            httpCaptureGroups = new int[4];
        }
    }

    @Benchmark
    public boolean httpPartialMatch(PracticalState state)
    {
        return state.re2Http.matchInto(state.httpInput, Re2.Anchor.UNANCHORED, state.httpCaptureGroups);
    }

    @Benchmark
    public boolean smallHttpPartialMatch(PracticalState state)
    {
        return state.re2Http.matchInto(state.smallHttpInput, Re2.Anchor.UNANCHORED, state.httpCaptureGroups);
    }

    @Benchmark
    public boolean simplePartialMatch(PracticalState state)
    {
        return state.re2Simple.partialMatch(state.simpleInput);
    }

    @Benchmark
    public boolean emptyPartialMatch(PracticalState state)
    {
        return state.re2Empty.partialMatch(state.emptyInput);
    }

    @Benchmark
    public boolean phoneDigitsSearch(PracticalState state)
    {
        return state.re2Phone.partialMatch(state.phoneSearchInput);
    }

    // Compile phase state (matches C++ BM_Regexp_Parse, BM_Regexp_Simplify, etc.)

    @State(Scope.Thread)
    public static class CompilePhaseState
    {
        byte[] compilePatternBytes;
        Regexp preParsedRegexp;

        @Setup(Level.Trial)
        public void setup()
        {
            compilePatternBytes = COMPILE_PATTERN.getBytes(StandardCharsets.UTF_8);
            Slice pat = Slices.wrappedBuffer(compilePatternBytes);
            ParseResult parsed = RegexpParser.parse(pat, Regexp.LIKE_PERL);
            preParsedRegexp = parsed.regexp();
        }
    }

    @Benchmark
    public Object compilePhaseParse(CompilePhaseState state)
    {
        Slice pat = Slices.wrappedBuffer(state.compilePatternBytes);
        return RegexpParser.parse(pat, Regexp.LIKE_PERL);
    }

    @Benchmark
    public Object compilePhaseSimplify(CompilePhaseState state)
    {
        Slice pat = Slices.wrappedBuffer(state.compilePatternBytes);
        ParseResult parsed = RegexpParser.parse(pat, Regexp.LIKE_PERL);
        return Simplifier.simplify(parsed.regexp());
    }

    @Benchmark
    public Object compilePhaseCompileToProg(CompilePhaseState state)
    {
        return Compiler.compile(state.preParsedRegexp, false, 0);
    }

    @Benchmark
    public Object compilePhaseSimplifyCompile(CompilePhaseState state)
    {
        Slice pat = Slices.wrappedBuffer(state.compilePatternBytes);
        ParseResult parsed = RegexpParser.parse(pat, Regexp.LIKE_PERL);
        Regexp simplified = Simplifier.simplify(parsed.regexp());
        return Compiler.compile(simplified, false, 0);
    }

    @Benchmark
    public Object compilePhaseRe2Compile(CompilePhaseState state)
    {
        return Re2.compile(Slices.wrappedBuffer(state.compilePatternBytes));
    }

    // Per-pattern compile cost state (Java extras, no C++ baseline)

    @State(Scope.Thread)
    public static class CompileState
    {
        byte[] easy0Bytes;
        byte[] hardBytes;
        byte[] parensBytes;

        @Setup(Level.Trial)
        public void setup()
        {
            easy0Bytes = EASY0.getBytes(StandardCharsets.UTF_8);
            hardBytes = HARD.getBytes(StandardCharsets.UTF_8);
            parensBytes = PARENS.getBytes(StandardCharsets.UTF_8);
        }
    }

    @Benchmark
    public Object compileEasy0(CompileState state)
    {
        Slice pat = Slices.wrappedBuffer(state.easy0Bytes);
        ParseResult parsed = RegexpParser.parse(pat, Regexp.LIKE_PERL);
        return Compiler.compile(parsed.regexp(), false, 0);
    }

    @Benchmark
    public Object compileHard(CompileState state)
    {
        Slice pat = Slices.wrappedBuffer(state.hardBytes);
        ParseResult parsed = RegexpParser.parse(pat, Regexp.LIKE_PERL);
        return Compiler.compile(parsed.regexp(), false, 0);
    }

    @Benchmark
    public Object compileParens(CompileState state)
    {
        Slice pat = Slices.wrappedBuffer(state.parensBytes);
        ParseResult parsed = RegexpParser.parse(pat, Regexp.LIKE_PERL);
        return Compiler.compile(parsed.regexp(), false, 0);
    }

    @Benchmark
    public Object re2ConstructEasy0(CompileState state)
    {
        return Re2.compile(Slices.wrappedBuffer(state.easy0Bytes));
    }

    @Benchmark
    public Object re2ConstructHard(CompileState state)
    {
        return Re2.compile(Slices.wrappedBuffer(state.hardBytes));
    }

    @Benchmark
    public Object re2ConstructParens(CompileState state)
    {
        return Re2.compile(Slices.wrappedBuffer(state.parensBytes));
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = Re2BenchmarkRunner.buildOptions(BenchmarkRe2Practical.class, args);
        new Runner(options).run();
    }
}
