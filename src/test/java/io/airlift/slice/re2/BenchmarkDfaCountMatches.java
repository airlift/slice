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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.re2.Re2BenchmarkRunner.buildOptions;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkDfaCountMatches
{
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"denseWords", "boundedContext", "captureShape"})
        String workload;

        @Param("32768")
        int sourceLength;

        private Prog program;
        private Slice source;

        @Setup
        public void setup()
        {
            String pattern;
            Slice token;
            switch (workload) {
                case "denseWords" -> {
                    pattern = "[A-Za-z]{8,13}";
                    token = utf8Slice("abcdefgh 12 ");
                }
                case "boundedContext" -> {
                    pattern = "[A-Za-z]{10}\\s+[\\s\\S]{0,100}Result[\\s\\S]{0,100}\\s+[A-Za-z]{10}";
                    token = utf8Slice("abcdefghij 0123456789 Result 0123456789 klmnopqrst -- ");
                }
                case "captureShape" -> {
                    pattern = "([a-z]+)-([0-9]+)";
                    token = utf8Slice("abc-123.........................................................");
                }
                default -> throw new IllegalArgumentException("unknown workload: " + workload);
            }
            program = compile(pattern);
            source = repeat(token, sourceLength);
        }
    }

    @Benchmark
    public long batched(BenchmarkData data)
    {
        return Dfa.countMatches(data.program, data.source, Prog.MatchKind.FIRST_MATCH);
    }

    @Benchmark
    public long repeated(BenchmarkData data)
    {
        long count = 0;
        int start = 0;
        while (start <= data.source.length()) {
            long matchEnd = Dfa.search(
                    data.program,
                    data.source,
                    start,
                    data.source.length(),
                    false,
                    Prog.MatchKind.FIRST_MATCH,
                    true);
            if (matchEnd == Dfa.SEARCH_FAILED) {
                return Dfa.COUNT_UNSUPPORTED;
            }
            if (matchEnd == Dfa.SEARCH_NO_MATCH) {
                return count;
            }
            if (matchEnd <= 0) {
                return Dfa.COUNT_UNSUPPORTED;
            }
            start += (int) matchEnd;
            count++;
        }
        return count;
    }

    private static Prog compile(String pattern)
    {
        ParseResult parsed = RegexpParser.parse(utf8Slice(pattern), Regexp.LIKE_PERL);
        return Compiler.compile(parsed.regexp(), false, 0);
    }

    private static Slice repeat(Slice token, int length)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(length);
        while (output.size() + token.length() <= length) {
            output.writeBytes(token);
        }
        while (output.size() < length) {
            output.writeByte('.');
        }
        return output.slice();
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = buildOptions(BenchmarkDfaCountMatches.class, args);
        new Runner(options).run();
    }
}
