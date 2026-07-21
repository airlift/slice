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

import static io.airlift.slice.re2.Re2BenchmarkRunner.buildOptions;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkRe2BooleanPartialMatch
{
    public enum Route
    {
        OPTIMIZED,
        ENGINE,
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"literalSparse", "captureSparse", "delimiterDense", "emptyMatches", "unicodeSparse"})
        String workload;

        @Param({"1024", "32768"})
        int sourceLength;

        @Param
        Route route;

        private Re2 pattern;
        private Slice source;

        @Setup
        public void setup()
        {
            TestingTrinoRegexpBenchmarkInputs.Input input = TestingTrinoRegexpBenchmarkInputs.create(workload, sourceLength);
            pattern = Re2.compile(input.pattern());
            source = input.source();
        }
    }

    @Benchmark
    public boolean match(BenchmarkData data)
    {
        return switch (data.route) {
            case OPTIMIZED -> data.pattern.partialMatch(data.source);
            case ENGINE -> data.pattern.matchInto(data.source, Re2.Anchor.UNANCHORED, null);
        };
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = buildOptions(BenchmarkRe2BooleanPartialMatch.class, args);
        new Runner(options).run();
    }
}
