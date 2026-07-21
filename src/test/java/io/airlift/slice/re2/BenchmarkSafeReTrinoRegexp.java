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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.airlift.slice.re2.Re2BenchmarkRunner.buildOptions;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkSafeReTrinoRegexp
{
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"literalSparse", "captureSparse", "delimiterDense", "emptyMatches", "unicodeSparse"})
        String workload;

        @Param({"1024", "32768"})
        int sourceLength;

        private TestingSafeReTrinoRegexp regexp;
        private Slice source;
        private Slice replacement;
        private Function<List<Slice>, Slice> lambdaReplacement;

        @Setup
        public void setup()
        {
            TestingTrinoRegexpBenchmarkInputs.Input input = TestingTrinoRegexpBenchmarkInputs.create(workload, sourceLength);
            regexp = TestingSafeReTrinoRegexp.compile(input.pattern());
            source = input.source();
            replacement = Slices.utf8Slice("_");
            lambdaReplacement = groups -> groups.isEmpty() ? replacement : groups.getFirst();
        }
    }

    @Benchmark
    public boolean containsSafeRe(BenchmarkData data)
    {
        return data.regexp.contains(data.source);
    }

    @Benchmark
    public long countSafeRe(BenchmarkData data)
    {
        return data.regexp.count(data.source);
    }

    @Benchmark
    public long positionThirdSafeRe(BenchmarkData data)
    {
        return data.regexp.position(data.source, 3);
    }

    @Benchmark
    public Slice extractSafeRe(BenchmarkData data)
    {
        return data.regexp.extract(data.source);
    }

    @Benchmark
    public List<Slice> extractAllSafeRe(BenchmarkData data)
    {
        return data.regexp.extractAll(data.source);
    }

    @Benchmark
    public List<Slice> splitSafeRe(BenchmarkData data)
    {
        return data.regexp.split(data.source);
    }

    @Benchmark
    public Slice replaceSafeRe(BenchmarkData data)
    {
        return data.regexp.replace(data.source, data.replacement);
    }

    @Benchmark
    public Slice replaceLambdaSafeRe(BenchmarkData data)
    {
        return data.regexp.replace(data.source, data.lambdaReplacement);
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = buildOptions(BenchmarkSafeReTrinoRegexp.class, args);
        new Runner(options).run();
    }
}
