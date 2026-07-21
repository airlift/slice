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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;

import java.util.concurrent.TimeUnit;

import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static java.nio.charset.StandardCharsets.UTF_8;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BenchmarkNfaCaptureScaling
{
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1", "4", "16", "64"})
        int capturingGroupCount;

        Prog program;
        Slice input;
        int[] groups;

        @Setup(Level.Trial)
        public void setup()
        {
            StringBuilder pattern = new StringBuilder("(?:");
            for (int group = 0; group < capturingGroupCount; group++) {
                if (group > 0) {
                    pattern.append('|');
                }
                pattern.append("(token").append(group).append(')');
            }
            pattern.append(')');

            program = compileProg(pattern.toString());
            input = Slices.wrappedBuffer("token0".getBytes(UTF_8));
            groups = new int[2 * (capturingGroupCount + 1)];
        }
    }

    @Benchmark
    public int cachedStackSize(BenchmarkData data)
    {
        return Nfa.captureStackSize(data.program);
    }

    @Benchmark
    public int instructionScanStackSize(BenchmarkData data)
    {
        int captureCount = 0;
        int emptyWidthCount = 0;
        int noOpCount = 0;
        for (int instructionId = 0; instructionId < data.program.size(); instructionId++) {
            switch (data.program.inst(instructionId).opcode()) {
                case CAPTURE -> captureCount++;
                case EMPTY_WIDTH -> emptyWidthCount++;
                case NOP -> noOpCount++;
                default -> {}
            }
        }
        return Math.max(8, 2 * captureCount + emptyWidthCount + noOpCount + 1);
    }

    @Benchmark
    public boolean captureSearch(BenchmarkData data)
    {
        return Nfa.search(data.program, data.input, true, Prog.MatchKind.FULL_MATCH, data.groups);
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = Re2BenchmarkRunner.buildOptions(BenchmarkNfaCaptureScaling.class, args);
        new Runner(options).run();
    }
}
