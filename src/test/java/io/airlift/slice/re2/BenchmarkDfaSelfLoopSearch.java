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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static io.airlift.slice.re2.Re2BenchmarkRunner.randomText;

/** Measures the production compact-DFA route for a large graph with a hot self-loop. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDfaSelfLoopSearch
{
    private static final String SUFFIX = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".repeat(20);
    private static final String PATTERN = "[ -~]*" + SUFFIX + "$";

    @Param("16777216")
    int textLength;

    private Prog program;
    private Slice input;
    private Dfa.DfaInstance dfa;

    @Setup(Level.Trial)
    public void setup()
    {
        program = compileProg(PATTERN);

        byte[] matchingBytes = randomText(1024 + SUFFIX.length());
        for (int index = 0; index < SUFFIX.length(); index++) {
            matchingBytes[matchingBytes.length - SUFFIX.length() + index] = (byte) SUFFIX.charAt(index);
        }
        if (Dfa.search(program, Slices.wrappedBuffer(matchingBytes), false, Prog.MatchKind.FIRST_MATCH, true) != matchingBytes.length) {
            throw new IllegalStateException("DFA warmup input unexpectedly failed");
        }

        input = Slices.wrappedBuffer(randomText(textLength));
        if (search() != Dfa.SEARCH_NO_MATCH) {
            throw new IllegalStateException("Benchmark input unexpectedly matched");
        }

        dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        if (dfa.estimatedPairedTransitionMemory() <= Dfa.MAX_PAIRED_TRANSITION_MEMORY ||
                dfa.pairedTransitionRowCount() != 0 ||
                dfa.selfLoopTransitionCount() == 0 ||
                dfa.canFixedDistanceByteAcceleration()) {
            throw new IllegalStateException("Benchmark did not select the unpaired self-loop DFA route");
        }
    }

    @Benchmark
    public long search()
    {
        return Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true);
    }

    int stateCount()
    {
        return dfa.stateCount;
    }

    long estimatedPairedTransitionMemory()
    {
        return dfa.estimatedPairedTransitionMemory();
    }

    int pairedTransitionRowCount()
    {
        return dfa.pairedTransitionRowCount();
    }

    int selfLoopTransitionCount()
    {
        return dfa.selfLoopTransitionCount();
    }

    boolean fixedDistanceByteAcceleration()
    {
        return dfa.canFixedDistanceByteAcceleration();
    }
}
