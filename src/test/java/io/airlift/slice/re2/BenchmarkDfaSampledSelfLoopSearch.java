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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;

/** Measures the sampled-loop fallback on an end-constrained compact DFA with no self-loops. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDfaSampledSelfLoopSearch
{
    private static final byte[] SEQUENCE = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.US_ASCII);
    private static final String PATTERN = buildPattern();

    @Param("16777216")
    int textLength;

    private Prog program;
    private Slice input;
    private Dfa.DfaInstance dfa;

    @Setup(Level.Trial)
    public void setup()
    {
        byte[] bytes = new byte[textLength];
        for (int position = 0; position < bytes.length; position++) {
            bytes[position] = SEQUENCE[position % SEQUENCE.length];
        }
        input = Slices.wrappedBuffer(bytes);
        program = compileProg(PATTERN);
        program.setDfaMemory((Re2.Options.DEFAULT_MAX_MEMORY / 3) * 2);
        if (search() != Dfa.SEARCH_NO_MATCH) {
            throw new IllegalStateException("Benchmark input unexpectedly matched");
        }

        dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        if (dfa.estimatedPairedTransitionMemory() <= Dfa.MAX_PAIRED_TRANSITION_MEMORY ||
                dfa.pairedTransitionRowCount() != 0 ||
                dfa.selfLoopTransitionCount() != 0 ||
                dfa.canFixedDistanceByteAcceleration()) {
            throw new IllegalStateException("Benchmark did not select the state-changing compact DFA route");
        }
        if (Dfa.nativeAccessEnabled() != (dfa.absolutePointerTransitionMemory() > 0)) {
            throw new IllegalStateException("Benchmark did not select the expected one-byte DFA layout");
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

    long absolutePointerTransitionMemory()
    {
        return dfa.absolutePointerTransitionMemory();
    }

    private static String buildPattern()
    {
        StringBuilder expression = new StringBuilder("^(?:");
        for (char letter = 'a'; letter <= 'z'; letter++) {
            expression.append('[').append(letter).append(Character.toUpperCase(letter)).append(']');
        }
        return expression.append(")*(?:0|11)$").toString();
    }
}
