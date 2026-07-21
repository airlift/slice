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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;

/** Measures partial paired rows followed by a compact-DFA suffix handoff. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDfaPartialPairedTransitions
{
    private static final String PATTERN = "[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$";
    private static final byte[] MATCHING_SUFFIX = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes(StandardCharsets.US_ASCII);

    @Param({"MATCH", "NO_MATCH"})
    String workload;

    @Param("1048576")
    int textLength;

    @Param("true")
    boolean expectPartialTable;

    private Prog program;
    private Slice input;
    private long expectedResult;

    @Setup(Level.Trial)
    public void setup()
    {
        if (textLength < MATCHING_SUFFIX.length) {
            throw new IllegalArgumentException("textLength is smaller than the required suffix");
        }

        byte[] text = new byte[textLength];
        Arrays.fill(text, (byte) 'x');
        System.arraycopy(MATCHING_SUFFIX, 0, text, text.length - MATCHING_SUFFIX.length, MATCHING_SUFFIX.length);
        expectedResult = switch (workload) {
            case "MATCH" -> text.length;
            case "NO_MATCH" -> {
                text[text.length - 1] = '_';
                yield Dfa.SEARCH_NO_MATCH;
            }
            default -> throw new IllegalArgumentException("Unknown workload: " + workload);
        };

        input = Slices.wrappedBuffer(text);
        program = compileProg(PATTERN);
        if (search() != expectedResult) {
            throw new IllegalStateException("Warm search produced an unexpected result");
        }

        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        if (dfa.estimatedPairedTransitionMemory() <= Dfa.MAX_PAIRED_TRANSITION_MEMORY) {
            throw new IllegalStateException("Benchmark DFA does not exceed the complete-table limit");
        }
        if (expectPartialTable) {
            if (dfa.pairedTransitionRowCount() == 0 ||
                    dfa.pairedTransitionRowCount() >= dfa.stateCount ||
                    dfa.pairedTransitionCount() == 0) {
                throw new IllegalStateException("Benchmark did not construct a usable partial paired table");
            }
        }
        else if (dfa.pairedTransitionRowCount() != 0) {
            throw new IllegalStateException("Control unexpectedly constructed a paired table");
        }
    }

    @Benchmark
    public long search()
    {
        return Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true);
    }

    long expectedResult()
    {
        return expectedResult;
    }
}
