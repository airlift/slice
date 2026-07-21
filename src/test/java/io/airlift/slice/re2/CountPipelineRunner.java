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

import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;
import static java.lang.System.nanoTime;

final class CountPipelineRunner
{
    private CountPipelineRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        if (args.length > 1) {
            throw new IllegalArgumentException("Usage: CountPipelineRunner [maximum-memory-bytes]");
        }

        long maximumMemory = args.length == 0 ? Re2.Options.DEFAULT_MAX_MEMORY : Long.parseLong(args[0]);
        run(System.in, System.out, maximumMemory);
    }

    static void run(InputStream input, OutputStream outputStream, long maximumMemory)
            throws Exception
    {
        RebarRunner.Benchmark benchmark = RebarRunner.Benchmark.read(input.readAllBytes());
        List<Sample> samples = run(benchmark, maximumMemory);

        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
        output.write("maximum_memory_bytes,duration_ns,count,cache_resets,state_count,cache_entries,state_budget_bytes,available_state_bytes,byte_scan_fallbacks,absolute_pointer_bytes,absolute_pointer_transitions,paired_bytes,paired_row_selections");
        output.newLine();
        for (Sample sample : samples) {
            output.write(sample.toCsv());
            output.newLine();
        }
        output.flush();
    }

    static List<Sample> run(RebarRunner.Benchmark benchmark, long maximumMemory)
    {
        if (!benchmark.model().equals("count")) {
            throw new IllegalArgumentException("count pipeline requires the count model: " + benchmark.model());
        }
        if (maximumMemory <= 0) {
            throw new IllegalArgumentException("maximumMemory must be positive");
        }

        Re2 pattern = Re2.compile(benchmark.pattern(), benchmark.options().setMaxMemory(maximumMemory));
        Prog program = pattern.forwardProgramForDiagnostics();
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        if (dfa == null || !dfa.ok()) {
            throw new IllegalStateException("first-match DFA is unavailable");
        }

        runWarmup(benchmark, pattern);

        int previousResetCount = dfa.resetCount();
        int previousByteScanFallbackCount = dfa.byteScanFallbackCount();
        int previousPairedRowSelectionCount = dfa.pairedRowSelectionCount();
        long runStart = nanoTime();
        List<Sample> samples = new ArrayList<>();
        for (long iteration = 0; iteration < max(1, benchmark.maximumIterations()); iteration++) {
            long start = nanoTime();
            long count = pattern.countMatches(benchmark.haystack());
            long duration = nanoTime() - start;
            if (count == Dfa.COUNT_UNSUPPORTED) {
                throw new IllegalStateException("count pipeline fell back from the DFA");
            }

            int resetCount = dfa.resetCount();
            int byteScanFallbackCount = dfa.byteScanFallbackCount();
            int pairedRowSelectionCount = dfa.pairedRowSelectionCount();
            samples.add(new Sample(
                    maximumMemory,
                    duration,
                    count,
                    resetCount - previousResetCount,
                    dfa.stateCount,
                    dfa.cacheEntryCount(),
                    dfa.stateBudget(),
                    dfa.availableStateMemory(),
                    byteScanFallbackCount - previousByteScanFallbackCount,
                    dfa.absolutePointerTransitionMemory(),
                    dfa.absolutePointerTransitionCount(),
                    dfa.pairedTransitionMemory(),
                    pairedRowSelectionCount - previousPairedRowSelectionCount));
            previousResetCount = resetCount;
            previousByteScanFallbackCount = byteScanFallbackCount;
            previousPairedRowSelectionCount = pairedRowSelectionCount;

            if (benchmark.maximumTimeNanos() > 0 && nanoTime() - runStart >= benchmark.maximumTimeNanos()) {
                break;
            }
        }
        return List.copyOf(samples);
    }

    private static void runWarmup(RebarRunner.Benchmark benchmark, Re2 pattern)
    {
        long warmupStart = nanoTime();
        for (long iteration = 0; iteration < benchmark.maximumWarmupIterations(); iteration++) {
            pattern.countMatches(benchmark.haystack());
            if (benchmark.maximumWarmupTimeNanos() > 0 && nanoTime() - warmupStart >= benchmark.maximumWarmupTimeNanos()) {
                break;
            }
        }
    }

    record Sample(
            long maximumMemoryBytes,
            long durationNanos,
            long count,
            int cacheResets,
            int stateCount,
            int cacheEntries,
            long stateBudgetBytes,
            long availableStateBytes,
            int byteScanFallbacks,
            long absolutePointerBytes,
            int absolutePointerTransitions,
            long pairedBytes,
            int pairedRowSelections)
    {
        String toCsv()
        {
            return String.join(",",
                    Long.toString(maximumMemoryBytes),
                    Long.toString(durationNanos),
                    Long.toString(count),
                    Integer.toString(cacheResets),
                    Integer.toString(stateCount),
                    Integer.toString(cacheEntries),
                    Long.toString(stateBudgetBytes),
                    Long.toString(availableStateBytes),
                    Integer.toString(byteScanFallbacks),
                    Long.toString(absolutePointerBytes),
                    Integer.toString(absolutePointerTransitions),
                    Long.toString(pairedBytes),
                    Integer.toString(pairedRowSelections));
        }
    }
}
