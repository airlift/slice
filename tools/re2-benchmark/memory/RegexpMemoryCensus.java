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

import io.airlift.jcodings.specific.NonStrictUTF8Encoding;
import io.airlift.joni.Matcher;
import io.airlift.joni.Option;
import io.airlift.joni.Regex;
import io.airlift.joni.Syntax;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.GraphLayout;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static io.airlift.slice.re2.Dfa.DfaInstance.Kind.FIRST_MATCH;
import static io.airlift.slice.re2.Dfa.DfaInstance.Kind.LONGEST_MATCH;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RegexpMemoryCensus
{
    private static final int[] ROOT_COUNTS = {1, 8, 64};
    private static final Slice REPLACEMENT = Slices.utf8Slice("_");

    private RegexpMemoryCensus() {}

    public static void main(String[] arguments)
            throws Exception
    {
        if (arguments.length != 1 && arguments.length != 3 && arguments.length != 4) {
            throw new IllegalArgumentException("usage: RegexpMemoryCensus <output-directory> [<pattern-file> <haystack-file> [<maximum-memory-mib>]]");
        }

        Path outputDirectory = Path.of(arguments[0]);
        Files.createDirectories(outputDirectory);
        try (PrintWriter output = new PrintWriter(Files.newBufferedWriter(outputDirectory.resolve("memory-census.csv")))) {
            output.println("engine,workload,source_length,lifecycle,operation,root_count,total_heap_bytes,average_heap_bytes,input_heap_bytes,owned_matcher_heap_bytes,off_heap_bytes,accounted_dfa_bytes,dfa_resets");
            for (String workload : TestingTrinoRegexpBenchmarkInputs.workloads()) {
                measureCompiled(output, workload);
                for (int sourceLength : TestingTrinoRegexpBenchmarkInputs.sourceLengths()) {
                    measureWarmOperations(output, outputDirectory, workload, sourceLength);
                    measureActiveMatcher(output, outputDirectory, workload, sourceLength);
                }
            }
            if (arguments.length == 3) {
                measureBoundedContext(output, outputDirectory, Path.of(arguments[1]), Path.of(arguments[2]), Re2.Options.DEFAULT_MAX_MEMORY);
            }
            else if (arguments.length == 4) {
                long maximumMemory = Math.multiplyExact(Long.parseLong(arguments[3]), 1024L * 1024L);
                measureBoundedContext(output, outputDirectory, Path.of(arguments[1]), Path.of(arguments[2]), maximumMemory);
            }
        }
    }

    private static void measureBoundedContext(PrintWriter output, Path outputDirectory, Path patternFile, Path haystackFile, long maximumMemory)
            throws IOException
    {
        Slice pattern = Slices.utf8Slice(Files.readString(patternFile, UTF_8).stripTrailing());
        Slice source = Slices.wrappedBuffer(Files.readAllBytes(haystackFile));
        long inputHeapBytes = GraphLayout.parseInstance(source).totalSize();

        Re2 re2 = Re2.compile(pattern, Re2.Options.latin1().setMaxMemory(maximumMemory));
        long re2MatchCount = re2.countMatches(source);

        JoniPattern joni = compileJoni(pattern);
        Matcher joniMatcher = joni.regex.matcher(source.byteArray(), source.byteArrayOffset(), source.byteArrayOffset() + source.length());
        long joniMatchCount = findAll(joniMatcher, source);
        if (re2MatchCount != joniMatchCount) {
            throw new IllegalStateException("bounded-context match count differs: RE2=" + re2MatchCount + ", Joni=" + joniMatchCount);
        }
        writeBoundedContextTiming(outputDirectory, re2, joni, source, re2MatchCount);

        GraphLayout re2PatternGraph = GraphLayout.parseInstance(re2);
        writeGraph(output, "RE2", "boundedContextLatin1", source.length(), "warm-pattern", "count", 1, re2PatternGraph, 0, 0, re2Memory(re2));
        GraphLayout joniPatternGraph = GraphLayout.parseInstance(joni);
        writeGraph(output, "Joni", "boundedContextLatin1", source.length(), "warm-pattern", "count", 1, joniPatternGraph, 0, 0, MemorySummary.EMPTY);

        Re2Matcher re2Matcher = re2.matcher(source);
        long re2MatcherCount = 0;
        while (re2Matcher.find()) {
            re2MatcherCount++;
        }
        if (re2MatcherCount != re2MatchCount) {
            throw new IllegalStateException("bounded-context matcher count differs: count=" + re2MatchCount + ", matcher=" + re2MatcherCount);
        }
        GraphLayout activeRe2PatternGraph = GraphLayout.parseInstance(re2);
        GraphLayout re2MatcherGraph = GraphLayout.parseInstance(re2, re2Matcher, source);
        long re2OwnedMatcherBytes = re2MatcherGraph.totalSize() - activeRe2PatternGraph.totalSize() - inputHeapBytes;
        writeGraph(output, "RE2", "boundedContextLatin1", source.length(), "active-matcher", "find-all", 1, re2MatcherGraph, inputHeapBytes, re2OwnedMatcherBytes, re2Memory(re2));

        GraphLayout joniMatcherGraph = GraphLayout.parseInstance(joni, joniMatcher, source);
        long joniOwnedMatcherBytes = joniMatcherGraph.totalSize() - joniPatternGraph.totalSize() - inputHeapBytes;
        writeGraph(output, "Joni", "boundedContextLatin1", source.length(), "active-matcher", "find-all", 1, joniMatcherGraph, inputHeapBytes, joniOwnedMatcherBytes, MemorySummary.EMPTY);

        writeFootprint(outputDirectory, "re2", "boundedContextLatin1", source.length(), "warm-pattern", re2PatternGraph);
        writeFootprint(outputDirectory, "joni", "boundedContextLatin1", source.length(), "warm-pattern", joniPatternGraph);
        writeFootprint(outputDirectory, "re2", "boundedContextLatin1", source.length(), "active-matcher", re2MatcherGraph);
        writeFootprint(outputDirectory, "joni", "boundedContextLatin1", source.length(), "active-matcher", joniMatcherGraph);
    }

    private static void writeBoundedContextTiming(Path outputDirectory, Re2 re2, JoniPattern joni, Slice source, long expectedMatchCount)
            throws IOException
    {
        for (int iteration = 0; iteration < 5; iteration++) {
            requireMatchCount("RE2", expectedMatchCount, re2.countMatches(source));
            requireMatchCount("Joni", expectedMatchCount, countJoni(joni, source));
        }

        try (PrintWriter output = new PrintWriter(Files.newBufferedWriter(outputDirectory.resolve("bounded-context-timing.csv")))) {
            output.println("engine,iteration,duration_ns,count");
            for (int iteration = 0; iteration < 7; iteration++) {
                writeTiming(output, "RE2-before", iteration, expectedMatchCount, () -> re2.countMatches(source));
                writeTiming(output, "Joni", iteration, expectedMatchCount, () -> countJoni(joni, source));
                writeTiming(output, "RE2-after", iteration, expectedMatchCount, () -> re2.countMatches(source));
            }
        }
    }

    private static void writeTiming(PrintWriter output, String engine, int iteration, long expectedMatchCount, Counter counter)
    {
        long start = System.nanoTime();
        long matchCount = counter.count();
        long duration = System.nanoTime() - start;
        requireMatchCount(engine, expectedMatchCount, matchCount);
        output.printf(Locale.US, "%s,%d,%d,%d%n", engine, iteration, duration, matchCount);
    }

    private static long countJoni(JoniPattern pattern, Slice source)
    {
        Matcher matcher = pattern.regex.matcher(source.byteArray(), source.byteArrayOffset(), source.byteArrayOffset() + source.length());
        return findAll(matcher, source);
    }

    private static void requireMatchCount(String engine, long expected, long actual)
    {
        if (actual != expected) {
            throw new IllegalStateException(engine + " bounded-context match count differs: expected=" + expected + ", actual=" + actual);
        }
    }

    private static void measureCompiled(PrintWriter output, String workload)
    {
        Slice pattern = TestingTrinoRegexpBenchmarkInputs.create(workload, 1_024).pattern();
        for (int rootCount : ROOT_COUNTS) {
            List<TrinoRegexp> re2Patterns = new ArrayList<>(rootCount);
            List<JoniPattern> joniPatterns = new ArrayList<>(rootCount);
            for (int index = 0; index < rootCount; index++) {
                re2Patterns.add(TrinoRegexp.compile(pattern));
                joniPatterns.add(compileJoni(pattern));
            }
            writeGraph(output, "RE2", workload, 0, "compiled", "none", re2Patterns.toArray(), 0, 0, re2Memory(re2Patterns));
            writeGraph(output, "Joni", workload, 0, "compiled", "none", joniPatterns.toArray(), 0, 0, MemorySummary.EMPTY);
        }
    }

    private static void measureWarmOperations(PrintWriter output, Path outputDirectory, String workload, int sourceLength)
            throws IOException
    {
        TestingTrinoRegexpBenchmarkInputs.Input input = TestingTrinoRegexpBenchmarkInputs.create(workload, sourceLength);
        for (Operation operation : Operation.values()) {
            TrinoRegexp re2 = TrinoRegexp.compile(input.pattern());
            operation.runRe2(re2, input.source());
            MemorySummary memory = re2Memory(List.of(re2));
            GraphLayout re2Graph = GraphLayout.parseInstance(re2);
            writeGraph(output, "RE2", workload, sourceLength, "warm-pattern", operation.label, 1, re2Graph, 0, 0, memory);

            JoniPattern joni = compileJoni(input.pattern());
            operation.runJoni(joni, input.source());
            GraphLayout joniGraph = GraphLayout.parseInstance(joni);
            writeGraph(output, "Joni", workload, sourceLength, "warm-pattern", operation.label, 1, joniGraph, 0, 0, MemorySummary.EMPTY);

            if (shouldWriteFootprint(workload, sourceLength, operation)) {
                writeFootprint(outputDirectory, "re2", workload, sourceLength, operation.label, re2Graph);
                writeFootprint(outputDirectory, "joni", workload, sourceLength, operation.label, joniGraph);
            }
        }
    }

    private static void measureActiveMatcher(PrintWriter output, Path outputDirectory, String workload, int sourceLength)
            throws IOException
    {
        TestingTrinoRegexpBenchmarkInputs.Input input = TestingTrinoRegexpBenchmarkInputs.create(workload, sourceLength);
        long inputHeapBytes = GraphLayout.parseInstance(input.source()).totalSize();

        TrinoRegexp re2 = TrinoRegexp.compile(input.pattern());
        Re2Matcher re2Matcher = re2.pattern().matcher(input.source());
        while (re2Matcher.find()) {
            // Retain the fully initialized matcher after traversing the complete input.
        }
        GraphLayout re2PatternGraph = GraphLayout.parseInstance(re2);
        GraphLayout re2MatcherGraph = GraphLayout.parseInstance(re2, re2Matcher, input.source());
        long re2OwnedMatcherBytes = re2MatcherGraph.totalSize() - re2PatternGraph.totalSize() - inputHeapBytes;
        writeGraph(output, "RE2", workload, sourceLength, "active-matcher", "find-all", 1, re2MatcherGraph, inputHeapBytes, re2OwnedMatcherBytes, re2Memory(List.of(re2)));

        JoniPattern joni = compileJoni(input.pattern());
        Matcher joniMatcher = joni.regex.matcher(input.source().byteArray(), input.source().byteArrayOffset(), input.source().byteArrayOffset() + input.source().length());
        findAll(joniMatcher, input.source());
        GraphLayout joniPatternGraph = GraphLayout.parseInstance(joni);
        GraphLayout joniMatcherGraph = GraphLayout.parseInstance(joni, joniMatcher, input.source());
        long joniOwnedMatcherBytes = joniMatcherGraph.totalSize() - joniPatternGraph.totalSize() - inputHeapBytes;
        writeGraph(output, "Joni", workload, sourceLength, "active-matcher", "find-all", 1, joniMatcherGraph, inputHeapBytes, joniOwnedMatcherBytes, MemorySummary.EMPTY);

        if (sourceLength == 32_768 && (workload.equals("captureSparse") || workload.equals("unicodeSparse"))) {
            writeFootprint(outputDirectory, "re2", workload, sourceLength, "active-matcher", re2MatcherGraph);
            writeFootprint(outputDirectory, "joni", workload, sourceLength, "active-matcher", joniMatcherGraph);
        }
    }

    private static void writeGraph(
            PrintWriter output,
            String engine,
            String workload,
            int sourceLength,
            String lifecycle,
            String operation,
            Object[] roots,
            long inputHeapBytes,
            long ownedMatcherHeapBytes,
            MemorySummary memory)
    {
        writeGraph(output, engine, workload, sourceLength, lifecycle, operation, roots.length, GraphLayout.parseInstance(roots), inputHeapBytes, ownedMatcherHeapBytes, memory);
    }

    private static void writeGraph(
            PrintWriter output,
            String engine,
            String workload,
            int sourceLength,
            String lifecycle,
            String operation,
            int rootCount,
            GraphLayout graph,
            long inputHeapBytes,
            long ownedMatcherHeapBytes,
            MemorySummary memory)
    {
        long totalHeapBytes = graph.totalSize();
        output.printf(Locale.US, "%s,%s,%d,%s,%s,%d,%d,%.3f,%d,%d,%d,%d,%d%n",
                engine,
                workload,
                sourceLength,
                lifecycle,
                operation,
                rootCount,
                totalHeapBytes,
                (double) totalHeapBytes / rootCount,
                inputHeapBytes,
                ownedMatcherHeapBytes,
                memory.offHeapBytes,
                memory.accountedDfaBytes,
                memory.resetCount);
        output.flush();
    }

    private static MemorySummary re2Memory(List<TrinoRegexp> patterns)
    {
        long offHeapBytes = 0;
        long accountedDfaBytes = 0;
        int resetCount = 0;
        for (TrinoRegexp regexp : patterns) {
            Re2 pattern = regexp.pattern();
            for (Prog program : List.of(pattern.forwardProgramForDiagnostics())) {
                MemorySummary memory = programMemory(program);
                offHeapBytes += memory.offHeapBytes;
                accountedDfaBytes += memory.accountedDfaBytes;
                resetCount += memory.resetCount;
            }
            Prog reverseProgram = pattern.reverseProgramIfComputedForDiagnostics();
            if (reverseProgram != null) {
                MemorySummary memory = programMemory(reverseProgram);
                offHeapBytes += memory.offHeapBytes;
                accountedDfaBytes += memory.accountedDfaBytes;
                resetCount += memory.resetCount;
            }
        }
        return new MemorySummary(offHeapBytes, accountedDfaBytes, resetCount);
    }

    private static MemorySummary re2Memory(Re2 pattern)
    {
        MemorySummary memory = programMemory(pattern.forwardProgramForDiagnostics());
        Prog reverseProgram = pattern.reverseProgramIfComputedForDiagnostics();
        if (reverseProgram == null) {
            return memory;
        }
        MemorySummary reverseMemory = programMemory(reverseProgram);
        return new MemorySummary(
                memory.offHeapBytes + reverseMemory.offHeapBytes,
                memory.accountedDfaBytes + reverseMemory.accountedDfaBytes,
                memory.resetCount + reverseMemory.resetCount);
    }

    private static MemorySummary programMemory(Prog program)
    {
        long offHeapBytes = 0;
        long accountedDfaBytes = 0;
        int resetCount = 0;
        for (Dfa.DfaInstance.Kind kind : List.of(FIRST_MATCH, LONGEST_MATCH)) {
            Dfa.DfaInstance dfa = program.cachedDfaIfPresent(kind);
            if (dfa == null) {
                continue;
            }
            offHeapBytes += dfa.absolutePointerTransitionMemory();
            accountedDfaBytes += dfa.retainedStateMemory();
            resetCount += dfa.resetCount();
        }
        return new MemorySummary(offHeapBytes, accountedDfaBytes, resetCount);
    }

    private static JoniPattern compileJoni(Slice pattern)
    {
        Slice patternCopy = pattern.copy();
        byte[] bytes = patternCopy.getBytes();
        Regex regex = new Regex(bytes, 0, bytes.length, Option.DEFAULT, NonStrictUTF8Encoding.INSTANCE, Syntax.Java);
        return new JoniPattern(patternCopy, regex);
    }

    private static long findAll(Matcher matcher, Slice source)
    {
        int base = source.byteArrayOffset();
        int end = base + source.length();
        int nextStart = base;
        long matchCount = 0;
        while (nextStart <= end && matcher.search(nextStart, end, Option.DEFAULT) >= 0) {
            matchCount++;
            int matchStart = matcher.getBegin();
            int matchEnd = matcher.getEnd();
            nextStart = matchEnd > matchStart ? matchEnd : matchEnd + 1;
        }
        return matchCount;
    }

    private static boolean shouldWriteFootprint(String workload, int sourceLength, Operation operation)
    {
        return sourceLength == 32_768 && operation == Operation.COUNT &&
                (workload.equals("literalSparse") || workload.equals("captureSparse") || workload.equals("unicodeSparse"));
    }

    private static void writeFootprint(Path outputDirectory, String engine, String workload, int sourceLength, String lifecycle, GraphLayout graph)
            throws IOException
    {
        String baseName = engine + "-" + workload + "-" + sourceLength + "-" + lifecycle;
        Files.writeString(outputDirectory.resolve(baseName + "-footprint.txt"), graph.toFootprint());
    }

    private record JoniPattern(Slice pattern, Regex regex) {}

    private record MemorySummary(long offHeapBytes, long accountedDfaBytes, int resetCount)
    {
        private static final MemorySummary EMPTY = new MemorySummary(0, 0, 0);
    }

    @FunctionalInterface
    private interface Counter
    {
        long count();
    }

    private enum Operation
    {
        CONTAINS("contains")
                {
                    @Override
                    void runRe2(TrinoRegexp regexp, Slice source)
                    {
                        regexp.contains(source);
                    }

                    @Override
                    void runJoni(JoniPattern pattern, Slice source)
                    {
                        Matcher matcher = pattern.regex.matcher(source.byteArray(), source.byteArrayOffset(), source.byteArrayOffset() + source.length());
                        matcher.search(source.byteArrayOffset(), source.byteArrayOffset() + source.length(), Option.DEFAULT);
                    }
                },
        COUNT("count")
                {
                    @Override
                    void runRe2(TrinoRegexp regexp, Slice source)
                    {
                        regexp.count(source);
                    }

                    @Override
                    void runJoni(JoniPattern pattern, Slice source)
                    {
                        Matcher matcher = pattern.regex.matcher(source.byteArray(), source.byteArrayOffset(), source.byteArrayOffset() + source.length());
                        findAll(matcher, source);
                    }
                },
        POSITION("position-third")
                {
                    @Override
                    void runRe2(TrinoRegexp regexp, Slice source)
                    {
                        regexp.position(source, 1, 3);
                    }

                    @Override
                    void runJoni(JoniPattern pattern, Slice source)
                    {
                        Matcher matcher = pattern.regex.matcher(source.byteArray(), source.byteArrayOffset(), source.byteArrayOffset() + source.length());
                        findAll(matcher, source);
                    }
                },
        EXTRACT("extract")
                {
                    @Override
                    void runRe2(TrinoRegexp regexp, Slice source)
                    {
                        regexp.extract(source);
                    }

                    @Override
                    void runJoni(JoniPattern pattern, Slice source)
                    {
                        Matcher matcher = pattern.regex.matcher(source.byteArray(), source.byteArrayOffset(), source.byteArrayOffset() + source.length());
                        matcher.search(source.byteArrayOffset(), source.byteArrayOffset() + source.length(), Option.DEFAULT);
                    }
                },
        EXTRACT_ALL("extract-all")
                {
                    @Override
                    void runRe2(TrinoRegexp regexp, Slice source)
                    {
                        regexp.extractAll(source);
                    }

                    @Override
                    void runJoni(JoniPattern pattern, Slice source)
                    {
                        Matcher matcher = pattern.regex.matcher(source.byteArray(), source.byteArrayOffset(), source.byteArrayOffset() + source.length());
                        findAll(matcher, source);
                    }
                },
        SPLIT("split")
                {
                    @Override
                    void runRe2(TrinoRegexp regexp, Slice source)
                    {
                        regexp.split(source);
                    }

                    @Override
                    void runJoni(JoniPattern pattern, Slice source)
                    {
                        Matcher matcher = pattern.regex.matcher(source.byteArray(), source.byteArrayOffset(), source.byteArrayOffset() + source.length());
                        findAll(matcher, source);
                    }
                },
        REPLACE("replace")
                {
                    @Override
                    void runRe2(TrinoRegexp regexp, Slice source)
                    {
                        regexp.replace(source, REPLACEMENT);
                    }

                    @Override
                    void runJoni(JoniPattern pattern, Slice source)
                    {
                        Matcher matcher = pattern.regex.matcher(source.byteArray(), source.byteArrayOffset(), source.byteArrayOffset() + source.length());
                        findAll(matcher, source);
                    }
                },
        REPLACE_LAMBDA("replace-lambda")
                {
                    @Override
                    void runRe2(TrinoRegexp regexp, Slice source)
                    {
                        regexp.replace(source, groups -> groups.isEmpty() ? REPLACEMENT : groups.getFirst());
                    }

                    @Override
                    void runJoni(JoniPattern pattern, Slice source)
                    {
                        Matcher matcher = pattern.regex.matcher(source.byteArray(), source.byteArrayOffset(), source.byteArrayOffset() + source.length());
                        findAll(matcher, source);
                    }
                };

        private final String label;

        Operation(String label)
        {
            this.label = label;
        }

        abstract void runRe2(TrinoRegexp regexp, Slice source);

        abstract void runJoni(JoniPattern pattern, Slice source);
    }
}
