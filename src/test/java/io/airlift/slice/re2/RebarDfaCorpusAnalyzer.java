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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;

/** Expands Rebar definitions with Rebar itself and reports the resulting DFA shape. */
public final class RebarDfaCorpusAnalyzer
{
    private static final int MAX_SEARCH_CALLS = 100_000;
    private static final long FORWARD_MEMORY = (Re2.Options.DEFAULT_MAX_MEMORY / 3L) * 2;

    private RebarDfaCorpusAnalyzer() {}

    public static void main(String[] args)
            throws Exception
    {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: RebarDfaCorpusAnalyzer <rebar-directory>");
        }

        Path rebarDirectory = Path.of(args[0]).toAbsolutePath();
        Path rebarExecutable = rebarDirectory.resolve("target/release/rebar");
        List<BenchmarkReference> benchmarks = listBenchmarks(rebarDirectory, rebarExecutable);

        PrintWriter output = new PrintWriter(System.out, false, UTF_8);
        output.println("name,model,status,route,unicode,case_insensitive,haystack_bytes,pattern_bytes,program_size,byte_classes,dfa_states,paired_rows,paired_transitions,paired_estimated_bytes,paired_retained_bytes,available_state_bytes,cache_resets,search_calls,matches,elapsed_micros,error");
        for (int benchmarkIndex = 0; benchmarkIndex < benchmarks.size(); benchmarkIndex++) {
            BenchmarkReference reference = benchmarks.get(benchmarkIndex);
            long start = System.nanoTime();
            AnalysisResult result;
            try {
                RebarBenchmark benchmark = loadBenchmark(rebarDirectory, rebarExecutable, reference.name());
                result = analyze(benchmark);
            }
            catch (RuntimeException | IOException e) {
                result = AnalysisResult.error(reference.name(), reference.model(), e.getMessage());
            }
            long elapsedMicros = (System.nanoTime() - start) / 1_000;
            output.println(result.toCsv(elapsedMicros));
            output.flush();

            if ((benchmarkIndex + 1) % 10 == 0 || benchmarkIndex + 1 == benchmarks.size()) {
                System.err.printf("Analyzed %d/%d Rebar benchmarks%n", benchmarkIndex + 1, benchmarks.size());
            }
        }
    }

    static AnalysisResult analyze(RebarBenchmark benchmark)
    {
        if (benchmark.patterns().size() != 1) {
            return AnalysisResult.unsupported(benchmark, "expected one pattern");
        }
        if (benchmark.model().equals("regex-redux")) {
            return AnalysisResult.unsupported(benchmark, "regex-redux compiles multiple internal patterns");
        }

        String pattern = benchmark.patterns().getFirst();
        int parseFlags = Regexp.LIKE_PERL;
        if (!benchmark.unicode()) {
            parseFlags |= Regexp.LATIN1;
        }
        if (benchmark.caseInsensitive()) {
            parseFlags |= Regexp.FOLD_CASE;
        }

        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), parseFlags);
        Regexp.RequiredPrefixResult requiredPrefix = parsed.regexp().requiredPrefix();
        Regexp forwardRegexp = requiredPrefix == null ? parsed.regexp() : requiredPrefix.suffix();
        Prog program = Compiler.compile(forwardRegexp, false, FORWARD_MEMORY);

        if (benchmark.model().equals("compile")) {
            return AnalysisResult.compileOnly(benchmark, pattern, program, requiredPrefix != null);
        }

        Dfa.DfaInstance.Kind dfaKind = usesBooleanGrepModel(benchmark.model()) || program.anchorEnd()
                ? Dfa.DfaInstance.Kind.LONGEST_MATCH
                : Dfa.DfaInstance.Kind.FIRST_MATCH;
        Dfa.DfaInstance dfa = program.getCachedDfa(dfaKind);
        if (dfa == null || !dfa.ok()) {
            return AnalysisResult.dfaUnavailable(benchmark, pattern, program);
        }
        SearchSummary search = requiredPrefix == null
                ? executeModel(benchmark, program)
                : new SearchSummary(0, 0, false);

        String route;
        if (requiredPrefix != null) {
            route = "REQUIRED_PREFIX";
        }
        else if (program.canPrefixAccel()) {
            route = "PREFIX_ACCELERATION";
        }
        else if (dfa.fixedDistanceByteCandidatesInitialized() && dfa.canFixedDistanceByteAcceleration()) {
            route = "FIXED_DISTANCE_BYTE_ACCELERATION";
        }
        else if (dfa.canStartByteAcceleration()) {
            route = "START_BYTE_ACCELERATION";
        }
        else if (dfa.pairedTransitionMemory() > 0) {
            route = dfa.pairedTransitionRowCount() == dfa.stateCount ? "PAIRED_DFA_FULL" : "PAIRED_DFA_PARTIAL";
        }
        else if (program.canMatchEmpty()) {
            route = "COMPACT_NULLABLE";
        }
        else if (benchmark.haystack().length < 256) {
            route = "COMPACT_SHORT_INPUT";
        }
        else if (dfa.estimatedPairedTransitionMemory() > Dfa.MAX_PAIRED_TRANSITION_MEMORY) {
            route = "COMPACT_PAIR_CAP";
        }
        else if (dfa.estimatedPairedTransitionMemory() > dfa.availableStateMemory()) {
            route = "COMPACT_MEMORY_BUDGET";
        }
        else {
            route = "COMPACT_NOT_REQUESTED";
        }

        String status = search.failed() ? "SEARCH_FAILED" : "OK";
        return new AnalysisResult(
                benchmark.name(),
                benchmark.model(),
                status,
                route,
                benchmark.unicode(),
                benchmark.caseInsensitive(),
                benchmark.haystack().length,
                pattern.getBytes(UTF_8).length,
                program.size(),
                dfa.nextSize,
                dfa.stateCount,
                dfa.pairedTransitionRowCount(),
                dfa.pairedTransitionCount(),
                dfa.estimatedPairedTransitionMemory(),
                dfa.pairedTransitionMemory(),
                dfa.availableStateMemory(),
                dfa.resetCount(),
                search.calls(),
                search.matches(),
                "");
    }

    private static SearchSummary executeModel(RebarBenchmark benchmark, Prog program)
    {
        Slice haystack = Slices.wrappedBuffer(benchmark.haystack());
        if (usesBooleanGrepModel(benchmark.model())) {
            return executeGrep(haystack, program);
        }
        return executeFind(haystack, program, benchmark.unicode());
    }

    private static SearchSummary executeFind(Slice haystack, Prog program, boolean unicode)
    {
        int start = 0;
        int calls = 0;
        int matches = 0;
        while (start <= haystack.length() && calls < MAX_SEARCH_CALLS) {
            long matchEnd = Dfa.search(program, haystack, start, haystack.length(), false, Prog.MatchKind.FIRST_MATCH, true);
            calls++;
            if (matchEnd == Dfa.SEARCH_FAILED) {
                return new SearchSummary(calls, matches, true);
            }
            if (matchEnd == Dfa.SEARCH_NO_MATCH) {
                break;
            }
            matches++;
            if (matchEnd > 0) {
                start += toIntExact(matchEnd);
            }
            else if (start < haystack.length()) {
                start = nextCodePointBoundary(haystack, start, unicode);
            }
            else {
                break;
            }
        }
        return new SearchSummary(calls, matches, false);
    }

    private static SearchSummary executeGrep(Slice haystack, Prog program)
    {
        int lineStart = 0;
        int calls = 0;
        int matches = 0;
        while (lineStart <= haystack.length() && calls < MAX_SEARCH_CALLS) {
            int lineEnd = lineStart;
            while (lineEnd < haystack.length() && haystack.getByte(lineEnd) != '\n') {
                lineEnd++;
            }
            Slice line = haystack.slice(lineStart, lineEnd - lineStart);
            long result = Dfa.search(program, line, false, Prog.MatchKind.FIRST_MATCH, false);
            calls++;
            if (result == Dfa.SEARCH_FAILED) {
                return new SearchSummary(calls, matches, true);
            }
            if (result >= 0) {
                matches++;
            }
            if (lineEnd == haystack.length()) {
                break;
            }
            lineStart = lineEnd + 1;
        }
        return new SearchSummary(calls, matches, false);
    }

    private static int nextCodePointBoundary(Slice input, int position, boolean unicode)
    {
        if (!unicode) {
            return position + 1;
        }
        int firstByte = input.getUnsignedByte(position);
        int codePointBytes;
        if (firstByte < 0x80) {
            codePointBytes = 1;
        }
        else if (firstByte < 0xE0) {
            codePointBytes = 2;
        }
        else if (firstByte < 0xF0) {
            codePointBytes = 3;
        }
        else {
            codePointBytes = 4;
        }
        return Math.min(position + codePointBytes, input.length());
    }

    private static boolean usesBooleanGrepModel(String model)
    {
        return model.equals("grep");
    }

    private static List<BenchmarkReference> listBenchmarks(Path rebarDirectory, Path rebarExecutable)
            throws IOException, InterruptedException
    {
        Process process = new ProcessBuilder(rebarExecutable.toString(), "measure", "--list", "-e", "^re2$")
                .directory(rebarDirectory.toFile())
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
        List<BenchmarkReference> benchmarks = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8))) {
            for (String line; (line = reader.readLine()) != null; ) {
                String[] fields = line.split(",", -1);
                if (fields.length != 4) {
                    throw new IOException("Invalid Rebar benchmark listing: " + line);
                }
                benchmarks.add(new BenchmarkReference(fields[0], fields[1]));
            }
        }
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("Rebar benchmark listing failed with exit code " + exitCode);
        }
        return benchmarks;
    }

    private static RebarBenchmark loadBenchmark(Path rebarDirectory, Path rebarExecutable, String benchmarkName)
            throws IOException
    {
        Process process = new ProcessBuilder(rebarExecutable.toString(), "klv", benchmarkName)
                .directory(rebarDirectory.toFile())
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
        RebarBenchmark benchmark;
        try (InputStream input = process.getInputStream()) {
            benchmark = readKlv(input);
        }
        try {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("Rebar KLV expansion failed for " + benchmarkName + " with exit code " + exitCode);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while expanding " + benchmarkName, e);
        }
        return benchmark;
    }

    static RebarBenchmark readKlv(InputStream input)
            throws IOException
    {
        String name = "";
        String model = "";
        boolean caseInsensitive = false;
        boolean unicode = false;
        List<String> patterns = new ArrayList<>();
        byte[] haystack = new byte[0];

        while (true) {
            String key = readUntil(input, ':');
            if (key == null) {
                break;
            }
            String lengthValue = readUntil(input, ':');
            if (lengthValue == null) {
                throw new EOFException("Missing KLV length for " + key);
            }
            int length = Integer.parseInt(lengthValue);
            byte[] value = input.readNBytes(length);
            if (value.length != length) {
                throw new EOFException("Truncated KLV value for " + key);
            }
            if (input.read() != '\n') {
                throw new IOException("Missing KLV newline for " + key);
            }

            switch (key) {
                case "name" -> name = new String(value, UTF_8);
                case "model" -> model = new String(value, UTF_8);
                case "case-insensitive" -> caseInsensitive = Boolean.parseBoolean(new String(value, StandardCharsets.US_ASCII));
                case "unicode" -> unicode = Boolean.parseBoolean(new String(value, StandardCharsets.US_ASCII));
                case "pattern" -> patterns.add(new String(value, UTF_8));
                case "haystack" -> haystack = value;
            }
        }
        return new RebarBenchmark(name, model, List.copyOf(patterns), caseInsensitive, unicode, haystack);
    }

    private static String readUntil(InputStream input, int delimiter)
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        while (true) {
            int value = input.read();
            if (value < 0) {
                return output.size() == 0 ? null : throwUnexpectedEnd();
            }
            if (value == delimiter) {
                return output.toString(StandardCharsets.US_ASCII);
            }
            output.write(value);
        }
    }

    private static String throwUnexpectedEnd()
            throws EOFException
    {
        throw new EOFException("Truncated KLV header");
    }

    record RebarBenchmark(String name, String model, List<String> patterns, boolean caseInsensitive, boolean unicode, byte[] haystack) {}

    private record BenchmarkReference(String name, String model) {}

    private record SearchSummary(int calls, int matches, boolean failed) {}

    record AnalysisResult(
            String name,
            String model,
            String status,
            String route,
            boolean unicode,
            boolean caseInsensitive,
            long haystackBytes,
            int patternBytes,
            int programSize,
            int byteClasses,
            int dfaStates,
            int pairedRows,
            int pairedTransitions,
            long pairedEstimatedBytes,
            long pairedRetainedBytes,
            long availableStateBytes,
            int cacheResets,
            int searchCalls,
            int matches,
            String error)
    {
        static AnalysisResult compileOnly(RebarBenchmark benchmark, String pattern, Prog program, boolean hasRequiredPrefix)
        {
            return new AnalysisResult(
                    benchmark.name(), benchmark.model(), "COMPILE_ONLY", hasRequiredPrefix ? "REQUIRED_PREFIX" : "NOT_EXECUTED",
                    benchmark.unicode(), benchmark.caseInsensitive(), benchmark.haystack().length, pattern.getBytes(UTF_8).length,
                    program.size(), program.bytemapRange() + 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "");
        }

        static AnalysisResult unsupported(RebarBenchmark benchmark, String error)
        {
            return new AnalysisResult(
                    benchmark.name(), benchmark.model(), "UNSUPPORTED", "NOT_EXECUTED", benchmark.unicode(), benchmark.caseInsensitive(),
                    benchmark.haystack().length, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, error);
        }

        static AnalysisResult dfaUnavailable(RebarBenchmark benchmark, String pattern, Prog program)
        {
            return new AnalysisResult(
                    benchmark.name(), benchmark.model(), "DFA_UNAVAILABLE", "ENGINE_FALLBACK", benchmark.unicode(), benchmark.caseInsensitive(),
                    benchmark.haystack().length, pattern.getBytes(UTF_8).length, program.size(), program.bytemapRange() + 1,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, "DFA cache does not satisfy its initialization budget");
        }

        static AnalysisResult error(String name, String model, String error)
        {
            return new AnalysisResult(name, model, "ERROR", "NOT_EXECUTED", false, false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, error);
        }

        String toCsv(long elapsedMicros)
        {
            return String.join(",",
                    csv(name),
                    csv(model),
                    status,
                    route,
                    Boolean.toString(unicode),
                    Boolean.toString(caseInsensitive),
                    Long.toString(haystackBytes),
                    Integer.toString(patternBytes),
                    Integer.toString(programSize),
                    Integer.toString(byteClasses),
                    Integer.toString(dfaStates),
                    Integer.toString(pairedRows),
                    Integer.toString(pairedTransitions),
                    Long.toString(pairedEstimatedBytes),
                    Long.toString(pairedRetainedBytes),
                    Long.toString(availableStateBytes),
                    Integer.toString(cacheResets),
                    Integer.toString(searchCalls),
                    Integer.toString(matches),
                    Long.toString(elapsedMicros),
                    csv(error));
        }

        private static String csv(String value)
        {
            if (!value.contains(",") && !value.contains("\"") && !value.contains("\n") && !value.contains("\r")) {
                return value;
            }
            return '"' + value.replace("\"", "\"\"") + '"';
        }
    }
}
