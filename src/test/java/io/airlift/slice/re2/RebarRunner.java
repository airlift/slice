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

import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.StringJoiner;

import static io.airlift.slice.Slices.wrappedBuffer;

/**
 * Byte-oriented runner for the official Rebar benchmark protocol.
 * <p>
 * This lives with benchmark sources so the protocol adapter and its timing machinery are not part
 * of the published Slice API.
 */
public final class RebarRunner
{
    private RebarRunner() {}

    public static void main(String[] arguments)
            throws Exception
    {
        if (arguments.length == 1 && arguments[0].equals("--version")) {
            System.out.printf(
                    "Slice RE2 (%s %s, native-access=%s)%n",
                    System.getProperty("java.vm.name"),
                    System.getProperty("java.vm.version"),
                    Dfa.nativeAccessEnabled());
            return;
        }
        if (arguments.length == 1 && arguments[0].equals("--manifest")) {
            writeManifest(System.in, System.out);
            return;
        }
        if (arguments.length == 1 && arguments[0].equals("--calibrate")) {
            calibrate(System.in, System.out);
            return;
        }
        if (arguments.length == 1 && arguments[0].equals("--capture-pipeline-manifest")) {
            CapturePipelineRunner.writeManifest(System.in, System.out);
            return;
        }
        if (arguments.length == 1 && arguments[0].startsWith("--capture-pipeline-stage=")) {
            CapturePipelineRunner.run(
                    System.in,
                    System.out,
                    CapturePipelineRunner.Stage.fromCli(arguments[0].substring("--capture-pipeline-stage=".length())));
            return;
        }
        if (arguments.length == 1 && arguments[0].startsWith("--count-pipeline=")) {
            CountPipelineRunner.run(
                    System.in,
                    System.out,
                    Long.parseLong(arguments[0].substring("--count-pipeline=".length())));
            return;
        }
        if (arguments.length != 0) {
            throw new IllegalArgumentException(
                    "usage: RebarRunner [--calibrate|--manifest|--version|--capture-pipeline-manifest|--capture-pipeline-stage=<stage>|--count-pipeline=<maximum-memory-bytes>]");
        }
        run(System.in, System.out);
    }

    static void calibrate(InputStream input, OutputStream output)
            throws Exception
    {
        Benchmark benchmark = Benchmark.read(input.readAllBytes());
        Re2 pattern = benchmark.compile();
        long deadline = System.nanoTime() + 5_000_000_000L;
        long iterations = 0;
        long expectedResult = 0;
        do {
            if (benchmark.model().equals("compile")) {
                pattern = benchmark.compile();
            }
            expectedResult = executeOnce(benchmark, pattern);
            iterations++;
        }
        while (System.nanoTime() < deadline);

        output.write(("calibration_iterations=" + iterations + ",expected_result=" + expectedResult + "\n")
                .getBytes(StandardCharsets.US_ASCII));
    }

    static void writeManifest(InputStream input, OutputStream output)
            throws Exception
    {
        Benchmark benchmark = Benchmark.read(input.readAllBytes());
        Re2 pattern = benchmark.compile();
        long expectedResult = executeOnce(benchmark, pattern);
        RouteDiagnostics routes = RouteDiagnostics.capture(pattern);

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
        writer.write(String.join(",",
                csv(benchmark.name()),
                csv(benchmark.model()),
                Boolean.toString(benchmark.caseInsensitive()),
                Boolean.toString(benchmark.unicode()),
                Integer.toString(benchmark.pattern().length()),
                sha256(benchmark.pattern()),
                Integer.toString(benchmark.haystack().length()),
                sha256(benchmark.haystack()),
                Long.toString(expectedResult),
                csv(resultDemand(benchmark.model())),
                Boolean.toString(Dfa.nativeAccessEnabled()),
                Boolean.toString(routes.requiredPrefix()),
                Boolean.toString(routes.prefixAcceleration()),
                Boolean.toString(routes.onePassEligible()),
                Boolean.toString(routes.bitStateEligible()),
                csv(routes.forward().kinds()),
                Long.toString(routes.forward().absolutePointerBytes()),
                Integer.toString(routes.forward().absolutePointerTransitions()),
                Long.toString(routes.forward().pairedBytes()),
                Integer.toString(routes.forward().pairedSelections()),
                Integer.toString(routes.forward().cacheResets()),
                Boolean.toString(routes.reverseComputed()),
                csv(routes.reverse().kinds()),
                Long.toString(routes.reverse().absolutePointerBytes()),
                Integer.toString(routes.reverse().absolutePointerTransitions()),
                Long.toString(routes.reverse().pairedBytes()),
                Integer.toString(routes.reverse().pairedSelections()),
                Integer.toString(routes.reverse().cacheResets())));
        writer.newLine();
        writer.flush();
    }

    static void run(InputStream input, OutputStream output)
            throws Exception
    {
        Benchmark benchmark = Benchmark.read(input.readAllBytes());
        Samples samples = switch (benchmark.model()) {
            case "compile" -> benchmarkCompile(benchmark);
            case "count" -> benchmarkCount(benchmark);
            case "count-spans" -> benchmarkMatches(benchmark, MatchMeasurement.SPAN_LENGTH);
            case "count-captures" -> benchmarkMatches(benchmark, MatchMeasurement.CAPTURES);
            case "grep" -> benchmarkGrep(benchmark);
            case "grep-captures" -> benchmarkGrepCaptures(benchmark);
            default -> throw new IllegalArgumentException("unsupported Rebar model: " + benchmark.model());
        };

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.US_ASCII));
        for (int index = 0; index < samples.size(); index++) {
            writer.write(Long.toString(samples.duration(index)));
            writer.write(',');
            writer.write(Long.toString(samples.count(index)));
            writer.newLine();
        }
        writer.flush();
    }

    private static Samples benchmarkCompile(Benchmark benchmark)
    {
        Re2.Options options = benchmark.options();
        warmup(benchmark, () -> countMatches(Re2.compile(benchmark.pattern(), options), benchmark.haystack()));

        Samples samples = new Samples();
        long runStart = System.nanoTime();
        for (long iteration = 0; iteration < benchmark.maximumIterations(); iteration++) {
            long start = System.nanoTime();
            Re2 pattern = Re2.compile(benchmark.pattern(), options);
            long duration = System.nanoTime() - start;
            samples.add(duration, countMatches(pattern, benchmark.haystack()));
            if (System.nanoTime() - runStart >= benchmark.maximumTimeNanos()) {
                break;
            }
        }
        return samples;
    }

    private static Samples benchmarkCount(Benchmark benchmark)
    {
        Re2 pattern = benchmark.compile();
        return benchmarkOperation(benchmark, () -> countMatches(pattern, benchmark.haystack()));
    }

    private static Samples benchmarkMatches(Benchmark benchmark, MatchMeasurement measurement)
    {
        Re2 pattern = benchmark.compile();
        Re2Matcher matcher = measurement.createMatcher(pattern, benchmark.haystack());
        return benchmarkOperation(benchmark, () -> matchResult(benchmark.haystack(), matcher, measurement));
    }

    private static Samples benchmarkGrep(Benchmark benchmark)
    {
        Re2 pattern = benchmark.compile();
        Slice haystack = benchmark.haystack();
        byte[] bytes = haystack.byteArray();
        int byteArrayOffset = haystack.byteArrayOffset();

        return benchmarkOperation(benchmark, () -> grepResult(pattern, haystack, bytes, byteArrayOffset));
    }

    private static Samples benchmarkGrepCaptures(Benchmark benchmark)
    {
        Re2 pattern = benchmark.compile();
        Re2Matcher matcher = pattern.matcher(benchmark.haystack());
        Slice haystack = benchmark.haystack();
        byte[] bytes = haystack.byteArray();
        int byteArrayOffset = haystack.byteArrayOffset();

        return benchmarkOperation(benchmark, () -> grepCapturesResult(matcher, haystack, bytes, byteArrayOffset));
    }

    static long executeOnce(Benchmark benchmark, Re2 pattern)
    {
        return switch (benchmark.model()) {
            case "compile", "count" -> countMatches(pattern, benchmark.haystack());
            case "count-spans" -> matchResult(
                    benchmark.haystack(),
                    MatchMeasurement.SPAN_LENGTH.createMatcher(pattern, benchmark.haystack()),
                    MatchMeasurement.SPAN_LENGTH);
            case "count-captures" -> matchResult(
                    benchmark.haystack(),
                    MatchMeasurement.CAPTURES.createMatcher(pattern, benchmark.haystack()),
                    MatchMeasurement.CAPTURES);
            case "grep" -> {
                Slice haystack = benchmark.haystack();
                yield grepResult(pattern, haystack, haystack.byteArray(), haystack.byteArrayOffset());
            }
            case "grep-captures" -> {
                Slice haystack = benchmark.haystack();
                yield grepCapturesResult(pattern.matcher(haystack), haystack, haystack.byteArray(), haystack.byteArrayOffset());
            }
            default -> throw new IllegalArgumentException("unsupported Rebar model: " + benchmark.model());
        };
    }

    static long matchResult(Slice haystack, Re2Matcher matcher, MatchMeasurement measurement)
    {
        matcher.reset(haystack);
        long count = 0;
        while (matcher.find()) {
            count += switch (measurement) {
                case COUNT -> 1;
                case SPAN_LENGTH -> matcher.end() - matcher.start();
                case CAPTURES -> countParticipatingGroups(matcher);
            };
        }
        return count;
    }

    private static long grepResult(Re2 pattern, Slice haystack, byte[] bytes, int byteArrayOffset)
    {
        long count = 0;
        int lineStart = 0;
        while (lineStart < haystack.length()) {
            int lineEnd = lineEnd(haystack, bytes, byteArrayOffset, lineStart);
            int contentEnd = contentEnd(bytes, byteArrayOffset, lineStart, lineEnd);
            if (pattern.partialMatch(haystack.slice(lineStart, contentEnd - lineStart))) {
                count++;
            }
            lineStart = lineEnd + 1;
        }
        return count;
    }

    static long grepCapturesResult(Re2Matcher matcher, Slice haystack, byte[] bytes, int byteArrayOffset)
    {
        long count = 0;
        int lineStart = 0;
        while (lineStart < haystack.length()) {
            int lineEnd = lineEnd(haystack, bytes, byteArrayOffset, lineStart);
            int contentEnd = contentEnd(bytes, byteArrayOffset, lineStart, lineEnd);
            matcher.reset(haystack, lineStart, contentEnd);
            while (matcher.find()) {
                count += countParticipatingGroups(matcher);
            }
            lineStart = lineEnd + 1;
        }
        return count;
    }

    private static int lineEnd(Slice haystack, byte[] bytes, int byteArrayOffset, int lineStart)
    {
        int lineEnd = lineStart;
        while (lineEnd < haystack.length() && bytes[byteArrayOffset + lineEnd] != '\n') {
            lineEnd++;
        }
        return lineEnd;
    }

    private static int contentEnd(byte[] bytes, int byteArrayOffset, int lineStart, int lineEnd)
    {
        if (lineEnd > lineStart && bytes[byteArrayOffset + lineEnd - 1] == '\r') {
            return lineEnd - 1;
        }
        return lineEnd;
    }

    static long countMatches(Re2 pattern, Slice haystack)
    {
        long count = pattern.countMatches(haystack);
        if (count >= 0) {
            return count;
        }

        Re2Matcher matcher = pattern.matcher(haystack, 0);
        count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    private static int countParticipatingGroups(Re2Matcher matcher)
    {
        int count = 0;
        for (int group = 0; group <= matcher.groupCount(); group++) {
            if (matcher.matched(group)) {
                count++;
            }
        }
        return count;
    }

    private static String resultDemand(String model)
    {
        return switch (model) {
            case "compile" -> "compile-with-untimed-count-verification";
            case "count" -> "match-count";
            case "count-spans" -> "group-zero-spans";
            case "count-captures" -> "captures";
            case "grep" -> "boolean-lines";
            case "grep-captures" -> "line-captures";
            default -> throw new IllegalArgumentException("unsupported Rebar model: " + model);
        };
    }

    private static String sha256(Slice value)
            throws Exception
    {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(value.byteArray(), value.byteArrayOffset(), value.length());
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String csv(String value)
    {
        if (value.indexOf(',') < 0 && value.indexOf('"') < 0 && value.indexOf('\n') < 0 && value.indexOf('\r') < 0) {
            return value;
        }
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private static Samples benchmarkOperation(Benchmark benchmark, Operation operation)
    {
        warmup(benchmark, operation);
        Samples samples = new Samples();
        long runStart = System.nanoTime();
        for (long iteration = 0; iteration < benchmark.maximumIterations(); iteration++) {
            long start = System.nanoTime();
            long count = operation.run();
            long duration = System.nanoTime() - start;
            samples.add(duration, count);
            if (System.nanoTime() - runStart >= benchmark.maximumTimeNanos()) {
                break;
            }
        }
        return samples;
    }

    private static void warmup(Benchmark benchmark, Operation operation)
    {
        long warmupStart = System.nanoTime();
        for (long iteration = 0; iteration < benchmark.maximumWarmupIterations(); iteration++) {
            operation.run();
            if (System.nanoTime() - warmupStart >= benchmark.maximumWarmupTimeNanos()) {
                break;
            }
        }
    }

    enum MatchMeasurement
    {
        COUNT(false),
        SPAN_LENGTH(false),
        CAPTURES(true);

        private final boolean includesCaptures;

        MatchMeasurement(boolean includesCaptures)
        {
            this.includesCaptures = includesCaptures;
        }

        Re2Matcher createMatcher(Re2 pattern, Slice input)
        {
            return includesCaptures ? pattern.matcher(input) : pattern.matcher(input, 0);
        }
    }

    @FunctionalInterface
    private interface Operation
    {
        long run();
    }

    record Benchmark(
            String name,
            String model,
            Slice pattern,
            boolean caseInsensitive,
            boolean unicode,
            Slice haystack,
            long maximumIterations,
            long maximumWarmupIterations,
            long maximumTimeNanos,
            long maximumWarmupTimeNanos)
    {
        static Benchmark read(byte[] input)
        {
            String name = null;
            String model = null;
            List<Slice> patterns = new ArrayList<>();
            boolean caseInsensitive = false;
            boolean unicode = false;
            Slice haystack = null;
            long maximumIterations = 0;
            long maximumWarmupIterations = 0;
            long maximumTimeNanos = 0;
            long maximumWarmupTimeNanos = 0;

            int position = 0;
            while (position < input.length) {
                KlvItem item = KlvItem.read(input, position);
                position = item.nextPosition();
                switch (item.key()) {
                    case "name" -> name = item.asAscii();
                    case "model" -> model = item.asAscii();
                    case "pattern" -> patterns.add(item.value());
                    case "case-insensitive" -> caseInsensitive = item.asBoolean();
                    case "unicode" -> unicode = item.asBoolean();
                    case "haystack" -> haystack = item.value();
                    case "max-iters" -> maximumIterations = item.asLong();
                    case "max-warmup-iters" -> maximumWarmupIterations = item.asLong();
                    case "max-time" -> maximumTimeNanos = item.asLong();
                    case "max-warmup-time" -> maximumWarmupTimeNanos = item.asLong();
                    default -> throw new IllegalArgumentException("unrecognized KLV key: " + item.key());
                }
            }

            if (name == null) {
                throw new IllegalArgumentException("missing Rebar name");
            }
            if (model == null) {
                throw new IllegalArgumentException("missing Rebar model");
            }
            if (patterns.size() != 1) {
                throw new IllegalArgumentException("Rebar model requires exactly one pattern: " + patterns.size());
            }
            if (haystack == null) {
                throw new IllegalArgumentException("missing Rebar haystack");
            }
            return new Benchmark(
                    name,
                    model,
                    patterns.getFirst(),
                    caseInsensitive,
                    unicode,
                    haystack,
                    maximumIterations,
                    maximumWarmupIterations,
                    maximumTimeNanos,
                    maximumWarmupTimeNanos);
        }

        Re2 compile()
        {
            return Re2.compile(pattern, options());
        }

        Re2.Options options()
        {
            return (unicode ? Re2.Options.defaults() : Re2.Options.latin1())
                    .setCaseSensitive(!caseInsensitive);
        }
    }

    private record RouteDiagnostics(
            boolean requiredPrefix,
            boolean prefixAcceleration,
            boolean onePassEligible,
            boolean bitStateEligible,
            DfaRoutes forward,
            boolean reverseComputed,
            DfaRoutes reverse)
    {
        private static RouteDiagnostics capture(Re2 pattern)
        {
            Prog forwardProgram = pattern.forwardProgramForDiagnostics();
            Prog reverseProgram = pattern.reverseProgramIfComputedForDiagnostics();
            return new RouteDiagnostics(
                    pattern.hasRequiredPrefixForDiagnostics(),
                    forwardProgram.canPrefixAccel(),
                    forwardProgram.isOnePass(),
                    forwardProgram.canBitState(),
                    DfaRoutes.capture(forwardProgram),
                    pattern.isReverseProgramComputed(),
                    DfaRoutes.capture(reverseProgram));
        }
    }

    private record DfaRoutes(
            String kinds,
            long absolutePointerBytes,
            int absolutePointerTransitions,
            long pairedBytes,
            int pairedSelections,
            int cacheResets)
    {
        private static DfaRoutes capture(Prog program)
        {
            if (program == null) {
                return new DfaRoutes("", 0, 0, 0, 0, 0);
            }
            StringJoiner kinds = new StringJoiner("|");
            long absolutePointerBytes = 0;
            int absolutePointerTransitions = 0;
            long pairedBytes = 0;
            int pairedSelections = 0;
            int cacheResets = 0;
            for (Dfa.DfaInstance.Kind kind : Dfa.DfaInstance.Kind.values()) {
                Dfa.DfaInstance dfa = program.cachedDfaIfPresent(kind);
                if (dfa == null) {
                    continue;
                }
                kinds.add(kind.name());
                absolutePointerBytes += dfa.absolutePointerTransitionMemory();
                absolutePointerTransitions += dfa.absolutePointerTransitionCount();
                pairedBytes += dfa.pairedTransitionMemory();
                pairedSelections += dfa.pairedRowSelectionCount();
                cacheResets += dfa.resetCount();
            }
            return new DfaRoutes(
                    kinds.toString(),
                    absolutePointerBytes,
                    absolutePointerTransitions,
                    pairedBytes,
                    pairedSelections,
                    cacheResets);
        }
    }

    private record KlvItem(String key, Slice value, int nextPosition)
    {
        private static KlvItem read(byte[] input, int position)
        {
            int keyEnd = find(input, position, (byte) ':');
            String key = new String(input, position, keyEnd - position, StandardCharsets.US_ASCII);
            int lengthEnd = find(input, keyEnd + 1, (byte) ':');
            int valueLength = parseLength(input, keyEnd + 1, lengthEnd);
            int valueStart = lengthEnd + 1;
            int nextPosition = Math.addExact(valueStart, Math.addExact(valueLength, 1));
            if (nextPosition > input.length || input[nextPosition - 1] != '\n') {
                throw new IllegalArgumentException("KLV item is missing its line terminator: " + key);
            }
            return new KlvItem(key, wrappedBuffer(input, valueStart, valueLength), nextPosition);
        }

        private String asAscii()
        {
            return new String(value.byteArray(), value.byteArrayOffset(), value.length(), StandardCharsets.US_ASCII);
        }

        private boolean asBoolean()
        {
            return switch (asAscii()) {
                case "true" -> true;
                case "false" -> false;
                default -> throw new IllegalArgumentException("invalid boolean for " + key + ": " + asAscii());
            };
        }

        private long asLong()
        {
            return Long.parseLong(asAscii());
        }

        private static int find(byte[] input, int position, byte target)
        {
            for (int index = position; index < input.length; index++) {
                if (input[index] == target) {
                    return index;
                }
            }
            throw new IllegalArgumentException("invalid KLV item at byte " + position);
        }

        private static int parseLength(byte[] input, int start, int end)
        {
            if (start == end) {
                throw new IllegalArgumentException("empty KLV length");
            }
            int result = 0;
            for (int index = start; index < end; index++) {
                int digit = input[index] - '0';
                if (digit < 0 || digit > 9) {
                    throw new IllegalArgumentException("invalid KLV length");
                }
                result = Math.addExact(Math.multiplyExact(result, 10), digit);
            }
            return result;
        }
    }

    private static final class Samples
    {
        private long[] durations = new long[16];
        private long[] counts = new long[16];
        private int size;

        public void add(long duration, long count)
        {
            if (size == durations.length) {
                int newSize = Math.multiplyExact(size, 2);
                durations = java.util.Arrays.copyOf(durations, newSize);
                counts = java.util.Arrays.copyOf(counts, newSize);
            }
            durations[size] = duration;
            counts[size] = count;
            size++;
        }

        public int size()
        {
            return size;
        }

        public long duration(int index)
        {
            return durations[index];
        }

        public long count(int index)
        {
            return counts[index];
        }
    }
}
