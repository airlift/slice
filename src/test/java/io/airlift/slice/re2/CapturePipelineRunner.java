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
import io.airlift.slice.SliceUtf8;

import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

import static io.airlift.slice.re2.Prog.MatchKind.FIRST_MATCH;
import static io.airlift.slice.re2.Prog.MatchKind.FULL_MATCH;

/**
 * Benchmark-only decomposition of the public capture pipeline into independently timed stages.
 * Every stage is validated against a trace produced by the public matcher before timing begins.
 */
public final class CapturePipelineRunner
{
    private CapturePipelineRunner() {}

    public static void main(String[] arguments)
            throws Exception
    {
        if (arguments.length == 1 && arguments[0].equals("--manifest")) {
            writeManifest(System.in, System.out);
            return;
        }
        if (arguments.length != 1 || !arguments[0].startsWith("--stage=")) {
            throw new IllegalArgumentException("usage: CapturePipelineRunner --manifest|--stage=<stage>");
        }
        run(System.in, System.out, Stage.fromCli(arguments[0].substring("--stage=".length())));
    }

    static void writeManifest(InputStream input, OutputStream output)
            throws Exception
    {
        Pipeline pipeline = Pipeline.prepare(RebarRunner.Benchmark.read(input.readAllBytes()));
        long publicResult = pipeline.execute(Stage.PUBLIC);
        if (publicResult != pipeline.publicResult()) {
            throw new IllegalStateException("public manifest result changed");
        }
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.US_ASCII));
        writeValue(writer, "name", pipeline.benchmark().name());
        writeValue(writer, "model", pipeline.benchmark().model());
        writeValue(writer, "attempts", pipeline.attempts().size());
        writeValue(writer, "matches", pipeline.matchCount());
        writeValue(writer, "matcher_resets", pipeline.matcherResetCount());
        writeValue(writer, "groups", pipeline.groupCount());
        writeValue(writer, "program_instructions", pipeline.program().size());
        writeValue(writer, "program_one_pass", pipeline.program().isOnePass());
        writeValue(writer, "one_pass_capture_eligible", pipeline.program().isOnePass() && pipeline.program().supportsOnePassCaptureSlots(pipeline.groupCount() * 2));
        writeValue(writer, "bit_state_eligible", pipeline.program().canBitState());
        writeValue(writer, "anchored_dfa_skipped", pipeline.anchoredDfaSkipped());
        writeValue(writer, "bit_state_lists", pipeline.program().listCount());
        writeValue(writer, "bit_state_text_max_size", pipeline.program().bitStateTextMaxSize());
        writeValue(writer, "forward_calls", pipeline.callCount(Stage.FORWARD));
        writeValue(writer, "forward_bytes", pipeline.byteCount(Stage.FORWARD));
        writeValue(writer, "reverse_calls", pipeline.callCount(Stage.REVERSE));
        writeValue(writer, "reverse_bytes", pipeline.byteCount(Stage.REVERSE));
        writeValue(writer, "capture_calls", pipeline.callCount(Stage.CAPTURE));
        writeValue(writer, "capture_bytes", pipeline.byteCount(Stage.CAPTURE));
        writeValue(writer, "direct_capture_calls", pipeline.attempts().size());
        writeValue(writer, "direct_capture_bytes", pipeline.directCaptureBytes());
        writeValue(writer, "bit_state_capture_calls", pipeline.callCount(Stage.CAPTURE));
        writeValue(writer, "bit_state_capture_bytes", pipeline.byteCount(Stage.CAPTURE));
        writeValue(writer, "bit_state_capture_reused_calls", pipeline.callCount(Stage.CAPTURE));
        writeValue(writer, "bit_state_capture_reused_bytes", pipeline.byteCount(Stage.CAPTURE));
        writeValue(writer, "direct_bit_state_capture_calls", pipeline.attempts().size());
        writeValue(writer, "direct_bit_state_capture_bytes", pipeline.directCaptureBytes());
        writeValue(writer, "capture_engine", pipeline.captureEngines());
        writeValue(writer, "candidate_start_enabled", pipeline.publicMatcher().candidateStartCursorEnabledForDiagnostics());
        writeValue(writer, "candidate_start_routes", pipeline.publicMatcher().candidateStartRouteCountForDiagnostics());
        writeValue(writer, "candidate_start_fallbacks", pipeline.publicMatcher().candidateStartFallbackCountForDiagnostics());
        writeValue(writer, "public_result", pipeline.publicResult());
        writer.flush();
    }

    static void run(InputStream input, OutputStream output, Stage stage)
            throws Exception
    {
        RebarRunner.Benchmark benchmark = RebarRunner.Benchmark.read(input.readAllBytes());
        Pipeline pipeline = Pipeline.prepare(benchmark);
        long expectedChecksum = pipeline.execute(stage);

        warmup(benchmark, pipeline, stage, expectedChecksum);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.US_ASCII));
        long runStart = System.nanoTime();
        for (long iteration = 0; iteration < benchmark.maximumIterations(); iteration++) {
            long start = System.nanoTime();
            long checksum = pipeline.execute(stage);
            long duration = System.nanoTime() - start;
            if (checksum != expectedChecksum) {
                throw new IllegalStateException("stage result changed: " + stage + " expected " + expectedChecksum + " but got " + checksum);
            }
            writer.write(Long.toString(duration));
            writer.write(',');
            writer.write(Long.toString(pipeline.publicResult()));
            writer.newLine();
            if (System.nanoTime() - runStart >= benchmark.maximumTimeNanos()) {
                break;
            }
        }
        writer.flush();
    }

    private static void warmup(RebarRunner.Benchmark benchmark, Pipeline pipeline, Stage stage, long expectedChecksum)
    {
        long warmupStart = System.nanoTime();
        for (long iteration = 0; iteration < benchmark.maximumWarmupIterations(); iteration++) {
            long checksum = pipeline.execute(stage);
            if (checksum != expectedChecksum) {
                throw new IllegalStateException("stage result changed during warmup: " + stage);
            }
            if (System.nanoTime() - warmupStart >= benchmark.maximumWarmupTimeNanos()) {
                break;
            }
        }
    }

    private static void writeValue(BufferedWriter writer, String name, Object value)
            throws Exception
    {
        writer.write(name);
        writer.write('=');
        writer.write(String.valueOf(value));
        writer.newLine();
    }

    enum Stage
    {
        PUBLIC,
        FORWARD,
        REVERSE,
        CAPTURE,
        COMPOSED,
        SETUP,
        RESULT,
        CONTROL,
        DIRECT_CAPTURE,
        BIT_STATE_CAPTURE,
        BIT_STATE_CAPTURE_REUSED,
        DIRECT_BIT_STATE_CAPTURE;

        static Stage fromCli(String value)
        {
            return valueOf(value.replace('-', '_').toUpperCase(java.util.Locale.ENGLISH));
        }
    }

    private enum CaptureEngine
    {
        ONE_PASS,
        BIT_STATE,
        NFA
    }

    private record Attempt(
            Slice context,
            int searchStart,
            int searchEnd,
            int[] groups,
            boolean matched,
            boolean forward,
            long forwardResult,
            boolean reverse,
            long reverseResult,
            boolean capture,
            CaptureEngine captureEngine,
            int captureStart,
            int captureEnd,
            Prog.MatchKind captureKind,
            int[] captureGroups)
    {
        int matchStart()
        {
            return groups[0];
        }

        int matchEnd()
        {
            return groups[1];
        }
    }

    private record Pipeline(
            RebarRunner.Benchmark benchmark,
            Re2 pattern,
            Prog program,
            Prog reverseProgram,
            List<Attempt> attempts,
            List<Slice> resetInputs,
            long publicResult,
            int groupCount,
            int matchCount,
            int fixedMatchLength,
            Re2Matcher setupMatcher,
            Re2Matcher publicMatcher)
    {
        private static Pipeline prepare(RebarRunner.Benchmark benchmark)
        {
            if (!Set.of("count-spans", "count-captures", "grep-captures").contains(benchmark.model())) {
                throw new IllegalArgumentException("capture pipeline does not support model: " + benchmark.model());
            }

            Re2 pattern = benchmark.compile();
            if (pattern.hasRequiredPrefixForDiagnostics()) {
                throw new IllegalArgumentException("capture pipeline does not yet support stripped required prefixes");
            }
            if (pattern.longestMatchForDiagnostics()) {
                throw new IllegalArgumentException("capture pipeline does not yet support longest-match options");
            }

            int groupCount = benchmark.model().equals("count-spans") ? 1 : pattern.capturingGroupCount() + 1;
            List<Slice> resetInputs = inputs(benchmark);
            List<Attempt> attempts = new ArrayList<>();
            int matchCount = 0;
            long publicResult = 0;
            Prog program = pattern.forwardProgramForDiagnostics();
            int fixedMatchLength = pattern.fixedMatchLengthForDiagnostics();
            BitState.Workspace traceBitStateWorkspace = new BitState.Workspace();
            Nfa.Workspace traceNfaWorkspace = new Nfa.Workspace();

            for (Slice context : resetInputs) {
                int searchStart = 0;
                while (searchStart <= context.length()) {
                    int[] groups = new int[groupCount * 2];
                    boolean matched = pattern.matchInto(
                            context,
                            searchStart,
                            context.length(),
                            Re2.Anchor.UNANCHORED,
                            groups,
                            traceBitStateWorkspace,
                            traceNfaWorkspace);
                    Attempt attempt = planAttempt(pattern, program, context, searchStart, groups, matched, fixedMatchLength);
                    attempts.add(attempt);
                    if (!matched) {
                        break;
                    }
                    matchCount++;
                    publicResult += benchmark.model().equals("count-spans")
                            ? groups[1] - groups[0]
                            : participatingGroupCount(groups);
                    searchStart = nextSearchStart(context, groups[0], groups[1], benchmark.unicode());
                }
            }

            long expectedPublicResult = RebarRunner.executeOnce(benchmark, pattern);
            if (publicResult != expectedPublicResult) {
                throw new IllegalStateException("trace result differs from public matcher: " + publicResult + " != " + expectedPublicResult);
            }

            Prog reverseProgram = pattern.reverseProgramIfComputedForDiagnostics();
            Pipeline pipeline = new Pipeline(
                    benchmark,
                    pattern,
                    program,
                    reverseProgram,
                    List.copyOf(attempts),
                    List.copyOf(resetInputs),
                    publicResult,
                    groupCount,
                    matchCount,
                    fixedMatchLength,
                    pattern.matcher(benchmark.haystack(), groupCount - 1),
                    pattern.matcher(benchmark.haystack(), groupCount - 1));
            pipeline.validateReplay();
            return pipeline;
        }

        private static Attempt planAttempt(Re2 pattern, Prog program, Slice context, int searchStart, int[] groups, boolean matched, int fixedMatchLength)
        {
            int searchEnd = context.length();
            boolean anchored = program.anchorStart();
            boolean directBitStateCapture = !anchored && pattern.canUseDirectBitStateCapture(
                    searchEnd - searchStart,
                    groups.length / 2);
            boolean skipDfa = anchored && shouldSkipAnchoredDfa(program, searchEnd - searchStart, groups.length / 2);
            boolean forward = !directBitStateCapture && !skipDfa && !program.anchorEnd();
            boolean reverse = false;
            boolean capture = false;
            long forwardResult = Dfa.SEARCH_NO_MATCH;
            long reverseResult = Dfa.SEARCH_NO_MATCH;
            CaptureEngine captureEngine = null;
            int captureStart = searchStart;
            int captureEnd = searchEnd;
            Prog.MatchKind captureKind = FIRST_MATCH;
            int[] captureGroups = null;

            if (program.anchorStart() && searchStart != 0) {
                return new Attempt(
                        context, searchStart, searchEnd, groups.clone(), matched,
                        false, forwardResult, false, reverseResult,
                        false, null, captureStart, captureEnd, captureKind, null);
            }

            if (forward) {
                forwardResult = Dfa.search(program, context, searchStart, searchEnd, anchored, FIRST_MATCH, true);
                requireDfaResult("forward", matched ? groups[1] - searchStart : Dfa.SEARCH_NO_MATCH, forwardResult);
            }

            if (matched && !directBitStateCapture && !anchored && !skipDfa && groups.length > 0 && !program.anchorEnd() && fixedMatchLength < 0 && groups[1] > searchStart) {
                Prog reverseProgram = pattern.reverseProgramIfComputedForDiagnostics();
                if (reverseProgram == null) {
                    throw new IllegalStateException("public match did not compute a required reverse program");
                }
                reverse = true;
                reverseResult = Dfa.search(reverseProgram, context, searchStart, groups[1], true, Prog.MatchKind.LONGEST_MATCH, true);
                requireDfaResult("reverse", groups[0] - searchStart, reverseResult);
            }

            if (directBitStateCapture) {
                capture = true;
                captureEngine = CaptureEngine.BIT_STATE;
                captureGroups = new int[groups.length];
                boolean captureMatched = searchCapture(
                        captureEngine,
                        program,
                        context,
                        captureStart,
                        captureEnd,
                        false,
                        captureKind,
                        captureGroups);
                if (captureMatched != matched) {
                    throw new IllegalStateException("direct BitState replay match differs from public matcher");
                }
                if (matched) {
                    int[] adjusted = captureGroups.clone();
                    shiftOffsets(adjusted, captureStart);
                    if (!Arrays.equals(adjusted, groups)) {
                        throw new IllegalStateException("direct BitState replay groups differ from public matcher");
                    }
                }
            }
            else if (groups.length > 2 && (skipDfa || matched)) {
                capture = true;
                if (matched && !skipDfa) {
                    captureStart = groups[0];
                    captureEnd = groups[1];
                    captureKind = FULL_MATCH;
                }
                captureEngine = selectCaptureEngine(program, captureEnd - captureStart, true, groups.length);
                captureGroups = new int[groups.length];
                boolean captureMatched = searchCapture(captureEngine, program, context, captureStart, captureEnd, true, captureKind, captureGroups);
                if (captureMatched != matched) {
                    throw new IllegalStateException("capture replay match differs from public matcher");
                }
                if (matched) {
                    int[] adjusted = captureGroups.clone();
                    shiftOffsets(adjusted, captureStart);
                    if (!Arrays.equals(adjusted, groups)) {
                        throw new IllegalStateException("capture replay groups differ from public matcher");
                    }
                }
            }

            return new Attempt(
                    context, searchStart, searchEnd, groups.clone(), matched,
                    forward, forwardResult, reverse, reverseResult,
                    capture, captureEngine, captureStart, captureEnd,
                    captureKind, captureGroups == null ? null : captureGroups.clone());
        }

        private void validateReplay()
        {
            List<Stage> stages = new ArrayList<>(List.of(
                    Stage.FORWARD,
                    Stage.REVERSE,
                    Stage.CAPTURE,
                    Stage.COMPOSED,
                    Stage.RESULT,
                    Stage.CONTROL,
                    Stage.DIRECT_CAPTURE));
            if (program.canBitState()) {
                stages.add(Stage.BIT_STATE_CAPTURE);
                stages.add(Stage.BIT_STATE_CAPTURE_REUSED);
                stages.add(Stage.DIRECT_BIT_STATE_CAPTURE);
            }
            for (Stage stage : stages) {
                execute(stage);
            }
        }

        long execute(Stage stage)
        {
            return switch (stage) {
                case PUBLIC -> executePublic();
                case FORWARD -> executeForward();
                case REVERSE -> executeReverse();
                case CAPTURE -> executeCapture();
                case COMPOSED -> executeComposed();
                case SETUP -> executeSetup();
                case RESULT -> executeResult();
                case CONTROL -> executeControl();
                case DIRECT_CAPTURE -> executeDirectCapture();
                case BIT_STATE_CAPTURE -> executeBitStateCapture();
                case BIT_STATE_CAPTURE_REUSED -> executeBitStateCaptureReused();
                case DIRECT_BIT_STATE_CAPTURE -> executeDirectBitStateCapture();
            };
        }

        private long executePublic()
        {
            return switch (benchmark.model()) {
                case "count-spans" -> RebarRunner.matchResult(
                        benchmark.haystack(), publicMatcher, RebarRunner.MatchMeasurement.SPAN_LENGTH);
                case "count-captures" -> RebarRunner.matchResult(
                        benchmark.haystack(), publicMatcher, RebarRunner.MatchMeasurement.CAPTURES);
                case "grep-captures" -> {
                    Slice haystack = benchmark.haystack();
                    yield RebarRunner.grepCapturesResult(
                            publicMatcher, haystack, haystack.byteArray(), haystack.byteArrayOffset());
                }
                default -> throw new IllegalStateException("unsupported model: " + benchmark.model());
            };
        }

        private long executeForward()
        {
            long checksum = 1;
            for (Attempt attempt : attempts) {
                if (attempt.forward()) {
                    long result = Dfa.search(program, attempt.context(), attempt.searchStart(), attempt.searchEnd(), program.anchorStart(), FIRST_MATCH, true);
                    requireDfaResult("forward", attempt.forwardResult(), result);
                    checksum = mix(checksum, result);
                }
            }
            return checksum;
        }

        private long executeReverse()
        {
            long checksum = 1;
            for (Attempt attempt : attempts) {
                if (attempt.reverse()) {
                    long result = Dfa.search(reverseProgram, attempt.context(), attempt.searchStart(), attempt.matchEnd(), true, Prog.MatchKind.LONGEST_MATCH, true);
                    requireDfaResult("reverse", attempt.reverseResult(), result);
                    checksum = mix(checksum, result);
                }
            }
            return checksum;
        }

        private long executeCapture()
        {
            long checksum = 1;
            int[] groups = new int[groupCount * 2];
            for (Attempt attempt : attempts) {
                if (attempt.capture()) {
                    boolean matched = searchCapture(
                            attempt.captureEngine(), program, attempt.context(), attempt.captureStart(), attempt.captureEnd(),
                            true, attempt.captureKind(), groups);
                    if (matched != attempt.matched() || !Arrays.equals(groups, attempt.captureGroups())) {
                        throw new IllegalStateException("capture stage changed");
                    }
                    checksum = mix(checksum, matched ? Arrays.hashCode(groups) : 0);
                }
            }
            return checksum;
        }

        private long executeComposed()
        {
            long checksum = 1;
            int[] groups = new int[groupCount * 2];
            for (Attempt attempt : attempts) {
                long forward = attempt.forward()
                        ? Dfa.search(program, attempt.context(), attempt.searchStart(), attempt.searchEnd(), program.anchorStart(), FIRST_MATCH, true)
                        : 0;
                long reverse = 0;
                if (attempt.reverse() && forward != Dfa.SEARCH_NO_MATCH) {
                    int actualMatchEnd = attempt.searchStart() + (int) forward;
                    reverse = Dfa.search(
                            reverseProgram, attempt.context(), attempt.searchStart(), actualMatchEnd,
                            true, Prog.MatchKind.LONGEST_MATCH, true);
                }

                int actualCaptureStart = attempt.captureStart();
                int actualCaptureEnd = attempt.captureEnd();
                if (attempt.capture() && attempt.captureKind() == FULL_MATCH && attempt.forward()) {
                    actualCaptureEnd = attempt.searchStart() + (int) forward;
                    if (program.anchorStart()) {
                        actualCaptureStart = attempt.searchStart();
                    }
                    else {
                        actualCaptureStart = attempt.reverse()
                                ? attempt.searchStart() + (int) reverse
                                : actualCaptureEnd - fixedMatchLength;
                    }
                }
                boolean captured = !attempt.capture() || searchCapture(
                        attempt.captureEngine(), program, attempt.context(), actualCaptureStart, actualCaptureEnd,
                        true, attempt.captureKind(), groups);
                if ((attempt.forward() && forward != attempt.forwardResult()) ||
                        (attempt.reverse() && reverse != attempt.reverseResult()) ||
                        (attempt.capture() && (captured != attempt.matched() || !Arrays.equals(groups, attempt.captureGroups())))) {
                    throw new IllegalStateException("composed stage changed");
                }
                checksum = mix(checksum, forward);
                checksum = mix(checksum, reverse);
                checksum = mix(checksum, attempt.capture() ? Arrays.hashCode(groups) : 0);
            }
            return checksum;
        }

        private long executeSetup()
        {
            if (benchmark.model().equals("grep-captures")) {
                return executeGrepSetup();
            }
            long checksum = 1;
            for (Slice input : resetInputs) {
                setupMatcher.reset(input);
                checksum = mix(checksum, input.length());
            }
            return checksum;
        }

        private long executeGrepSetup()
        {
            Slice haystack = benchmark.haystack();
            byte[] bytes = haystack.byteArray();
            int offset = haystack.byteArrayOffset();
            long checksum = 1;
            int lineStart = 0;
            while (lineStart < haystack.length()) {
                int lineEnd = lineStart;
                while (lineEnd < haystack.length() && bytes[offset + lineEnd] != '\n') {
                    lineEnd++;
                }
                int contentEnd = lineEnd;
                if (contentEnd > lineStart && bytes[offset + contentEnd - 1] == '\r') {
                    contentEnd--;
                }
                setupMatcher.reset(haystack, lineStart, contentEnd);
                checksum = mix(checksum, contentEnd - lineStart);
                lineStart = lineEnd + 1;
            }
            return checksum;
        }

        private long executeResult()
        {
            long result = 0;
            for (Attempt attempt : attempts) {
                if (attempt.matched()) {
                    result += benchmark.model().equals("count-spans")
                            ? attempt.matchEnd() - attempt.matchStart()
                            : participatingGroupCount(attempt.groups());
                }
            }
            if (result != publicResult) {
                throw new IllegalStateException("result stage changed");
            }
            return result;
        }

        private long executeControl()
        {
            long checksum = 1;
            for (Attempt attempt : attempts) {
                checksum = mix(checksum, attempt.searchStart());
                checksum = mix(checksum, attempt.searchEnd());
                checksum = mix(checksum, attempt.forwardResult());
                checksum = mix(checksum, attempt.reverseResult());
                checksum = mix(checksum, attempt.matched() ? Arrays.hashCode(attempt.groups()) : 0);
            }
            return checksum;
        }

        private long executeDirectCapture()
        {
            long checksum = 1;
            int[] groups = new int[groupCount * 2];
            for (Attempt attempt : attempts) {
                CaptureEngine engine = selectCaptureEngine(
                        program,
                        attempt.searchEnd() - attempt.searchStart(),
                        program.anchorStart(),
                        groups.length);
                boolean matched = searchCapture(
                        engine,
                        program,
                        attempt.context(),
                        attempt.searchStart(),
                        attempt.searchEnd(),
                        program.anchorStart(),
                        FIRST_MATCH,
                        groups);
                if (matched != attempt.matched()) {
                    throw new IllegalStateException("direct capture match differs from public matcher");
                }
                if (matched) {
                    shiftOffsets(groups, attempt.searchStart());
                    if (!Arrays.equals(groups, attempt.groups())) {
                        throw new IllegalStateException("direct capture groups differ from public matcher");
                    }
                }
                checksum = mix(checksum, matched ? Arrays.hashCode(groups) : 0);
            }
            return checksum;
        }

        private long executeBitStateCapture()
        {
            if (!program.canBitState()) {
                throw new IllegalStateException("program is not eligible for BitState");
            }
            long checksum = 1;
            int[] groups = new int[groupCount * 2];
            for (Attempt attempt : attempts) {
                if (attempt.capture()) {
                    boolean matched = BitState.search(
                            program,
                            attempt.context(),
                            attempt.captureStart(),
                            attempt.captureEnd(),
                            true,
                            attempt.captureKind(),
                            groups);
                    if (matched != attempt.matched() || !Arrays.equals(groups, attempt.captureGroups())) {
                        throw new IllegalStateException("BitState capture differs from public matcher");
                    }
                    checksum = mix(checksum, matched ? Arrays.hashCode(groups) : 0);
                }
            }
            return checksum;
        }

        private long executeBitStateCaptureReused()
        {
            if (!program.canBitState()) {
                throw new IllegalStateException("program is not eligible for BitState");
            }
            BitState.Workspace workspace = new BitState.Workspace();
            long checksum = 1;
            int[] groups = new int[groupCount * 2];
            for (Attempt attempt : attempts) {
                if (attempt.capture()) {
                    boolean matched = BitState.search(
                            program,
                            attempt.context(),
                            attempt.captureStart(),
                            attempt.captureEnd(),
                            true,
                            attempt.captureKind(),
                            groups,
                            workspace);
                    if (matched != attempt.matched() || !Arrays.equals(groups, attempt.captureGroups())) {
                        throw new IllegalStateException("reused BitState capture differs from public matcher");
                    }
                    checksum = mix(checksum, matched ? Arrays.hashCode(groups) : 0);
                }
            }
            return checksum;
        }

        private long executeDirectBitStateCapture()
        {
            if (!program.canBitState()) {
                throw new IllegalStateException("program is not eligible for BitState");
            }
            long checksum = 1;
            int[] groups = new int[groupCount * 2];
            for (Attempt attempt : attempts) {
                boolean matched = BitState.search(
                        program,
                        attempt.context(),
                        attempt.searchStart(),
                        attempt.searchEnd(),
                        program.anchorStart(),
                        FIRST_MATCH,
                        groups);
                if (matched != attempt.matched()) {
                    throw new IllegalStateException("direct BitState match differs from public matcher");
                }
                if (matched) {
                    shiftOffsets(groups, attempt.searchStart());
                    if (!Arrays.equals(groups, attempt.groups())) {
                        throw new IllegalStateException("direct BitState groups differ from public matcher");
                    }
                }
                checksum = mix(checksum, matched ? Arrays.hashCode(groups) : 0);
            }
            return checksum;
        }

        int matcherResetCount()
        {
            return resetInputs.size();
        }

        long callCount(Stage stage)
        {
            return attempts.stream().filter(attempt -> called(attempt, stage)).count();
        }

        long byteCount(Stage stage)
        {
            return attempts.stream()
                    .filter(attempt -> called(attempt, stage))
                    .mapToLong(attempt -> switch (stage) {
                        case FORWARD -> attempt.searchEnd() - attempt.searchStart();
                        case REVERSE -> attempt.matchEnd() - attempt.searchStart();
                        case CAPTURE -> attempt.captureEnd() - attempt.captureStart();
                        default -> throw new IllegalArgumentException("stage has no byte count: " + stage);
                    })
                    .sum();
        }

        long directCaptureBytes()
        {
            return attempts.stream()
                    .mapToLong(attempt -> attempt.searchEnd() - attempt.searchStart())
                    .sum();
        }

        String captureEngines()
        {
            EnumSet<CaptureEngine> engines = EnumSet.noneOf(CaptureEngine.class);
            attempts.stream().filter(Attempt::capture).map(Attempt::captureEngine).forEach(engines::add);
            StringJoiner joiner = new StringJoiner("+");
            engines.forEach(engine -> joiner.add(engine.name()));
            return joiner.toString();
        }

        boolean anchoredDfaSkipped()
        {
            return program.anchorStart() && attempts.stream().noneMatch(Attempt::forward);
        }

        private static boolean called(Attempt attempt, Stage stage)
        {
            return switch (stage) {
                case FORWARD -> attempt.forward();
                case REVERSE -> attempt.reverse();
                case CAPTURE -> attempt.capture();
                default -> throw new IllegalArgumentException("not an engine stage: " + stage);
            };
        }
    }

    private static List<Slice> inputs(RebarRunner.Benchmark benchmark)
    {
        if (!benchmark.model().equals("grep-captures")) {
            return List.of(benchmark.haystack());
        }

        Slice haystack = benchmark.haystack();
        byte[] bytes = haystack.byteArray();
        int offset = haystack.byteArrayOffset();
        List<Slice> lines = new ArrayList<>();
        int lineStart = 0;
        while (lineStart < haystack.length()) {
            int lineEnd = lineStart;
            while (lineEnd < haystack.length() && bytes[offset + lineEnd] != '\n') {
                lineEnd++;
            }
            int contentEnd = lineEnd;
            if (contentEnd > lineStart && bytes[offset + contentEnd - 1] == '\r') {
                contentEnd--;
            }
            lines.add(haystack.slice(lineStart, contentEnd - lineStart));
            lineStart = lineEnd + 1;
        }
        return lines;
    }

    private static boolean shouldSkipAnchoredDfa(Prog program, int searchLength, int captureCount)
    {
        if (program.isOnePass() &&
                program.supportsOnePassCaptureSlots(captureCount * 2) &&
                searchLength <= 4096 &&
                (captureCount > 1 || searchLength <= 16)) {
            return true;
        }
        return program.canBitState() && searchLength <= program.bitStateTextMaxSize() && captureCount > 1;
    }

    private static CaptureEngine selectCaptureEngine(Prog program, int searchLength, boolean anchored, int captureSlots)
    {
        if (program.isOnePass() && anchored && program.supportsOnePassCaptureSlots(captureSlots)) {
            return CaptureEngine.ONE_PASS;
        }
        if (program.canBitState() && searchLength <= program.bitStateTextMaxSize()) {
            return CaptureEngine.BIT_STATE;
        }
        return CaptureEngine.NFA;
    }

    private static boolean searchCapture(
            CaptureEngine engine,
            Prog program,
            Slice context,
            int start,
            int end,
            boolean anchored,
            Prog.MatchKind matchKind,
            int[] groups)
    {
        Arrays.fill(groups, -1);
        return switch (engine) {
            case ONE_PASS -> OnePass.search(program, context, start, end, anchored, matchKind, groups);
            case BIT_STATE -> BitState.search(program, context, start, end, anchored, matchKind, groups);
            case NFA -> Nfa.search(program, context, start, end, anchored, matchKind, groups);
        };
    }

    private static int nextSearchStart(Slice input, int matchStart, int matchEnd, boolean unicode)
    {
        if (matchStart != matchEnd) {
            return matchEnd;
        }
        if (matchEnd == input.length()) {
            return input.length() + 1;
        }
        if (!unicode) {
            return matchEnd + 1;
        }
        return matchEnd + SliceUtf8.lengthOfCodePointSafe(
                input.byteArray(), input.byteArrayOffset(), input.length(), matchEnd);
    }

    private static int participatingGroupCount(int[] groups)
    {
        int count = 0;
        for (int group = 0; group < groups.length / 2; group++) {
            if (groups[group * 2] >= 0) {
                count++;
            }
        }
        return count;
    }

    private static void shiftOffsets(int[] groups, int delta)
    {
        for (int index = 0; index < groups.length; index++) {
            if (groups[index] >= 0) {
                groups[index] += delta;
            }
        }
    }

    private static void requireDfaResult(String stage, long expected, long actual)
    {
        if (actual != expected) {
            throw new IllegalStateException(stage + " DFA result differs from public matcher: " + actual + " != " + expected);
        }
    }

    private static long mix(long checksum, long value)
    {
        return (checksum * 0x9E3779B97F4A7C15L) ^ value;
    }
}
