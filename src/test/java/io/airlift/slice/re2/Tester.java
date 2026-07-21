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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static java.util.Objects.requireNonNull;

// Ported from RE2 re2/testing/tester.cc (subset; prog engines only)
public final class Tester
{
    private static final int MAX_SUBMATCH = 1 + 16; // $0...$16
    private static final int ONEPASS_MAX_SUBMATCH = 5; // matches upstream Prog::kMaxOnePassCapture

    private enum Anchor
    {
        ANCHORED,
        UNANCHORED,
    }

    public enum MatchKind
    {
        FIRST_MATCH,
        LONGEST_MATCH,
        FULL_MATCH,
    }

    public record Config(int[] parseFlags, MatchKind[] kinds, boolean includeBacktrack, boolean includeOnePass, boolean includeDfa, boolean includePublicApi)
    {
        public Config
        {
            requireNonNull(parseFlags, "parseFlags is null");
            requireNonNull(kinds, "kinds is null");
        }

        public static Config fullMatrix()
        {
            return new Config(
                    new int[] {
                            SINGLE_LINE,
                            SINGLE_LINE | Regexp.LATIN1,
                            MULTI_LINE,
                            MULTI_LINE | Regexp.NON_GREEDY,
                            MULTI_LINE | Regexp.LATIN1,
                    },
                    new MatchKind[] {
                            MatchKind.FIRST_MATCH,
                            MatchKind.LONGEST_MATCH,
                            MatchKind.FULL_MATCH,
                    },
                    true,
                    true,
                    true,
                    true);
        }
    }

    private record ParseMode(int parseFlags, String desc) {}

    private static final int SINGLE_LINE = Regexp.LIKE_PERL;
    private static final int MULTI_LINE = Regexp.LIKE_PERL & ~Regexp.ONE_LINE;

    private static String formatMode(int flags)
    {
        if (flags == SINGLE_LINE) {
            return "single-line";
        }
        if (flags == (SINGLE_LINE | Regexp.LATIN1)) {
            return "single-line, latin1";
        }
        if (flags == MULTI_LINE) {
            return "multiline";
        }
        if (flags == (MULTI_LINE | Regexp.NON_GREEDY)) {
            return "multiline, nonGreedy";
        }
        if (flags == (MULTI_LINE | Regexp.LATIN1)) {
            return "multiline, latin1";
        }
        return String.format("0x%08x", flags);
    }

    private final Slice regexpBytes;
    private final String regexpBase64;
    private final Config config;
    private final List<TestInstance> instances;

    private String failureMessage;

    public Tester(String regexp)
    {
        this(utf8(requireNonNull(regexp, "regexp is null")), Config.fullMatrix());
    }

    public Tester(Slice regexpBytes)
    {
        this(regexpBytes, Config.fullMatrix());
    }

    public Tester(Slice regexpBytes, Config config)
    {
        this.regexpBytes = requireNonNull(regexpBytes, "regexpBytes is null");
        this.config = requireNonNull(config, "config is null");
        byte[] b = new byte[regexpBytes.length()];
        System.arraycopy(regexpBytes.byteArray(), regexpBytes.byteArrayOffset(), b, 0, regexpBytes.length());
        this.regexpBase64 = Base64.getEncoder().encodeToString(b);
        this.instances = new ArrayList<>();
        for (MatchKind kind : config.kinds()) {
            for (int flags : config.parseFlags()) {
                instances.add(new TestInstance(kind, new ParseMode(flags, formatMode(flags))));
            }
        }
    }

    public String failureMessage()
    {
        return failureMessage;
    }

    public boolean error()
    {
        for (TestInstance t : instances) {
            if (t.error()) {
                return true;
            }
        }
        return false;
    }

    public boolean testInput(String text)
    {
        requireNonNull(text, "text is null");
        return testInput(utf8(text));
    }

    public boolean testInput(Slice text)
    {
        requireNonNull(text, "text is null");
        return testInput(text, 0, text.length());
    }

    private boolean testInput(Slice context, int start, int end)
    {
        boolean ok = testInputInContext(context, start, end);
        if (context.length() > 0) {
            ok &= testInputInContext(context, start + 1, end);
            ok &= testInputInContext(context, start, end - 1);
        }
        return ok;
    }

    private boolean testInputInContext(Slice context, int start, int end)
    {
        boolean ok = true;
        for (Anchor anchor : Anchor.values()) {
            ok &= testCase(context, start, end, anchor);
        }
        return ok;
    }

    private boolean testCase(Slice context, int start, int end, Anchor anchor)
    {
        boolean ok = true;
        for (TestInstance t : instances) {
            if (t.error()) {
                failureMessage = t.errorMessage();
                ok = false;
                continue;
            }
            ok &= t.runCase(context, start, end, anchor);
        }
        return ok;
    }

    private static Slice utf8(String s)
    {
        // Use UTF-8 bytes for the raw byte corpus, matching upstream test string literals.
        return Slices.wrappedBuffer(s.getBytes(StandardCharsets.UTF_8));
    }

    private final class TestInstance
    {
        private final MatchKind kind;
        private final ParseMode mode;

        private final boolean error;
        private final String errorMessage;

        private final Regexp parsedRegexp;
        private final int numCaptures;
        private final Prog prog;
        private final Prog reverseProg;
        private final Re2 publicRegexp;

        private TestInstance(MatchKind kind, ParseMode mode)
        {
            this.kind = requireNonNull(kind, "kind is null");
            this.mode = requireNonNull(mode, "mode is null");

            ParseResult parsed;
            try {
                parsed = RegexpParser.parse(regexpBytes, mode.parseFlags());
            }
            catch (RegexpParseException parseException) {
                this.error = true;
                this.errorMessage = "parse failed: regexpBase64=" + regexpBase64 + " mode=" + mode.desc() + " status=" + parseException.statusCode();
                this.parsedRegexp = null;
                this.numCaptures = 0;
                this.prog = null;
                this.reverseProg = null;
                this.publicRegexp = null;
                return;
            }

            Prog compiled;
            Prog reverseCompiled;
            try {
                compiled = Compiler.compile(parsed.regexp(), false, 0);
                reverseCompiled = config.includeDfa() ? Compiler.compile(parsed.regexp(), true, 0) : null;
            }
            catch (RegexpCompileException compileException) {
                this.error = true;
                this.errorMessage = "compile failed: regexpBase64=" + regexpBase64 + " mode=" + mode.desc() + " reason=" + compileException.getMessage();
                this.parsedRegexp = null;
                this.numCaptures = 0;
                this.prog = null;
                this.reverseProg = null;
                this.publicRegexp = null;
                return;
            }

            this.error = false;
            this.errorMessage = null;
            this.parsedRegexp = parsed.regexp();
            this.numCaptures = parsed.numCaptures();
            this.prog = compiled;
            this.reverseProg = reverseCompiled;
            this.publicRegexp = config.includePublicApi() ? Re2TestAccess.compile(Slices.wrappedBuffer(regexpBytes.byteArray(), regexpBytes.byteArrayOffset(), regexpBytes.length()), mode.parseFlags()) : null;
        }

        public boolean error()
        {
            return error;
        }

        public String errorMessage()
        {
            return errorMessage;
        }

        private int submatchCount()
        {
            int submatchCount = 1 + numCaptures;
            if (submatchCount > MAX_SUBMATCH) {
                submatchCount = MAX_SUBMATCH;
            }
            return submatchCount;
        }

        private boolean runCase(Slice context, int start, int end, Anchor anchor)
        {
            int submatchCount = submatchCount();

            Result backtrack = config.includeBacktrack() ? runSearch(Engine.BACKTRACK, context, start, end, anchor, submatchCount) : null;
            Result nfa = runSearch(Engine.NFA, context, start, end, anchor, submatchCount);
            Result bitstate = runSearch(Engine.BITSTATE, context, start, end, anchor, submatchCount);
            Result onepass = config.includeOnePass() ? runSearch(Engine.ONEPASS, context, start, end, anchor, submatchCount) : null;

            Result baseline = chooseBaseline(backtrack, nfa, bitstate, onepass);
            if (baseline == null) {
                // All engines skipped (should not happen for our subset).
                return true;
            }

            boolean ok = true;
            ok &= compare(baseline, backtrack, Engine.BACKTRACK, submatchCount, start, end, anchor);
            ok &= compare(baseline, nfa, Engine.NFA, submatchCount, start, end, anchor);
            ok &= compare(baseline, bitstate, Engine.BITSTATE, submatchCount, start, end, anchor);
            ok &= compare(baseline, onepass, Engine.ONEPASS, submatchCount, start, end, anchor);
            ok &= compareDfa(baseline, context, start, end, anchor);
            ok &= comparePublicApi(baseline, context, start, end, anchor, submatchCount);
            return ok;
        }

        private Result chooseBaseline(Result... results)
        {
            for (Result result : results) {
                if (result != null && !result.skipped) {
                    return result;
                }
            }
            return null;
        }

        private boolean compare(Result expected, Result actual, Engine engine, int submatchCount, int start, int end, Anchor anchor)
        {
            if (actual == null || actual.skipped) {
                return true;
            }
            if (expected.matched != actual.matched) {
                failureMessage = mismatchMessage(engine, "matched differs", start, end, anchor, expected, actual, submatchCount);
                return false;
            }
            if (!expected.matched) {
                return true;
            }
            for (int i = 0; i < 2 * submatchCount; i++) {
                if (expected.submatch[i] != actual.submatch[i]) {
                    failureMessage = mismatchMessage(engine, "submatch differs at index " + i, start, end, anchor, expected, actual, submatchCount);
                    return false;
                }
            }
            return true;
        }

        private String mismatchMessage(Engine engine, String why, int start, int end, Anchor anchor, Result expected, Result actual, int submatchCount)
        {
            return "regexpBase64=" + regexpBase64 +
                    " kind=" + kind +
                    " mode=" + mode.desc() +
                    " anchor=" + anchor +
                    " textRange=(" + start + "," + end + ")" +
                    " why=" + why +
                    " engine=" + engine +
                    " expected=" + formatSubmatch(expected.submatch, submatchCount) +
                    " actual=" + formatSubmatch(actual.submatch, submatchCount);
        }

        private boolean compareDfa(Result baseline, Slice context, int start, int end, Anchor anchor)
        {
            if (!config.includeDfa()) {
                return true;
            }

            boolean anchored = anchor == Anchor.ANCHORED || kind == MatchKind.FULL_MATCH;
            Prog.MatchKind dfaKind = switch (kind) {
                case FIRST_MATCH -> Prog.MatchKind.FIRST_MATCH;
                case LONGEST_MATCH -> Prog.MatchKind.LONGEST_MATCH;
                case FULL_MATCH -> Prog.MatchKind.FULL_MATCH;
            };
            long booleanDfa = Dfa.search(prog, context, start, end, anchored, dfaKind, false);
            if (booleanDfa == Dfa.SEARCH_FAILED) {
                failureMessage = dfaFailureMessage("DFA", start, end, anchor);
                return false;
            }
            if (baseline.matched != (booleanDfa >= 0)) {
                failureMessage = dfaMismatchMessage("DFA", "matched differs", start, end, anchor, baseline, booleanDfa);
                return false;
            }

            long forwardDfa = Dfa.search(prog, context, start, end, anchored, dfaKind, true);
            if (forwardDfa == Dfa.SEARCH_FAILED) {
                failureMessage = dfaFailureMessage("DFA1 forward", start, end, anchor);
                return false;
            }

            if (baseline.matched != (forwardDfa >= 0)) {
                failureMessage = dfaMismatchMessage("DFA1 forward", "matched differs", start, end, anchor, baseline, forwardDfa);
                return false;
            }
            if (!baseline.matched) {
                return true;
            }
            if (baseline.submatch[1] != (int) forwardDfa) {
                failureMessage = dfaMismatchMessage("DFA1 forward", "end differs", start, end, anchor, baseline, forwardDfa);
                return false;
            }

            long reverseDfa = Dfa.search(
                    reverseProg,
                    context,
                    start,
                    start + (int) forwardDfa,
                    true,
                    Prog.MatchKind.LONGEST_MATCH,
                    true);
            if (reverseDfa == Dfa.SEARCH_FAILED) {
                failureMessage = dfaFailureMessage("DFA1 reverse", start, end, anchor);
                return false;
            }
            if (reverseDfa < 0) {
                failureMessage = dfaMismatchMessage("DFA1 reverse", "matched differs", start, end, anchor, baseline, reverseDfa);
                return false;
            }
            if (baseline.submatch[0] != (int) reverseDfa) {
                failureMessage = dfaMismatchMessage("DFA1 reverse", "start differs", start, end, anchor, baseline, reverseDfa);
                return false;
            }
            return true;
        }

        private boolean comparePublicApi(Result baseline, Slice context, int start, int end, Anchor anchor, int submatchCount)
        {
            if (publicRegexp == null || kind == MatchKind.LONGEST_MATCH) {
                return true;
            }

            Re2.Anchor publicAnchor = switch (kind) {
                case FULL_MATCH -> Re2.Anchor.ANCHOR_BOTH;
                case FIRST_MATCH -> anchor == Anchor.ANCHORED ? Re2.Anchor.ANCHOR_START : Re2.Anchor.UNANCHORED;
                case LONGEST_MATCH -> throw new IllegalStateException("longest match handled above");
            };

            Result actual = new Result(submatchCount);
            actual.matched = publicRegexp.matchInto(
                    context,
                    start,
                    end,
                    publicAnchor,
                    actual.submatch);
            if (actual.matched && start != 0) {
                for (int index = 0; index < actual.submatch.length; index++) {
                    if (actual.submatch[index] >= 0) {
                        actual.submatch[index] -= start;
                    }
                }
            }
            return compare(baseline, actual, Engine.PUBLIC_API, submatchCount, start, end, anchor);
        }

        private String dfaFailureMessage(String engine, int start, int end, Anchor anchor)
        {
            return "dfa failed: regexpBase64=" + regexpBase64 +
                    " kind=" + kind +
                    " mode=" + mode.desc() +
                    " anchor=" + anchor +
                    " textRange=(" + start + "," + end + ")" +
                    " engine=" + engine;
        }

        private String dfaMismatchMessage(String engine, String why, int start, int end, Anchor anchor, Result expected, long dfaBoundary)
        {
            return "regexpBase64=" + regexpBase64 +
                    " kind=" + kind +
                    " mode=" + mode.desc() +
                    " anchor=" + anchor +
                    " textRange=(" + start + "," + end + ")" +
                    " why=" + why +
                    " engine=" + engine +
                    " expectedMatch=(" + (expected.matched ? expected.submatch[0] : -1) + "," + (expected.matched ? expected.submatch[1] : -1) + ")" +
                    " actualBoundary=" + (dfaBoundary >= 0 ? (int) dfaBoundary : -1);
        }

        private String formatSubmatch(int[] submatch, int submatchCount)
        {
            StringBuilder builder = new StringBuilder();
            builder.append('[');
            for (int i = 0; i < submatchCount; i++) {
                int start = submatch[2 * i];
                int end = submatch[2 * i + 1];
                if (i > 0) {
                    builder.append(", ");
                }
                if (start < 0 || end < 0) {
                    builder.append("(?,?)");
                }
                else {
                    builder.append('(').append(start).append(',').append(end).append(')');
                }
            }
            builder.append(']');
            return builder.toString();
        }

        private Result runSearch(Engine engine, Slice context, int start, int end, Anchor anchor, int submatchCount)
        {
            Result result = new Result(submatchCount);

            if (prog == null) {
                result.skipped = true;
                return result;
            }

            boolean anchored = anchor == Anchor.ANCHORED;
            boolean longest;
            boolean requireFullMatch = false;
            switch (kind) {
                case FIRST_MATCH -> longest = false;
                case LONGEST_MATCH -> longest = true;
                case FULL_MATCH -> {
                    anchored = true;
                    longest = true;
                    requireFullMatch = true;
                }
                default -> throw new IllegalStateException("unexpected kind: " + kind);
            }

            boolean matched;
            try {
                switch (engine) {
                    case BACKTRACK -> matched = Backtrack.search(prog, context, start, end, anchored, longest, result.submatch);
                    case NFA -> matched = Nfa.search(prog, context, start, end, anchored, (longest ? Prog.MatchKind.LONGEST_MATCH : Prog.MatchKind.FIRST_MATCH), result.submatch);
                    case BITSTATE -> {
                        if (!prog.canBitState()) {
                            result.skipped = true;
                            return result;
                        }
                        matched = BitState.search(prog, context, start, end, anchored, (longest ? Prog.MatchKind.LONGEST_MATCH : Prog.MatchKind.FIRST_MATCH), result.submatch);
                    }
                    case ONEPASS -> {
                        if (!prog.isOnePass() || anchor == Anchor.UNANCHORED || submatchCount > ONEPASS_MAX_SUBMATCH) {
                            result.skipped = true;
                            return result;
                        }
                        matched = OnePass.search(prog, context, start, end, anchored, (longest ? Prog.MatchKind.LONGEST_MATCH : Prog.MatchKind.FIRST_MATCH), result.submatch);
                    }
                    default -> throw new IllegalStateException("unexpected engine: " + engine);
                }
            }
            catch (RuntimeException e) {
                throw new IllegalStateException(
                        "engine=" + engine +
                                " regexpBase64=" + regexpBase64 +
                                " kind=" + kind +
                                " mode=" + mode.desc() +
                                " anchor=" + anchor +
                                " textRange=(" + start + "," + end + ")",
                        e);
            }

            if (matched && requireFullMatch) {
                if (result.submatch[0] != 0 || result.submatch[1] != end - start) {
                    result.reset();
                    matched = false;
                }
            }

            result.matched = matched;
            return result;
        }
    }

    private enum Engine
    {
        BACKTRACK,
        NFA,
        BITSTATE,
        ONEPASS,
        PUBLIC_API,
    }

    private static final class Result
    {
        boolean skipped;
        boolean matched;
        final int[] submatch;

        Result(int submatchCount)
        {
            this.submatch = new int[2 * submatchCount];
            reset();
        }

        void reset()
        {
            for (int i = 0; i < submatch.length; i++) {
                submatch[i] = -1;
            }
            matched = false;
        }
    }
}
