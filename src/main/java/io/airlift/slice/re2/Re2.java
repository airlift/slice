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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("CharUsedInArithmeticContext")
/**
 * A compiled RE2 pattern.
 * <p>
 * Instances are immutable and thread-safe. Matching operates directly on {@link Slice} byte ranges;
 * offsets are byte offsets relative to the logical Slice, not its backing array.
 */
public final class Re2
{
    private enum BooleanPartialMatchStrategy
    {
        GENERAL,
        EXACT_LITERAL,
        NULLABLE_START,
    }

    // Above this size the existing rejection paths are already cheap, while loading
    // minimum-width metadata measurably affected the sub-nanosecond anchored path.
    private static final int MINIMUM_LENGTH_CHECK_LIMIT = 4 * 1024;
    private static final int DIRECT_BIT_STATE_CAPTURE_MINIMUM_LENGTH = 4 * 1024;

    private static final int MAX_ARGS = 16;
    private static final int MAX_REWRITE_SUBMATCH = 1 + MAX_ARGS;

    public enum Anchor
    {
        UNANCHORED,
        ANCHOR_START,
        ANCHOR_BOTH,
    }

    public record ReplaceResult(boolean replaced, Slice result) {}

    public record GlobalReplaceResult(int count, Slice result) {}

    record CheckRewriteResult(boolean ok, Slice error) {}

    record FanoutResult(int maxBucket, int[] histogram) {}

    public static final class Options
    {
        public static final int DEFAULT_MAX_MEMORY = 96 << 20;

        public enum Encoding
        {
            UTF8,
            LATIN1,
        }

        private long maxMemory = DEFAULT_MAX_MEMORY;
        private Encoding encoding = Encoding.UTF8;
        private boolean posixSyntax;
        private boolean longestMatch;
        private boolean literal;
        private boolean neverNewline;
        private boolean dotMatchesNewline;
        private boolean neverCapture;
        private boolean caseSensitive = true;
        private boolean perlClasses;
        private boolean wordBoundary;
        private boolean oneLine;

        private Options() {}

        public static Options defaults()
        {
            return new Options();
        }

        public static Options posix()
        {
            Options options = new Options();
            options.posixSyntax = true;
            options.longestMatch = true;
            return options;
        }

        public static Options latin1()
        {
            Options options = new Options();
            options.encoding = Encoding.LATIN1;
            return options;
        }

        public Options copy()
        {
            Options copy = new Options();
            copy.maxMemory = maxMemory;
            copy.encoding = encoding;
            copy.posixSyntax = posixSyntax;
            copy.longestMatch = longestMatch;
            copy.literal = literal;
            copy.neverNewline = neverNewline;
            copy.dotMatchesNewline = dotMatchesNewline;
            copy.neverCapture = neverCapture;
            copy.caseSensitive = caseSensitive;
            copy.perlClasses = perlClasses;
            copy.wordBoundary = wordBoundary;
            copy.oneLine = oneLine;
            return copy;
        }

        public long maxMemory()
        {
            return maxMemory;
        }

        public Options setMaxMemory(long maxMemory)
        {
            this.maxMemory = maxMemory;
            return this;
        }

        public Encoding encoding()
        {
            return encoding;
        }

        public Options setEncoding(Encoding encoding)
        {
            this.encoding = requireNonNull(encoding, "encoding is null");
            return this;
        }

        public boolean posixSyntax()
        {
            return posixSyntax;
        }

        public Options setPosixSyntax(boolean posixSyntax)
        {
            this.posixSyntax = posixSyntax;
            return this;
        }

        public boolean longestMatch()
        {
            return longestMatch;
        }

        public Options setLongestMatch(boolean longestMatch)
        {
            this.longestMatch = longestMatch;
            return this;
        }

        public boolean literal()
        {
            return literal;
        }

        public Options setLiteral(boolean literal)
        {
            this.literal = literal;
            return this;
        }

        public boolean neverNewline()
        {
            return neverNewline;
        }

        public Options setNeverNewline(boolean neverNewline)
        {
            this.neverNewline = neverNewline;
            return this;
        }

        public boolean dotMatchesNewline()
        {
            return dotMatchesNewline;
        }

        public Options setDotMatchesNewline(boolean dotMatchesNewline)
        {
            this.dotMatchesNewline = dotMatchesNewline;
            return this;
        }

        public boolean neverCapture()
        {
            return neverCapture;
        }

        public Options setNeverCapture(boolean neverCapture)
        {
            this.neverCapture = neverCapture;
            return this;
        }

        public boolean caseSensitive()
        {
            return caseSensitive;
        }

        public Options setCaseSensitive(boolean caseSensitive)
        {
            this.caseSensitive = caseSensitive;
            return this;
        }

        public boolean perlClasses()
        {
            return perlClasses;
        }

        public Options setPerlClasses(boolean perlClasses)
        {
            this.perlClasses = perlClasses;
            return this;
        }

        public boolean wordBoundary()
        {
            return wordBoundary;
        }

        public Options setWordBoundary(boolean wordBoundary)
        {
            this.wordBoundary = wordBoundary;
            return this;
        }

        public boolean oneLine()
        {
            return oneLine;
        }

        public Options setOneLine(boolean oneLine)
        {
            this.oneLine = oneLine;
            return this;
        }

        int parseFlags()
        {
            int flags = Regexp.CLASS_NEWLINE;
            if (encoding == Encoding.LATIN1) {
                flags |= Regexp.LATIN1;
            }
            if (!posixSyntax) {
                flags |= Regexp.LIKE_PERL;
            }
            if (literal) {
                flags |= Regexp.LITERAL;
            }
            if (neverNewline) {
                flags |= Regexp.NEVER_NEWLINE;
            }
            if (dotMatchesNewline) {
                flags |= Regexp.DOT_MATCHES_NEWLINE;
            }
            if (neverCapture) {
                flags |= Regexp.NEVER_CAPTURE;
            }
            if (!caseSensitive) {
                flags |= Regexp.FOLD_CASE;
            }
            if (perlClasses) {
                flags |= Regexp.PERL_CLASSES;
            }
            if (wordBoundary) {
                flags |= Regexp.PERL_WORD_BOUNDARY;
            }
            if (oneLine) {
                flags |= Regexp.ONE_LINE;
            }
            return flags;
        }
    }

    private final int flags;
    private final long maxMemory;
    private final Slice pattern;
    private final Regexp regexp;
    private final Prog partialProg;
    private final boolean longestMatch;
    private final int numCaptures;
    private volatile Map<String, Integer> namedCapturingGroups;
    private volatile Map<Integer, String> capturingGroupNames;

    // The forward program compiles this suffix, and the reverse program reuses it without parsing.
    private final Regexp suffixRegexp;
    private final byte[] requiredPrefix;
    private final boolean requiredPrefixFoldCase;
    private final int matchLength;
    private final int exactLiteralLength;
    private final BooleanPartialMatchStrategy booleanPartialMatchStrategy;

    private volatile Prog reverseProg;
    private volatile boolean reverseProgComputed;
    private volatile BoundedCharacterClassCounter boundedCharacterClassCounter;

    private Re2(
            int flags,
            long maxMemory,
            boolean longestMatch,
            Slice pattern,
            Regexp regexp,
            Regexp suffixRegexp,
            byte[] requiredPrefix,
            boolean requiredPrefixFoldCase,
            int matchLength,
            int exactLiteralLength,
            Prog partialProg,
            int numCaptures,
            Map<String, Integer> namedCapturingGroups,
            Map<Integer, String> capturingGroupNames)
    {
        this.flags = flags;
        this.maxMemory = maxMemory;
        this.longestMatch = longestMatch;
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.regexp = regexp;
        this.suffixRegexp = suffixRegexp;
        this.requiredPrefix = requiredPrefix;
        this.requiredPrefixFoldCase = requiredPrefixFoldCase;
        this.matchLength = matchLength;
        this.exactLiteralLength = exactLiteralLength;
        this.partialProg = requireNonNull(partialProg, "partialProg is null");
        this.booleanPartialMatchStrategy = exactLiteralLength >= 0
                ? BooleanPartialMatchStrategy.EXACT_LITERAL
                : (requiredPrefix == null && partialProg.canMatchEmpty()
                        ? BooleanPartialMatchStrategy.NULLABLE_START
                        : BooleanPartialMatchStrategy.GENERAL);
        this.numCaptures = numCaptures;
        this.namedCapturingGroups = namedCapturingGroups;
        this.capturingGroupNames = capturingGroupNames;
    }

    public static Re2 compile(Slice pattern)
    {
        return compile(pattern, Options.defaults());
    }

    static Re2 compile(Slice pattern, int flags)
    {
        requireNonNull(pattern, "pattern is null");
        return compile(pattern.copy(), flags, Options.DEFAULT_MAX_MEMORY, false);
    }

    public static Re2 compile(Slice pattern, Options options)
    {
        requireNonNull(pattern, "pattern is null");
        requireNonNull(options, "options is null");
        return compile(pattern.copy(), options.parseFlags(), options.maxMemory(), options.longestMatch());
    }

    public ReplaceResult replace(Slice text, Slice rewrite)
    {
        requireNonNull(text, "text is null");
        requireNonNull(rewrite, "rewrite is null");

        int captureSlotCount = 1 + maxSubmatch(rewrite);
        if (captureSlotCount > 1 + capturingGroupCount() || captureSlotCount > MAX_REWRITE_SUBMATCH) {
            return new ReplaceResult(false, text);
        }
        int[] groupOffsets = new int[2 * captureSlotCount];
        if (!matchInto(text, Anchor.UNANCHORED, groupOffsets)) {
            return new ReplaceResult(false, text);
        }

        int matchStart = groupOffsets[0];
        int matchEnd = groupOffsets[1];
        if (matchStart < 0 || matchEnd < matchStart) {
            return new ReplaceResult(false, text);
        }

        DynamicSliceOutput out = new DynamicSliceOutput(text.length() + rewrite.length());
        appendBytes(out, text, 0, matchStart);
        if (!rewrite(out, rewrite, text, groupOffsets, captureSlotCount)) {
            return new ReplaceResult(false, text);
        }
        appendBytes(out, text, matchEnd, text.length() - matchEnd);
        return new ReplaceResult(true, out.slice());
    }

    public GlobalReplaceResult globalReplace(Slice text, Slice rewrite)
    {
        requireNonNull(text, "text is null");
        requireNonNull(rewrite, "rewrite is null");

        int captureSlotCount = 1 + maxSubmatch(rewrite);
        if (captureSlotCount > 1 + capturingGroupCount() || captureSlotCount > MAX_REWRITE_SUBMATCH) {
            return new GlobalReplaceResult(0, text);
        }

        int[] groupOffsets = new int[2 * captureSlotCount];
        byte[] bytes = text.byteArray();
        int base = text.byteArrayOffset();
        int end = base + text.length();
        int position = base;
        int lastEnd = -1;
        int count = 0;
        DynamicSliceOutput out = new DynamicSliceOutput(text.length() + rewrite.length());

        while (position <= end) {
            int start = position - base;
            if (!matchInto(text, start, text.length(), Anchor.UNANCHORED, groupOffsets)) {
                break;
            }

            int matchStart = base + groupOffsets[0];
            int matchEnd = base + groupOffsets[1];
            if (position < matchStart) {
                appendBytes(out, bytes, position, matchStart - position);
            }

            if (matchStart == lastEnd && matchStart == matchEnd) {
                // Disallow empty match at end of last match: skip ahead by one rune/byte.
                int advance = advanceByRuneIfPossible(bytes, position, end, (flags & Regexp.LATIN1) == 0, out);
                if (advance == 0) {
                    break;
                }
                position += advance;
                continue;
            }

            if (!rewrite(out, rewrite, text, groupOffsets, captureSlotCount)) {
                return new GlobalReplaceResult(0, text);
            }
            position = matchEnd;
            lastEnd = position;
            count++;
        }

        if (count == 0) {
            return new GlobalReplaceResult(0, text);
        }
        if (position < end) {
            appendBytes(out, bytes, position, end - position);
        }
        return new GlobalReplaceResult(count, out.slice());
    }

    public Slice extract(Slice text, Slice rewrite)
    {
        requireNonNull(text, "text is null");
        requireNonNull(rewrite, "rewrite is null");

        int captureSlotCount = 1 + maxSubmatch(rewrite);
        if (captureSlotCount > 1 + capturingGroupCount() || captureSlotCount > MAX_REWRITE_SUBMATCH) {
            return null;
        }
        int[] groupOffsets = new int[2 * captureSlotCount];
        if (!matchInto(text, Anchor.UNANCHORED, groupOffsets)) {
            return null;
        }

        DynamicSliceOutput out = new DynamicSliceOutput(rewrite.length());
        if (!rewrite(out, rewrite, text, groupOffsets, captureSlotCount)) {
            return null;
        }
        return out.slice();
    }

    CheckRewriteResult checkRewriteString(Slice rewrite)
    {
        requireNonNull(rewrite, "rewrite is null");
        int maxToken = -1;
        byte[] bytes = rewrite.byteArray();
        int start = rewrite.byteArrayOffset();
        int end = start + rewrite.length();
        for (int i = start; i < end; i++) {
            int c = bytes[i] & 0xFF;
            if (c != '\\') {
                continue;
            }
            if (++i >= end) {
                return new CheckRewriteResult(false, errorBytes("Rewrite schema error: '\\\\' not allowed at end."));
            }
            c = bytes[i] & 0xFF;
            if (c == '\\') {
                continue;
            }
            if (c < '0' || c > '9') {
                return new CheckRewriteResult(false, errorBytes("Rewrite schema error: '\\\\' must be followed by a digit or '\\\\'."));
            }
            int n = c - '0';
            if (maxToken < n) {
                maxToken = n;
            }
        }

        if (maxToken > capturingGroupCount()) {
            String msg = "Rewrite schema requests " + maxToken + " matches, but the regexp only has " +
                    capturingGroupCount() + " parenthesized subexpressions.";
            return new CheckRewriteResult(false, errorBytes(msg));
        }
        return new CheckRewriteResult(true, null);
    }

    public static Slice quote(Slice unquoted)
    {
        requireNonNull(unquoted, "unquoted is null");
        byte[] bytes = unquoted.byteArray();
        int start = unquoted.byteArrayOffset();
        int end = start + unquoted.length();

        DynamicSliceOutput out = new DynamicSliceOutput(unquoted.length() * 2);
        for (int i = start; i < end; i++) {
            int b = bytes[i] & 0xFF;
            if (b == 0) {
                out.writeByte('\\');
                out.writeByte('x');
                out.writeByte('0');
                out.writeByte('0');
                continue;
            }
            if ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || (b & 0x80) != 0) {
                out.writeByte(b);
                continue;
            }
            out.writeByte('\\');
            out.writeByte(b);
        }
        return out.slice();
    }

    private static Re2 compile(Slice pattern, int flags, long maxMemory, boolean longestMatch)
    {
        requireNonNull(pattern, "pattern is null");
        return build(pattern, flags, maxMemory, longestMatch);
    }

    private static Re2 build(Slice pattern, int flags, long maxMemory, boolean longestMatch)
    {
        ParseResult parsed = RegexpParser.parse(pattern, flags);

        long forwardMemory = (maxMemory > 0) ? (maxMemory / 3) * 2 : maxMemory;

        Regexp entireRegexp = parsed.regexp();

        // For "^literal...", compile only the suffix and check the required prefix before matching.
        // Retain the suffix AST so reverse-program construction also avoids the stripped literal.
        Regexp suffixRegexp;
        byte[] requiredPrefix = null;
        boolean prefixFoldCase = false;
        Regexp.RequiredPrefixResult prefixResult = entireRegexp.requiredPrefix();
        if (prefixResult != null) {
            requiredPrefix = prefixResult.prefix().getBytes();
            prefixFoldCase = prefixResult.foldCase();
            suffixRegexp = prefixResult.suffix();
        }
        else {
            suffixRegexp = entireRegexp;
        }

        // Compute captures from the entire regexp (named groups come from the full pattern).
        int numCaptures = parsed.numCaptures();

        Prog partial = Compiler.compile(suffixRegexp, false, forwardMemory);
        MatchLength.Analysis matchLength = MatchLength.analyze(suffixRegexp);
        Slice exactLiteral = entireRegexp.exactLiteral();
        if (exactLiteral != null && exactLiteral.length() != 0) {
            partial.configurePrefixAccel(exactLiteral, false);
        }

        return new Re2(
                flags,
                maxMemory,
                longestMatch,
                pattern,
                entireRegexp,
                suffixRegexp,
                requiredPrefix,
                prefixFoldCase,
                matchLength.encoded(),
                exactLiteral == null ? -1 : exactLiteral.length(),
                partial,
                numCaptures,
                null,
                null);
    }

    int flags()
    {
        return flags;
    }

    public Slice pattern()
    {
        return pattern.copy();
    }

    public int capturingGroupCount()
    {
        return numCaptures;
    }

    public Map<String, Integer> namedCapturingGroups()
    {
        Map<String, Integer> cachedNamedCapturingGroups = namedCapturingGroups;
        if (cachedNamedCapturingGroups != null) {
            return cachedNamedCapturingGroups;
        }
        cachedNamedCapturingGroups = regexp.namedCaptures();
        namedCapturingGroups = cachedNamedCapturingGroups;
        return cachedNamedCapturingGroups;
    }

    boolean isNamedCapturingGroupsComputed()
    {
        return namedCapturingGroups != null;
    }

    public Map<Integer, String> capturingGroupNames()
    {
        Map<Integer, String> cachedCapturingGroupNames = capturingGroupNames;
        if (cachedCapturingGroupNames != null) {
            return cachedCapturingGroupNames;
        }
        cachedCapturingGroupNames = regexp.captureNames();
        capturingGroupNames = cachedCapturingGroupNames;
        return cachedCapturingGroupNames;
    }

    /**
     * Package-private accessor for prefilter extraction (not for general use).
     * Matches upstream RE2::Regexp() const.
     */
    Regexp regexp()
    {
        return regexp;
    }

    int programSize()
    {
        return partialProg.size();
    }

    int reverseProgramSize()
    {
        Prog prog = reverseProg();
        if (prog == null) {
            return -1;
        }
        return prog.size();
    }

    boolean canMatchEmpty()
    {
        return requiredPrefix == null && partialProg.canMatchEmpty();
    }

    boolean isReverseProgramComputed()
    {
        return reverseProgComputed;
    }

    boolean isBoundedCharacterClassCounterComputed()
    {
        return boundedCharacterClassCounter != null;
    }

    Prog forwardProgramForDiagnostics()
    {
        return partialProg;
    }

    Prog reverseProgramIfComputedForDiagnostics()
    {
        return reverseProg;
    }

    boolean hasRequiredPrefixForDiagnostics()
    {
        return requiredPrefix != null;
    }

    int fixedMatchLengthForDiagnostics()
    {
        return MatchLength.fixed(matchLength);
    }

    boolean longestMatchForDiagnostics()
    {
        return longestMatch;
    }

    Slice exactLiteralForDiagnostics()
    {
        return regexp.exactLiteral();
    }

    boolean usesExactLiteralPartialMatchForDiagnostics()
    {
        return booleanPartialMatchStrategy == BooleanPartialMatchStrategy.EXACT_LITERAL;
    }

    boolean canUseDirectBitStateCapture(int searchLength, int captureCount)
    {
        return !longestMatch &&
                searchLength >= DIRECT_BIT_STATE_CAPTURE_MINIMUM_LENGTH &&
                captureCount > 1 &&
                !partialProg.isOnePass() &&
                partialProg.canBitState() &&
                searchLength <= partialProg.bitStateTextMaxSize();
    }

    FanoutResult programFanout()
    {
        return fanout(partialProg);
    }

    FanoutResult reverseProgramFanout()
    {
        Prog prog = reverseProg();
        if (prog == null) {
            return new FanoutResult(-1, new int[0]);
        }
        return fanout(prog);
    }

    public boolean partialMatch(Slice input)
    {
        requireNonNull(input, "input is null");
        if (booleanPartialMatchStrategy != BooleanPartialMatchStrategy.GENERAL) {
            return optimizedPartialMatch(input);
        }
        return matchInto(input, Anchor.UNANCHORED, null);
    }

    private boolean optimizedPartialMatch(Slice input)
    {
        if (booleanPartialMatchStrategy == BooleanPartialMatchStrategy.EXACT_LITERAL) {
            if (exactLiteralLength == 0) {
                return true;
            }
            return partialProg.prefixAccel(input.byteArray(), input.byteArrayOffset(), input.length()) >= 0;
        }
        if (canReturnEmptyAtStartForBooleanMatch(input, 0)) {
            return true;
        }
        return matchInto(input, Anchor.UNANCHORED, null);
    }

    public boolean fullMatch(Slice input)
    {
        return matchInto(input, Anchor.ANCHOR_BOTH, null);
    }

    public Re2Matcher matcher(Slice input)
    {
        return new Re2Matcher(this, input);
    }

    /**
     * Creates a reusable matcher that retains group zero and the requested prefix of explicit
     * capturing groups. Use zero when only complete-match boundaries are required.
     */
    public Re2Matcher matcher(Slice input, int capturingGroupCount)
    {
        return new Re2Matcher(this, input, null, capturingGroupCount);
    }

    Re2Matcher matcher(Slice input, SingleByteMatcher singleByteMatcher)
    {
        return new Re2Matcher(this, input, singleByteMatcher);
    }

    Re2Matcher groupZeroMatcher(Slice input, SingleByteMatcher singleByteMatcher)
    {
        return new Re2Matcher(this, input, singleByteMatcher, 0);
    }

    Dfa.CandidateStartCursor createCandidateStartCursor()
    {
        if (longestMatch ||
                requiredPrefix != null ||
                partialProg.anchorStart() ||
                partialProg.anchorEnd() ||
                partialProg.canMatchEmpty() ||
                MatchLength.fixed(matchLength) >= 0) {
            return null;
        }
        for (int instructionId = 0; instructionId < partialProg.size(); instructionId++) {
            if (partialProg.inst(instructionId).opcode() == InstOp.EMPTY_WIDTH) {
                return null;
            }
        }
        return Dfa.createCandidateStartCursor(partialProg);
    }

    boolean canReturnEmptyAtStart(Slice input, int start)
    {
        return canReturnEmptyAtStart(input, 0, input.length(), start);
    }

    boolean canReturnEmptyAtStartForBooleanMatch(Slice input, int start)
    {
        requireNonNull(input, "input is null");
        if (!canMatchEmpty()) {
            return false;
        }
        Prog.MatchKind matchKind = longestMatch ? Prog.MatchKind.LONGEST_MATCH : Prog.MatchKind.FIRST_MATCH;
        return Dfa.canReturnEmptyAtStart(partialProg, input, start, matchKind);
    }

    boolean canReturnEmptyAtStart(Slice input, int contextStart, int contextEnd, int start)
    {
        if (!canMatchEmpty() || capturingGroupCount() != 0) {
            return false;
        }
        Prog.MatchKind matchKind = longestMatch ? Prog.MatchKind.LONGEST_MATCH : Prog.MatchKind.FIRST_MATCH;
        return Dfa.canReturnEmptyAtStart(partialProg, input, contextStart, contextEnd, start, matchKind);
    }

    long countMatches(Slice text)
    {
        requireNonNull(text, "text is null");
        if (canMatchEmpty()) {
            return -1;
        }

        // A required prefix is stripped from the forward program, and a globally
        // anchored program can produce at most one non-empty match.
        if (requiredPrefix != null || partialProg.anchorStart()) {
            return matchInternal(text, 0, text.length(), Anchor.UNANCHORED, null, null, null) ? 1 : 0;
        }

        BoundedCharacterClassCounter characterClassCounter = boundedCharacterClassCounter();
        if (characterClassCounter != BoundedCharacterClassCounter.unsupported()) {
            return characterClassCounter.count(text);
        }

        Prog.MatchKind matchKind = longestMatch ? Prog.MatchKind.LONGEST_MATCH : Prog.MatchKind.FIRST_MATCH;
        return Dfa.countMatches(partialProg, text, matchKind);
    }

    SingleByteMatcher createSingleByteMatcher()
    {
        if (!mayHaveSingleByteMatcher()) {
            return null;
        }
        return SingleByteMatcher.analyze(suffixRegexp);
    }

    boolean mayHaveSingleByteMatcher()
    {
        return requiredPrefix == null && MatchLength.fixed(matchLength) == 1;
    }

    boolean supportsSingleByteMatcher()
    {
        return mayHaveSingleByteMatcher() && SingleByteMatcher.supports(suffixRegexp);
    }

    SingleByteRepeatMatcher createSingleByteRepeatMatcher()
    {
        if (requiredPrefix != null) {
            return null;
        }
        return SingleByteRepeatMatcher.analyze(suffixRegexp);
    }

    BoundedCharacterClassCounter createBoundedCharacterClassCounter()
    {
        if (requiredPrefix != null || partialProg.anchorStart() || partialProg.anchorEnd()) {
            return null;
        }
        return BoundedCharacterClassCounter.analyze(suffixRegexp);
    }

    private BoundedCharacterClassCounter boundedCharacterClassCounter()
    {
        BoundedCharacterClassCounter counter = boundedCharacterClassCounter;
        if (counter == null) {
            counter = loadBoundedCharacterClassCounter();
        }
        return counter;
    }

    private synchronized BoundedCharacterClassCounter loadBoundedCharacterClassCounter()
    {
        BoundedCharacterClassCounter counter = boundedCharacterClassCounter;
        if (counter == null) {
            counter = createBoundedCharacterClassCounter();
            if (counter == null) {
                counter = BoundedCharacterClassCounter.unsupported();
            }
            boundedCharacterClassCounter = counter;
        }
        return counter;
    }

    /**
     * Matches into a caller-owned capture buffer without allocating capture storage.
     * The buffer contains start/end pairs for group zero followed by each capture.
     * Unmatched and unavailable groups are set to {@code -1, -1}.
     */
    public boolean matchInto(Slice text, Anchor anchor, int[] groups)
    {
        requireNonNull(text, "text is null");
        return matchInto(text, 0, text.length(), anchor, groups);
    }

    public boolean matchInto(Slice text, int start, int end, Anchor anchor, int[] groups)
    {
        return matchInto(text, start, end, anchor, groups, null, null);
    }

    boolean matchInto(
            Slice text,
            int start,
            int end,
            Anchor anchor,
            int[] groups,
            BitState.Workspace bitStateWorkspace,
            Nfa.Workspace nfaWorkspace)
    {
        requireNonNull(text, "text is null");
        return matchRegionInto(text, 0, text.length(), start, end, anchor, groups, bitStateWorkspace, nfaWorkspace);
    }

    boolean matchRegionInto(
            Slice text,
            int contextStart,
            int contextEnd,
            int start,
            int end,
            Anchor anchor,
            int[] groups,
            BitState.Workspace bitStateWorkspace,
            Nfa.Workspace nfaWorkspace)
    {
        requireNonNull(text, "text is null");
        if (groups != null) {
            if ((groups.length % 2) != 0) {
                throw new IllegalArgumentException("groups length must be even: " + groups.length);
            }
            if (groups.length < 2) {
                throw new IllegalArgumentException("groups must include group 0 start/end");
            }
            Arrays.fill(groups, -1);
        }
        return matchInternal(text, contextStart, contextEnd, start, end, anchor, groups, bitStateWorkspace, nfaWorkspace);
    }

    public MatchResult partialMatchResult(Slice input)
    {
        return matchResult(input, Anchor.UNANCHORED);
    }

    public MatchResult fullMatchResult(Slice input)
    {
        return matchResult(input, Anchor.ANCHOR_BOTH);
    }

    public MatchResult matchResult(Slice text, Anchor anchor)
    {
        requireNonNull(text, "text is null");
        return matchResult(text, 0, text.length(), anchor);
    }

    public MatchResult matchResult(Slice text, int start, int end, Anchor anchor)
    {
        requireNonNull(text, "text is null");
        int[] groups = new int[2 * (capturingGroupCount() + 1)];
        if (!matchInto(text, start, end, anchor, groups)) {
            return null;
        }
        return new MatchResult(text, groups, namedCapturingGroups());
    }

    // PERFORMANCE-SENSITIVE ENGINE DISPATCH: phase boundaries, temporary capture storage, and
    // engine-selection branches affect the generated hot path. Do not apply readability-only
    // changes without direct path tests and focused Intel and Graviton benchmarks.
    private boolean matchInternal(
            Slice text,
            int start,
            int end,
            Anchor anchor,
            int[] groupOffsets,
            BitState.Workspace bitStateWorkspace,
            Nfa.Workspace nfaWorkspace)
    {
        return matchInternal(text, 0, text.length(), start, end, anchor, groupOffsets, bitStateWorkspace, nfaWorkspace);
    }

    private boolean matchInternal(
            Slice text,
            int contextStart,
            int contextEnd,
            int start,
            int end,
            Anchor anchor,
            int[] groupOffsets,
            BitState.Workspace bitStateWorkspace,
            Nfa.Workspace nfaWorkspace)
    {
        requireNonNull(text, "text is null");
        if (contextStart < 0 || contextEnd < contextStart || contextEnd > text.length() ||
                start < contextStart || end < start || end > contextEnd) {
            return false;
        }
        if (groupOffsets != null && (groupOffsets.length % 2) != 0) {
            throw new IllegalArgumentException("groupOffsets length must be even: " + groupOffsets.length);
        }

        Anchor anchorMode = (anchor == null) ? Anchor.UNANCHORED : anchor;
        Prog prog = partialProg;

        int strippedPrefixLength = 0;
        if (requiredPrefix != null) {
            if (start != contextStart || !matchesRequiredPrefix(text, start, end - start)) {
                return false;
            }
            strippedPrefixLength = requiredPrefix.length;
            start += strippedPrefixLength;
            if (anchorMode != Anchor.ANCHOR_BOTH) {
                anchorMode = Anchor.ANCHOR_START;
            }
        }

        // If regexp is anchored explicitly, it cannot match a middle slice.
        if (prog.anchorStart() && start != contextStart) {
            return false;
        }
        if (prog.anchorEnd() && end != contextEnd) {
            return false;
        }

        // If regexp is anchored explicitly, update anchor mode
        // so that we can potentially fall into a faster case below.
        if (prog.anchorStart() && prog.anchorEnd()) {
            anchorMode = Anchor.ANCHOR_BOTH;
        }
        else if (prog.anchorStart() && anchorMode != Anchor.ANCHOR_BOTH) {
            anchorMode = Anchor.ANCHOR_START;
        }

        boolean anchored = anchorMode != Anchor.UNANCHORED;
        Prog.MatchKind matchKind = anchorMode == Anchor.ANCHOR_BOTH
                ? Prog.MatchKind.FULL_MATCH
                : (longestMatch ? Prog.MatchKind.LONGEST_MATCH : Prog.MatchKind.FIRST_MATCH);

        int searchLength = end - start;
        // Keep the metadata load off long searches whose existing rejection path is already constant time.
        if (searchLength < MINIMUM_LENGTH_CHECK_LIMIT && searchLength < MatchLength.minimum(matchLength)) {
            return false;
        }

        int[] mutableGroupOffsets = groupOffsets;
        boolean writingCallerGroups = groupOffsets != null;
        int captureCount = (mutableGroupOffsets == null) ? 0 : (mutableGroupOffsets.length / 2);

        // A bounded BitState search can extract all groups in one pass. For eligible long inputs,
        // this avoids forward and reverse DFA boundary searches followed by an NFA capture pass.
        if (anchorMode == Anchor.UNANCHORED &&
                bitStateWorkspace != null &&
                canUseDirectBitStateCapture(searchLength, captureCount)) {
            Slice logicalContext = logicalContext(text, contextStart, contextEnd);
            boolean matched = BitState.search(
                    prog,
                    logicalContext,
                    start - contextStart,
                    end - contextStart,
                    false,
                    matchKind,
                    mutableGroupOffsets,
                    bitStateWorkspace);
            if (matched && start > contextStart) {
                shiftGroupOffsets(mutableGroupOffsets, start - contextStart);
            }
            return matched;
        }

        // ===== Phase 1-2: DFA Forward + Reverse Search (Unanchored) =====
        // Use two-phase DFA to find match boundaries without extracting submatches.
        boolean dfaSearchSkippedOrFailed = false;
        int matchStart = -1;
        int matchEnd = -1;
        // Patterns such as \C* and Latin1 (?s).* accept every byte sequence.
        if (anchorMode == Anchor.ANCHOR_BOTH && prog.matchesAnyByteString() && captureCount <= 1) {
            if (mutableGroupOffsets != null && mutableGroupOffsets.length >= 2) {
                mutableGroupOffsets[0] = start - contextStart - strippedPrefixLength;
                mutableGroupOffsets[1] = end - contextStart;
            }
            return true;
        }

        if (anchorMode == Anchor.UNANCHORED) {
            if (!dfaSearchSkippedOrFailed) {
                if (prog.anchorEnd()) {
                    // The match must end with the search window, so only the reverse DFA is needed.
                    Prog reverseProgram = reverseProg();
                    if (reverseProgram != null) {
                        long reverse = Dfa.search(
                                reverseProgram, text, contextStart, contextEnd, start, end,
                                true,
                                Prog.MatchKind.LONGEST_MATCH,
                                true);

                        if (reverse == Dfa.SEARCH_FAILED) {
                            dfaSearchSkippedOrFailed = true;
                        }
                        else if (reverse == Dfa.SEARCH_NO_MATCH) {
                            return false;
                        }
                        else {
                            if (captureCount == 0) {
                                return true;
                            }
                            matchStart = (int) reverse;
                            matchEnd = searchLength;
                            if (captureCount == 1) {
                                mutableGroupOffsets[0] = matchStart + start - contextStart;
                                mutableGroupOffsets[1] = matchEnd + start - contextStart;
                                return true;
                            }
                        }
                    }
                    else {
                        dfaSearchSkippedOrFailed = true;
                    }
                }
                else {
                    long forward = Dfa.search(prog, text, contextStart, contextEnd, start, end, false, matchKind, captureCount > 0);

                    if (forward == Dfa.SEARCH_FAILED) {
                        dfaSearchSkippedOrFailed = true;
                    }
                    else if (forward == Dfa.SEARCH_NO_MATCH) {
                        return false;
                    }
                    else {
                        matchEnd = (int) forward;
                        if (captureCount == 0) {
                            return true;
                        }
                        if (captureCount == 1 && matchEnd == 0) {
                            mutableGroupOffsets[0] = start - contextStart;
                            mutableGroupOffsets[1] = start - contextStart;
                            return true;
                        }

                        int fixedMatchLength = MatchLength.fixed(matchLength);
                        if (fixedMatchLength >= 0) {
                            matchStart = matchEnd - fixedMatchLength;
                            if (captureCount == 1) {
                                mutableGroupOffsets[0] = matchStart + start - contextStart;
                                mutableGroupOffsets[1] = matchEnd + start - contextStart;
                                return true;
                            }
                        }

                        if (matchStart < 0) {
                            Prog reverseProgram = reverseProg();
                            if (reverseProgram != null) {
                                long reverse = Dfa.search(
                                        reverseProgram, text, contextStart, contextEnd, start, start + matchEnd,
                                        true,
                                        Prog.MatchKind.LONGEST_MATCH,
                                        true);

                                if (reverse == Dfa.SEARCH_FAILED) {
                                    dfaSearchSkippedOrFailed = true;
                                }
                                else if (reverse == Dfa.SEARCH_NO_MATCH) {
                                    return false;
                                }
                                else {
                                    matchStart = (int) reverse;
                                    if (captureCount == 1) {
                                        mutableGroupOffsets[0] = matchStart + start - contextStart;
                                        mutableGroupOffsets[1] = matchEnd + start - contextStart;
                                        return true;
                                    }
                                }
                            }
                            else {
                                dfaSearchSkippedOrFailed = true;
                            }
                        }
                    }
                }
            }
        }

        // ===== Phase 3: Anchored Search Optimization =====
        // For anchored patterns, decide whether to use DFA or skip directly to submatch engines.
        else if (anchorMode == Anchor.ANCHOR_START || anchorMode == Anchor.ANCHOR_BOTH) {
            boolean canOnePass = prog.isOnePass() && prog.supportsOnePassCaptureSlots(captureCount * 2);
            boolean canBitState = prog.canBitState();
            int bitStateMaxSize = prog.bitStateTextMaxSize();

            // Skip DFA if we have fast engines and need submatches
            boolean skipDfa = false;
            if (canOnePass && searchLength <= 4096 && (captureCount > 1 || searchLength <= 16)) {
                skipDfa = true;
            }
            else if (canBitState && searchLength <= bitStateMaxSize && captureCount > 1) {
                skipDfa = true;
            }

            if (!skipDfa) {
                // Try DFA for anchored search
                long result = Dfa.search(prog, text, contextStart, contextEnd, start, end, true, matchKind, writingCallerGroups);

                if (result == Dfa.SEARCH_FAILED) {
                    dfaSearchSkippedOrFailed = true;
                }
                else if (result == Dfa.SEARCH_NO_MATCH) {
                    return false;
                }
                else {
                    matchStart = 0;
                    matchEnd = (int) result;

                    // If no submatch capture requested, return immediately.
                    // Use writingCallerGroups (not mutableGroupOffsets.length) because we may have created
                    // a temporary group array for ANCHOR_BOTH validation, but user doesn't need it.
                    if (!writingCallerGroups) {
                        return true;
                    }
                    // Have match range, need submatches → continue to engines
                }
            }
            else {
                dfaSearchSkippedOrFailed = true;
            }
        }

        // ===== Phase 4: Submatch Engine Cascade =====
        // Extract capturing group positions using OnePass → BitState → NFA cascade.

        boolean hasExactDfaRange = !dfaSearchSkippedOrFailed && matchStart >= 0 && matchEnd >= 0;
        int submatchStart = hasExactDfaRange ? start + matchStart : start;
        int submatchEnd = hasExactDfaRange ? start + matchEnd : end;
        boolean submatchAnchored = hasExactDfaRange || anchored;
        Prog.MatchKind submatchKind = hasExactDfaRange ? Prog.MatchKind.FULL_MATCH : matchKind;

        Slice submatchContext = logicalContext(text, contextStart, contextEnd);
        boolean matched = runSubmatchEngine(
                prog,
                submatchContext,
                submatchStart - contextStart,
                submatchEnd - contextStart,
                submatchAnchored,
                submatchKind,
                mutableGroupOffsets,
                bitStateWorkspace,
                nfaWorkspace);

        if (!matched) {
            return false;
        }

        // ===== Phase 5: Result Adjustment =====
        // Adjust positions for narrowed search range and start offset.

        // First, adjust for narrowed search range (if DFA was used)
        if (!dfaSearchSkippedOrFailed && matchStart > 0 && mutableGroupOffsets != null) {
            shiftGroupOffsets(mutableGroupOffsets, matchStart);
        }

        // Finally, adjust for start offset
        if (writingCallerGroups) {
            shiftGroupOffsets(mutableGroupOffsets, start - contextStart);
            if (strippedPrefixLength > 0 && mutableGroupOffsets[0] >= 0) {
                mutableGroupOffsets[0] -= strippedPrefixLength;
            }
        }

        return true;
    }

    private static Slice logicalContext(Slice text, int contextStart, int contextEnd)
    {
        if (contextStart == 0 && contextEnd == text.length()) {
            return text;
        }
        return text.slice(contextStart, contextEnd - contextStart);
    }

    private static boolean runSubmatchEngine(
            Prog prog,
            Slice context,
            int start,
            int end,
            boolean submatchAnchored,
            Prog.MatchKind matchKind,
            int[] groupOffsets,
            BitState.Workspace bitStateWorkspace,
            Nfa.Workspace nfaWorkspace)
    {
        boolean canOnePass = prog.isOnePass() &&
                submatchAnchored &&
                (groupOffsets == null || prog.supportsOnePassCaptureSlots(groupOffsets.length));
        if (canOnePass) {
            return OnePass.search(prog, context, start, end, true, matchKind, groupOffsets);
        }

        boolean canBitState = prog.canBitState();
        int bitStateMaxSize = prog.bitStateTextMaxSize();
        if (canBitState && end - start <= bitStateMaxSize) {
            return BitState.search(
                    prog,
                    context,
                    start,
                    end,
                    submatchAnchored,
                    matchKind,
                    groupOffsets,
                    bitStateWorkspace);
        }
        return Nfa.search(
                prog,
                context,
                start,
                end,
                submatchAnchored,
                matchKind,
                groupOffsets,
                nfaWorkspace);
    }

    private static void shiftGroupOffsets(int[] groupOffsets, int delta)
    {
        for (int pairIndex = 0; pairIndex < (groupOffsets.length / 2); pairIndex++) {
            int groupOffsetIndex = 2 * pairIndex;
            if (groupOffsets[groupOffsetIndex] >= 0) {
                groupOffsets[groupOffsetIndex] += delta;
                groupOffsets[groupOffsetIndex + 1] += delta;
            }
        }
    }

    private boolean matchesRequiredPrefix(Slice text, int start, int length)
    {
        if (length < requiredPrefix.length) {
            return false;
        }

        byte[] bytes = text.byteArray();
        int offset = text.byteArrayOffset() + start;
        if (!requiredPrefixFoldCase) {
            return Arrays.mismatch(bytes, offset, offset + requiredPrefix.length, requiredPrefix, 0, requiredPrefix.length) < 0;
        }

        for (int index = 0; index < requiredPrefix.length; index++) {
            int actual = bytes[offset + index] & 0xFF;
            int expected = requiredPrefix[index] & 0xFF;
            actual = asciiLower(actual);
            expected = asciiLower(expected);
            if (actual != expected) {
                return false;
            }
        }
        return true;
    }

    private static int asciiLower(int value)
    {
        if ('A' <= value && value <= 'Z') {
            return value + ('a' - 'A');
        }
        return value;
    }

    /**
     * Get or lazily compile the reverse program for this regex.
     * <p>
     * Used in {@code matchInto()} for:
     * <ul>
     * <li>Anchor-end quick rejection: when {@code prog.anchorEnd()}, runs reverse DFA anchored
     *     from text end for constant-time rejection of non-matching text
     * <li>Two-phase DFA search: forward DFA finds match end, reverse DFA finds match start
     * </ul>
     * <p>
     * <b>Memory Budget:</b>
     * Reverse program gets 1/3 of {@code maxMemory}, forward gets 2/3.
     * This split matches upstream RE2::Init behavior.
     *
     * @return the reverse program, or null if compilation failed
     */
    private Prog reverseProg()
    {
        if (reverseProgComputed) {
            return reverseProg;
        }
        synchronized (this) {
            if (!reverseProgComputed) {
                reverseProg = compileReverse();
                reverseProgComputed = true;
            }
        }
        return reverseProg;
    }

    /**
     * Compile the reverse program with appropriate memory budget (1/3 of total).
     * <p>
     * Memory allocation strategy (matching upstream):
     * <ul>
     * <li>Forward program: 2/3 of maxMemory (more common, can have 2 DFAs: first-match and longest-match)
     * <li>Reverse program: 1/3 of maxMemory (less frequent, only used in two-phase search)
     * </ul>
     * <p>
     * The {@code reversed=true} flag causes:
     * <ul>
     * <li>Right-to-left byte scanning in Dfa.search
     * <li>Anchor flag swapping (^ becomes $, $ becomes ^)
     * <li>Program marked with {@code prog.setReversed(true)}
     * </ul>
     * <p>
     * <b>Optimization:</b> Uses stored {@code suffixRegexp} instead of re-parsing the pattern.
     * This saves memory and compilation time.
     *
     * @return compiled reverse program, or null if compilation failed
     */
    private Prog compileReverse()
    {
        if (suffixRegexp == null) {
            return null;
        }
        long reverseMemory = (maxMemory > 0) ? (maxMemory / 3) : maxMemory;
        // Compile from stored suffixRegexp instead of re-parsing.
        Prog prog;
        try {
            prog = Compiler.compile(suffixRegexp, true, reverseMemory);
        }
        catch (RegexpCompileOutOfMemoryException ignored) {
            return null;
        }
        return prog;
    }

    private static int maxSubmatch(Slice rewrite)
    {
        int maxCaptureReference = 0;
        byte[] bytes = rewrite.byteArray();
        int start = rewrite.byteArrayOffset();
        int end = start + rewrite.length();
        for (int i = start; i < end; i++) {
            if ((bytes[i] & 0xFF) != '\\') {
                continue;
            }
            if (++i >= end) {
                break;
            }
            int c = bytes[i] & 0xFF;
            if (c >= '0' && c <= '9') {
                int captureReference = c - '0';
                if (captureReference > maxCaptureReference) {
                    maxCaptureReference = captureReference;
                }
            }
        }
        return maxCaptureReference;
    }

    private static boolean rewrite(DynamicSliceOutput out, Slice rewrite, Slice text, int[] groupOffsets, int captureSlotCount)
    {
        byte[] bytes = rewrite.byteArray();
        int start = rewrite.byteArrayOffset();
        int end = start + rewrite.length();
        for (int i = start; i < end; i++) {
            int c = bytes[i] & 0xFF;
            if (c != '\\') {
                out.writeByte(c);
                continue;
            }
            if (++i >= end) {
                return false;
            }
            c = bytes[i] & 0xFF;
            if (c == '\\') {
                out.writeByte('\\');
                continue;
            }
            if (c < '0' || c > '9') {
                return false;
            }
            int n = c - '0';
            if (n >= captureSlotCount) {
                return false;
            }
            int groupStart = groupOffsets[2 * n];
            int groupEnd = groupOffsets[2 * n + 1];
            if (groupStart >= 0 && groupEnd >= groupStart) {
                appendBytes(out, text, groupStart, groupEnd - groupStart);
            }
        }
        return true;
    }

    private static FanoutResult fanout(Prog prog)
    {
        SparseIntArray fanout = new SparseIntArray(prog.size());
        prog.fanout(fanout);

        int[] buckets = new int[32];
        int size = 0;
        for (int i = 0; i < fanout.size(); i++) {
            int value = fanout.denseValueAt(i);
            if (value == 0) {
                continue;
            }
            int bucket = mostSignificantBit(value);
            if ((value & (value - 1)) != 0) {
                bucket++;
            }
            buckets[bucket]++;
            size = Math.max(size, bucket + 1);
        }
        int[] histogram = new int[size];
        System.arraycopy(buckets, 0, histogram, 0, size);
        return new FanoutResult(size - 1, histogram);
    }

    private static int mostSignificantBit(int value)
    {
        if (value <= 0) {
            throw new IllegalArgumentException("value must be > 0: " + value);
        }
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    private static int advanceByRuneIfPossible(byte[] bytes, int position, int end, boolean utf8, DynamicSliceOutput out)
    {
        if (position >= end) {
            return 0;
        }
        if (utf8) {
            long decoded = Utf8.decode(bytes, position, end);
            int width = Utf8.decodedWidth(decoded);
            int cp = Utf8.decodedCodePoint(decoded);
            if (width > 0) {
                if (!(width == 1 && cp == Utf8.RUNE_ERROR && (bytes[position] & 0xFF) >= 0x80)) {
                    appendBytes(out, bytes, position, width);
                    return width;
                }
            }
        }
        appendBytes(out, bytes, position, 1);
        return 1;
    }

    private static void appendBytes(DynamicSliceOutput out, Slice slice, int offset, int length)
    {
        if (length <= 0) {
            return;
        }
        out.writeBytes(slice.byteArray(), slice.byteArrayOffset() + offset, length);
    }

    private static void appendBytes(DynamicSliceOutput out, byte[] bytes, int offset, int length)
    {
        if (length <= 0) {
            return;
        }
        out.writeBytes(bytes, offset, length);
    }

    private static Slice errorBytes(String message)
    {
        return Slices.wrappedBuffer(message.getBytes(StandardCharsets.UTF_8));
    }
}
