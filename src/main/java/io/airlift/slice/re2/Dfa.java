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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * DFA-based regex search implementation (ported from upstream {@code re2/dfa.cc}).
 * <p>
 * Ordinary transitions use compact object-reference rows with an integer
 * sidecar for abnormal transitions. Eligible long forward searches can use
 * either a bounded depth-two table or an optional absolute-pointer
 * sidecar, while object rows remain authoritative for every fallback.
 */
final class Dfa
{
    static final long MAX_PAIRED_TRANSITION_MEMORY = 64 * 1024;
    private static final long MAX_PARTIAL_PAIRED_TRANSITION_MEMORY = 16 * 1024;
    private static final int CONSERVATIVE_REFERENCE_BYTES = Long.BYTES;
    private static final int MIN_PAIRED_SEARCH_BYTES = 256;
    private static final int MIN_SELECTIVE_SCAN_SEARCH_BYTES = 4 * 1024;
    private static final int MIN_BYTE_SCAN_PRODUCTIVITY_CHECK_BYTES = 64 * 1024;
    private static final int BYTE_SCAN_SAMPLE_SIZE = 64;
    private static final int BYTE_SCAN_SAMPLE_WINDOWS = 4;
    private static final int BYTE_SCAN_SAMPLE_CANDIDATE_LIMIT = 8;
    private static final boolean BYTE_SCAN_PRODUCTIVITY_CHECK_ENABLED = true;
    private static final int MIN_SELF_LOOP_SAMPLE_SEARCH_BYTES = 64 * 1024;
    private static final int SELF_LOOP_SAMPLE_SIZE = 64;
    private static final int SELF_LOOP_SAMPLE_PERCENTAGE = 90;
    private static final int SHORT_PAIRED_SEARCH_BYTES = 16;
    private static final int SHORT_PAIRED_SEARCH_LIMIT = 16;
    private static final int SHORT_ABSOLUTE_POINTER_SEARCH_LIMIT = 16;
    private static final int MAX_CANDIDATE_START_TRANSITIONS = 128;
    private static final int CANDIDATE_NO_MATCH = -1;
    private static final int CANDIDATE_FALLBACK = -2;
    private static final int PREFIX_SCAN = -1;
    private static final int MINIMUM_PRODUCTIVE_FIXED_DISTANCE_SKIP = 16;
    private static final long REFERENCE_BYTES = (sizeOfObjectArray(8) - sizeOfObjectArray(0)) / 8;
    private static final long STATE_DATA_BYTES = instanceSize(StateData.class);
    private static final long STATE_KEY_BYTES = instanceSize(StateKey.class);
    private static final long STATE_CACHE_ENTRY_BYTES = instanceSize(StateCacheEntryShape.class);
    private static final long STATE_CACHE_BYTES = instanceSize(HashMap.class);
    // One conservatively aligned plan record and its 256-byte membership array.
    private static final long FIXED_DISTANCE_BYTE_CANDIDATE_MEMORY = 32L + 16 + 256;
    private static final Prog.FixedDistanceByteCandidates NO_FIXED_DISTANCE_BYTE_CANDIDATES =
            new Prog.FixedDistanceByteCandidates(null, -1);
    private static final Prog.FixedDistanceByteCandidates RETRY_FIXED_DISTANCE_BYTE_CANDIDATES_AFTER_RESET =
            new Prog.FixedDistanceByteCandidates(null, -2);
    private static final RetrySearchException RETRY_SEARCH = new RetrySearchException();
    private static final ReferenceQueue<ReaderSlot> STALE_READER_SLOTS = new ReferenceQueue<>();
    private static final Object READER_SLOTS_LOCK = new Object();
    private static HashSet<ReaderSlotReference> readerSlots = new HashSet<>();
    private static final ThreadLocal<ReaderSlot> THREAD_READER_SLOT = ThreadLocal.withInitial(Dfa::registerReaderSlot);
    private static final AtomicLong NEXT_DFA_ID = new AtomicLong();

    private Dfa() {}

    static boolean nativeAccessEnabled()
    {
        return Dfa.class.getModule().isNativeAccessEnabled();
    }

    public static final long SEARCH_FAILED = Long.MIN_VALUE;
    public static final long SEARCH_NO_MATCH = -1;
    static final long COUNT_UNSUPPORTED = -1;
    static final long CANDIDATE_SEARCH_FALLBACK = Long.MIN_VALUE + 1;

    // Like upstream dfa_should_bail_when_slow. When true (default), the DFA
    // returns SEARCH_FAILED if it detects cache thrashing. Tests can set this
    // to false to force the DFA to keep going despite thrashing.
    static volatile boolean dfaShouldBailWhenSlow = true;

    public static long search(Prog prog, Slice context, int start, int end, boolean anchored, Prog.MatchKind matchKind, boolean wantMatchBoundary)
    {
        requireNonNull(context, "context is null");
        return search(prog, context, 0, context.length(), start, end, anchored, matchKind, wantMatchBoundary);
    }

    static long search(
            Prog prog,
            Slice context,
            int contextStart,
            int contextEnd,
            int start,
            int end,
            boolean anchored,
            Prog.MatchKind matchKind,
            boolean wantMatchBoundary)
    {
        requireNonNull(prog, "prog is null");
        requireNonNull(context, "context is null");
        requireNonNull(matchKind, "matchKind is null");
        if (contextStart < 0 || contextEnd < contextStart || contextEnd > context.length() ||
                start < contextStart || end < start || end > contextEnd) {
            return SEARCH_NO_MATCH;
        }
        int contextOffset = context.byteArrayOffset();
        int ctxBegin = contextOffset + contextStart;
        int ctxEnd = contextOffset + contextEnd;
        int textBegin = contextOffset + start;
        int textEnd = contextOffset + end;

        if (prog.start() == 0) {
            return SEARCH_NO_MATCH;
        }

        boolean anchorStart = prog.anchorStart();
        boolean anchorEnd = prog.anchorEnd();

        boolean caret = anchorStart;
        boolean dollar = anchorEnd;
        if (prog.reversed()) {
            boolean temporary = caret;
            caret = dollar;
            dollar = temporary;
        }
        if (caret && ctxBegin != textBegin) {
            return SEARCH_NO_MATCH;
        }
        if (dollar && ctxEnd != textEnd) {
            return SEARCH_NO_MATCH;
        }

        boolean runForward = !prog.reversed();
        boolean fullMatch = matchKind == Prog.MatchKind.FULL_MATCH;
        boolean endMatch = matchKind != Prog.MatchKind.MANY_MATCH && (anchorEnd || fullMatch);
        if (endMatch && matchKind != Prog.MatchKind.MANY_MATCH) {
            matchKind = Prog.MatchKind.LONGEST_MATCH;
        }

        boolean wantEarliestMatch = !wantMatchBoundary && !endMatch;
        if (wantEarliestMatch && matchKind != Prog.MatchKind.MANY_MATCH) {
            matchKind = Prog.MatchKind.LONGEST_MATCH;
        }

        if (!prog.didFlatten()) {
            throw new IllegalStateException("expected flattened program");
        }
        if (prog.bytemapRange() == 0) {
            throw new IllegalStateException("bytemap not computed");
        }

        DfaInstance.Kind kind = switch (matchKind) {
            case FIRST_MATCH -> DfaInstance.Kind.FIRST_MATCH;
            case LONGEST_MATCH, FULL_MATCH -> DfaInstance.Kind.LONGEST_MATCH;
            case MANY_MATCH -> DfaInstance.Kind.MANY_MATCH;
        };
        DfaInstance dfa = prog.getCachedDfa(kind);

        if (dfa == null || !dfa.ok()) {
            return SEARCH_FAILED;
        }

        boolean anchoredEffective = anchored || anchorStart || fullMatch;
        Prog.FixedDistanceByteCandidates fixedDistanceCandidates =
                        runForward &&
                        !anchoredEffective &&
                        textEnd - textBegin >= MIN_SELECTIVE_SCAN_SEARCH_BYTES ? dfa.fixedDistanceByteCandidates() : null;

        boolean requiresExclusiveSearch = false;
        long observedCacheVersion = 0;
        while (true) {
            boolean exclusiveSearch = requiresExclusiveSearch;
            if (!exclusiveSearch) {
                observedCacheVersion = dfa.cacheVersion();
            }
            ReaderSlot readerSlot = dfa.beginSearch(exclusiveSearch);
            if (exclusiveSearch && dfa.cacheVersion() != observedCacheVersion) {
                dfa.cancelExclusiveSearch();
                requiresExclusiveSearch = false;
                continue;
            }
            try {
                StateData startData = dfa.analyzeStart(context.byteArray(), ctxBegin, ctxEnd, textBegin, textEnd, anchoredEffective, runForward);
                if (startData.offset() == T_DEAD) {
                    return SEARCH_NO_MATCH;
                }
                if (startData.offset() == T_FULL_MATCH) {
                    return (runForward == wantEarliestMatch) ? 0 : end - start;
                }

                int startOffset = startData.offset();
                boolean startIsMatch = (startData.flag() & FLAG_MATCH) != 0;
                byte[] bytes = context.byteArray();
                if (runForward && canReturnEmptyAtStart(dfa, bytes, textBegin, textEnd, startIsMatch, endMatch)) {
                    return 0;
                }
                boolean canPrefixAccel = runForward && prog.canPrefixAccel() && !anchoredEffective && (startData.flag() & FLAG_EMPTY_MASK) == 0;
                boolean canSingleBytePrefixAccel = canPrefixAccel && prog.canUseSingleBytePrefixAccelFastPath();
                byte[] requiredPrefix = prog.getRequiredPrefix();
                boolean canAnchoredLongPrefixIntPath = runForward &&
                        anchoredEffective &&
                        requiredPrefix != null &&
                        requiredPrefix.length >= 16 &&
                        !prog.isRequiredPrefixFoldCase();

                // Prefix accel is intentionally split:
                // - single-byte, case-sensitive prefix ("indexOf-style") has very high false-positive
                //   frequency on random text and tends to bounce start <-> prefix scan many times.
                //   That path is tuned around a fused int[] DFA loop.
                // - all other prefix accel modes (multi-byte FrontAndBack, folded-prefix candidate scanning) stay on
                //   the Object[] loop optimized for general DFA throughput.
                if (!runForward) {
                    return searchBackward(dfa, bytes, textBegin, textEnd, ctxBegin, startOffset, startIsMatch, endMatch, wantEarliestMatch);
                }
                if (canSingleBytePrefixAccel) {
                    return searchForwardPrefixAccelSingleByte(dfa, prog, bytes, textBegin, textEnd, ctxEnd, startOffset, startIsMatch, endMatch, wantEarliestMatch);
                }
                if (canPrefixAccel) {
                    return searchForwardScanAcceleration(
                            dfa,
                            prog,
                            bytes,
                            textBegin,
                            textEnd,
                            ctxEnd,
                            startOffset,
                            startIsMatch,
                            endMatch,
                            wantEarliestMatch,
                            null,
                            PREFIX_SCAN);
                }
                if (canAnchoredLongPrefixIntPath) {
                    return searchForwardAnchoredLongPrefixInt(dfa, bytes, textBegin, textEnd, ctxEnd, startOffset, startIsMatch, endMatch, wantEarliestMatch);
                }
                boolean fixedDistanceScanRejected = false;
                if (fixedDistanceCandidates != null &&
                        canUseFixedDistanceByteAcceleration(anchoredEffective, true, startData.flag(), textEnd - textBegin)) {
                    fixedDistanceScanRejected = BYTE_SCAN_PRODUCTIVITY_CHECK_ENABLED && !isByteCandidateScanProductive(
                            fixedDistanceCandidates.candidates(),
                            bytes,
                            textBegin + fixedDistanceCandidates.offset(),
                            textEnd);
                    if (!fixedDistanceScanRejected) {
                        return searchForwardScanAcceleration(
                                dfa,
                                prog,
                                bytes,
                                textBegin,
                                textEnd,
                                ctxEnd,
                                startOffset,
                                startIsMatch,
                                endMatch,
                                wantEarliestMatch,
                                fixedDistanceCandidates.candidates(),
                                fixedDistanceCandidates.offset());
                    }
                }
                if (!startIsMatch &&
                        textEnd - textBegin < MIN_PAIRED_SEARCH_BYTES &&
                        dfa.shouldUseAbsolutePointersForShortSearch()) {
                    dfa.requestAbsolutePointerTransitions();
                    if (dfa.hasAbsolutePointerTransitions(startOffset)) {
                        return searchForward(dfa, bytes, textBegin, textEnd, ctxEnd, startOffset, false, false, endMatch, wantEarliestMatch);
                    }
                }
                if (canUseStartByteAcceleration(dfa, anchoredEffective, true, startData.flag())) {
                    if (BYTE_SCAN_PRODUCTIVITY_CHECK_ENABLED && !isByteCandidateScanProductive(dfa.startByteCandidates, bytes, textBegin, textEnd)) {
                        dfa.byteScanFallbackCount++;
                    }
                    else {
                        return searchForwardScanAcceleration(
                                dfa,
                                prog,
                                bytes,
                                textBegin,
                                textEnd,
                                ctxEnd,
                                startOffset,
                                startIsMatch,
                                endMatch,
                                wantEarliestMatch,
                                dfa.startByteCandidates,
                                0);
                    }
                }
                else if (fixedDistanceScanRejected) {
                    dfa.byteScanFallbackCount++;
                }
                if (!startIsMatch && textEnd - textBegin >= MIN_PAIRED_SEARCH_BYTES) {
                    dfa.requestPairedTransitions();
                    if (dfa.hasPairedTransitions(startOffset)) {
                        return searchForwardPairs(dfa, bytes, textBegin, textEnd, ctxEnd, startOffset, endMatch, wantEarliestMatch);
                    }
                }
                return searchForward(dfa, bytes, textBegin, textEnd, ctxEnd, startOffset, startIsMatch, false, endMatch, wantEarliestMatch);
            }
            catch (RetrySearchException ignored) {
                if (exclusiveSearch) {
                    throw new AssertionError("exclusive DFA search requested a retry");
                }
                requiresExclusiveSearch = true;
            }
            finally {
                dfa.endSearch(exclusiveSearch, readerSlot);
            }
        }
    }

    static long countMatches(Prog prog, Slice context, Prog.MatchKind matchKind)
    {
        requireNonNull(prog, "prog is null");
        requireNonNull(context, "context is null");
        requireNonNull(matchKind, "matchKind is null");
        if (matchKind != Prog.MatchKind.FIRST_MATCH && matchKind != Prog.MatchKind.LONGEST_MATCH) {
            throw new IllegalArgumentException("unsupported count match kind: " + matchKind);
        }
        if (prog.start() == 0) {
            return 0;
        }
        if (prog.anchorStart()) {
            return COUNT_UNSUPPORTED;
        }
        if (!prog.didFlatten()) {
            throw new IllegalStateException("expected flattened program");
        }
        if (prog.bytemapRange() == 0) {
            throw new IllegalStateException("bytemap not computed");
        }

        Prog.MatchKind effectiveMatchKind = prog.anchorEnd() ? Prog.MatchKind.LONGEST_MATCH : matchKind;
        DfaInstance.Kind kind = effectiveMatchKind == Prog.MatchKind.FIRST_MATCH
                ? DfaInstance.Kind.FIRST_MATCH
                : DfaInstance.Kind.LONGEST_MATCH;
        DfaInstance dfa = prog.getCachedDfa(kind);
        if (dfa == null || !dfa.ok()) {
            return COUNT_UNSUPPORTED;
        }

        byte[] bytes = context.byteArray();
        int contextBegin = context.byteArrayOffset();
        int contextEnd = contextBegin + context.length();
        int textBegin = contextBegin;
        Prog.FixedDistanceByteCandidates fixedDistanceCandidates = context.length() >= MIN_SELECTIVE_SCAN_SEARCH_BYTES
                ? dfa.fixedDistanceByteCandidates()
                : null;
        long count = 0;
        boolean requiresExclusiveSearch = false;
        long observedCacheVersion = 0;
        while (true) {
            boolean exclusiveSearch = requiresExclusiveSearch;
            if (!exclusiveSearch) {
                observedCacheVersion = dfa.cacheVersion();
            }
            ReaderSlot readerSlot = dfa.beginSearch(exclusiveSearch);
            if (exclusiveSearch && dfa.cacheVersion() != observedCacheVersion) {
                dfa.cancelExclusiveSearch();
                requiresExclusiveSearch = false;
                continue;
            }
            try {
                while (textBegin <= contextEnd) {
                    long matchEnd = searchForwardForCount(
                            dfa,
                            prog,
                            context,
                            bytes,
                            contextBegin,
                            contextEnd,
                            textBegin,
                            fixedDistanceCandidates);
                    if (matchEnd == SEARCH_FAILED) {
                        return COUNT_UNSUPPORTED;
                    }
                    if (matchEnd == SEARCH_NO_MATCH) {
                        return count;
                    }
                    if (matchEnd <= 0) {
                        return COUNT_UNSUPPORTED;
                    }
                    textBegin += (int) matchEnd;
                    count++;
                }
                return count;
            }
            catch (RetrySearchException ignored) {
                if (exclusiveSearch) {
                    throw new AssertionError("exclusive DFA count requested a retry");
                }
                requiresExclusiveSearch = true;
            }
            finally {
                dfa.endSearch(exclusiveSearch, readerSlot);
            }
        }
    }

    static CandidateStartCursor createCandidateStartCursor(Prog prog)
    {
        requireNonNull(prog, "prog is null");
        DfaInstance dfa = prog.getCachedDfa(DfaInstance.Kind.FIRST_MATCH);
        if (dfa == null || !dfa.ok() || !dfa.canStartByteAcceleration()) {
            return null;
        }
        return new CandidateStartCursor(dfa);
    }

    // PERFORMANCE-SENSITIVE HOT LOOP: this cursor amortizes reader registration and
    // boundary discovery across dense repeated matches. Keep candidate scanning and
    // anchored verification in this concrete loop, and preserve the linear work budget.
    static long searchCandidateBoundary(
            CandidateStartCursor cursor,
            Slice context,
            int contextStart,
            int contextEnd,
            int start)
    {
        requireNonNull(cursor, "cursor is null");
        requireNonNull(context, "context is null");
        if (cursor.disabled ||
                contextStart < 0 || contextEnd < contextStart || contextEnd > context.length() ||
                start < contextStart || start > contextEnd) {
            return CANDIDATE_SEARCH_FALLBACK;
        }

        byte[] bytes = context.byteArray();
        int contextBegin = context.byteArrayOffset() + contextStart;
        int contextLimit = context.byteArrayOffset() + contextEnd;
        int searchBegin = context.byteArrayOffset() + start;

        boolean requiresExclusiveSearch = false;
        long observedCacheVersion = 0;
        while (true) {
            boolean exclusiveSearch = requiresExclusiveSearch;
            if (!exclusiveSearch) {
                observedCacheVersion = cursor.dfa.cacheVersion();
            }
            ReaderSlot readerSlot = cursor.dfa.beginSearch(exclusiveSearch);
            if (exclusiveSearch && cursor.dfa.cacheVersion() != observedCacheVersion) {
                cursor.dfa.cancelExclusiveSearch();
                requiresExclusiveSearch = false;
                continue;
            }
            try {
                long result = searchCandidateBoundary(cursor, bytes, contextBegin, contextLimit, searchBegin);
                if (result == CANDIDATE_SEARCH_FALLBACK) {
                    cursor.disabled = true;
                    cursor.fallbackCount++;
                }
                else if (result >= 0) {
                    cursor.routeCount++;
                }
                return result;
            }
            catch (RetrySearchException ignored) {
                if (exclusiveSearch) {
                    cursor.disabled = true;
                    cursor.fallbackCount++;
                    return CANDIDATE_SEARCH_FALLBACK;
                }
                requiresExclusiveSearch = true;
            }
            finally {
                cursor.dfa.endSearch(exclusiveSearch, readerSlot);
            }
        }
    }

    private static long searchCandidateBoundary(
            CandidateStartCursor cursor,
            byte[] bytes,
            int contextBegin,
            int contextEnd,
            int searchBegin)
    {
        for (int candidate = searchBegin; candidate < contextEnd; candidate++) {
            if (!cursor.consumeWork()) {
                return CANDIDATE_SEARCH_FALLBACK;
            }
            if (!cursor.dfa.isStartByteCandidate(bytes[candidate] & 0xFF)) {
                continue;
            }
            if (!cursor.consumeWork()) {
                return CANDIDATE_SEARCH_FALLBACK;
            }

            int matchEnd = searchAnchoredCandidate(cursor, bytes, contextBegin, contextEnd, candidate);
            if (matchEnd == CANDIDATE_FALLBACK) {
                return CANDIDATE_SEARCH_FALLBACK;
            }
            if (matchEnd >= 0) {
                int relativeStart = candidate - contextBegin;
                int relativeEnd = matchEnd - contextBegin;
                return ((long) relativeStart << 32) | (relativeEnd & 0xFFFF_FFFFL);
            }
        }
        return SEARCH_NO_MATCH;
    }

    private static int searchAnchoredCandidate(
            CandidateStartCursor cursor,
            byte[] bytes,
            int contextBegin,
            int contextEnd,
            int candidate)
    {
        DfaInstance dfa = cursor.dfa;
        StateData startData = dfa.analyzeStart(bytes, contextBegin, contextEnd, candidate, contextEnd, true, true);
        if (startData.offset() == T_DEAD) {
            return CANDIDATE_NO_MATCH;
        }
        if (startData.offset() == T_FULL_MATCH || (startData.flag() & FLAG_MATCH) != 0) {
            return CANDIDATE_FALLBACK;
        }

        int stateOffset = startData.offset();
        int[] transitions = dfa.transitions;
        byte[] byteMap = dfa.bytemap;
        int nextSize = dfa.nextSize;
        Object[] stateReferences = dfa.stateRefArrays[stateOffset / nextSize];
        int transitionCount = 0;
        int lastMatchBoundary = CANDIDATE_NO_MATCH;

        for (int position = candidate; position < contextEnd; position++) {
            if (transitionCount++ == MAX_CANDIDATE_START_TRANSITIONS || !cursor.consumeWork()) {
                return CANDIDATE_FALLBACK;
            }

            int byteClass = byteMap[bytes[position] & 0xFF] & 0xFF;
            Object nextReference = getStateReference(stateReferences, byteClass);
            if (nextReference != null) {
                stateReferences = (Object[]) nextReference;
                continue;
            }

            stateOffset = ((StateData) getStateReference(stateReferences, nextSize)).offset();
            int nextState = getTransition(transitions, stateOffset + byteClass);
            if (nextState == T_UNCOMPUTED) {
                nextState = dfa.computeTransition(stateOffset, bytes[position] & 0xFF);
                transitions = dfa.transitions;
                if (nextState == Integer.MIN_VALUE) {
                    return CANDIDATE_FALLBACK;
                }
            }
            if (nextState == T_DEAD) {
                return lastMatchBoundary;
            }
            if (nextState == T_FULL_MATCH) {
                return contextEnd;
            }
            if ((nextState & T_MATCH_BIT) != 0) {
                lastMatchBoundary = position;
                nextState &= ~T_MATCH_BIT;
            }

            stateOffset = nextState;
            stateReferences = dfa.stateRefArrays[stateOffset / nextSize];
        }

        // Cached object transitions advance the row reference without updating the offset.
        stateOffset = ((StateData) getStateReference(stateReferences, nextSize)).offset();
        if (transitionCount == MAX_CANDIDATE_START_TRANSITIONS || !cursor.consumeWork()) {
            return CANDIDATE_FALLBACK;
        }
        int finalState = getTransition(transitions, stateOffset + dfa.bytemapRange);
        if (finalState == T_UNCOMPUTED) {
            finalState = dfa.computeTransition(stateOffset, BYTE_END_TEXT);
            if (finalState == Integer.MIN_VALUE) {
                return CANDIDATE_FALLBACK;
            }
        }
        if (finalState == T_FULL_MATCH || (finalState > 0 && (finalState & T_MATCH_BIT) != 0)) {
            return contextEnd;
        }
        return lastMatchBoundary;
    }

    private static long searchForwardForCount(
            DfaInstance dfa,
            Prog prog,
            Slice context,
            byte[] bytes,
            int contextBegin,
            int contextEnd,
            int textBegin,
            Prog.FixedDistanceByteCandidates fixedDistanceCandidates)
    {
        if (prog.anchorStart() && contextBegin != textBegin) {
            return SEARCH_NO_MATCH;
        }

        boolean endMatch = prog.anchorEnd();
        StateData startData = dfa.analyzeStart(context, textBegin, contextEnd, false, true);
        if (startData.offset() == T_DEAD) {
            return SEARCH_NO_MATCH;
        }
        if (startData.offset() == T_FULL_MATCH) {
            return contextEnd - textBegin;
        }

        int startOffset = startData.offset();
        boolean startIsMatch = (startData.flag() & FLAG_MATCH) != 0;
        if (canReturnEmptyAtStart(dfa, bytes, textBegin, contextEnd, startIsMatch, endMatch)) {
            return 0;
        }

        boolean canPrefixAccel = prog.canPrefixAccel() && (startData.flag() & FLAG_EMPTY_MASK) == 0;
        if (canPrefixAccel && prog.canUseSingleBytePrefixAccelFastPath()) {
            return searchForwardPrefixAccelSingleByte(
                    dfa, prog, bytes, textBegin, contextEnd, contextEnd, startOffset, startIsMatch, endMatch, false);
        }
        if (canPrefixAccel) {
            return searchForwardScanAcceleration(
                    dfa,
                    prog,
                    bytes,
                    textBegin,
                    contextEnd,
                    contextEnd,
                    startOffset,
                    startIsMatch,
                    endMatch,
                    false,
                    null,
                    PREFIX_SCAN);
        }

        if (contextEnd - textBegin < MIN_SELECTIVE_SCAN_SEARCH_BYTES) {
            fixedDistanceCandidates = null;
        }
        boolean fixedDistanceScanRejected = false;
        if (fixedDistanceCandidates != null &&
                canUseFixedDistanceByteAcceleration(false, true, startData.flag(), contextEnd - textBegin)) {
            fixedDistanceScanRejected = BYTE_SCAN_PRODUCTIVITY_CHECK_ENABLED && !isByteCandidateScanProductive(
                    fixedDistanceCandidates.candidates(),
                    bytes,
                    textBegin + fixedDistanceCandidates.offset(),
                    contextEnd);
            if (!fixedDistanceScanRejected) {
                return searchForwardScanAcceleration(
                        dfa,
                        prog,
                        bytes,
                        textBegin,
                        contextEnd,
                        contextEnd,
                        startOffset,
                        startIsMatch,
                        endMatch,
                        false,
                        fixedDistanceCandidates.candidates(),
                        fixedDistanceCandidates.offset());
            }
        }
        if (canUseStartByteAcceleration(dfa, false, true, startData.flag())) {
            if (BYTE_SCAN_PRODUCTIVITY_CHECK_ENABLED &&
                    !isByteCandidateScanProductive(dfa.startByteCandidates, bytes, textBegin, contextEnd)) {
                dfa.byteScanFallbackCount++;
            }
            else {
                return searchForwardScanAcceleration(
                        dfa,
                        prog,
                        bytes,
                        textBegin,
                        contextEnd,
                        contextEnd,
                        startOffset,
                        startIsMatch,
                        endMatch,
                        false,
                        dfa.startByteCandidates,
                        0);
            }
        }
        else if (fixedDistanceScanRejected) {
            dfa.byteScanFallbackCount++;
        }

        if (!startIsMatch && contextEnd - textBegin >= MIN_PAIRED_SEARCH_BYTES) {
            dfa.requestPairedTransitions();
            if (dfa.hasPairedTransitions(startOffset)) {
                return searchForwardPairs(dfa, bytes, textBegin, contextEnd, contextEnd, startOffset, endMatch, false);
            }
        }
        return searchForward(dfa, bytes, textBegin, contextEnd, contextEnd, startOffset, startIsMatch, false, endMatch, false);
    }

    static boolean canUseStartByteAcceleration(DfaInstance dfa, boolean anchored, boolean runForward, int startFlags)
    {
        return runForward && !anchored && dfa.canStartByteAcceleration() && (startFlags & FLAG_EMPTY_MASK) == 0;
    }

    static boolean canUseFixedDistanceByteAcceleration(boolean anchored, boolean runForward, int startFlags, int searchLength)
    {
        return searchLength >= MIN_SELECTIVE_SCAN_SEARCH_BYTES &&
                runForward &&
                !anchored &&
                (startFlags & FLAG_EMPTY_MASK) == 0;
    }

    private static boolean isByteCandidateScanProductive(byte[] candidates, byte[] bytes, int position, int textEnd)
    {
        int searchLength = textEnd - position;
        if (searchLength < MIN_BYTE_SCAN_PRODUCTIVITY_CHECK_BYTES) {
            return true;
        }

        int windowSize = BYTE_SCAN_SAMPLE_SIZE / BYTE_SCAN_SAMPLE_WINDOWS;
        int windowRange = searchLength - windowSize;
        int candidateCount = 0;
        for (int window = 0; window < BYTE_SCAN_SAMPLE_WINDOWS; window++) {
            int windowStart = position + (int) (((long) windowRange * window) / (BYTE_SCAN_SAMPLE_WINDOWS - 1));
            int windowEnd = windowStart + windowSize;
            for (int samplePosition = windowStart; samplePosition < windowEnd; samplePosition++) {
                if (candidates[bytes[samplePosition] & 0xFF] != 0 &&
                        ++candidateCount >= BYTE_SCAN_SAMPLE_CANDIDATE_LIMIT) {
                    return false;
                }
            }
        }
        return true;
    }

    static boolean canReturnEmptyAtStart(DfaInstance dfa, byte[] bytes, int position, int textEnd, boolean startIsMatch, boolean endMatch)
    {
        return startIsMatch &&
                !endMatch &&
                dfa.canUseNullableStartByteCheck() &&
                (position == textEnd || !dfa.isStartByteCandidate(bytes[position] & 0xFF));
    }

    public static boolean canReturnEmptyAtStart(Prog prog, Slice text, int start, Prog.MatchKind matchKind)
    {
        requireNonNull(text, "text is null");
        return canReturnEmptyAtStart(prog, text, 0, text.length(), start, matchKind);
    }

    static boolean canReturnEmptyAtStart(
            Prog prog,
            Slice text,
            int contextStart,
            int contextEnd,
            int start,
            Prog.MatchKind matchKind)
    {
        requireNonNull(prog, "prog is null");
        requireNonNull(text, "text is null");
        requireNonNull(matchKind, "matchKind is null");
        if (contextStart < 0 || contextEnd < contextStart || contextEnd > text.length() ||
                start < contextStart || start > contextEnd) {
            return false;
        }

        DfaInstance.Kind kind = switch (matchKind) {
            case FIRST_MATCH -> DfaInstance.Kind.FIRST_MATCH;
            case LONGEST_MATCH, FULL_MATCH -> DfaInstance.Kind.LONGEST_MATCH;
            case MANY_MATCH -> DfaInstance.Kind.MANY_MATCH;
        };
        DfaInstance dfa = prog.getCachedDfa(kind);
        if (dfa == null || !dfa.ok() || !dfa.canUseNullableStartByteCheck()) {
            return false;
        }
        return start == contextEnd || !dfa.isStartByteCandidate(text.getUnsignedByte(start));
    }

    public static long search(Prog prog, Slice text, boolean anchored, Prog.MatchKind matchKind, boolean wantMatchBoundary)
    {
        return search(prog, text, 0, text.length(), anchored, matchKind, wantMatchBoundary);
    }

    public static boolean searchMany(Prog prog, Slice text, SparseSet matches)
    {
        requireNonNull(prog, "prog is null");
        requireNonNull(text, "text is null");
        if (matches == null) {
            return search(prog, text, 0, text.length(), true, Prog.MatchKind.MANY_MATCH, false) >= 0;
        }
        if (prog.start() == 0) {
            return false;
        }

        DfaInstance dfa = prog.getCachedDfa(DfaInstance.Kind.MANY_MATCH);
        if (dfa == null || !dfa.ok()) {
            return false;
        }

        boolean requiresExclusiveSearch = false;
        long observedCacheVersion = 0;
        while (true) {
            boolean exclusiveSearch = requiresExclusiveSearch;
            if (!exclusiveSearch) {
                observedCacheVersion = dfa.cacheVersion();
            }
            ReaderSlot readerSlot = dfa.beginSearch(exclusiveSearch);
            if (exclusiveSearch && dfa.cacheVersion() != observedCacheVersion) {
                dfa.cancelExclusiveSearch();
                requiresExclusiveSearch = false;
                continue;
            }
            try {
                int textBegin = text.byteArrayOffset();
                int textEnd = textBegin + text.length();
                StateData start = dfa.analyzeStart(text, textBegin, textEnd, true, true);
                if (start.offset() == T_DEAD) {
                    return false;
                }

                int startOffset = start.offset();
                int stateOffset = startOffset;
                boolean matched = (start.flag() & FLAG_MATCH) != 0;
                if (matched) {
                    recordMatches(start, matches);
                }

                byte[] bytes = text.byteArray();
                int position = textBegin;
                int[] transitions = dfa.transitions;
                while (position < textEnd) {
                    int inputByte = bytes[position++] & 0xFF;
                    int transition = getTransition(transitions, stateOffset + (dfa.bytemap[inputByte] & 0xFF));
                    if (transition == T_UNCOMPUTED) {
                        transition = dfa.computeTransition(stateOffset, inputByte);
                        transitions = dfa.transitions;
                    }
                    if (transition == Integer.MIN_VALUE) {
                        StateSaver savedStart = new StateSaver(dfa, startOffset);
                        StateSaver savedState = new StateSaver(dfa, stateOffset);
                        RestoredStates restoredStates = dfa.resetAndRestore(savedStart, savedState);
                        startOffset = restoredStates.startOffset();
                        stateOffset = restoredStates.stateOffset();
                        if (startOffset < 0 || stateOffset < 0) {
                            return false;
                        }
                        transition = dfa.computeTransition(stateOffset, inputByte);
                        transitions = dfa.transitions;
                        if (transition == Integer.MIN_VALUE) {
                            return false;
                        }
                    }
                    if (transition == T_DEAD) {
                        return matched;
                    }
                    if (transition == T_FULL_MATCH) {
                        return true;
                    }

                    boolean stateMatched = (transition & T_MATCH_BIT) != 0;
                    stateOffset = transition & ~T_MATCH_BIT;
                    if (stateMatched) {
                        matched = true;
                        recordMatches(dfa.stateData[stateOffset / dfa.nextSize], matches);
                    }
                }

                int transition = getTransition(transitions, stateOffset + dfa.bytemapRange);
                if (transition == T_UNCOMPUTED) {
                    transition = dfa.computeTransition(stateOffset, BYTE_END_TEXT);
                }
                if (transition == Integer.MIN_VALUE) {
                    StateSaver savedState = new StateSaver(dfa, stateOffset);
                    RestoredStates restoredStates = dfa.resetAndRestore(savedState, savedState);
                    stateOffset = restoredStates.stateOffset();
                    if (stateOffset < 0) {
                        return false;
                    }
                    transition = dfa.computeTransition(stateOffset, BYTE_END_TEXT);
                    if (transition == Integer.MIN_VALUE) {
                        return false;
                    }
                }
                if (transition > 0 && (transition & T_MATCH_BIT) != 0) {
                    matched = true;
                    stateOffset = transition & ~T_MATCH_BIT;
                    recordMatches(dfa.stateData[stateOffset / dfa.nextSize], matches);
                }
                return matched;
            }
            catch (RetrySearchException ignored) {
                if (exclusiveSearch) {
                    throw new AssertionError("exclusive DFA search requested a retry");
                }
                requiresExclusiveSearch = true;
            }
            finally {
                dfa.endSearch(exclusiveSearch, readerSlot);
            }
        }
    }

    private static void recordMatches(StateData state, SparseSet matches)
    {
        int[] instructions = state.inst();
        int separator = instructions.length - 1;
        while (separator >= 0 && instructions[separator] != MatchSep) {
            separator--;
        }
        if (separator < 0) {
            return;
        }
        for (int index = instructions.length - 1; index > separator; index--) {
            matches.insert(instructions[index]);
        }
    }

    private static int getTransition(int[] transitions, int index)
    {
        return transitions[index];
    }

    private static void setTransition(int[] transitions, int index, int value)
    {
        transitions[index] = value;
    }

    private static Object getStateReference(Object[] references, int index)
    {
        return references[index];
    }

    private static void setStateReference(Object[] references, int index, Object value)
    {
        references[index] = value;
    }

    // PERFORMANCE-SENSITIVE HOT LOOPS: C2 code shape, method boundaries, dispatch
    // placement, and dependent loads have all produced measurable target-host changes.
    // Do not perform readability-only refactors in these search methods. Any change must
    // retain direct path tests and pass focused Intel and Graviton benchmarks.
    private static long searchForward(
            DfaInstance dfa,
            byte[] bytes,
            int textBegin,
            int textEnd,
            int ctxEnd,
            int startOffset,
            boolean startIsMatch,
            boolean pairedSearch,
            boolean endMatch,
            boolean wantEarliestMatch)
    {
        if (shouldSampleSelfLoopTransitions(endMatch, textEnd - textBegin, pairedSearch)) {
            return searchForwardSampledSelfLoops(
                    dfa,
                    bytes,
                    textBegin,
                    textEnd,
                    ctxEnd,
                    startOffset,
                    startIsMatch,
                    endMatch,
                    wantEarliestMatch);
        }
        if (!pairedSearch && !startIsMatch) {
            if (textEnd - textBegin >= MIN_PAIRED_SEARCH_BYTES) {
                dfa.requestAbsolutePointerTransitions();
            }
            if (dfa.hasAbsolutePointerTransitions(startOffset)) {
                return searchForwardAbsolutePointers(
                        dfa,
                        bytes,
                        textBegin,
                        textBegin,
                        textEnd,
                        ctxEnd,
                        startOffset,
                        -1,
                        endMatch,
                        wantEarliestMatch);
            }
        }
        return searchForwardContinuation(
                dfa,
                bytes,
                textBegin,
                textBegin,
                textEnd,
                ctxEnd,
                startOffset,
                startIsMatch,
                -1,
                endMatch,
                wantEarliestMatch);
    }

    private static long searchForwardAbsolutePointers(
            DfaInstance dfa,
            byte[] bytes,
            int resultBegin,
            int position,
            int textEnd,
            int ctxEnd,
            int startOffset,
            int initialMatchBoundary,
            boolean endMatch,
            boolean wantEarliestMatch)
    {
        AbsolutePointerTransitions absolutePointers = dfa.absolutePointerTransitions;
        long tableAddress = absolutePointers.baseAddress();
        long transitionRowAddress = tableAddress + ((long) startOffset * Long.BYTES);
        try {
            MemorySegment addressSpace = AbsoluteAddressSpace.EVERYTHING;
            byte[] byteMap = dfa.bytemap;
            for (; position < textEnd; position++) {
                long nextTransitionRowAddress = addressSpace.get(
                        ValueLayout.JAVA_LONG_UNALIGNED,
                        transitionRowAddress + ((long) (byteMap[bytes[position] & 0xFF] & 0xFF) * Long.BYTES));
                if (nextTransitionRowAddress == 0) {
                    break;
                }
                transitionRowAddress = nextTransitionRowAddress;
            }
        }
        finally {
            // Raw addresses do not keep their automatic-arena owner reachable.
            Reference.reachabilityFence(absolutePointers);
        }

        int stateOffset = toIntExact((transitionRowAddress - tableAddress) / Long.BYTES);
        return searchForwardContinuation(
                dfa,
                bytes,
                resultBegin,
                position,
                textEnd,
                ctxEnd,
                stateOffset,
                false,
                initialMatchBoundary,
                endMatch,
                wantEarliestMatch);
    }

    private static long searchForwardSampledSelfLoops(
            DfaInstance dfa,
            byte[] bytes,
            int textBegin,
            int textEnd,
            int ctxEnd,
            int startOffset,
            boolean startIsMatch,
            boolean endMatch,
            boolean wantEarliestMatch)
    {
        byte[] byteMap = dfa.bytemap;
        int nextSize = dfa.nextSize;
        int position = textBegin;
        int sampledSelfLoopCount = 0;
        Object[] stateRefs = dfa.stateRefArrays[startOffset / nextSize];

        for (int sampledTransitionCount = 0;
                position < textEnd && sampledTransitionCount < SELF_LOOP_SAMPLE_SIZE;
                sampledTransitionCount++, position++) {
            Object nextRef = getStateReference(stateRefs, byteMap[bytes[position] & 0xFF] & 0xFF);
            if (nextRef == stateRefs) {
                sampledSelfLoopCount++;
                continue;
            }
            if (nextRef == null) {
                break;
            }
            stateRefs = (Object[]) nextRef;
        }

        if (position - textBegin == SELF_LOOP_SAMPLE_SIZE &&
                sampledSelfLoopCount * 100 >= SELF_LOOP_SAMPLE_SIZE * SELF_LOOP_SAMPLE_PERCENTAGE) {
            long scanResult = scanSelfLoopTransitions(bytes, position, textEnd, stateRefs, byteMap, nextSize);
            position = (int) (scanResult >>> 32);
            startOffset = (int) scanResult;
        }
        else {
            startOffset = ((StateData) getStateReference(stateRefs, nextSize)).offset();
        }

        if (textEnd - position >= MIN_PAIRED_SEARCH_BYTES) {
            dfa.requestAbsolutePointerTransitions();
            if (dfa.hasAbsolutePointerTransitions(startOffset)) {
                return searchForwardAbsolutePointers(
                        dfa,
                        bytes,
                        textBegin,
                        position,
                        textEnd,
                        ctxEnd,
                        startOffset,
                        startIsMatch ? textBegin : -1,
                        endMatch,
                        wantEarliestMatch);
            }
        }
        return searchForwardContinuation(
                dfa,
                bytes,
                textBegin,
                position,
                textEnd,
                ctxEnd,
                startOffset,
                false,
                startIsMatch ? textBegin : -1,
                endMatch,
                wantEarliestMatch);
    }

    private static long searchForwardContinuation(
            DfaInstance dfa,
            byte[] bytes,
            int resultBegin,
            int position,
            int textEnd,
            int ctxEnd,
            int startOffset,
            boolean startIsMatch,
            int initialMatchBoundary,
            boolean endMatch,
            boolean wantEarliestMatch)
    {
        int lastMatchBoundary = initialMatchBoundary;
        boolean matched = initialMatchBoundary >= 0;
        int resetPosition = -1;

        int stateOffset = startOffset;
        int[] transitions = dfa.transitions;
        byte[] byteMap = dfa.bytemap;
        int nextSize = dfa.nextSize;
        boolean deadState = false;

        Object[] stateRefs = dfa.stateRefArrays[stateOffset / nextSize];

        if (startIsMatch) {
            matched = true;
            lastMatchBoundary = position;
            if (wantEarliestMatch && !endMatch) {
                return finalizeResult(resultBegin, textEnd, endMatch, true, lastMatchBoundary, true);
            }
        }

        scan:
        while (position < textEnd) {
            // null = any abnormal transition (uncomputed, dead, full-match, or match state).
            for (; position < textEnd; position++) {
                Object nextRef = getStateReference(stateRefs, byteMap[bytes[position] & 0xFF] & 0xFF);
                if (nextRef == null) {
                    break;
                }
                stateRefs = (Object[]) nextRef;
            }
            if (position >= textEnd) {
                // Recover int offset from stateRefs for end-of-text handling
                stateOffset = ((StateData) getStateReference(stateRefs, nextSize)).offset();
                break;
            }

            // Inner loop broke on null — consult int[] sidecard for details
            stateOffset = ((StateData) getStateReference(stateRefs, nextSize)).offset();
            int nextState = getTransition(transitions, stateOffset + (byteMap[bytes[position] & 0xFF] & 0xFF));

            // Sentinels are negative; matches are positive with T_MATCH_BIT set.
            if (nextState < 0) {
                if (nextState == T_DEAD) {
                    position++;
                    deadState = true;
                    break scan;
                }
                if (nextState == T_FULL_MATCH) {
                    return finalizeResult(resultBegin, textEnd, endMatch, true, textEnd, true);
                }

                // T_UNCOMPUTED: run slow path
                int inputByte = bytes[position] & 0xFF;
                nextState = dfa.computeTransition(stateOffset, inputByte);
                transitions = dfa.transitions;

                if (nextState != Integer.MIN_VALUE) {
                    // Slow path succeeded
                    if (nextState == T_DEAD) {
                        position++;
                        deadState = true;
                        break scan;
                    }
                    if (nextState == T_FULL_MATCH) {
                        return finalizeResult(resultBegin, textEnd, endMatch, true, textEnd, true);
                    }
                    stateOffset = nextState;
                    if ((stateOffset & T_MATCH_BIT) != 0) {
                        matched = true;
                        lastMatchBoundary = position;
                        stateOffset &= ~T_MATCH_BIT;
                        if (wantEarliestMatch && !endMatch) {
                            position++;
                            break scan;
                        }
                    }
                    position++;
                    stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
                    continue;
                }

                // Cache exhaustion: save/reset/restore
                position++;
                if (dfaShouldBailWhenSlow && resetPosition >= 0 && (position - resetPosition) < 10 * dfa.stateCount) {
                    return SEARCH_FAILED;
                }
                resetPosition = position;
                StateSaver saveStart = new StateSaver(dfa, startOffset);
                StateSaver saveState = new StateSaver(dfa, stateOffset);
                RestoredStates restoredStates = dfa.resetAndRestore(saveStart, saveState);
                transitions = dfa.transitions;
                startOffset = restoredStates.startOffset();
                stateOffset = restoredStates.stateOffset();
                if (startOffset < 0 || stateOffset < 0) {
                    return SEARCH_FAILED;
                }
                nextState = dfa.computeTransition(stateOffset, inputByte);
                transitions = dfa.transitions;
                if (nextState == Integer.MIN_VALUE) {
                    return SEARCH_FAILED;
                }
                if (nextState == T_DEAD) {
                    deadState = true;
                    break;
                }
                if (nextState == T_FULL_MATCH) {
                    return finalizeResult(resultBegin, textEnd, endMatch, true, textEnd, true);
                }
                stateOffset = nextState;
                if ((stateOffset & T_MATCH_BIT) != 0) {
                    matched = true;
                    lastMatchBoundary = position - 1;
                    stateOffset &= ~T_MATCH_BIT;
                    if (wantEarliestMatch && !endMatch) {
                        break;
                    }
                }
                stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
                continue;
            }

            // Normal non-match transition that was null (start self-loop for prefix accel)
            if ((nextState & T_MATCH_BIT) == 0) {
                stateOffset = nextState;
                position++;
                stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
                continue;
            }

            // Match handling: nextState is positive with T_MATCH_BIT set
            matched = true;
            lastMatchBoundary = position;
            stateOffset = nextState & ~T_MATCH_BIT;
            if (wantEarliestMatch && !endMatch) {
                position++;
                break scan;
            }
            position++;
            stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
        }

        // End-of-text transition (skip if scan ended at dead state)
        if (!deadState) {
            int lastClass;
            int lastByte;
            if (textEnd == ctxEnd) {
                lastClass = dfa.bytemapRange;
                lastByte = BYTE_END_TEXT;
            }
            else {
                lastByte = bytes[textEnd] & 0xFF;
                lastClass = byteMap[lastByte] & 0xFF;
            }
            int finalState = getTransition(transitions, stateOffset + lastClass);
            if (finalState == T_UNCOMPUTED) {
                finalState = dfa.computeTransition(stateOffset, lastByte);
                if (finalState == Integer.MIN_VALUE) {
                    return SEARCH_FAILED;
                }
            }
            if (finalState == T_FULL_MATCH || (finalState > 0 && (finalState & T_MATCH_BIT) != 0)) {
                matched = true;
                lastMatchBoundary = position;
            }
        }
        return finalizeResult(resultBegin, textEnd, endMatch, matched, lastMatchBoundary, true);
    }

    private static long scanSelfLoopTransitions(
            byte[] bytes,
            int position,
            int textEnd,
            Object[] stateRefs,
            byte[] byteMap,
            int nextSize)
    {
        // A stable self-loop keeps reference decoding and the array type check off the serial
        // transition dependency chain. Keeping this loop separate also preserves the old loop's
        // generated code when sampling rejects this path.
        for (; position < textEnd; position++) {
            Object nextRef = getStateReference(stateRefs, byteMap[bytes[position] & 0xFF] & 0xFF);
            if (nextRef == stateRefs) {
                continue;
            }
            if (nextRef == null) {
                break;
            }
            stateRefs = (Object[]) nextRef;
        }
        int stateOffset = ((StateData) getStateReference(stateRefs, nextSize)).offset();
        return ((long) position << 32) | (stateOffset & 0xFFFF_FFFFL);
    }

    static boolean shouldSampleSelfLoopTransitions(boolean endMatch, int remainingBytes, boolean pairedSearch)
    {
        return !pairedSearch && endMatch && remainingBytes >= MIN_SELF_LOOP_SAMPLE_SEARCH_BYTES;
    }

    static long searchForwardPairs(
            DfaInstance dfa,
            byte[] bytes,
            int textBegin,
            int textEnd,
            int ctxEnd,
            int startOffset,
            boolean endMatch,
            boolean wantEarliestMatch)
    {
        Object[][] pairedStateRefArrays = dfa.pairedStateRefArrays;
        int nextSize = dfa.nextSize;
        int pairedClassCount = nextSize * nextSize;
        Object[] stateRefs = pairedStateRefArrays[startOffset / nextSize];
        int position = textBegin;

        while (position + 1 < textEnd) {
            int pairedClass = ((dfa.bytemap[bytes[position] & 0xFF] & 0xFF) * nextSize) +
                    (dfa.bytemap[bytes[position + 1] & 0xFF] & 0xFF);
            Object nextRef = getStateReference(stateRefs, pairedClass);
            if (nextRef == null) {
                if (dfa.kind == DfaInstance.Kind.FIRST_MATCH && !endMatch && !wantEarliestMatch) {
                    int stateOffset = ((StateData) getStateReference(stateRefs, pairedClassCount)).offset();
                    long pairedTransition = decodePairedTransition(
                            dfa,
                            stateOffset,
                            dfa.bytemap[bytes[position] & 0xFF] & 0xFF,
                            dfa.bytemap[bytes[position + 1] & 0xFF] & 0xFF);
                    int outcome = (int) pairedTransition;
                    if (outcome == PAIR_MATCH_FIRST || outcome == PAIR_MATCH_SECOND) {
                        long result = searchForwardPairsAfterMatch(
                                dfa,
                                bytes,
                                textBegin,
                                position,
                                textEnd,
                                ctxEnd,
                                pairedTransition,
                                endMatch,
                                wantEarliestMatch);
                        if (result >= 0) {
                            dfa.recordPairedSearch((int) result);
                        }
                        return result;
                    }
                }
                break;
            }
            stateRefs = (Object[]) nextRef;
            position += 2;
        }

        int stateOffset = ((StateData) getStateReference(stateRefs, pairedClassCount)).offset();
        long result = searchForward(dfa, bytes, position, textEnd, ctxEnd, stateOffset, false, true, endMatch, wantEarliestMatch);
        if (result >= 0) {
            return result + (position - textBegin);
        }
        return result;
    }

    private static long searchForwardPairsAfterMatch(
            DfaInstance dfa,
            byte[] bytes,
            int resultBegin,
            int position,
            int textEnd,
            int ctxEnd,
            long pairedTransition,
            boolean endMatch,
            boolean wantEarliestMatch)
    {
        int nextSize = dfa.nextSize;
        int pairedClassCount = nextSize * nextSize;
        int lastMatchBoundary;
        Object[] stateRefs;

        while (true) {
            int consumedBytes = (int) pairedTransition;
            int stateOffset = (int) (pairedTransition >>> 32);
            lastMatchBoundary = position + consumedBytes - 1;
            position += consumedBytes;

            int matchingSelfLoopEnd = scanMatchingSelfLoopTransitions(dfa, bytes, position, textEnd, stateOffset);
            if (matchingSelfLoopEnd != position) {
                dfa.matchingSelfLoopScanCount++;
                position = matchingSelfLoopEnd;
                lastMatchBoundary = position - 1;
            }

            stateRefs = dfa.pairedStateRefArrays[stateOffset / nextSize];
            if (stateRefs == null) {
                return searchForwardContinuation(
                        dfa,
                        bytes,
                        resultBegin,
                        position,
                        textEnd,
                        ctxEnd,
                        stateOffset,
                        false,
                        lastMatchBoundary,
                        endMatch,
                        wantEarliestMatch);
            }
            pairedTransition = 0;

            while (position + 1 < textEnd) {
                int firstClass = dfa.bytemap[bytes[position] & 0xFF] & 0xFF;
                int secondClass = dfa.bytemap[bytes[position + 1] & 0xFF] & 0xFF;
                int pairedClass = (firstClass * nextSize) + secondClass;
                Object nextRef = getStateReference(stateRefs, pairedClass);
                if (nextRef != null) {
                    stateRefs = (Object[]) nextRef;
                    position += 2;
                    continue;
                }

                stateOffset = ((StateData) getStateReference(stateRefs, pairedClassCount)).offset();
                pairedTransition = decodePairedTransition(dfa, stateOffset, firstClass, secondClass);
                int outcome = (int) pairedTransition;
                switch (outcome) {
                    case PAIR_MATCH_FIRST, PAIR_MATCH_SECOND -> {
                        break;
                    }
                    case PAIR_DEAD -> {
                        return finalizeResult(resultBegin, textEnd, endMatch, true, lastMatchBoundary, true);
                    }
                    case PAIR_FULL_MATCH -> {
                        return finalizeResult(resultBegin, textEnd, endMatch, true, textEnd, true);
                    }
                    case PAIR_NORMAL_FIRST, PAIR_NORMAL_SECOND -> {
                        stateOffset = (int) (pairedTransition >>> 32);
                        position += outcome == PAIR_NORMAL_FIRST ? 1 : 2;
                        stateRefs = dfa.pairedStateRefArrays[stateOffset / nextSize];
                        if (stateRefs == null) {
                            return searchForwardContinuation(
                                    dfa,
                                    bytes,
                                    resultBegin,
                                    position,
                                    textEnd,
                                    ctxEnd,
                                    stateOffset,
                                    false,
                                    lastMatchBoundary,
                                    endMatch,
                                    wantEarliestMatch);
                        }
                        pairedTransition = 0;
                        continue;
                    }
                    case PAIR_UNCOMPUTED -> {
                        return searchForwardContinuation(
                                dfa,
                                bytes,
                                resultBegin,
                                position,
                                textEnd,
                                ctxEnd,
                                stateOffset,
                                false,
                                lastMatchBoundary,
                                endMatch,
                                wantEarliestMatch);
                    }
                    default -> throw new AssertionError("Unexpected paired transition outcome: " + outcome);
                }
                break;
            }

            if (pairedTransition != 0) {
                continue;
            }
            stateOffset = ((StateData) getStateReference(stateRefs, pairedClassCount)).offset();
            return searchForwardContinuation(
                    dfa,
                    bytes,
                    resultBegin,
                    position,
                    textEnd,
                    ctxEnd,
                    stateOffset,
                    false,
                    lastMatchBoundary,
                    endMatch,
                    wantEarliestMatch);
        }
    }

    private static int scanMatchingSelfLoopTransitions(
            DfaInstance dfa,
            byte[] bytes,
            int position,
            int textEnd,
            int stateOffset)
    {
        int matchingSelfLoop = stateOffset | T_MATCH_BIT;
        int[] transitions = dfa.transitions;
        byte[] byteMap = dfa.bytemap;
        for (; position < textEnd; position++) {
            if (getTransition(transitions, stateOffset + (byteMap[bytes[position] & 0xFF] & 0xFF)) != matchingSelfLoop) {
                break;
            }
        }
        return position;
    }

    private static long decodePairedTransition(DfaInstance dfa, int stateOffset, int firstClass, int secondClass)
    {
        int firstTransition = getTransition(dfa.transitions, stateOffset + firstClass);
        if (firstTransition == T_UNCOMPUTED) {
            return PAIR_UNCOMPUTED;
        }
        if (firstTransition == T_DEAD) {
            return PAIR_DEAD;
        }
        if (firstTransition == T_FULL_MATCH) {
            return PAIR_FULL_MATCH;
        }
        if (firstTransition > 0 && (firstTransition & T_MATCH_BIT) != 0) {
            return encodePairedTransition(firstTransition & ~T_MATCH_BIT, PAIR_MATCH_FIRST);
        }

        int secondTransition = getTransition(dfa.transitions, firstTransition + secondClass);
        if (secondTransition == T_UNCOMPUTED) {
            return encodePairedTransition(firstTransition, PAIR_NORMAL_FIRST);
        }
        if (secondTransition == T_DEAD) {
            return PAIR_DEAD;
        }
        if (secondTransition == T_FULL_MATCH) {
            return PAIR_FULL_MATCH;
        }
        if (secondTransition > 0 && (secondTransition & T_MATCH_BIT) != 0) {
            return encodePairedTransition(secondTransition & ~T_MATCH_BIT, PAIR_MATCH_SECOND);
        }
        return encodePairedTransition(secondTransition, PAIR_NORMAL_SECOND);
    }

    private static long encodePairedTransition(int stateOffset, int outcome)
    {
        return ((long) stateOffset << 32) | outcome;
    }

    private static long searchForwardPrefixAccelSingleByte(
            DfaInstance dfa,
            Prog prog,
            byte[] bytes,
            int textBegin,
            int textEnd,
            int ctxEnd,
            int startOffset,
            boolean startIsMatch,
            boolean endMatch,
            boolean wantEarliestMatch)
    {
        int position = textBegin;
        int lastMatchBoundary = -1;
        boolean matched = false;
        int resetPosition = -1;

        int stateOffset = startOffset;
        int[] transitions = dfa.transitions;
        byte[] byteMap = dfa.bytemap;
        boolean deadState = false;

        // Single-byte prefix path uses fused int[] transitions.
        // Rationale: this mode has high false-positive rate, so it frequently flips between
        // prefix scanning and a few DFA steps. Keeping start-state self-loops on the int[]
        // hot path avoids per-cycle Object[] null-break + sidecard handoff overhead.
        if (startIsMatch) {
            matched = true;
            lastMatchBoundary = position;
            if (wantEarliestMatch && !endMatch) {
                return finalizeResult(textBegin, textEnd, endMatch, true, lastMatchBoundary, true);
            }
        }

        scan:
        while (position < textEnd) {
            if (stateOffset == startOffset) {
                int next = prog.prefixAccelSingleByteNoFoldCase(bytes, position, textEnd - position);
                if (next < 0) {
                    position = textEnd;
                    break;
                }
                position = next;
            }

            int nextState = 0;
            for (; position < textEnd; position++) {
                nextState = getTransition(transitions, stateOffset + (byteMap[bytes[position] & 0xFF] & 0xFF));
                // Cold-path gate: sentinels are negative.
                if (nextState < 0) {
                    break;
                }
                stateOffset = nextState;
                if ((stateOffset & T_MATCH_BIT) != 0) {
                    matched = true;
                    lastMatchBoundary = position;
                    stateOffset &= ~T_MATCH_BIT;
                    if (wantEarliestMatch && !endMatch) {
                        position++;
                        break scan;
                    }
                }
                if (stateOffset == startOffset) {
                    position++;
                    continue scan;
                }
            }
            if (position >= textEnd) {
                break;
            }

            if (nextState == T_DEAD) {
                position++;
                deadState = true;
                break scan;
            }
            if (nextState == T_FULL_MATCH) {
                return finalizeResult(textBegin, textEnd, endMatch, true, textEnd, true);
            }

            int inputByte = bytes[position] & 0xFF;
            nextState = dfa.computeTransition(stateOffset, inputByte);
            transitions = dfa.transitions;

            if (nextState != Integer.MIN_VALUE) {
                if (nextState == T_DEAD) {
                    position++;
                    deadState = true;
                    break scan;
                }
                if (nextState == T_FULL_MATCH) {
                    return finalizeResult(textBegin, textEnd, endMatch, true, textEnd, true);
                }
                stateOffset = nextState;
                if ((stateOffset & T_MATCH_BIT) != 0) {
                    matched = true;
                    lastMatchBoundary = position;
                    stateOffset &= ~T_MATCH_BIT;
                    if (wantEarliestMatch && !endMatch) {
                        position++;
                        break scan;
                    }
                }
                position++;
                continue;
            }

            position++;
            if (dfaShouldBailWhenSlow && resetPosition >= 0 && (position - resetPosition) < 10 * dfa.stateCount) {
                return SEARCH_FAILED;
            }
            resetPosition = position;
            StateSaver saveStart = new StateSaver(dfa, startOffset);
            StateSaver saveState = new StateSaver(dfa, stateOffset);
            RestoredStates restoredStates = dfa.resetAndRestore(saveStart, saveState);
            transitions = dfa.transitions;
            startOffset = restoredStates.startOffset();
            stateOffset = restoredStates.stateOffset();
            if (startOffset < 0 || stateOffset < 0) {
                return SEARCH_FAILED;
            }
            nextState = dfa.computeTransition(stateOffset, inputByte);
            transitions = dfa.transitions;
            if (nextState == Integer.MIN_VALUE) {
                return SEARCH_FAILED;
            }
            if (nextState == T_DEAD) {
                deadState = true;
                break;
            }
            if (nextState == T_FULL_MATCH) {
                return finalizeResult(textBegin, textEnd, endMatch, true, textEnd, true);
            }
            stateOffset = nextState;
            if ((stateOffset & T_MATCH_BIT) != 0) {
                matched = true;
                lastMatchBoundary = position - 1;
                stateOffset &= ~T_MATCH_BIT;
                if (wantEarliestMatch && !endMatch) {
                    break;
                }
            }
        }

        if (!deadState) {
            int lastClass;
            int lastByte;
            if (textEnd == ctxEnd) {
                lastClass = dfa.bytemapRange;
                lastByte = BYTE_END_TEXT;
            }
            else {
                lastByte = bytes[textEnd] & 0xFF;
                lastClass = byteMap[lastByte] & 0xFF;
            }
            int finalState = getTransition(transitions, stateOffset + lastClass);
            if (finalState == T_UNCOMPUTED) {
                finalState = dfa.computeTransition(stateOffset, lastByte);
                if (finalState == Integer.MIN_VALUE) {
                    return SEARCH_FAILED;
                }
            }
            if (finalState == T_FULL_MATCH || (finalState > 0 && (finalState & T_MATCH_BIT) != 0)) {
                matched = true;
                lastMatchBoundary = position;
            }
        }
        return finalizeResult(textBegin, textEnd, endMatch, matched, lastMatchBoundary, true);
    }

    private static long searchForwardScanAcceleration(
            DfaInstance dfa,
            Prog prog,
            byte[] bytes,
            int textBegin,
            int textEnd,
            int ctxEnd,
            int startOffset,
            boolean startIsMatch,
            boolean endMatch,
            boolean wantEarliestMatch,
            byte[] byteCandidates,
            int byteCandidateOffset)
    {
        int position = textBegin;
        int lastMatchBoundary = -1;
        boolean matched = false;
        int resetPosition = -1;

        int stateOffset = startOffset;
        int[] transitions = dfa.transitions;
        byte[] byteMap = dfa.bytemap;
        int nextSize = dfa.nextSize;
        boolean deadState = false;

        Object[] stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
        Object[] startRefs = stateRefs;

        // Generic scan acceleration for sparse start bytes and literal prefixes other than
        // the single-byte fast path. Keep scanner control local so later searches can still
        // use the unchanged compact transition rows when sampling rejects acceleration.

        if (startIsMatch) {
            matched = true;
            lastMatchBoundary = position;
            if (wantEarliestMatch && !endMatch) {
                return finalizeResult(textBegin, textEnd, endMatch, true, lastMatchBoundary, true);
            }
        }

        scan:
        while (position < textEnd) {
            // Skip to the first byte that can begin a match.
            if (stateRefs == startRefs) {
                int next;
                if (byteCandidateOffset == PREFIX_SCAN) {
                    next = prog.prefixAccel(bytes, position, textEnd - position);
                }
                else {
                    if (byteCandidateOffset >= textEnd - position) {
                        next = -1;
                    }
                    else {
                        int candidatePosition = position + byteCandidateOffset;
                        if (byteCandidateOffset == 0) {
                            next = findByteCandidate(byteCandidates, bytes, candidatePosition, textEnd - candidatePosition);
                        }
                        else {
                            int candidateBytePosition = findByteCandidate(byteCandidates, bytes, candidatePosition, textEnd - candidatePosition);
                            next = candidateBytePosition < 0 ? -1 : candidateBytePosition - byteCandidateOffset;
                        }
                    }
                }
                if (next < 0) {
                    position = textEnd;
                    break;
                }
                if (byteCandidateOffset > 0) {
                    if (next - position < MINIMUM_PRODUCTIVE_FIXED_DISTANCE_SKIP) {
                        byteCandidates = dfa.startByteCandidates;
                        byteCandidateOffset = 0;
                        continue;
                    }
                }
                position = next;
            }

            // Tight inner loop using Object[] refs — identical to searchForward.
            // A transition back to the start row resumes scanning at the next byte.
            for (; position < textEnd; position++) {
                Object nextRef = getStateReference(stateRefs, byteMap[bytes[position] & 0xFF] & 0xFF);
                if (nextRef == startRefs) {
                    stateRefs = (Object[]) nextRef;
                    stateOffset = startOffset;
                    position++;
                    continue scan;
                }
                if (nextRef == null) {
                    break;
                }
                stateRefs = (Object[]) nextRef;
            }
            if (position >= textEnd) {
                stateOffset = ((StateData) getStateReference(stateRefs, nextSize)).offset();
                break;
            }

            // Inner loop broke on null — consult int[] sidecard
            stateOffset = ((StateData) getStateReference(stateRefs, nextSize)).offset();
            int nextState = getTransition(transitions, stateOffset + (byteMap[bytes[position] & 0xFF] & 0xFF));

            // Normal non-match transition back to the start state.
            if (nextState > 0 && (nextState & T_MATCH_BIT) == 0) {
                stateOffset = nextState;
                stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
                position++;
                continue; // -> prefix accel at top of outer loop
            }

            // Match state handling (positive with T_MATCH_BIT)
            if (nextState > 0 && (nextState & T_MATCH_BIT) != 0) {
                matched = true;
                lastMatchBoundary = position;
                stateOffset = nextState & ~T_MATCH_BIT;
                if (wantEarliestMatch && !endMatch) {
                    position++;
                    break scan;
                }
                position++;
                stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
                continue;
            }

            // Sentinel handling
            if (nextState == T_DEAD) {
                position++;
                deadState = true;
                break scan;
            }
            if (nextState == T_FULL_MATCH) {
                return finalizeResult(textBegin, textEnd, endMatch, true, textEnd, true);
            }

            // T_UNCOMPUTED: run slow path
            int inputByte = bytes[position] & 0xFF;
            nextState = dfa.computeTransition(stateOffset, inputByte);
            transitions = dfa.transitions;

            if (nextState != Integer.MIN_VALUE) {
                if (nextState == T_DEAD) {
                    position++;
                    deadState = true;
                    break scan;
                }
                if (nextState == T_FULL_MATCH) {
                    return finalizeResult(textBegin, textEnd, endMatch, true, textEnd, true);
                }
                stateOffset = nextState;
                if ((stateOffset & T_MATCH_BIT) != 0) {
                    matched = true;
                    lastMatchBoundary = position;
                    stateOffset &= ~T_MATCH_BIT;
                    if (wantEarliestMatch && !endMatch) {
                        position++;
                        break scan;
                    }
                }
                position++;
                stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
                startRefs = dfa.stateRefArrays[startOffset / nextSize];
                continue;
            }

            // Cache exhaustion
            position++;
            if (dfaShouldBailWhenSlow && resetPosition >= 0 && (position - resetPosition) < 10 * dfa.stateCount) {
                return SEARCH_FAILED;
            }
            resetPosition = position;
            StateSaver saveStart = new StateSaver(dfa, startOffset);
            StateSaver saveState = new StateSaver(dfa, stateOffset);
            RestoredStates restoredStates = dfa.resetAndRestore(saveStart, saveState);
            transitions = dfa.transitions;
            startOffset = restoredStates.startOffset();
            stateOffset = restoredStates.stateOffset();
            if (startOffset < 0 || stateOffset < 0) {
                return SEARCH_FAILED;
            }
            nextState = dfa.computeTransition(stateOffset, inputByte);
            transitions = dfa.transitions;
            if (nextState == Integer.MIN_VALUE) {
                return SEARCH_FAILED;
            }
            if (nextState == T_DEAD) {
                deadState = true;
                break;
            }
            if (nextState == T_FULL_MATCH) {
                return finalizeResult(textBegin, textEnd, endMatch, true, textEnd, true);
            }
            stateOffset = nextState;
            if ((stateOffset & T_MATCH_BIT) != 0) {
                matched = true;
                lastMatchBoundary = position - 1;
                stateOffset &= ~T_MATCH_BIT;
                if (wantEarliestMatch && !endMatch) {
                    break;
                }
            }
            stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
            startRefs = dfa.stateRefArrays[startOffset / nextSize];
            // The restored start state is new, so its references are already null.
        }

        // End-of-text transition (skip if scan ended at dead state)
        if (!deadState) {
            int lastClass;
            int lastByte;
            if (textEnd == ctxEnd) {
                lastClass = dfa.bytemapRange;
                lastByte = BYTE_END_TEXT;
            }
            else {
                lastByte = bytes[textEnd] & 0xFF;
                lastClass = byteMap[lastByte] & 0xFF;
            }
            int finalState = getTransition(transitions, stateOffset + lastClass);
            if (finalState == T_UNCOMPUTED) {
                finalState = dfa.computeTransition(stateOffset, lastByte);
                if (finalState == Integer.MIN_VALUE) {
                    return SEARCH_FAILED;
                }
            }
            if (finalState == T_FULL_MATCH || (finalState > 0 && (finalState & T_MATCH_BIT) != 0)) {
                matched = true;
                lastMatchBoundary = position;
            }
        }
        return finalizeResult(textBegin, textEnd, endMatch, matched, lastMatchBoundary, true);
    }

    private static int findByteCandidate(byte[] candidates, byte[] data, int offset, int length)
    {
        int end = offset + length;
        for (int position = offset; position < end; position++) {
            if (candidates[data[position] & 0xFF] != 0) {
                return position;
            }
        }
        return -1;
    }

    private static long searchForwardAnchoredLongPrefixInt(
            DfaInstance dfa,
            byte[] bytes,
            int textBegin,
            int textEnd,
            int ctxEnd,
            int startOffset,
            boolean startIsMatch,
            boolean endMatch,
            boolean wantEarliestMatch)
    {
        int position = textBegin;
        int lastMatchBoundary = -1;
        boolean matched = false;
        int resetPosition = -1;

        int stateOffset = startOffset;
        int[] transitions = dfa.transitions;
        byte[] byteMap = dfa.bytemap;
        boolean deadState = false;

        // Anchored long-literal-prefix patterns are dominated by straight-line DFA traversal.
        // Use the fused int[] loop to avoid Object[] indirection in this regime.
        if (startIsMatch) {
            matched = true;
            lastMatchBoundary = position;
            if (wantEarliestMatch && !endMatch) {
                return finalizeResult(textBegin, textEnd, endMatch, true, lastMatchBoundary, true);
            }
        }

        scan:
        while (position < textEnd) {
            int nextState = 0;
            for (; position < textEnd; position++) {
                nextState = getTransition(transitions, stateOffset + (byteMap[bytes[position] & 0xFF] & 0xFF));
                if (nextState < 0) {
                    break;
                }
                stateOffset = nextState;
                if ((stateOffset & T_MATCH_BIT) != 0) {
                    matched = true;
                    lastMatchBoundary = position;
                    stateOffset &= ~T_MATCH_BIT;
                    if (wantEarliestMatch && !endMatch) {
                        position++;
                        break scan;
                    }
                }
            }
            if (position >= textEnd) {
                break;
            }

            if (nextState == T_DEAD) {
                position++;
                deadState = true;
                break scan;
            }
            if (nextState == T_FULL_MATCH) {
                return finalizeResult(textBegin, textEnd, endMatch, true, textEnd, true);
            }

            int inputByte = bytes[position] & 0xFF;
            nextState = dfa.computeTransition(stateOffset, inputByte);
            transitions = dfa.transitions;

            if (nextState != Integer.MIN_VALUE) {
                if (nextState == T_DEAD) {
                    position++;
                    deadState = true;
                    break scan;
                }
                if (nextState == T_FULL_MATCH) {
                    return finalizeResult(textBegin, textEnd, endMatch, true, textEnd, true);
                }
                stateOffset = nextState;
                if ((stateOffset & T_MATCH_BIT) != 0) {
                    matched = true;
                    lastMatchBoundary = position;
                    stateOffset &= ~T_MATCH_BIT;
                    if (wantEarliestMatch && !endMatch) {
                        position++;
                        break scan;
                    }
                }
                position++;
                continue;
            }

            position++;
            if (dfaShouldBailWhenSlow && resetPosition >= 0 && (position - resetPosition) < 10 * dfa.stateCount) {
                return SEARCH_FAILED;
            }
            resetPosition = position;
            StateSaver saveStart = new StateSaver(dfa, startOffset);
            StateSaver saveState = new StateSaver(dfa, stateOffset);
            RestoredStates restoredStates = dfa.resetAndRestore(saveStart, saveState);
            transitions = dfa.transitions;
            startOffset = restoredStates.startOffset();
            stateOffset = restoredStates.stateOffset();
            if (startOffset < 0 || stateOffset < 0) {
                return SEARCH_FAILED;
            }
            nextState = dfa.computeTransition(stateOffset, inputByte);
            transitions = dfa.transitions;
            if (nextState == Integer.MIN_VALUE) {
                return SEARCH_FAILED;
            }
            if (nextState == T_DEAD) {
                deadState = true;
                break;
            }
            if (nextState == T_FULL_MATCH) {
                return finalizeResult(textBegin, textEnd, endMatch, true, textEnd, true);
            }
            stateOffset = nextState;
            if ((stateOffset & T_MATCH_BIT) != 0) {
                matched = true;
                lastMatchBoundary = position - 1;
                stateOffset &= ~T_MATCH_BIT;
                if (wantEarliestMatch && !endMatch) {
                    break;
                }
            }
        }

        if (!deadState) {
            int lastClass;
            int lastByte;
            if (textEnd == ctxEnd) {
                lastClass = dfa.bytemapRange;
                lastByte = BYTE_END_TEXT;
            }
            else {
                lastByte = bytes[textEnd] & 0xFF;
                lastClass = byteMap[lastByte] & 0xFF;
            }
            int finalState = getTransition(transitions, stateOffset + lastClass);
            if (finalState == T_UNCOMPUTED) {
                finalState = dfa.computeTransition(stateOffset, lastByte);
                if (finalState == Integer.MIN_VALUE) {
                    return SEARCH_FAILED;
                }
            }
            if (finalState == T_FULL_MATCH || (finalState > 0 && (finalState & T_MATCH_BIT) != 0)) {
                matched = true;
                lastMatchBoundary = position;
            }
        }
        return finalizeResult(textBegin, textEnd, endMatch, matched, lastMatchBoundary, true);
    }

    private static long searchBackward(
            DfaInstance dfa,
            byte[] bytes,
            int textBegin,
            int textEnd,
            int ctxBegin,
            int startOffset,
            boolean startIsMatch,
            boolean endMatch,
            boolean wantEarliestMatch)
    {
        int position = textEnd;
        int lastMatchBoundary = -1;
        boolean matched = false;
        int resetPosition = -1;

        int stateOffset = startOffset;
        int[] transitions = dfa.transitions;
        byte[] byteMap = dfa.bytemap;
        int nextSize = dfa.nextSize;
        boolean deadState = false;

        Object[] stateRefs = dfa.stateRefArrays[stateOffset / nextSize];

        if (startIsMatch) {
            matched = true;
            lastMatchBoundary = position;
            if (wantEarliestMatch && !endMatch) {
                return finalizeResult(textBegin, textEnd, endMatch, true, lastMatchBoundary, false);
            }
        }

        scan:
        while (position > textBegin) {
            // Tight inner loop using Object[] refs (backward direction)
            for (; position > textBegin; position--) {
                Object nextRef = getStateReference(stateRefs, byteMap[bytes[position - 1] & 0xFF] & 0xFF);
                if (nextRef == null) {
                    break;
                }
                stateRefs = (Object[]) nextRef;
            }
            if (position <= textBegin) {
                stateOffset = ((StateData) getStateReference(stateRefs, nextSize)).offset();
                break;
            }

            // Inner loop broke on null — consult int[] sidecard
            stateOffset = ((StateData) getStateReference(stateRefs, nextSize)).offset();
            int nextState = getTransition(transitions, stateOffset + (byteMap[bytes[position - 1] & 0xFF] & 0xFF));

            // Match state handling (positive with T_MATCH_BIT)
            if (nextState > 0 && (nextState & T_MATCH_BIT) != 0) {
                matched = true;
                lastMatchBoundary = position;
                stateOffset = nextState & ~T_MATCH_BIT;
                if (wantEarliestMatch && !endMatch) {
                    position--;
                    break scan;
                }
                position--;
                stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
                continue;
            }

            // Normal non-match transition that was null (start self-loop for prefix accel)
            if (nextState > 0) {
                stateOffset = nextState;
                position--;
                stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
                continue;
            }

            // Sentinel handling
            if (nextState == T_DEAD) {
                position--;
                deadState = true;
                break scan;
            }
            if (nextState == T_FULL_MATCH) {
                return finalizeResult(textBegin, textEnd, endMatch, true, textBegin, false);
            }

            // T_UNCOMPUTED: run slow path
            int inputByte = bytes[position - 1] & 0xFF;
            nextState = dfa.computeTransition(stateOffset, inputByte);
            transitions = dfa.transitions;

            if (nextState != Integer.MIN_VALUE) {
                if (nextState == T_DEAD) {
                    position--;
                    deadState = true;
                    break scan;
                }
                if (nextState == T_FULL_MATCH) {
                    return finalizeResult(textBegin, textEnd, endMatch, true, textBegin, false);
                }
                stateOffset = nextState;
                if ((stateOffset & T_MATCH_BIT) != 0) {
                    matched = true;
                    lastMatchBoundary = position;
                    stateOffset &= ~T_MATCH_BIT;
                    if (wantEarliestMatch && !endMatch) {
                        position--;
                        break scan;
                    }
                }
                position--;
                stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
                continue;
            }

            // Cache exhaustion
            position--;
            if (dfaShouldBailWhenSlow && resetPosition >= 0 && Math.abs(position - resetPosition) < 10 * dfa.stateCount) {
                return SEARCH_FAILED;
            }
            resetPosition = position;
            StateSaver saveStart = new StateSaver(dfa, startOffset);
            StateSaver saveState = new StateSaver(dfa, stateOffset);
            RestoredStates restoredStates = dfa.resetAndRestore(saveStart, saveState);
            transitions = dfa.transitions;
            startOffset = restoredStates.startOffset();
            stateOffset = restoredStates.stateOffset();
            if (startOffset < 0 || stateOffset < 0) {
                return SEARCH_FAILED;
            }
            nextState = dfa.computeTransition(stateOffset, inputByte);
            transitions = dfa.transitions;
            if (nextState == Integer.MIN_VALUE) {
                return SEARCH_FAILED;
            }
            if (nextState == T_DEAD) {
                deadState = true;
                break;
            }
            if (nextState == T_FULL_MATCH) {
                return finalizeResult(textBegin, textEnd, endMatch, true, textBegin, false);
            }
            stateOffset = nextState;
            if ((stateOffset & T_MATCH_BIT) != 0) {
                matched = true;
                lastMatchBoundary = position + 1;
                stateOffset &= ~T_MATCH_BIT;
                if (wantEarliestMatch && !endMatch) {
                    break;
                }
            }
            stateRefs = dfa.stateRefArrays[stateOffset / nextSize];
        }

        // End-of-text transition
        if (!deadState) {
            int lastClass;
            int lastByte;
            if (textBegin == ctxBegin) {
                lastClass = dfa.bytemapRange;
                lastByte = BYTE_END_TEXT;
            }
            else {
                lastByte = bytes[textBegin - 1] & 0xFF;
                lastClass = byteMap[lastByte] & 0xFF;
            }
            int finalState = getTransition(transitions, stateOffset + lastClass);
            if (finalState == T_UNCOMPUTED) {
                finalState = dfa.computeTransition(stateOffset, lastByte);
                if (finalState == Integer.MIN_VALUE) {
                    return SEARCH_FAILED;
                }
            }
            if (finalState == T_FULL_MATCH || (finalState > 0 && (finalState & T_MATCH_BIT) != 0)) {
                matched = true;
                lastMatchBoundary = position;
            }
        }
        return finalizeResult(textBegin, textEnd, endMatch, matched, lastMatchBoundary, false);
    }

    private static long finalizeResult(int textBegin, int textEnd, boolean endMatch, boolean matched, int lastMatchBoundaryAbs, boolean runForward)
    {
        if (!matched || lastMatchBoundaryAbs < 0) {
            return SEARCH_NO_MATCH;
        }
        int expectedBoundary = runForward ? textEnd : textBegin;
        int end = lastMatchBoundaryAbs - textBegin;
        if (endMatch && lastMatchBoundaryAbs != expectedBoundary) {
            return SEARCH_NO_MATCH;
        }
        return end;
    }

    private static final int BYTE_END_TEXT = 256;
    private static final int FLAG_MATCH = 0x0100;
    private static final int FLAG_LAST_WORD = 0x0200;
    private static final int FLAG_EMPTY_MASK = 0x00FF;
    private static final int FLAG_NEED_SHIFT = 16;

    // Transition table sentinel values (stored in transitions[])
    static final int T_UNCOMPUTED = -3;    // not yet computed (must have sign bit set for isAbnormal)
    static final int T_DEAD = -1;          // dead state
    static final int T_FULL_MATCH = -2;    // rest of string matches

    // Match bit: OR'd into transition values pointing to match states
    static final int T_MATCH_BIT = 1 << 30;

    private static final int PAIR_UNCOMPUTED = 0;
    private static final int PAIR_DEAD = -1;
    private static final int PAIR_FULL_MATCH = -2;
    private static final int PAIR_MATCH_FIRST = 1;
    private static final int PAIR_MATCH_SECOND = 2;
    private static final int PAIR_NORMAL_FIRST = 3;
    private static final int PAIR_NORMAL_SECOND = 4;

    // Maximum transitions array length - ensures offsets fit below T_MATCH_BIT
    private static final int MAX_TRANSITIONS_LENGTH = 1 << 30;

    // Special instruction IDs for mark support (dfa.cc:357-361)
    private static final int Mark = -1;
    private static final int MatchSep = -2;

    private static boolean isWordChar(int c)
    {
        int b = c & 0xFF;
        return ('A' <= b && b <= 'Z') ||
                ('a' <= b && b <= 'z') ||
                ('0' <= b && b <= '9') ||
                b == '_';
    }

    private static final class RetrySearchException
            extends RuntimeException
    {
        private RetrySearchException()
        {
            super(null, null, false, false);
        }
    }

    private record AbsolutePointerTransitions(
            Arena arena,
            MemorySegment segment,
            long baseAddress,
            int entryCapacity,
            long allocatedBytes) {}

    private static final class AbsoluteAddressSpace
    {
        private static final MemorySegment EVERYTHING = MemorySegment.NULL.reinterpret(Long.MAX_VALUE);

        private AbsoluteAddressSpace() {}
    }

    private static ReaderSlot registerReaderSlot()
    {
        synchronized (READER_SLOTS_LOCK) {
            removeStaleReaderSlotsLocked();
            ReaderSlot readerSlot = new ReaderSlot();
            readerSlots.add(new ReaderSlotReference(readerSlot));
            return readerSlot;
        }
    }

    private static int removeStaleReaderSlots()
    {
        synchronized (READER_SLOTS_LOCK) {
            return removeStaleReaderSlotsLocked();
        }
    }

    private static int removeStaleReaderSlotsLocked()
    {
        int removedReferences = 0;
        ReaderSlotReference staleReference;
        while ((staleReference = (ReaderSlotReference) STALE_READER_SLOTS.poll()) != null) {
            readerSlots.remove(staleReference);
            removedReferences++;
        }

        // Hash-based collections retain their peak table after removals. Reader registration is
        // a once-per-thread cold path, so compact a stale-heavy registry instead of retaining a
        // virtual-thread wave's high-water table for the lifetime of the class loader.
        if (removedReferences > 0 && (readerSlots.isEmpty() || removedReferences > readerSlots.size())) {
            readerSlots = new HashSet<>(readerSlots);
        }
        return removedReferences;
    }

    static int drainStaleReaderSlots()
    {
        return removeStaleReaderSlots();
    }

    static ReaderRegistrySnapshot readerRegistrySnapshot()
    {
        synchronized (READER_SLOTS_LOCK) {
            int liveReferences = 0;
            int activeReaders = 0;
            for (ReaderSlotReference reference : readerSlots) {
                ReaderSlot readerSlot = reference.get();
                if (readerSlot != null) {
                    liveReferences++;
                    if (readerSlot.activeDfaId != 0) {
                        activeReaders++;
                    }
                }
            }
            int references = readerSlots.size();
            return new ReaderRegistrySnapshot(references, liveReferences, references - liveReferences, activeReaders);
        }
    }

    static Object readerRegistryRoot()
    {
        synchronized (READER_SLOTS_LOCK) {
            return readerSlots;
        }
    }

    record ReaderRegistrySnapshot(int references, int liveReferences, int staleReferences, int activeReaders) {}

    record CacheSnapshot(
            int stateCount,
            int cacheEntries,
            int populatedStateData,
            int populatedStateReferences,
            int populatedStartStates,
            int pairedRows,
            long pairedTransitionMemory,
            long fixedDistanceByteCandidateMemory,
            long availableStateMemory,
            long stateBudget,
            int transitionCapacity,
            int stateCapacity,
            int stateReferenceCapacity,
            int resetCount) {}

    record StateMemorySnapshot(
            long totalBytes,
            long transitionBytes,
            long stateDataArrayBytes,
            long stateReferenceArrayBytes,
            long stateReferenceRowBytes,
            long instructionBytes,
            long stateMetadataBytes,
            long cacheBytes,
            int cacheTableCapacity) {}

    private static void awaitNoReaders(DfaInstance dfa)
    {
        ReaderSlotReference[] references;
        synchronized (READER_SLOTS_LOCK) {
            removeStaleReaderSlotsLocked();
            references = readerSlots.toArray(ReaderSlotReference[]::new);
        }
        int spinCount = 0;
        while (true) {
            boolean hasActiveReader = false;
            for (ReaderSlotReference reference : references) {
                ReaderSlot readerSlot = reference.get();
                if (readerSlot != null && readerSlot.activeDfaId == dfa.readerRegistryId) {
                    hasActiveReader = true;
                    break;
                }
            }
            if (!hasActiveReader) {
                return;
            }
            if (spinCount++ < 1_000) {
                Thread.onSpinWait();
            }
            else {
                LockSupport.parkNanos(1_000);
            }
        }
    }

    private static final class ReaderSlot
    {
        private volatile long activeDfaId;

        // Reader slots are written on every search, so adjacent slots must not share a cache line.
        @SuppressWarnings("unused")
        private long padding0;
        private long padding1;
        private long padding2;
        private long padding3;
        private long padding4;
        private long padding5;
        private long padding6;
    }

    private static final class ReaderSlotReference
            extends WeakReference<ReaderSlot>
    {
        private ReaderSlotReference(ReaderSlot readerSlot)
        {
            super(readerSlot, STALE_READER_SLOTS);
        }
    }

    /**
     * Cold-path state metadata. Replaces the old per-state object with next[] array.
     * The actual transitions live in the flat {@code DfaInstance.transitions[]} table.
     */
    record StateData(int[] inst, int flag, int offset) {}

    private record RestoredStates(int startOffset, int stateOffset) {}

    // Sentinel StateData values
    private static final StateData DEAD_DATA = new StateData(new int[0], 0, T_DEAD);
    private static final StateData FULL_MATCH_DATA = new StateData(new int[0], FLAG_MATCH, T_FULL_MATCH);

    private static final class StateKey
    {
        private final int flag;
        private final int[] inst;
        private final int hash;

        StateKey(int[] inst, int flag)
        {
            this.flag = flag;
            this.inst = inst;
            int h = Integer.hashCode(flag);
            for (int i = 0; i < inst.length; i++) {
                h = (31 * h) + inst[i];
            }
            this.hash = h;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StateKey other)) {
                return false;
            }
            if (flag != other.flag || inst.length != other.inst.length) {
                return false;
            }
            for (int i = 0; i < inst.length; i++) {
                if (inst[i] != other.inst[i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode()
        {
            return hash;
        }
    }

    // Mirrors HashMap.Node so SizeOf can account for entries without opening java.base.
    @SuppressWarnings("unused")
    private static final class StateCacheEntryShape
    {
        private int hash;
        private StateKey key;
        private StateData value;
        private StateCacheEntryShape next;
    }

    /**
     * Work queue for DFA state construction.
     * Ported from dfa.cc:368-418.
     */
    private static final class WorkQueue
    {
        private final SparseSet set;
        private final int n;
        private final int maxmark;
        private int nextmark;
        private boolean lastWasMark;

        WorkQueue(int n, int maxmark)
        {
            this.set = new SparseSet(n + maxmark);
            this.n = n;
            this.maxmark = maxmark;
            this.nextmark = n;
            this.lastWasMark = true;
        }

        boolean isMark(int i)
        {
            return i >= n;
        }

        int maxmark()
        {
            return maxmark;
        }

        void clear()
        {
            set.clear();
            nextmark = n;
        }

        void mark()
        {
            if (lastWasMark) {
                return;
            }
            lastWasMark = true;
            set.insertNew(nextmark++);
        }

        boolean contains(int id)
        {
            return set.contains(id);
        }

        void insertNew(int id)
        {
            lastWasMark = false;
            set.insertNew(id);
        }

        int size()
        {
            return set.size();
        }

        int denseAt(int i)
        {
            return set.denseAt(i);
        }
    }

    /**
     * Saves a state so it can be restored after cache reset.
     */
    private static class StateSaver
    {
        private final int[] inst;
        private final int flag;
        private final int sentinelOffset;

        StateSaver(DfaInstance dfa, int stateOffset)
        {
            if (stateOffset == T_DEAD || stateOffset == T_FULL_MATCH) {
                this.inst = null;
                this.flag = 0;
                this.sentinelOffset = stateOffset;
            }
            else {
                StateData sd = dfa.stateData[stateOffset / dfa.nextSize];
                this.inst = sd.inst().clone();
                this.flag = sd.flag();
                this.sentinelOffset = 0;
            }
        }

        int restoreLocked(DfaInstance dfa)
        {
            if (inst == null) {
                return sentinelOffset;
            }
            StateData sd = dfa.restoreStateLocked(inst, flag);
            if (sd == null) {
                return -1;
            }
            return sd.offset();
        }
    }

    @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
    static final class CandidateStartCursor
    {
        private final DfaInstance dfa;

        private long remainingWork;
        private boolean disabled;
        private long routeCount;
        private long fallbackCount;

        private CandidateStartCursor(DfaInstance dfa)
        {
            this.dfa = dfa;
        }

        void reset(int inputLength)
        {
            remainingWork = 3L * inputLength;
            disabled = false;
        }

        boolean enabled()
        {
            return !disabled;
        }

        long routeCount()
        {
            return routeCount;
        }

        long fallbackCount()
        {
            return fallbackCount;
        }

        private boolean consumeWork()
        {
            if (remainingWork == 0) {
                return false;
            }
            remainingWork--;
            return true;
        }
    }

    public static final class DfaInstance
    {
        private final Prog prog;
        private final long readerRegistryId = NEXT_DFA_ID.incrementAndGet();
        final byte[] bytemap;
        private final byte[] startByteCandidates;
        private final boolean nullableStartByteCandidates;
        private volatile Prog.FixedDistanceByteCandidates fixedDistanceByteCandidates;
        private long fixedDistanceByteCandidateMemory;
        final int bytemapRange;
        final int nextSize;
        private final Kind kind;

        public enum Kind
        {
            FIRST_MATCH,
            LONGEST_MATCH,
            MANY_MATCH,
        }

        private final long stateBudget;
        private long availableStateMemory;
        private long retainedStateMemory;
        private final boolean initFailed;
        private final int maxTransitionsLength;

        private final ReentrantLock cacheMutationLock = new ReentrantLock();
        private volatile boolean hasPendingCacheMutation;
        private volatile long cacheVersion;

        private WorkQueue q0;
        private WorkQueue q1;
        private final int[] stack;

        // Flat transition table (hot path)
        volatile int[] transitions;
        int nextFreeOffset;

        // State metadata (cold path)
        volatile StateData[] stateData;
        volatile Object[][] stateRefArrays;  // parallel to stateData[], indexed by stateIndex
        volatile Object[][] pairedStateRefArrays;
        private long pairedTransitionMemory;
        private volatile boolean pairedTransitionsRequested;
        private boolean pairedTransitionsRejected;
        private boolean pairedTransitionsDisabled;
        private int shortPairedSearchCount;
        private volatile AbsolutePointerTransitions absolutePointerTransitions;
        private boolean absolutePointerTransitionsRequested;
        private boolean absolutePointerTransitionsUnavailable;
        private int shortAbsolutePointerSearchCount;
        private volatile int absolutePointerTransitionCount;
        volatile int stateCount;

        // Cache: StateKey -> StateData
        private final HashMap<StateKey, StateData> cache = new HashMap<>();
        private int cacheTableCapacity;

        // Start state caches
        private final StateData[][] startBeginText = new StateData[2][2];
        private final StateData[][] startBeginLine = new StateData[2][2];
        private final StateData[][] startAfterWord = new StateData[2][2];
        private final StateData[][] startAfterNonWord = new StateData[2][2];

        // Observability
        private volatile int resetCount;
        private int pairedRowSelectionCount;
        private int byteScanFallbackCount;
        private int matchingSelfLoopScanCount;

        public DfaInstance(Prog prog, Kind kind, long maxMemory)
        {
            this.prog = requireNonNull(prog, "prog is null");
            this.bytemap = prog.bytemapArray();
            this.bytemapRange = prog.bytemapRange();
            this.kind = requireNonNull(kind, "kind is null");
            this.nextSize = prog.bytemapRange() + 1;
            int nmark = (kind == Kind.LONGEST_MATCH) ? prog.size() : 0;

            this.q0 = new WorkQueue(prog.size(), nmark);
            this.q1 = new WorkQueue(prog.size(), nmark);
            this.stack = new int[Math.max(16, prog.size() * 3 + 32 + nmark + 1)];
            this.startByteCandidates = prog.buildStartByteCandidates(stack);
            this.nullableStartByteCandidates = startByteCandidates != null && prog.canMatchEmpty();

            if (maxMemory == 0) {
                this.stateBudget = Long.MAX_VALUE;
                this.availableStateMemory = Long.MAX_VALUE;
                this.initFailed = false;
                this.maxTransitionsLength = MAX_TRANSITIONS_LENGTH;
                int initialCapacity = Math.max(prog.size(), 16);
                this.transitions = new int[initialCapacity * nextSize];
                Arrays.fill(this.transitions, T_UNCOMPUTED);
                this.stateData = new StateData[initialCapacity];
                this.stateRefArrays = new Object[initialCapacity][];
                this.retainedStateMemory = stateBackingMemory();
                this.nextFreeOffset = nextSize; // skip offset 0 (reserved)
                this.stateCount = 0;
                return;
            }

            long overhead = 64;
            overhead += CONSERVATIVE_REFERENCE_BYTES + Long.BYTES;
            overhead += CONSERVATIVE_REFERENCE_BYTES + Integer.BYTES;
            overhead += (prog.size() + (long) nmark) * 8L * 2;
            overhead += Math.max(16L, (prog.size() * 3L) + 32 + nmark + 1) * Integer.BYTES;
            if (startByteCandidates != null) {
                overhead += 16L + startByteCandidates.length;
            }

            long memoryBudget = maxMemory - overhead;
            if (memoryBudget < 0) {
                this.initFailed = true;
                this.stateBudget = 0;
                this.availableStateMemory = 0;
                this.maxTransitionsLength = 0;
                this.transitions = new int[0];
                this.stateData = new StateData[0];
                this.stateRefArrays = new Object[0][];
                return;
            }

            this.stateBudget = memoryBudget;

            // Upstream uses the largest possible state only for this initialization gate.
            // Actual cache growth is charged by each state's instruction count.
            long largestStateSize = estimateStateSize(prog.listCount() + nmark);
            if (stateBudget / largestStateSize < 20) {
                this.initFailed = true;
                this.maxTransitionsLength = 0;
                this.transitions = new int[0];
                this.stateData = new StateData[0];
                this.stateRefArrays = new Object[0][];
                return;
            }

            this.initFailed = false;
            long maxStateCount = stateBudget / estimateStateSize(0);
            long maxTransitionStateCount = MAX_TRANSITIONS_LENGTH / nextSize;
            this.maxTransitionsLength = (int) (Math.min(maxStateCount + 1, maxTransitionStateCount) * nextSize);
            int initialCapacity = Math.max(Math.min(prog.size(), maxTransitionsLength / nextSize), 16);
            this.transitions = new int[initialCapacity * nextSize];
            Arrays.fill(this.transitions, T_UNCOMPUTED);
            this.stateData = new StateData[initialCapacity];
            this.stateRefArrays = new Object[initialCapacity][];
            this.retainedStateMemory = stateBackingMemory();
            this.availableStateMemory = memoryBudget - retainedStateMemory;
            this.nextFreeOffset = nextSize;
            this.stateCount = 0;
        }

        boolean canStartByteAcceleration()
        {
            return startByteCandidates != null && !nullableStartByteCandidates;
        }

        boolean canUseNullableStartByteCheck()
        {
            return nullableStartByteCandidates;
        }

        boolean canFixedDistanceByteAcceleration()
        {
            return fixedDistanceByteCandidates() != null;
        }

        boolean fixedDistanceByteCandidatesInitialized()
        {
            return fixedDistanceByteCandidates != null;
        }

        private Prog.FixedDistanceByteCandidates fixedDistanceByteCandidates()
        {
            Prog.FixedDistanceByteCandidates candidates = fixedDistanceByteCandidates;
            if (candidates == null) {
                cacheMutationLock.lock();
                try {
                    candidates = fixedDistanceByteCandidates;
                    if (candidates == null) {
                        candidates = prog.buildFixedDistanceByteCandidates(stack, startByteCandidates);
                        if (candidates != null && availableStateMemory >= FIXED_DISTANCE_BYTE_CANDIDATE_MEMORY) {
                            availableStateMemory -= FIXED_DISTANCE_BYTE_CANDIDATE_MEMORY;
                            fixedDistanceByteCandidateMemory = FIXED_DISTANCE_BYTE_CANDIDATE_MEMORY;
                        }
                        else if (candidates != null && stateBudget >= FIXED_DISTANCE_BYTE_CANDIDATE_MEMORY) {
                            candidates = RETRY_FIXED_DISTANCE_BYTE_CANDIDATES_AFTER_RESET;
                        }
                        else {
                            candidates = NO_FIXED_DISTANCE_BYTE_CANDIDATES;
                        }
                        fixedDistanceByteCandidates = candidates;
                    }
                }
                finally {
                    cacheMutationLock.unlock();
                }
            }
            return candidates == NO_FIXED_DISTANCE_BYTE_CANDIDATES ||
                    candidates == RETRY_FIXED_DISTANCE_BYTE_CANDIDATES_AFTER_RESET ? null : candidates;
        }

        int fixedDistanceByteOffset()
        {
            Prog.FixedDistanceByteCandidates candidates = fixedDistanceByteCandidates();
            return candidates == null ? -1 : candidates.offset();
        }

        boolean isStartByteCandidate(int value)
        {
            return startByteCandidates[value] != 0;
        }

        int findStartByteCandidate(byte[] data, int offset, int length)
        {
            return findByteCandidate(startByteCandidates, data, offset, length);
        }

        int findFixedDistanceByteCandidate(byte[] data, int offset, int length)
        {
            Prog.FixedDistanceByteCandidates candidates = fixedDistanceByteCandidates();
            if (candidates == null || candidates.offset() >= length) {
                return -1;
            }
            int candidateBytePosition = findByteCandidate(
                    candidates.candidates(),
                    data,
                    offset + candidates.offset(),
                    length - candidates.offset());
            return candidateBytePosition < 0 ? -1 : candidateBytePosition - candidates.offset();
        }

        public boolean ok()
        {
            return !initFailed;
        }

        public int resetCount()
        {
            return resetCount;
        }

        int cacheEntryCount()
        {
            return cache.size();
        }

        CacheSnapshot cacheSnapshot()
        {
            int populatedStateData = 0;
            for (StateData state : stateData) {
                if (state != null) {
                    populatedStateData++;
                }
            }

            int populatedStateReferences = 0;
            for (Object[] stateReferences : stateRefArrays) {
                if (stateReferences != null) {
                    populatedStateReferences++;
                }
            }

            int populatedStartStates = countPopulatedStartStates(startBeginText) +
                    countPopulatedStartStates(startBeginLine) +
                    countPopulatedStartStates(startAfterWord) +
                    countPopulatedStartStates(startAfterNonWord);

            return new CacheSnapshot(
                    stateCount,
                    cache.size(),
                    populatedStateData,
                    populatedStateReferences,
                    populatedStartStates,
                    pairedTransitionRowCount(),
                    pairedTransitionMemory,
                    fixedDistanceByteCandidateMemory,
                    availableStateMemory,
                    stateBudget,
                    transitions.length,
                    stateData.length,
                    stateRefArrays.length,
                    resetCount);
        }

        StateMemorySnapshot stateMemorySnapshot()
        {
            long transitionBytes = sizeOfIntArray(transitions.length);
            long stateDataArrayBytes = sizeOfObjectArray(stateData.length);
            long stateReferenceArrayBytes = sizeOfObjectArray(stateRefArrays.length);
            long stateReferenceRowBytes = 0;
            long instructionBytes = 0;
            int stateLimit = Math.min(nextFreeOffset / nextSize, stateData.length);
            for (int stateIndex = 0; stateIndex < stateLimit; stateIndex++) {
                StateData state = stateData[stateIndex];
                if (state == null) {
                    continue;
                }
                stateReferenceRowBytes += sizeOfObjectArray(stateRefArrays[stateIndex].length);
                instructionBytes += sizeOfIntArray(state.inst().length);
            }

            long stateMetadataBytes = (long) cache.size() *
                    (STATE_DATA_BYTES + STATE_KEY_BYTES);
            long cacheBytes = STATE_CACHE_BYTES +
                    hashMapTableBytes(cacheTableCapacity) +
                    ((long) cache.size() * STATE_CACHE_ENTRY_BYTES);
            long totalBytes = transitionBytes +
                    stateDataArrayBytes +
                    stateReferenceArrayBytes +
                    stateReferenceRowBytes +
                    instructionBytes +
                    stateMetadataBytes +
                    cacheBytes;
            return new StateMemorySnapshot(
                    totalBytes,
                    transitionBytes,
                    stateDataArrayBytes,
                    stateReferenceArrayBytes,
                    stateReferenceRowBytes,
                    instructionBytes,
                    stateMetadataBytes,
                    cacheBytes,
                    cacheTableCapacity);
        }

        private static int countPopulatedStartStates(StateData[][] startStates)
        {
            int count = 0;
            for (StateData[] startStateRow : startStates) {
                for (StateData startState : startStateRow) {
                    if (startState != null) {
                        count++;
                    }
                }
            }
            return count;
        }

        private long estimateStateSize(int instructionCount)
        {
            return stateObjectMemory(instructionCount) +
                    ((long) nextSize * Integer.BYTES) +
                    (2 * REFERENCE_BYTES) +
                    (2 * REFERENCE_BYTES);
        }

        private int allocateState(int instructionCount)
        {
            long stateMemory = stateObjectMemory(instructionCount);
            long newTransitionsLength = transitions.length;
            if (nextFreeOffset + nextSize > transitions.length) {
                newTransitionsLength = Math.min((long) transitions.length * 2, maxTransitionsLength);
                if (newTransitionsLength <= transitions.length || newTransitionsLength < nextFreeOffset + nextSize) {
                    return -1; // budget exhausted
                }
                int newStateCount = toIntExact(newTransitionsLength / nextSize);
                stateMemory += sizeOfIntArray(toIntExact(newTransitionsLength)) - sizeOfIntArray(transitions.length);
                stateMemory += sizeOfObjectArray(newStateCount) - sizeOfObjectArray(stateData.length);
                stateMemory += sizeOfObjectArray(newStateCount) - sizeOfObjectArray(stateRefArrays.length);
            }

            int newCacheTableCapacity = Math.max(cacheTableCapacity, hashMapCapacity(cache.size() + 1));
            stateMemory += hashMapTableBytes(newCacheTableCapacity) - hashMapTableBytes(cacheTableCapacity);
            if (!ensureAbsolutePointerTransitionCapacity(nextFreeOffset + nextSize, stateMemory)) {
                return -1;
            }
            if (availableStateMemory < stateMemory) {
                clearPairedTransitions();
                if (availableStateMemory < stateMemory) {
                    return -1;
                }
            }
            if (newTransitionsLength != transitions.length) {
                int oldLen = transitions.length;
                transitions = Arrays.copyOf(transitions, toIntExact(newTransitionsLength));
                Arrays.fill(transitions, oldLen, transitions.length, T_UNCOMPUTED);
                int newStateCount = toIntExact(newTransitionsLength / nextSize);
                stateData = Arrays.copyOf(stateData, newStateCount);
                stateRefArrays = Arrays.copyOf(stateRefArrays, newStateCount);
            }
            cacheTableCapacity = newCacheTableCapacity;
            int offset = nextFreeOffset;
            nextFreeOffset += nextSize;
            stateCount++;
            stateRefArrays[offset / nextSize] = new Object[nextSize + 1];
            availableStateMemory -= stateMemory;
            retainedStateMemory += stateMemory;
            return offset;
        }

        boolean hasPairedTransitions(int stateOffset)
        {
            Object[][] pairedStateRefArrays = this.pairedStateRefArrays;
            int stateIndex = stateOffset / nextSize;
            return pairedStateRefArrays != null &&
                    stateIndex < pairedStateRefArrays.length &&
                    pairedStateRefArrays[stateIndex] != null;
        }

        private void recordPairedSearch(int searchedBytes)
        {
            if (searchedBytes >= SHORT_PAIRED_SEARCH_BYTES) {
                shortPairedSearchCount = 0;
                return;
            }

            if (++shortPairedSearchCount < SHORT_PAIRED_SEARCH_LIMIT) {
                return;
            }

            // The hint is intentionally racy: concurrent readers may change when the
            // threshold is reached, but rejection only selects the equivalent compact path.
            pairedTransitionsRejected = true;
            pairedTransitionsRequested = false;
            throw RETRY_SEARCH;
        }

        void requestPairedTransitions()
        {
            if (pairedTransitionsRequested) {
                return;
            }
            if (!cacheMutationLock.isHeldByCurrentThread()) {
                throw RETRY_SEARCH;
            }
            if (pairedTransitionsRejected) {
                clearPairedTransitions();
                pairedTransitionsRejected = false;
                pairedTransitionsDisabled = true;
                pairedTransitionsRequested = true;
                return;
            }
            pairedTransitionsRequested = true;
        }

        void requestAbsolutePointerTransitions()
        {
            if (absolutePointerTransitions != null || absolutePointerTransitionsUnavailable) {
                return;
            }
            if (!nativeAccessEnabled() || prog.reversed() || kind == Kind.MANY_MATCH || stateBudget == Long.MAX_VALUE) {
                absolutePointerTransitionsUnavailable = true;
                return;
            }
            if (!cacheMutationLock.isHeldByCurrentThread()) {
                throw RETRY_SEARCH;
            }
            absolutePointerTransitionsRequested = true;
        }

        boolean shouldUseAbsolutePointersForShortSearch()
        {
            if (absolutePointerTransitions != null) {
                return true;
            }
            if (absolutePointerTransitionsUnavailable || pairedTransitionsRequested) {
                return false;
            }
            // This racy hint changes only when the equivalent pointer route is selected.
            return ++shortAbsolutePointerSearchCount >= SHORT_ABSOLUTE_POINTER_SEARCH_LIMIT;
        }

        boolean hasAbsolutePointerTransitions(int stateOffset)
        {
            AbsolutePointerTransitions absolutePointers = absolutePointerTransitions;
            return absolutePointers != null &&
                    stateOffset + nextSize <= absolutePointers.entryCapacity();
        }

        long absolutePointerTransitionMemory()
        {
            AbsolutePointerTransitions absolutePointers = absolutePointerTransitions;
            return absolutePointers == null ? 0 : absolutePointers.allocatedBytes();
        }

        long absolutePointerTransitionBaseAddress()
        {
            AbsolutePointerTransitions absolutePointers = absolutePointerTransitions;
            return absolutePointers == null ? 0 : absolutePointers.baseAddress();
        }

        int absolutePointerTransitionCount()
        {
            return absolutePointerTransitionCount;
        }

        boolean absolutePointerTransitionsAvailable()
        {
            return absolutePointerTransitions != null;
        }

        long pairedTransitionMemory()
        {
            return pairedTransitionMemory;
        }

        int pairedTransitionRowCount()
        {
            Object[][] pairedStateRefArrays = this.pairedStateRefArrays;
            if (pairedStateRefArrays == null) {
                return 0;
            }

            int rowCount = 0;
            for (Object[] pairedStateRefArray : pairedStateRefArrays) {
                if (pairedStateRefArray != null) {
                    rowCount++;
                }
            }
            return rowCount;
        }

        int pairedTransitionCount()
        {
            Object[][] pairedStateRefArrays = this.pairedStateRefArrays;
            if (pairedStateRefArrays == null) {
                return 0;
            }

            int pairedClassCount = nextSize * nextSize;
            int transitionCount = 0;
            for (Object[] pairedStateRefArray : pairedStateRefArrays) {
                if (pairedStateRefArray == null) {
                    continue;
                }
                for (int pairedClass = 0; pairedClass < pairedClassCount; pairedClass++) {
                    if (pairedStateRefArray[pairedClass] != null) {
                        transitionCount++;
                    }
                }
            }
            return transitionCount;
        }

        int selfLoopTransitionCount()
        {
            int transitionCount = 0;
            Object[][] stateRefArrays = this.stateRefArrays;
            for (Object[] stateRefs : stateRefArrays) {
                if (stateRefs == null) {
                    continue;
                }
                for (int byteClass = 0; byteClass < nextSize; byteClass++) {
                    if (stateRefs[byteClass] == stateRefs) {
                        transitionCount++;
                    }
                }
            }
            return transitionCount;
        }

        int pairedMatchContinuationCount()
        {
            if (kind != Kind.FIRST_MATCH) {
                return 0;
            }
            Object[][] pairedStateRefArrays = this.pairedStateRefArrays;
            if (pairedStateRefArrays == null) {
                return 0;
            }

            int continuationCount = 0;
            for (Object[] pairedStateRefs : pairedStateRefArrays) {
                if (pairedStateRefs == null) {
                    continue;
                }
                int stateOffset = ((StateData) pairedStateRefs[nextSize * nextSize]).offset();
                for (int firstClass = 0; firstClass < nextSize; firstClass++) {
                    for (int secondClass = 0; secondClass < nextSize; secondClass++) {
                        int outcome = (int) decodePairedTransition(this, stateOffset, firstClass, secondClass);
                        if (outcome == PAIR_MATCH_FIRST || outcome == PAIR_MATCH_SECOND) {
                            continuationCount++;
                        }
                    }
                }
            }
            return continuationCount;
        }

        int matchingSelfLoopScanCount()
        {
            return matchingSelfLoopScanCount;
        }

        int pairedMatchContinuationToUnpairedCount()
        {
            if (kind != Kind.FIRST_MATCH) {
                return 0;
            }
            Object[][] pairedStateRefArrays = this.pairedStateRefArrays;
            if (pairedStateRefArrays == null) {
                return 0;
            }

            int continuationCount = 0;
            for (Object[] pairedStateRefs : pairedStateRefArrays) {
                if (pairedStateRefs == null) {
                    continue;
                }
                int stateOffset = ((StateData) pairedStateRefs[nextSize * nextSize]).offset();
                for (int firstClass = 0; firstClass < nextSize; firstClass++) {
                    for (int secondClass = 0; secondClass < nextSize; secondClass++) {
                        long pairedTransition = decodePairedTransition(this, stateOffset, firstClass, secondClass);
                        int outcome = (int) pairedTransition;
                        if ((outcome == PAIR_MATCH_FIRST || outcome == PAIR_MATCH_SECOND) &&
                                !hasPairedTransitions((int) (pairedTransition >>> 32))) {
                            continuationCount++;
                        }
                    }
                }
            }
            return continuationCount;
        }

        boolean pairedTransitionsDisabled()
        {
            return pairedTransitionsDisabled;
        }

        int pairedRowSelectionCount()
        {
            return pairedRowSelectionCount;
        }

        int byteScanFallbackCount()
        {
            return byteScanFallbackCount;
        }

        long estimatedPairedTransitionMemory()
        {
            int stateLimit = Math.min(nextFreeOffset / nextSize, stateData.length);
            return pairedOuterMemory(stateLimit) + (long) stateCount * pairedRowMemory();
        }

        long availableStateMemory()
        {
            return availableStateMemory;
        }

        long retainedStateMemory()
        {
            return retainedStateMemory;
        }

        long stateBudget()
        {
            return stateBudget;
        }

        private void rebuildPairedTransitions()
        {
            clearPairedTransitions();
            if (prog.reversed() || kind == Kind.MANY_MATCH || stateCount == 0) {
                return;
            }

            int stateLimit = Math.min(nextFreeOffset / nextSize, stateData.length);
            int pairedClassCount = nextSize * nextSize;
            long outerMemory = pairedOuterMemory(stateLimit);
            long rowMemory = pairedRowMemory();
            long maximumMemory = Math.min(MAX_PAIRED_TRANSITION_MEMORY, availableStateMemory);
            if (outerMemory + rowMemory > maximumMemory) {
                return;
            }

            int maximumRows = (int) Math.min(stateCount, (maximumMemory - outerMemory) / rowMemory);
            boolean completeTable = maximumRows >= stateCount;
            if (!completeTable && outerMemory + (2 * rowMemory) > MAX_PARTIAL_PAIRED_TRANSITION_MEMORY) {
                return;
            }
            boolean[] selectedRows = selectPairedRows(stateLimit, maximumRows);
            int selectedRowCount = 0;
            for (boolean selected : selectedRows) {
                if (selected) {
                    selectedRowCount++;
                }
            }
            if (selectedRowCount == 0) {
                return;
            }

            long memory = outerMemory + (selectedRowCount * rowMemory);
            // A partial table is useful only for the common entry-to-hot-loop shape.
            // Broader partial tables consume state budget and cache without reducing
            // the dependent-load chain consistently enough to justify their cost.
            if (!completeTable &&
                    (selectedRowCount != 2 ||
                            memory > MAX_PARTIAL_PAIRED_TRANSITION_MEMORY)) {
                return;
            }

            Object[][] pairedRows = new Object[stateLimit][];
            for (int stateIndex = 1; stateIndex < stateLimit; stateIndex++) {
                if (selectedRows[stateIndex]) {
                    pairedRows[stateIndex] = new Object[pairedClassCount + 1];
                    pairedRows[stateIndex][pairedClassCount] = stateData[stateIndex];
                }
            }

            int transitionCount = 0;

            for (int stateIndex = 1; stateIndex < stateLimit; stateIndex++) {
                Object[] source = stateRefArrays[stateIndex];
                Object[] sourcePairs = pairedRows[stateIndex];
                if (sourcePairs == null) {
                    continue;
                }
                for (int firstClass = 0; firstClass < nextSize; firstClass++) {
                    if (!(source[firstClass] instanceof Object[] intermediate)) {
                        continue;
                    }
                    for (int secondClass = 0; secondClass < nextSize; secondClass++) {
                        if (intermediate[secondClass] instanceof Object[] target) {
                            StateData targetData = (StateData) target[nextSize];
                            Object[] pairedTarget = pairedRows[targetData.offset() / nextSize];
                            sourcePairs[(firstClass * nextSize) + secondClass] = pairedTarget;
                            if (pairedTarget != null) {
                                transitionCount++;
                            }
                        }
                    }
                }
            }
            if (transitionCount == 0) {
                return;
            }

            availableStateMemory -= memory;
            pairedTransitionMemory = memory;
            pairedStateRefArrays = pairedRows;
        }

        private void allocateAbsolutePointerTransitions()
        {
            if (!absolutePointerTransitionsRequested ||
                    absolutePointerTransitions != null ||
                    absolutePointerTransitionsUnavailable ||
                    pairedStateRefArrays != null) {
                return;
            }

            long rowBytes;
            long rowCapacity;
            long allocatedBytes;
            try {
                rowBytes = Math.multiplyExact((long) nextSize, Long.BYTES);
                long currentRowCount = nextFreeOffset / nextSize;
                long minimumStateMemory = stateObjectMemory(0);
                // Reserve enough budget for the Java states addressable by the sidecar instead
                // of eagerly consuming the complete DFA budget for a small graph.
                long compatibleRowCount = Math.addExact(
                        availableStateMemory,
                        Math.multiplyExact(currentRowCount, minimumStateMemory)) /
                        Math.addExact(rowBytes, minimumStateMemory);
                rowCapacity = Math.min(
                        maxTransitionsLength / nextSize,
                        Math.min(compatibleRowCount, Math.max(2, currentRowCount * 2)));
                allocatedBytes = Math.multiplyExact(rowCapacity, rowBytes);
            }
            catch (ArithmeticException ignored) {
                absolutePointerTransitionsUnavailable = true;
                return;
            }
            if (rowCapacity < 2 ||
                    rowCapacity > Integer.MAX_VALUE ||
                    allocatedBytes > availableStateMemory ||
                    nextFreeOffset > rowCapacity * nextSize) {
                absolutePointerTransitionsUnavailable = true;
                return;
            }

            Arena arena;
            MemorySegment segment;
            try {
                arena = Arena.ofAuto();
                segment = arena.allocate(allocatedBytes, Long.BYTES);
                segment.fill((byte) 0);
            }
            catch (RuntimeException | OutOfMemoryError ignored) {
                absolutePointerTransitionsUnavailable = true;
                return;
            }
            AbsolutePointerTransitions absolutePointers = new AbsolutePointerTransitions(
                    arena,
                    segment,
                    segment.address(),
                    toIntExact(rowCapacity * nextSize),
                    allocatedBytes);
            int transitionCount = backfillAbsolutePointerTransitions(absolutePointers);

            availableStateMemory -= allocatedBytes;
            absolutePointerTransitionCount = transitionCount;
            absolutePointerTransitions = absolutePointers;
            pairedTransitionsDisabled = true;
        }

        private boolean ensureAbsolutePointerTransitionCapacity(int requiredEntryCapacity, long reservedStateMemory)
        {
            AbsolutePointerTransitions existingPointers = absolutePointerTransitions;
            if (existingPointers == null || requiredEntryCapacity <= existingPointers.entryCapacity()) {
                return true;
            }

            // State allocation runs with exclusive cache access, so the native table can be
            // geometrically enlarged and rebased without an active reader retaining its address.
            long rowBytes;
            long requiredRowCount;
            long rowCapacity;
            long allocatedBytes;
            try {
                rowBytes = Math.multiplyExact((long) nextSize, Long.BYTES);
                requiredRowCount = Math.addExact(requiredEntryCapacity, nextSize - 1L) / nextSize;
                long currentRowCount = existingPointers.entryCapacity() / nextSize;
                long maximumRowCount = maxTransitionsLength / nextSize;
                long minimumStateMemory = stateObjectMemory(0);
                long remainingMemory = Math.max(0, availableStateMemory - reservedStateMemory);
                // Preserve enough budget for the minimum Java state behind every additional
                // native row. Failure means ordinary DFA budget exhaustion, not an object-row
                // representation change.
                long compatibleRowCount = Math.addExact(
                        remainingMemory,
                        Math.addExact(
                                existingPointers.allocatedBytes(),
                                Math.multiplyExact(requiredRowCount, minimumStateMemory))) /
                        Math.addExact(rowBytes, minimumStateMemory);
                rowCapacity = Math.min(maximumRowCount, Math.min(compatibleRowCount, currentRowCount * 2));
                allocatedBytes = Math.multiplyExact(rowCapacity, rowBytes);
            }
            catch (ArithmeticException ignored) {
                return false;
            }
            if (rowCapacity < requiredRowCount || rowCapacity > Integer.MAX_VALUE) {
                return false;
            }

            Arena arena;
            MemorySegment segment;
            try {
                arena = Arena.ofAuto();
                segment = arena.allocate(allocatedBytes, Long.BYTES);
                segment.fill((byte) 0);
            }
            catch (RuntimeException | OutOfMemoryError ignored) {
                return false;
            }

            AbsolutePointerTransitions expandedPointers = new AbsolutePointerTransitions(
                    arena,
                    segment,
                    segment.address(),
                    toIntExact(rowCapacity * nextSize),
                    allocatedBytes);
            int transitionCount = backfillAbsolutePointerTransitions(expandedPointers);
            availableStateMemory -= allocatedBytes - existingPointers.allocatedBytes();
            absolutePointerTransitionCount = transitionCount;
            absolutePointerTransitions = expandedPointers;
            return true;
        }

        private int backfillAbsolutePointerTransitions(AbsolutePointerTransitions absolutePointers)
        {
            int transitionCount = 0;
            int transitionLimit = Math.min(nextFreeOffset, absolutePointers.entryCapacity());
            for (int transitionIndex = nextSize; transitionIndex < transitionLimit; transitionIndex++) {
                int transition = transitions[transitionIndex];
                if (transition > 0 &&
                        (transition & T_MATCH_BIT) == 0 &&
                        transition + nextSize <= absolutePointers.entryCapacity()) {
                    setAbsolutePointerTransition(absolutePointers, transitionIndex, transition);
                    transitionCount++;
                }
            }
            return transitionCount;
        }

        private static void setAbsolutePointerTransition(AbsolutePointerTransitions absolutePointers, int transitionIndex, int targetOffset)
        {
            absolutePointers.segment().set(
                    ValueLayout.JAVA_LONG_UNALIGNED,
                    (long) transitionIndex * Long.BYTES,
                    absolutePointers.baseAddress() + ((long) targetOffset * Long.BYTES));
        }

        private boolean[] selectPairedRows(int stateLimit, int maximumRows)
        {
            pairedRowSelectionCount++;
            boolean[] selected = new boolean[stateLimit];
            int selectedCount = 0;

            selectedCount = selectCachedStartRows(selected, startBeginText[1], maximumRows, selectedCount);
            selectedCount = selectCachedStartRows(selected, startBeginLine[1], maximumRows, selectedCount);
            selectedCount = selectCachedStartRows(selected, startAfterWord[1], maximumRows, selectedCount);
            selectedCount = selectCachedStartRows(selected, startAfterNonWord[1], maximumRows, selectedCount);

            int[] selfLoopCounts = new int[stateLimit];
            for (int stateIndex = 1; stateIndex < stateLimit; stateIndex++) {
                Object[] state = stateRefArrays[stateIndex];
                if (state == null) {
                    continue;
                }
                for (int byteClass = 0; byteClass < nextSize; byteClass++) {
                    if (state[byteClass] == state) {
                        selfLoopCounts[stateIndex]++;
                    }
                }
            }

            for (int selfLoopCount = nextSize; selfLoopCount > 0 && selectedCount < maximumRows; selfLoopCount--) {
                for (int stateIndex = 1; stateIndex < stateLimit && selectedCount < maximumRows; stateIndex++) {
                    if (!selected[stateIndex] && selfLoopCounts[stateIndex] == selfLoopCount) {
                        selected[stateIndex] = true;
                        selectedCount++;
                    }
                }
            }

            // Whole-graph pairing remains the default whenever it fits. Partial
            // pairing is deliberately limited to cached entry rows and states
            // with observed self-loops; expanding arbitrary cold rows recreates
            // the cache-footprint regression this cap prevents.
            if (maximumRows >= stateCount) {
                for (int stateIndex = 1; stateIndex < stateLimit; stateIndex++) {
                    if (stateData[stateIndex] != null) {
                        selected[stateIndex] = true;
                    }
                }
            }
            return selected;
        }

        private int selectCachedStartRows(boolean[] selected, StateData[] startStates, int maximumRows, int selectedCount)
        {
            for (StateData startState : startStates) {
                if (startState == null || selectedCount >= maximumRows) {
                    continue;
                }
                int stateIndex = startState.offset() / nextSize;
                if (!selected[stateIndex]) {
                    selected[stateIndex] = true;
                    selectedCount++;
                }
            }
            return selectedCount;
        }

        private long pairedOuterMemory(int stateLimit)
        {
            return alignToEight(16L + (long) stateLimit * CONSERVATIVE_REFERENCE_BYTES);
        }

        private long pairedRowMemory()
        {
            int pairedClassCount = nextSize * nextSize;
            return alignToEight(16L + ((long) pairedClassCount + 1) * CONSERVATIVE_REFERENCE_BYTES);
        }

        private void clearPairedTransitions()
        {
            pairedStateRefArrays = null;
            availableStateMemory += pairedTransitionMemory;
            pairedTransitionMemory = 0;
        }

        private static long alignToEight(long value)
        {
            return (value + 7) & ~7L;
        }

        StateData analyzeStart(Slice context, int textBegin, int textEnd, boolean anchored, boolean runForward)
        {
            int ctxBegin = context.byteArrayOffset();
            int ctxEnd = ctxBegin + context.length();
            return analyzeStart(context.byteArray(), ctxBegin, ctxEnd, textBegin, textEnd, anchored, runForward);
        }

        StateData analyzeStart(byte[] bytes, int ctxBegin, int ctxEnd, int textBegin, int textEnd, boolean anchored, boolean runForward)
        {
            int flags;
            int dirIndex = runForward ? 1 : 0;
            int anchorIndex = anchored ? 1 : 0;
            StateData[] startCache;
            if (runForward) {
                if (textBegin == ctxBegin) {
                    flags = EmptyOp.EMPTY_BEGIN_TEXT | EmptyOp.EMPTY_BEGIN_LINE;
                    startCache = startBeginText[dirIndex];
                }
                else if (bytes[textBegin - 1] == '\n') {
                    flags = EmptyOp.EMPTY_BEGIN_LINE;
                    startCache = startBeginLine[dirIndex];
                }
                else if (isWordChar(bytes[textBegin - 1] & 0xFF)) {
                    flags = FLAG_LAST_WORD;
                    startCache = startAfterWord[dirIndex];
                }
                else {
                    flags = 0;
                    startCache = startAfterNonWord[dirIndex];
                }
            }
            else {
                if (textEnd == ctxEnd) {
                    flags = EmptyOp.EMPTY_BEGIN_TEXT | EmptyOp.EMPTY_BEGIN_LINE;
                    startCache = startBeginText[dirIndex];
                }
                else if (bytes[textEnd] == '\n') {
                    flags = EmptyOp.EMPTY_BEGIN_LINE;
                    startCache = startBeginLine[dirIndex];
                }
                else if (isWordChar(bytes[textEnd] & 0xFF)) {
                    flags = FLAG_LAST_WORD;
                    startCache = startAfterWord[dirIndex];
                }
                else {
                    flags = 0;
                    startCache = startAfterNonWord[dirIndex];
                }
            }

            StateData existing = (StateData) getStateReference(startCache, anchorIndex);
            if (existing != null) {
                return existing;
            }

            if (!cacheMutationLock.isHeldByCurrentThread()) {
                throw RETRY_SEARCH;
            }

            q0.clear();
            addToQueue(q0, anchored ? prog.start() : prog.startUnanchored(), flags);
            StateData start = workQueueToCachedState(q0, flags);
            if (start == null) {
                return DEAD_DATA;
            }
            setStateReference(startCache, anchorIndex, start);
            return start;
        }

        private int byteMap(int c)
        {
            if (c == BYTE_END_TEXT) {
                return bytemapRange;
            }
            return bytemap[c] & 0xFF;
        }

        /**
         * Compute and store the transition for the given state and byte.
         * Returns the transition value (with possible T_MATCH_BIT), T_DEAD, or T_FULL_MATCH.
         * Returns Integer.MIN_VALUE if allocation fails (cache exhaustion).
         */
        int computeTransition(int stateOffset, int c)
        {
            int cls = byteMap(c);
            int existing = getTransition(transitions, stateOffset + cls);
            if (existing != T_UNCOMPUTED) {
                return existing;
            }

            if (!cacheMutationLock.isHeldByCurrentThread()) {
                throw RETRY_SEARCH;
            }
            return computeTransitionLocked(stateOffset, c, cls);
        }

        private int computeTransitionLocked(int stateOffset, int c, int cls)
        {
            StateData state = stateData[stateOffset / nextSize];

            // Convert state into work queue
            stateToWorkQueue(state, q0);

            int needflag = (state.flag() >> FLAG_NEED_SHIFT) & 0xFF;
            int beforeflag = state.flag() & FLAG_EMPTY_MASK;
            int oldbeforeflag = beforeflag;
            int afterflag = 0;

            if (c == '\n') {
                beforeflag |= EmptyOp.EMPTY_END_LINE;
                afterflag |= EmptyOp.EMPTY_BEGIN_LINE;
            }
            if (c == BYTE_END_TEXT) {
                beforeflag |= EmptyOp.EMPTY_END_LINE | EmptyOp.EMPTY_END_TEXT;
            }

            boolean isLastWord = (state.flag() & FLAG_LAST_WORD) != 0;
            boolean isWord = c != BYTE_END_TEXT && isWordChar(c);
            if (isWord == isLastWord) {
                beforeflag |= EmptyOp.EMPTY_NO_WORD_BOUNDARY;
            }
            else {
                beforeflag |= EmptyOp.EMPTY_WORD_BOUNDARY;
            }

            if ((beforeflag & ~oldbeforeflag & needflag) != 0) {
                runWorkQueueOnEmptyString(q0, q1, beforeflag);
                swapWorkQueue();
            }

            boolean ismatch = false;
            ismatch = runWorkQueueOnByte(q0, q1, c, afterflag, ismatch);
            swapWorkQueue();

            int flag = afterflag;
            if (ismatch) {
                flag |= FLAG_MATCH;
            }
            if (isWord) {
                flag |= FLAG_LAST_WORD;
            }

            StateData ns;
            if (ismatch && kind == Kind.MANY_MATCH) {
                ns = workQueueToCachedState(q0, q1, flag);
            }
            else {
                ns = workQueueToCachedState(q0, null, flag);
            }

            if (ns == null) {
                // OOM - leave transition as T_UNCOMPUTED
                return Integer.MIN_VALUE;
            }

            int transValue = ns.offset();
            if (transValue > 0 && (ns.flag() & FLAG_MATCH) != 0) {
                transValue |= T_MATCH_BIT;
            }
            setTransition(transitions, stateOffset + cls, transValue);

            // Populate Object[] ref for normal non-match transitions (inner loop fast path).
            // Sentinels and match states are left as null — inner loop breaks on null.
            if (transValue > 0 && (transValue & T_MATCH_BIT) == 0) {
                setStateReference(stateRefArrays[stateOffset / nextSize], cls, stateRefArrays[transValue / nextSize]);
                AbsolutePointerTransitions absolutePointers = absolutePointerTransitions;
                if (absolutePointers != null &&
                        stateOffset + cls < absolutePointers.entryCapacity() &&
                        transValue + nextSize <= absolutePointers.entryCapacity()) {
                    setAbsolutePointerTransition(absolutePointers, stateOffset + cls, transValue);
                    absolutePointerTransitionCount++;
                }
            }

            return transValue;
        }

        private void swapWorkQueue()
        {
            WorkQueue temporary = q0;
            q0 = q1;
            q1 = temporary;
        }

        private void stateToWorkQueue(StateData s, WorkQueue q)
        {
            q.clear();
            int[] inst = s.inst();
            for (int i = 0; i < inst.length; i++) {
                int instId = inst[i];
                if (instId == Mark) {
                    q.mark();
                }
                else if (instId == MatchSep) {
                    break;
                }
                else {
                    addToQueue(q, instId, s.flag() & FLAG_EMPTY_MASK);
                }
            }
        }

        private StateData workQueueToCachedState(WorkQueue q, WorkQueue mq, int flag)
        {
            int qsize = q.size();
            int mqSize = (mq != null) ? mq.size() : 0;
            int[] inst = new int[qsize + mqSize + 1];
            int n = 0;

            int needflags = 0;
            boolean sawMatch = false;
            boolean sawMark = false;
            boolean isFirstEntry = true;

            for (int it = 0; it < qsize; it++) {
                int id = q.denseAt(it);

                if (sawMatch && (kind == Kind.FIRST_MATCH || q.isMark(id))) {
                    break;
                }

                if (q.isMark(id)) {
                    if (n > 0 && inst[n - 1] != Mark) {
                        sawMark = true;
                        inst[n++] = Mark;
                    }
                    continue;
                }

                Prog.Inst instruction = prog.inst(id);
                InstOp op = instruction.opcode();

                if (op == InstOp.ALT_MATCH &&
                        kind != Kind.MANY_MATCH &&
                        (kind != Kind.FIRST_MATCH || (isFirstEntry && instruction.greedy(prog))) &&
                        (kind != Kind.LONGEST_MATCH || !sawMark) &&
                        (flag & FLAG_MATCH) != 0) {
                    return FULL_MATCH_DATA;
                }

                if (op == InstOp.EMPTY_WIDTH) {
                    needflags |= instruction.empty();
                }

                if (id > 0 && prog.inst(id - 1).last()) {
                    inst[n++] = id;
                }

                if (op == InstOp.MATCH && !prog.anchorEnd()) {
                    sawMatch = true;
                }

                isFirstEntry = false;
            }

            if (n > 0 && inst[n - 1] == Mark) {
                n--;
            }

            if (needflags == 0) {
                flag &= FLAG_MATCH;
            }

            if (n == 0 && flag == 0) {
                return DEAD_DATA;
            }

            if (kind == Kind.LONGEST_MATCH) {
                int start = 0;
                for (int i = 0; i <= n; i++) {
                    if (i == n || inst[i] == Mark) {
                        if (i > start) {
                            Arrays.sort(inst, start, i);
                        }
                        start = i + 1;
                    }
                }
            }

            if (kind == Kind.MANY_MATCH) {
                Arrays.sort(inst, 0, n);
            }

            if (mq != null) {
                inst[n++] = MatchSep;
                for (int it = 0; it < mq.size(); it++) {
                    int id = mq.denseAt(it);
                    if (!mq.isMark(id)) {
                        Prog.Inst instruction = prog.inst(id);
                        if (instruction.opcode() == InstOp.MATCH) {
                            inst[n++] = instruction.matchId();
                        }
                    }
                }
            }

            flag |= (needflags << FLAG_NEED_SHIFT);

            int[] keyInst = Arrays.copyOf(inst, n);
            StateKey key = new StateKey(keyInst, flag);

            StateData existing = cache.get(key);
            if (existing != null) {
                return existing;
            }

            int offset = allocateState(keyInst.length);
            if (offset < 0) {
                return null; // OOM
            }

            StateData sd = new StateData(keyInst, flag, offset);
            stateData[offset / nextSize] = sd;
            setStateReference(stateRefArrays[offset / nextSize], nextSize, sd);
            cache.put(key, sd);
            return sd;
        }

        private StateData workQueueToCachedState(WorkQueue q, int flag)
        {
            return workQueueToCachedState(q, null, flag);
        }

        ReaderSlot beginSearch(boolean exclusiveSearch)
        {
            if (exclusiveSearch) {
                cacheMutationLock.lock();
                hasPendingCacheMutation = true;
                awaitNoReaders(this);
                return null;
            }

            // Readers publish this DFA's ID and then recheck the writer flag. A writer first
            // blocks new readers and then waits for every published ID to clear, so it has
            // exclusive access before replacing arrays or publishing plain transition writes.
            ReaderSlot readerSlot = THREAD_READER_SLOT.get();
            while (true) {
                int spinCount = 0;
                while (hasPendingCacheMutation) {
                    if (spinCount++ < 1_000) {
                        Thread.onSpinWait();
                    }
                    else {
                        LockSupport.parkNanos(1_000);
                    }
                }
                readerSlot.activeDfaId = readerRegistryId;
                if (!hasPendingCacheMutation) {
                    return readerSlot;
                }
                readerSlot.activeDfaId = 0;
            }
        }

        void endSearch(boolean exclusiveSearch, ReaderSlot readerSlot)
        {
            if (exclusiveSearch) {
                if (pairedTransitionsRequested && !pairedTransitionsDisabled) {
                    rebuildPairedTransitions();
                }
                allocateAbsolutePointerTransitions();
                cacheVersion++;
                hasPendingCacheMutation = false;
                cacheMutationLock.unlock();
            }
            else {
                readerSlot.activeDfaId = 0;
            }
        }

        long cacheVersion()
        {
            return cacheVersion;
        }

        void cancelExclusiveSearch()
        {
            hasPendingCacheMutation = false;
            cacheMutationLock.unlock();
        }

        RestoredStates resetAndRestore(StateSaver start, StateSaver state)
        {
            if (!cacheMutationLock.isHeldByCurrentThread()) {
                throw new IllegalStateException("DFA cache reset requires an exclusive search");
            }
            resetCache();
            int startOffset = start.restoreLocked(this);
            int stateOffset = state.restoreLocked(this);
            return new RestoredStates(startOffset, stateOffset);
        }

        private StateData restoreStateLocked(int[] inst, int flag)
        {
            StateKey key = new StateKey(inst, flag);
            StateData existing = cache.get(key);
            if (existing != null) {
                return existing;
            }

            int offset = allocateState(inst.length);
            if (offset < 0) {
                return null;
            }

            StateData state = new StateData(inst, flag, offset);
            stateData[offset / nextSize] = state;
            setStateReference(stateRefArrays[offset / nextSize], nextSize, state);
            cache.put(key, state);
            return state;
        }

        private long stateObjectMemory(int instructionCount)
        {
            return sizeOfObjectArray(nextSize + 1) +
                    sizeOfIntArray(instructionCount) +
                    STATE_DATA_BYTES +
                    STATE_KEY_BYTES +
                    STATE_CACHE_ENTRY_BYTES;
        }

        private long stateBackingMemory()
        {
            return sizeOfIntArray(transitions.length) +
                    sizeOfObjectArray(stateData.length) +
                    sizeOfObjectArray(stateRefArrays.length) +
                    STATE_CACHE_BYTES +
                    hashMapTableBytes(cacheTableCapacity);
        }

        private static long hashMapTableBytes(int capacity)
        {
            return capacity == 0 ? 0 : sizeOfObjectArray(capacity);
        }

        private static int hashMapCapacity(int size)
        {
            if (size == 0) {
                return 0;
            }
            int capacity = 16;
            while (size > (capacity * 3L) / 4) {
                capacity = Math.multiplyExact(capacity, 2);
            }
            return capacity;
        }

        private void resetCache()
        {
            clearPairedTransitions();
            pairedTransitionsRejected = false;
            pairedTransitionsDisabled = absolutePointerTransitions != null;
            shortPairedSearchCount = 0;
            shortAbsolutePointerSearchCount = 0;
            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < 2; j++) {
                    startBeginText[i][j] = null;
                    startBeginLine[i][j] = null;
                    startAfterWord[i][j] = null;
                    startAfterNonWord[i][j] = null;
                }
            }

            cache.clear();

            Arrays.fill(transitions, 0, nextFreeOffset, T_UNCOMPUTED);
            int stateLimit = Math.min(nextFreeOffset / nextSize, stateData.length);
            Arrays.fill(stateData, 0, stateLimit, null);
            Arrays.fill(stateRefArrays, 0, stateLimit, null);
            AbsolutePointerTransitions absolutePointers = absolutePointerTransitions;
            if (absolutePointers != null) {
                absolutePointers.segment().fill((byte) 0);
                absolutePointerTransitionCount = 0;
            }
            nextFreeOffset = nextSize;
            stateCount = 0;
            retainedStateMemory = stateBackingMemory();
            availableStateMemory = stateBudget -
                    retainedStateMemory -
                    fixedDistanceByteCandidateMemory -
                    absolutePointerTransitionMemory();
            if (fixedDistanceByteCandidates == RETRY_FIXED_DISTANCE_BYTE_CANDIDATES_AFTER_RESET) {
                fixedDistanceByteCandidates = null;
            }

            resetCount++;
        }

        public void resetCacheExternal()
        {
            cacheMutationLock.lock();
            try {
                hasPendingCacheMutation = true;
                awaitNoReaders(this);
                resetCache();
            }
            finally {
                cacheVersion++;
                hasPendingCacheMutation = false;
                cacheMutationLock.unlock();
            }
        }

        /**
         * Adds id to the work queue, following empty arrows according to flag.
         * Like upstream dfa.cc:837-919.
         */
        private void addToQueue(WorkQueue q, int id, int flag)
        {
            int stackPointer = 0;
            stack[stackPointer++] = id;

            while (stackPointer > 0) {
                id = stack[--stackPointer];

                if (id == Mark) {
                    q.mark();
                    continue;
                }

                while (true) {
                    if (id == 0) {
                        break;
                    }
                    if (q.contains(id)) {
                        break;
                    }
                    q.insertNew(id);

                    Prog.Inst instruction = prog.inst(id);
                    InstOp op = instruction.opcode();
                    switch (op) {
                        case BYTE_RANGE, MATCH -> {
                            if (!instruction.last()) {
                                id = id + 1;
                                continue;
                            }
                        }
                        case CAPTURE, NOP -> {
                            if (!instruction.last()) {
                                stack[stackPointer++] = id + 1;
                            }
                            if (op == InstOp.NOP && q.maxmark() > 0 &&
                                    id == prog.startUnanchored() && id != prog.start()) {
                                stack[stackPointer++] = Mark;
                            }
                            id = instruction.out();
                            continue;
                        }
                        case EMPTY_WIDTH -> {
                            if (!instruction.last()) {
                                stack[stackPointer++] = id + 1;
                            }
                            if ((instruction.empty() & ~flag) != 0) {
                                break;
                            }
                            id = instruction.out();
                            continue;
                        }
                        case ALT -> {
                            if (!instruction.last()) {
                                stack[stackPointer++] = id + 1;
                            }
                            stack[stackPointer++] = instruction.out1();
                            id = instruction.out();
                            continue;
                        }
                        case ALT_MATCH -> {
                            id = id + 1;
                            continue;
                        }
                        case FAIL -> {
                            if (!instruction.last()) {
                                stack[stackPointer++] = id + 1;
                            }
                        }
                    }
                    break;
                }
            }
        }

        private void runWorkQueueOnEmptyString(WorkQueue oldq, WorkQueue newq, int flag)
        {
            newq.clear();
            for (int it = 0; it < oldq.size(); it++) {
                int id = oldq.denseAt(it);
                if (oldq.isMark(id)) {
                    addToQueue(newq, Mark, flag);
                }
                else {
                    addToQueue(newq, id, flag);
                }
            }
        }

        private boolean runWorkQueueOnByte(WorkQueue oldq, WorkQueue newq, int c, int flag, boolean ismatch)
        {
            newq.clear();
            for (int it = 0; it < oldq.size(); it++) {
                int id = oldq.denseAt(it);

                if (oldq.isMark(id)) {
                    if (ismatch) {
                        return ismatch;
                    }
                    newq.mark();
                    continue;
                }

                Prog.Inst instruction = prog.inst(id);
                switch (instruction.opcode()) {
                    case BYTE_RANGE -> {
                        if (!instruction.matches(c)) {
                            break;
                        }
                        addToQueue(newq, instruction.out(), flag);
                        if (instruction.hint() != 0) {
                            it += instruction.hint() - 1;
                        }
                        else {
                            Prog.Inst j = instruction;
                            int jid = id;
                            while (!j.last()) {
                                jid++;
                                j = prog.inst(jid);
                            }
                            it += jid - id;
                        }
                    }
                    case MATCH -> {
                        if (prog.anchorEnd() && c != BYTE_END_TEXT &&
                                kind != Kind.MANY_MATCH) {
                            break;
                        }
                        ismatch = true;
                        if (kind == Kind.FIRST_MATCH) {
                            return true;
                        }
                    }
                    default -> {}
                }
            }
            return ismatch;
        }
    }
}
