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

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

final class OnePass
{
    private OnePass() {}

    enum CaptureFinalization
    {
        DIRECT_ZERO_ORIGIN,
        DIRECT_REBASE,
        SCRATCH_COPY,
    }

    // PERFORMANCE-SENSITIVE HOT LOOP: state layout, capture handling, and branch placement
    // directly affect generated code. Do not apply readability-only changes without direct path
    // tests and focused Intel and Graviton benchmarks.

    // Constants ported from upstream re2/onepass.cc.
    private static final int INDEX_SHIFT = 16;
    private static final int EMPTY_SHIFT = 6;
    private static final int REAL_CAP_SHIFT = EMPTY_SHIFT + 1;
    private static final int REAL_MAX_CAP = ((INDEX_SHIFT - REAL_CAP_SHIFT) / 2) * 2;

    private static final int CAP_SHIFT = REAL_CAP_SHIFT - 2;
    private static final int MAX_CAP = REAL_MAX_CAP + 2;

    private static final int MATCH_WINS = 1 << EMPTY_SHIFT;
    private static final int CAP_MASK = ((1 << REAL_MAX_CAP) - 1) << REAL_CAP_SHIFT;
    private static final int EMPTY_ALL_FLAGS = (1 << EMPTY_SHIFT) - 1;
    private static final int IMPOSSIBLE = EmptyOp.EMPTY_WORD_BOUNDARY | EmptyOp.EMPTY_NO_WORD_BOUNDARY;

    public static boolean search(Prog prog, Slice context, int start, int end, boolean anchored, Prog.MatchKind matchKind, int[] submatch)
    {
        requireNonNull(prog, "prog is null");
        requireNonNull(context, "context is null");
        requireNonNull(matchKind, "matchKind is null");

        if (submatch != null && (submatch.length % 2) != 0) {
            throw new IllegalArgumentException("submatch length must be even: " + submatch.length);
        }

        // Upstream SearchOnePass only supports anchored matches.
        if (!anchored && matchKind != Prog.MatchKind.FULL_MATCH) {
            return false;
        }

        if (!prog.isOnePass()) {
            return false;
        }

        int ctxBegin = context.byteArrayOffset();
        int ctxEnd = ctxBegin + context.length();
        if (start < 0 || end < start || end > context.length()) {
            return false;
        }
        int textBegin = ctxBegin + start;
        int textEnd = ctxBegin + end;

        if (prog.anchorStart() && ctxBegin != textBegin) {
            return false;
        }
        if (prog.anchorEnd() && ctxEnd != textEnd) {
            return false;
        }

        if (prog.anchorEnd()) {
            matchKind = Prog.MatchKind.FULL_MATCH;
        }

        int matchCount = (submatch == null) ? 0 : (submatch.length / 2);
        int captureSlotCount = 2 * matchCount;
        if (captureSlotCount < 2) {
            captureSlotCount = 2;
        }
        if (captureSlotCount > MAX_CAP) {
            if (!prog.supportsOnePassCaptureSlots(captureSlotCount)) {
                return false;
            }
            return searchExtendedCaptures(prog, context, textBegin, textEnd, matchKind, submatch, matchCount, captureSlotCount);
        }
        // Boolean-only fast path for patterns that match every input (for example, \C*).
        // SearchOnePass is anchored-only, and anchor/context constraints are validated above.
        if (matchCount == 0 && prog.matchesAnyByteString()) {
            return true;
        }

        CaptureFinalization captureFinalization = captureFinalization(submatch, matchKind, textBegin);
        boolean useCallerCaptures = captureFinalization != CaptureFinalization.SCRATCH_COPY;
        int[] captures = useCallerCaptures ? submatch : new int[captureSlotCount];
        int[] matchCaptures = useCallerCaptures ? submatch : new int[captureSlotCount];
        Arrays.fill(captures, -1);
        if (captures != matchCaptures) {
            Arrays.fill(matchCaptures, -1);
        }

        byte[] bytes = context.byteArray();
        byte[] bytemap = prog.bytemapArray();
        int range = prog.bytemapRange();
        int[] stateMatchCond = prog.onePassMatchCond();
        int[] stateAction = prog.onePassAction();

        int state = 0; // start() is always mapped to the zeroth OneState.
        int position = textBegin;
        boolean matched = false;

        matchCaptures[0] = textBegin;
        captures[0] = textBegin;

        int nextMatchCond = stateMatchCond[state];
        int stateOffset = 0; // = state * range

        matchSearch: {
            for (position = textBegin; position < textEnd; position++) {
                int cls = bytemap[bytes[position] & 0xFF] & 0xFF;

                int matchCond = nextMatchCond;
                int cond = stateAction[stateOffset + cls];

                // Determine whether we can reach the next state.
                if ((cond & EMPTY_ALL_FLAGS) == 0 || satisfy(cond, context, position)) {
                    state = cond >>> INDEX_SHIFT;
                    stateOffset = state * range;
                    nextMatchCond = stateMatchCond[state];
                }
                else {
                    state = -1;
                    nextMatchCond = IMPOSSIBLE;
                }

                // Not if we want a full match.
                if (matchKind == Prog.MatchKind.FULL_MATCH) {
                    // skip
                }
                else if (matchCond != IMPOSSIBLE) {
                    // Not if the possible match is beaten by the certain match at the next byte.
                    if ((cond & MATCH_WINS) != 0 || (nextMatchCond & EMPTY_ALL_FLAGS) != 0) {
                        if ((matchCond & EMPTY_ALL_FLAGS) == 0 || satisfy(matchCond, context, position)) {
                            // Copy captures so far, then apply captures for this match boundary.
                            for (int i = 2; i < captureSlotCount; i++) {
                                matchCaptures[i] = captures[i];
                            }
                            if (matchCount > 1 && (matchCond & CAP_MASK) != 0) {
                                applyCaptures(matchCond, position, matchCaptures, captureSlotCount);
                            }
                            matchCaptures[1] = position;
                            matched = true;

                            // In first-match mode, we can stop if the match takes priority.
                            if (matchKind == Prog.MatchKind.FIRST_MATCH && (cond & MATCH_WINS) != 0) {
                                break matchSearch;
                            }
                        }
                    }
                }

                if (state < 0) {
                    break;
                }
                if ((cond & CAP_MASK) != 0 && matchCount > 1) {
                    applyCaptures(cond, position, captures, captureSlotCount);
                }
            }

            // Look for match at end of input.
            if (state >= 0) {
                int matchCond = stateMatchCond[state];
                if (matchCond != IMPOSSIBLE &&
                        ((matchCond & EMPTY_ALL_FLAGS) == 0 || satisfy(matchCond, context, position))) {
                    if (matchCount > 1 && (matchCond & CAP_MASK) != 0) {
                        applyCaptures(matchCond, position, captures, captureSlotCount);
                    }
                    if (captures != matchCaptures || captureFinalization == CaptureFinalization.DIRECT_REBASE) {
                        for (int i = 2; i < captureSlotCount; i++) {
                            matchCaptures[i] = captures[i];
                        }
                    }
                    matchCaptures[1] = position;
                    matched = true;
                }
            }
        }

        if (!matched) {
            if (useCallerCaptures) {
                Arrays.fill(submatch, -1);
            }
            return false;
        }

        if (submatch != null && captureFinalization != CaptureFinalization.DIRECT_ZERO_ORIGIN) {
            for (int i = 0; i < matchCount; i++) {
                int a = matchCaptures[2 * i];
                int b = matchCaptures[2 * i + 1];
                int o = 2 * i;
                if (a < 0 || b < 0) {
                    submatch[o] = -1;
                    submatch[o + 1] = -1;
                }
                else {
                    submatch[o] = a - textBegin;
                    submatch[o + 1] = b - textBegin;
                }
            }
        }
        return true;
    }

    private static boolean searchExtendedCaptures(
            Prog prog,
            Slice context,
            int textBegin,
            int textEnd,
            Prog.MatchKind matchKind,
            int[] submatch,
            int matchCount,
            int captureSlotCount)
    {
        CaptureFinalization captureFinalization = captureFinalization(submatch, matchKind, textBegin);
        boolean useCallerCaptures = captureFinalization != CaptureFinalization.SCRATCH_COPY;
        int[] captures = useCallerCaptures ? submatch : new int[captureSlotCount];
        int[] matchCaptures = useCallerCaptures ? submatch : new int[captureSlotCount];
        Arrays.fill(captures, -1);
        if (captures != matchCaptures) {
            Arrays.fill(matchCaptures, -1);
        }

        byte[] bytes = context.byteArray();
        byte[] bytemap = prog.bytemapArray();
        int range = prog.bytemapRange();
        int[] stateMatchCondition = prog.onePassMatchCond();
        int[] stateAction = prog.onePassAction();
        long[] stateMatchCapture = prog.onePassMatchCapture();
        long[] stateActionCapture = prog.onePassActionCapture();

        int state = 0;
        int position = textBegin;
        boolean matched = false;

        matchCaptures[0] = textBegin;
        captures[0] = textBegin;

        int nextMatchCondition = stateMatchCondition[state];
        int stateOffset = 0;

        matchSearch: {
            for (position = textBegin; position < textEnd; position++) {
                int byteClass = bytemap[bytes[position] & 0xFF] & 0xFF;
                int actionIndex = stateOffset + byteClass;
                int matchCondition = nextMatchCondition;
                long matchCaptureMask = stateMatchCapture[state];
                int condition = stateAction[actionIndex];
                long captureMask = stateActionCapture[actionIndex];

                if ((condition & EMPTY_ALL_FLAGS) == 0 || satisfy(condition, context, position)) {
                    state = condition >>> INDEX_SHIFT;
                    stateOffset = state * range;
                    nextMatchCondition = stateMatchCondition[state];
                }
                else {
                    state = -1;
                    nextMatchCondition = IMPOSSIBLE;
                }

                if (matchKind == Prog.MatchKind.FULL_MATCH) {
                    // Only an end-of-input match can satisfy a full match.
                }
                else if (matchCondition != IMPOSSIBLE &&
                        ((condition & MATCH_WINS) != 0 || (nextMatchCondition & EMPTY_ALL_FLAGS) != 0) &&
                        ((matchCondition & EMPTY_ALL_FLAGS) == 0 || satisfy(matchCondition, context, position))) {
                    for (int captureSlot = 2; captureSlot < captureSlotCount; captureSlot++) {
                        matchCaptures[captureSlot] = captures[captureSlot];
                    }
                    applyExtendedCaptures(matchCaptureMask, position, matchCaptures);
                    matchCaptures[1] = position;
                    matched = true;
                    if (matchKind == Prog.MatchKind.FIRST_MATCH && (condition & MATCH_WINS) != 0) {
                        break matchSearch;
                    }
                }

                if (state < 0) {
                    break;
                }
                applyExtendedCaptures(captureMask, position, captures);
            }

            if (state >= 0) {
                int matchCondition = stateMatchCondition[state];
                if (matchCondition != IMPOSSIBLE &&
                        ((matchCondition & EMPTY_ALL_FLAGS) == 0 || satisfy(matchCondition, context, position))) {
                    applyExtendedCaptures(stateMatchCapture[state], position, captures);
                    if (captures != matchCaptures || captureFinalization == CaptureFinalization.DIRECT_REBASE) {
                        for (int captureSlot = 2; captureSlot < captureSlotCount; captureSlot++) {
                            matchCaptures[captureSlot] = captures[captureSlot];
                        }
                    }
                    matchCaptures[1] = position;
                    matched = true;
                }
            }
        }

        if (!matched) {
            if (useCallerCaptures) {
                Arrays.fill(submatch, -1);
            }
            return false;
        }

        if (submatch != null && captureFinalization != CaptureFinalization.DIRECT_ZERO_ORIGIN) {
            for (int group = 0; group < matchCount; group++) {
                int start = matchCaptures[2 * group];
                int end = matchCaptures[2 * group + 1];
                int groupOffset = 2 * group;
                if (start < 0 || end < 0) {
                    submatch[groupOffset] = -1;
                    submatch[groupOffset + 1] = -1;
                }
                else {
                    submatch[groupOffset] = start - textBegin;
                    submatch[groupOffset + 1] = end - textBegin;
                }
            }
        }
        return true;
    }

    private static void applyExtendedCaptures(long captureMask, int position, int[] captures)
    {
        captureMask &= ~0b11L;
        if (captures.length < Long.SIZE) {
            captureMask &= (1L << captures.length) - 1;
        }
        while (captureMask != 0) {
            int captureSlot = Long.numberOfTrailingZeros(captureMask);
            captures[captureSlot] = position;
            captureMask &= captureMask - 1;
        }
    }

    static CaptureFinalization captureFinalization(int[] submatch, Prog.MatchKind matchKind, int textBegin)
    {
        if (submatch == null || matchKind != Prog.MatchKind.FULL_MATCH) {
            return CaptureFinalization.SCRATCH_COPY;
        }
        // The direct zero-origin shape regresses the common single-group split case.
        if (textBegin == 0 && submatch.length > 4) {
            return CaptureFinalization.DIRECT_ZERO_ORIGIN;
        }
        return CaptureFinalization.DIRECT_REBASE;
    }

    public static boolean search(Prog prog, Slice text, boolean anchored, Prog.MatchKind matchKind, int[] submatch)
    {
        return search(prog, text, 0, text.length(), anchored, matchKind, submatch);
    }

    private static boolean satisfy(int cond, Slice context, int p)
    {
        int satisfied = Prog.emptyFlags(context, p);
        int need = cond & EMPTY_ALL_FLAGS;
        return (need & ~satisfied) == 0;
    }

    static void applyCaptures(int condition, int position, int[] captures, int captureSlotCount)
    {
        for (int captureSlot = 2; captureSlot < captureSlotCount; captureSlot++) {
            if ((condition & (1 << (CAP_SHIFT + captureSlot))) != 0) {
                captures[captureSlot] = position;
            }
        }
    }
}
