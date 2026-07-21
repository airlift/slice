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
import io.airlift.slice.re2.Prog.Inst;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

public final class Backtrack
{
    private Backtrack() {}

    /**
     * Testing-only backtracking engine (ported from upstream {@code re2/testing/backtrack.cc}).
     * <p>
     * This exists primarily as a simple reference implementation for cross-checking.
     * It is not intended for production use.
     */
    public static boolean search(Prog prog, Slice context, int start, int end, boolean anchored, boolean longest, int[] submatch)
    {
        requireNonNull(prog, "prog is null");
        requireNonNull(context, "context is null");

        if (submatch != null && (submatch.length % 2) != 0) {
            throw new IllegalArgumentException("submatch length must be even: " + submatch.length);
        }

        if (prog.start() == 0) {
            return false;
        }

        int contextBegin = context.byteArrayOffset();
        int contextEnd = contextBegin + context.length();
        if (start < 0 || end < start || end > context.length()) {
            return false;
        }
        int textBegin = contextBegin + start;
        int textEnd = contextBegin + end;

        if (prog.anchorStart() && contextBegin != textBegin) {
            return false;
        }
        if (prog.anchorEnd() && contextEnd != textEnd) {
            return false;
        }

        boolean anchoredSearch = anchored || prog.anchorStart();
        boolean longestSearch = longest || prog.anchorEnd();
        boolean endMatch = prog.anchorEnd();

        int submatchCount = (submatch == null) ? 0 : (submatch.length / 2);
        if (submatchCount > 0) {
            for (int i = 0; i < submatch.length; i++) {
                submatch[i] = -1;
            }
        }

        Backtracker backtracker = new Backtracker(prog, context, textBegin, textEnd, anchoredSearch, longestSearch, endMatch, submatch, submatchCount);
        return backtracker.search();
    }

    public static boolean search(Prog prog, Slice text, boolean anchored, boolean longest, int[] submatch)
    {
        return search(prog, text, 0, text.length(), anchored, longest, submatch);
    }

    public static boolean search(Prog prog, Slice context, int start, int end, boolean anchored, Prog.MatchKind matchKind, int[] submatch)
    {
        requireNonNull(matchKind, "matchKind is null");
        int[] effectiveSubmatch = submatch;
        if (matchKind == Prog.MatchKind.FULL_MATCH && (effectiveSubmatch == null || effectiveSubmatch.length < 2)) {
            effectiveSubmatch = new int[2];
        }

        boolean matched = search(
                prog,
                context,
                start,
                end,
                anchored || matchKind == Prog.MatchKind.FULL_MATCH,
                matchKind != Prog.MatchKind.FIRST_MATCH,
                effectiveSubmatch);
        if (!matched || matchKind != Prog.MatchKind.FULL_MATCH) {
            return matched;
        }
        return effectiveSubmatch[0] == 0 && effectiveSubmatch[1] == end - start;
    }

    public static boolean search(Prog prog, Slice text, boolean anchored, Prog.MatchKind matchKind, int[] submatch)
    {
        return search(prog, text, 0, text.length(), anchored, matchKind, submatch);
    }

    private static final class Backtracker
    {
        private final Prog prog;
        private final Slice context;
        private final boolean anchored;
        private final boolean longest;
        private final boolean endMatch;
        private final int[] submatch;
        private final int submatchCount;

        private final byte[] bytes;
        private final int textBegin;
        private final int textEnd;
        private final int contextEnd;

        private final int[] cap; // capture registers (absolute byte[] indices), -1 for unset
        private final int[] visited; // bitmap words: (inst, pos) pairs

        private Backtracker(
                Prog prog,
                Slice context,
                int textBegin,
                int textEnd,
                boolean anchored,
                boolean longest,
                boolean endMatch,
                int[] submatch,
                int submatchCount)
        {
            this.prog = requireNonNull(prog, "prog is null");
            this.context = requireNonNull(context, "context is null");
            this.anchored = anchored;
            this.longest = longest;
            this.endMatch = endMatch;
            this.submatch = submatch;
            this.submatchCount = submatchCount;

            this.bytes = context.byteArray();
            this.textBegin = textBegin;
            this.textEnd = textEnd;
            this.contextEnd = context.byteArrayOffset() + context.length();

            // Upstream keeps a fixed-size cap_[] array (64). This is testing-only; keep the same bound.
            int captureSlotCount = Math.max(2, 2 * submatchCount);
            if (captureSlotCount > 64) {
                throw new IllegalArgumentException("too many submatches for backtracking engine: " + submatchCount);
            }
            this.cap = new int[64];
            Arrays.fill(this.cap, -1);

            int visitedBitCount = prog.size() * (textEnd - textBegin + 1);
            int words = (visitedBitCount + 31) / 32;
            this.visited = new int[words];
        }

        private boolean search()
        {
            if (anchored) {
                cap[0] = textBegin;
                return visit(prog.start(), textBegin);
            }

            for (int position = textBegin; position <= textEnd; position++) {
                cap[0] = position;
                if (visit(prog.start(), position)) {
                    return true;
                }
            }
            return false;
        }

        private boolean visit(int id, int position)
        {
            int positionOffset = position - textBegin;
            int visitedIndex = id * (textEnd - textBegin + 1) + positionOffset;
            int word = visitedIndex >>> 5;
            int bit = 1 << (visitedIndex & 31);
            if ((visited[word] & bit) != 0) {
                return false;
            }
            visited[word] |= bit;

            Inst instruction = prog.inst(id);
            if (tryInst(id, position)) {
                if (longest && !instruction.last()) {
                    visit(id + 1, position);
                }
                return true;
            }
            if (!instruction.last()) {
                return visit(id + 1, position);
            }
            return false;
        }

        private boolean tryInst(int id, int position)
        {
            int c = (position < textEnd) ? (bytes[position] & 0xFF) : -1;

            Inst instruction = prog.inst(id);
            return switch (instruction.opcode()) {
                case ALT_MATCH -> false;
                case FAIL -> false;
                case BYTE_RANGE -> instruction.matches(c) && visit(instruction.out(), position + 1);
                case CAPTURE -> {
                    int capIndex = instruction.cap();
                    if (0 <= capIndex && capIndex < cap.length) {
                        int saved = cap[capIndex];
                        cap[capIndex] = position;
                        boolean ret = visit(instruction.out(), position);
                        cap[capIndex] = saved;
                        yield ret;
                    }
                    yield visit(instruction.out(), position);
                }
                case EMPTY_WIDTH -> ((instruction.empty() & ~Prog.emptyFlags(context, position)) == 0) && visit(instruction.out(), position);
                case NOP -> visit(instruction.out(), position);
                case MATCH -> {
                    if (endMatch && position != contextEnd) {
                        yield false;
                    }

                    cap[1] = position;
                    if (submatchCount == 0) {
                        yield true;
                    }

                    boolean haveBest = submatch[0] >= 0;
                    int bestEnd = haveBest ? (textBegin + submatch[1]) : -1;
                    if (!haveBest || (longest && position > bestEnd)) {
                        for (int i = 0; i < submatchCount; i++) {
                            int a = cap[2 * i];
                            int b = cap[2 * i + 1];
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
                    yield true;
                }
                case ALT -> false;
            };
        }
    }
}
