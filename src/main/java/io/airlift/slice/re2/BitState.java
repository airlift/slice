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

final class BitState
{
    private BitState() {}

    static int initialJobCapacity(int programSize)
    {
        return Math.max(16, Math.min(64, programSize));
    }

    // PERFORMANCE-SENSITIVE HOT LOOPS: job-stack layout, visited-state checks, and capture
    // handling are benchmark-sensitive. Do not perform readability-only refactors below without
    // direct path tests and focused Intel and Graviton benchmarks.

    public static boolean search(Prog prog, Slice context, int start, int end, boolean anchored, Prog.MatchKind matchKind, int[] submatch)
    {
        return search(prog, context, start, end, anchored, matchKind, submatch, null);
    }

    static boolean search(
            Prog prog,
            Slice context,
            int start,
            int end,
            boolean anchored,
            Prog.MatchKind matchKind,
            int[] submatch,
            Workspace workspace)
    {
        requireNonNull(prog, "prog is null");
        requireNonNull(context, "context is null");
        requireNonNull(matchKind, "matchKind is null");

        if (!prog.canBitState()) {
            return false;
        }
        if (submatch != null && (submatch.length % 2) != 0) {
            throw new IllegalArgumentException("submatch length must be even: " + submatch.length);
        }

        if (prog.start() == 0) {
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

        boolean anchoredSearch = anchored || prog.anchorStart() || matchKind == Prog.MatchKind.FULL_MATCH;
        boolean longestSearch = matchKind != Prog.MatchKind.FIRST_MATCH || prog.anchorEnd();
        boolean endMatch = prog.anchorEnd() || matchKind == Prog.MatchKind.FULL_MATCH;

        int submatchCount = (submatch == null) ? 0 : (submatch.length / 2);
        if (submatchCount > 0) {
            for (int i = 0; i < submatch.length; i++) {
                submatch[i] = -1;
            }
        }

        BitStateImpl b = new BitStateImpl(
                prog,
                context,
                textBegin,
                textEnd,
                anchoredSearch,
                longestSearch,
                endMatch,
                submatch,
                submatchCount,
                workspace);
        return b.search();
    }

    public static boolean search(Prog prog, Slice text, boolean anchored, Prog.MatchKind matchKind, int[] submatch)
    {
        return search(prog, text, 0, text.length(), anchored, matchKind, submatch);
    }

    private static final class Job
    {
        int id;
        int runLength;
        int position;
    }

    static final class Workspace
    {
        private long[] visited;
        private int[] captures;
        private Job[] jobs;

        private long[] prepareVisited(int wordCount)
        {
            if (visited == null || visited.length < wordCount) {
                visited = new long[wordCount];
            }
            else {
                Arrays.fill(visited, 0, wordCount, 0);
            }
            return visited;
        }

        private int[] prepareCaptures(int slotCount)
        {
            if (captures == null || captures.length < slotCount) {
                captures = new int[slotCount];
            }
            Arrays.fill(captures, 0, slotCount, -1);
            return captures;
        }

        private Job[] prepareJobs(int programSize)
        {
            if (jobs == null) {
                jobs = BitStateImpl.createJobs(initialJobCapacity(programSize));
            }
            return jobs;
        }

        private void updateJobs(Job[] jobs)
        {
            this.jobs = jobs;
        }
    }

    private static final class BitStateImpl
    {
        private static final int VISITED_BITS = 64;
        private static final int OP_ALT = 0;
        private static final int OP_ALT_MATCH = 1;
        private static final int OP_BYTE_RANGE = 2;
        private static final int OP_CAPTURE = 3;
        private static final int OP_EMPTY_WIDTH = 4;
        private static final int OP_MATCH = 5;
        private static final int OP_NOP = 6;
        private static final int OP_FAIL = 7;
        private static final long OUTPUT_MASK = (1L << 28) - 1;

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

        private final short[] listHeads;
        private final long[] visited;
        private final int[] cap;
        private final Workspace workspace;

        private Job[] jobs;
        private int jobCount;

        private BitStateImpl(
                Prog prog,
                Slice context,
                int textBegin,
                int textEnd,
                boolean anchored,
                boolean longest,
                boolean endMatch,
                int[] submatch,
                int submatchCount,
                Workspace workspace)
        {
            this.prog = requireNonNull(prog, "prog is null");
            this.context = requireNonNull(context, "context is null");
            this.anchored = anchored;
            this.longest = longest;
            this.endMatch = endMatch;
            this.submatch = submatch;
            this.submatchCount = submatchCount;
            this.workspace = workspace;

            this.bytes = context.byteArray();
            this.textBegin = textBegin;
            this.textEnd = textEnd;

            this.listHeads = requireNonNull(prog.listHeads(), "prog.listHeads() is null");

            int visitedBitCount = prog.listCount() * ((textEnd - textBegin) + 1);
            int visitedWordCount = (visitedBitCount + VISITED_BITS - 1) / VISITED_BITS;
            int captureSlotCount = Math.max(2, 2 * submatchCount);
            if (workspace == null) {
                this.visited = new long[visitedWordCount];
                this.cap = new int[captureSlotCount];
                Arrays.fill(this.cap, -1);
                this.jobs = createJobs(initialJobCapacity(prog.size()));
            }
            else {
                this.visited = workspace.prepareVisited(visitedWordCount);
                this.cap = workspace.prepareCaptures(captureSlotCount);
                this.jobs = workspace.prepareJobs(prog.size());
            }
            this.jobCount = 0;
        }

        private static Job[] createJobs(int capacity)
        {
            Job[] jobs = new Job[capacity];
            for (int i = 0; i < jobs.length; i++) {
                jobs[i] = new Job();
            }
            return jobs;
        }

        private boolean search()
        {
            // Anchored search must start at text.begin().
            if (anchored) {
                cap[0] = textBegin;
                return trySearch(prog.start(), textBegin);
            }

            // Unanchored search from each possible text position.
            for (int position = textBegin; position <= textEnd; position++) {
                if (position < textEnd && prog.canPrefixAccel()) {
                    int found = prog.prefixAccel(bytes, position, textEnd - position);
                    if (found < 0) {
                        position = textEnd;
                    }
                    else {
                        position = found;
                    }
                }

                cap[0] = position;
                if (trySearch(prog.start(), position)) {
                    return true;
                }
            }

            return false;
        }

        private static boolean shouldVisit(int textLen, long[] visited, int listId, int positionOffset)
        {
            int visitedIndex = listId * (textLen + 1) + positionOffset;
            int word = visitedIndex / VISITED_BITS;
            long bit = 1L << (visitedIndex & (VISITED_BITS - 1));
            if ((visited[word] & bit) != 0) {
                return false;
            }
            visited[word] |= bit;
            return true;
        }

        private void push(int id, int position)
        {
            if (jobCount >= jobs.length) {
                growStack();
            }

            if (id >= 0 && jobCount > 0) {
                Job top = jobs[jobCount - 1];
                if (id == top.id &&
                        position == top.position + top.runLength + 1 &&
                        top.runLength < Integer.MAX_VALUE) {
                    top.runLength++;
                    return;
                }
            }

            Job top = jobs[jobCount++];
            top.id = id;
            top.runLength = 0;
            top.position = position;
        }

        private void growStack()
        {
            int oldLength = jobs.length;
            jobs = Arrays.copyOf(jobs, oldLength * 2);
            for (int i = oldLength; i < jobs.length; i++) {
                jobs[i] = new Job();
            }
            if (workspace != null) {
                workspace.updateJobs(jobs);
            }
        }

        private boolean trySearch(int startId, int startPosition)
        {
            long[] instructionWords = prog.getOrCreateCaptureInstructionWords();
            boolean matched = false;
            jobCount = 0;

            int textLen = textEnd - textBegin;
            if (shouldVisit(textLen, visited, listHeads[startId], startPosition - textBegin)) {
                push(startId, startPosition);
            }

            while (jobCount > 0) {
                Job job = jobs[--jobCount];
                int id = job.id;
                int runLength = job.runLength;
                int position = job.position;

                if (id < 0) {
                    // Undo the Capture.
                    int capIndex = prog.inst(-id).cap();
                    if (0 <= capIndex && capIndex < cap.length) {
                        cap[capIndex] = position;
                    }
                    continue;
                }

                if (runLength > 0) {
                    position += runLength;
                    job.runLength = runLength - 1;
                    jobCount++;
                }

                while (true) {
                    long instructionWord = instructionWords[id];
                    int output = (int) ((instructionWord >>> 4) & OUTPUT_MASK);
                    boolean last = (instructionWord & (1L << 3)) != 0;
                    int payload = (int) (instructionWord >>> 32);
                    switch ((int) (instructionWord & 0b111)) {
                        case OP_FAIL -> {
                            id = 0;
                        }
                        case OP_ALT_MATCH -> {
                            if (payload < 0) {
                                id = payload & (int) OUTPUT_MASK;
                                position = textEnd;
                                continue;
                            }
                            if (longest) {
                                id = output;
                                position = textEnd;
                                continue;
                            }
                            id = nextInList(last, id);
                        }
                        case OP_BYTE_RANGE -> {
                            int c = (position < textEnd) ? (bytes[position] & 0xFF) : -1;
                            if (payload < 0 && 'A' <= c && c <= 'Z') {
                                c += 'a' - 'A';
                            }
                            int lowerBound = payload & 0xFF;
                            int upperBound = (payload >>> 8) & 0xFF;
                            if (c < lowerBound || c > upperBound) {
                                id = nextInList(last, id);
                                break;
                            }

                            int hint = (payload >>> 16) & 0x7FFF;
                            if (hint != 0) {
                                push(id + hint, position);
                            }
                            id = output;
                            position++;
                            if (!checkAndLoop(textLen, id, position)) {
                                id = 0;
                            }
                        }
                        case OP_CAPTURE -> {
                            if (!last) {
                                push(id + 1, position);
                            }
                            if (0 <= payload && payload < cap.length) {
                                push(-id, cap[payload]);
                                cap[payload] = position;
                            }
                            id = output;
                            if (!checkAndLoop(textLen, id, position)) {
                                id = 0;
                            }
                        }
                        case OP_EMPTY_WIDTH -> {
                            int empty = Prog.emptyFlags(context, position);
                            if ((payload & ~empty) != 0) {
                                id = nextInList(last, id);
                                break;
                            }
                            if (!last) {
                                push(id + 1, position);
                            }
                            id = output;
                            if (!checkAndLoop(textLen, id, position)) {
                                id = 0;
                            }
                        }
                        case OP_NOP -> {
                            if (!last) {
                                push(id + 1, position);
                            }
                            id = output;
                            if (!checkAndLoop(textLen, id, position)) {
                                id = 0;
                            }
                        }
                        case OP_MATCH -> {
                            if (endMatch && position != textEnd) {
                                id = nextInList(last, id);
                                break;
                            }

                            if (submatchCount == 0) {
                                return true;
                            }

                            matched = true;
                            cap[1] = position;

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

                            if (!longest) {
                                return true;
                            }
                            if (position == textEnd) {
                                return true;
                            }

                            id = nextInList(last, id);
                        }
                        case OP_ALT -> {
                            // Unreachable in flattened programs.
                            id = 0;
                        }
                    }

                    if (id == 0) {
                        break;
                    }
                }
            }

            return matched;
        }

        private int nextInList(boolean last, int id)
        {
            if (!last) {
                return id + 1;
            }
            return 0;
        }

        private boolean checkAndLoop(int textLen, int id, int p)
        {
            if (id == 0) {
                return false;
            }

            return shouldVisit(textLen, visited, listHeads[id], p - textBegin);
        }
    }
}
