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

final class Nfa
{
    private Nfa() {}

    // PERFORMANCE-SENSITIVE HOT LOOPS: queue representation, loop boundaries, and capture
    // handling have measured effects. Do not perform readability-only refactors in the search
    // implementations below without direct path tests and focused Intel and Graviton benchmarks.

    /**
     * Searches for a match in {@code text}.
     * <p>
     * This is a faithful port of upstream {@code re2/nfa.cc} and therefore:
     * - runs in time linear in the length of the input (no catastrophic backtracking)
     * - tracks capture groups (submatches)
     * - notices a match only once it is one byte past it
     * <p>
     * If {@code anchored} is true, only considers matches starting at the beginning of {@code text}.
     * Otherwise, finds the leftmost match at or after the beginning of {@code text}.
     * <p>
     * {@code matchKind} selects leftmost-first, leftmost-longest, or full-match behavior.
     * <p>
     * On success, writes up to {@code submatchCount = submatch.length / 2} pairs of offsets into
     * {@code submatch}, in bytes, relative to the beginning of {@code text}. The 0th pair is the
     * entire match. Unmatched groups are reported as {@code (-1,-1)}.
     */
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

        // Enforce anchor constraints against the supplied context/text slices.
        if (prog.anchorStart() && ctxBegin != textBegin) {
            return false;
        }
        if (prog.anchorEnd() && ctxEnd != textEnd) {
            return false;
        }

        anchored |= prog.anchorStart() || matchKind == Prog.MatchKind.FULL_MATCH;
        boolean longest = matchKind != Prog.MatchKind.FIRST_MATCH;
        boolean endMatch = prog.anchorEnd() || matchKind == Prog.MatchKind.FULL_MATCH;

        byte[] bytes = context.byteArray();
        int submatchCount = (submatch == null) ? 0 : (submatch.length / 2);
        if (submatchCount == 0) {
            NoSubmatchNfaImpl nfa = new NoSubmatchNfaImpl(
                    bytes,
                    prog,
                    prog.start(),
                    textBegin,
                    textEnd,
                    ctxBegin,
                    ctxEnd,
                    anchored,
                    longest,
                    endMatch);
            return nfa.search();
        }

        try {
            NfaImpl nfa = new NfaImpl(
                    bytes,
                    prog,
                    prog.start(),
                    textBegin,
                    textEnd,
                    ctxBegin,
                    ctxEnd,
                    submatchCount,
                    anchored,
                    longest,
                    endMatch,
                    workspace);
            boolean matched = nfa.search();
            if (matched) {
                nfa.writeSubmatch(submatch);
            }
            nfa.releaseWorkspace();
            return matched;
        }
        catch (RuntimeException | Error failure) {
            if (workspace != null) {
                workspace.invalidate();
            }
            throw failure;
        }
    }

    public static boolean search(Prog prog, Slice text, boolean anchored, Prog.MatchKind matchKind, int[] submatch)
    {
        return search(prog, text, 0, text.length(), anchored, matchKind, submatch);
    }

    /**
     * Anchored full match (equivalent to RE2 {@code ANCHOR_BOTH}): the regexp must match
     * starting at the beginning of {@code text} and ending at the end of {@code text}.
     */
    public static boolean fullMatch(Prog prog, Slice text)
    {
        requireNonNull(prog, "prog is null");
        requireNonNull(text, "text is null");

        return search(prog, text, 0, text.length(), true, Prog.MatchKind.FULL_MATCH, null);
    }

    /**
     * Unanchored partial match (equivalent to RE2 {@code ANCHOR_NONE} + {@code kFirstMatch}):
     * finds a leftmost-first match anywhere in {@code text}.
     */
    public static boolean partialMatch(Prog prog, Slice text)
    {
        requireNonNull(prog, "prog is null");
        requireNonNull(text, "text is null");
        return search(prog, text, 0, text.length(), false, Prog.MatchKind.FIRST_MATCH, null);
    }

    static int captureStackSize(Prog prog)
    {
        int stackSize = 2 * prog.getInstCount(InstOp.CAPTURE) +
                prog.getInstCount(InstOp.EMPTY_WIDTH) +
                prog.getInstCount(InstOp.NOP) + 1;
        return Math.max(8, stackSize);
    }

    // Sparse queue specialized for NFA hot loops. Membership is tracked with sparse positions
    // and validated against dense ids to keep clear() O(1) without clearing backing arrays.
    private static final class NfaThreadQueue
    {
        private int size;
        private final int[] sparsePos;
        private final int[] denseId;
        // Encoded start position payload (startPos + 1). Zero means placeholder/non-thread entry.
        private final int[] denseThread;

        private NfaThreadQueue(int maxId)
        {
            this.sparsePos = new int[maxId];
            this.denseId = new int[maxId];
            this.denseThread = new int[maxId];
            this.size = 0;
        }

        private void clear()
        {
            size = 0;
        }

        private boolean isEmpty()
        {
            return size == 0;
        }

        private int size()
        {
            return size;
        }

        private int denseIdAt(int pos)
        {
            return denseId[pos];
        }

        private int denseThreadAt(int pos)
        {
            return denseThread[pos];
        }

        // Returns dense position for a newly inserted id; -1 if id is already present.
        private int markVisitedIfAbsent(int id)
        {
            int pos = sparsePos[id];
            if (pos < size && denseId[pos] == id) {
                return -1;
            }
            int newPos = size;
            sparsePos[id] = newPos;
            denseId[newPos] = id;
            denseThread[newPos] = 0;
            size = newPos + 1;
            return newPos;
        }

        private void setThreadAtPos(int pos, int encodedStartPos)
        {
            denseThread[pos] = encodedStartPos;
        }

        private int posOfExisting(int id)
        {
            return sparsePos[id];
        }
    }

    private static final class NoSubmatchNfaImpl
    {
        // InstOp ordinals encoded as literals so they can be used in switch labels.
        private static final byte OP_ALT = 0;
        private static final byte OP_ALT_MATCH = 1;
        private static final byte OP_BYTE_RANGE = 2;
        private static final byte OP_CAPTURE = 3;
        private static final byte OP_EMPTY_WIDTH = 4;
        private static final byte OP_MATCH = 5;
        private static final byte OP_NOP = 6;
        private static final byte OP_FAIL = 7;
        private static final boolean[] WORD_CHAR_TABLE = buildWordCharTable();

        private final Prog prog;
        private final byte[] bytes;

        private final int start;

        private final int ctxBegin;
        private final int ctxEnd;

        private final int textBegin;
        private final int textEnd;

        private final boolean anchored;
        private final boolean longest;
        private final boolean endMatch;

        private final NfaThreadQueue q0;
        private final NfaThreadQueue q1;

        private final byte[] op;
        private final int[] out;
        private final int[] out1;
        private final int[] hint;
        private final int[] lo;
        private final int[] hi;
        private final int[] empty;
        private final int[] outLast;
        private final boolean[] foldCase;
        private final boolean[] altMatchGreedy;
        private final boolean hasCaptureOps;

        // AddToThreadq stack.
        private final int[] addIdStack;

        private boolean matched;
        private int matchStart;
        private int matchEnd;

        private NoSubmatchNfaImpl(
                byte[] bytes,
                Prog prog,
                int start,
                int textBegin,
                int textEnd,
                int ctxBegin,
                int ctxEnd,
                boolean anchored,
                boolean longest,
                boolean endMatch)
        {
            this.bytes = requireNonNull(bytes, "bytes is null");
            this.prog = requireNonNull(prog, "prog is null");

            this.start = start;
            this.textBegin = textBegin;
            this.textEnd = textEnd;
            this.ctxBegin = ctxBegin;
            this.ctxEnd = ctxEnd;
            this.anchored = anchored;
            this.longest = longest;
            this.endMatch = endMatch;

            this.matched = false;
            this.matchStart = -1;
            this.matchEnd = -1;

            Prog.NoSubmatchTables tables = prog.getOrCreateNoSubmatchTables();
            int progSize = tables.op().length;
            this.q0 = new NfaThreadQueue(progSize);
            this.q1 = new NfaThreadQueue(progSize);
            this.op = tables.op();
            this.out = tables.out();
            this.out1 = tables.out1();
            this.hint = tables.hint();
            this.lo = tables.lo();
            this.hi = tables.hi();
            this.empty = tables.empty();
            this.outLast = tables.outLast();
            this.foldCase = tables.foldCase();
            this.altMatchGreedy = tables.altMatchGreedy();
            this.hasCaptureOps = tables.hasCaptureOps();
            this.addIdStack = new int[tables.addStackSize()];
        }

        private boolean search()
        {
            NfaThreadQueue runQueue = q0;
            NfaThreadQueue nextQueue = q1;
            runQueue.clear();
            nextQueue.clear();

            for (int position = textBegin; ; position++) {
                int c = (position < textEnd) ? (bytes[position] & 0xFF) : -1;

                int shortcut = step(runQueue, nextQueue, c, position);

                NfaThreadQueue temporary = runQueue;
                runQueue = nextQueue;
                nextQueue = temporary;
                nextQueue.clear();

                if (shortcut != 0) {
                    int id = shortcut;
                    for (; ; ) {
                        byte opcode = op[id];
                        switch (opcode) {
                            case OP_CAPTURE, OP_NOP -> id = outLast[id] >>> 1;
                            case OP_MATCH -> {
                                matched = true;
                                id = 0;
                            }
                            default -> id = 0;
                        }
                        if (id == 0) {
                            break;
                        }
                    }
                    break;
                }

                if (position > textEnd) {
                    break;
                }

                if (!matched && (!anchored || position == textBegin)) {
                    if (!anchored && runQueue.isEmpty() && position < textEnd && prog.canPrefixAccel()) {
                        int found = prog.prefixAccel(bytes, position, textEnd - position);
                        if (found < 0) {
                            position = textEnd;
                        }
                        else {
                            position = found;
                        }
                        c = (position < textEnd) ? (bytes[position] & 0xFF) : -1;
                    }

                    addToThreadq(runQueue, start, c, position, position);
                }

                if (runQueue.isEmpty()) {
                    break;
                }
            }
            runQueue.clear();

            return matched;
        }

        private int step(NfaThreadQueue runQueue, NfaThreadQueue nextQueue, int c, int position)
        {
            nextQueue.clear();

            for (int it = 0; it < runQueue.size(); it++) {
                int encodedStartPos = runQueue.denseThreadAt(it);
                if (encodedStartPos == 0) {
                    continue;
                }
                int startPos = encodedStartPos - 1;

                if (longest && matched && matchStart >= 0 && matchStart < startPos) {
                    continue;
                }

                int id = runQueue.denseIdAt(it);
                byte opcode = op[id];
                switch (opcode) {
                    case OP_BYTE_RANGE -> addToThreadq(nextQueue, out[id], c, position, startPos);
                    case OP_ALT_MATCH -> {
                        if (it != 0) {
                            break;
                        }
                        boolean greedy = altMatchGreedy[id];
                        if (greedy || longest) {
                            matched = true;
                            matchStart = startPos;
                            matchEnd = position - 1;

                            runQueue.clear();
                            if (greedy) {
                                return out1[id];
                            }
                            return out[id];
                        }
                    }
                    case OP_MATCH -> {
                        if (endMatch && (position - 1) != textEnd) {
                            break;
                        }

                        int endPos = position - 1;
                        if (longest) {
                            if (!matched ||
                                    startPos < matchStart ||
                                    (startPos == matchStart && endPos > matchEnd)) {
                                matched = true;
                                matchStart = startPos;
                                matchEnd = endPos;
                            }
                        }
                        else {
                            matched = true;
                            matchStart = startPos;
                            matchEnd = endPos;

                            runQueue.clear();
                            return 0;
                        }
                    }
                    default -> {
                        // Unexpected in flattened programs (runQueue should contain only ByteRange/AltMatch/Match).
                    }
                }
            }
            runQueue.clear();
            return 0;
        }

        private void addToThreadq(NfaThreadQueue q, int id0, int c, int position, int startPos)
        {
            if (hasCaptureOps) {
                addToThreadqWithIndexedCaptureTables(q, id0, c, position, startPos);
                return;
            }

            if (id0 == 0) {
                return;
            }

            int stackPointer = 0;
            addIdStack[stackPointer] = id0;
            stackPointer++;

            while (stackPointer > 0) {
                stackPointer--;
                int id = addIdStack[stackPointer];

                for (; ; ) {
                    if (id == 0) {
                        break;
                    }

                    int pos = q.markVisitedIfAbsent(id);
                    if (pos < 0) {
                        break;
                    }

                    byte opcode = op[id];
                    switch (opcode) {
                        case OP_FAIL -> id = 0;
                        case OP_ALT_MATCH -> {
                            q.setThreadAtPos(pos, startPos + 1);
                            if ((outLast[id] & 1) != 0) {
                                id = 0;
                                break;
                            }
                            id = id + 1;
                        }
                        case OP_NOP -> {
                            int outLastValue = outLast[id];
                            if ((outLastValue & 1) == 0) {
                                pushAddState(id + 1, stackPointer++);
                            }
                            id = outLastValue >>> 1;
                        }
                        case OP_CAPTURE -> {
                            int outLastValue = outLast[id];
                            if ((outLastValue & 1) == 0) {
                                pushAddState(id + 1, stackPointer++);
                            }
                            id = outLastValue >>> 1;
                        }
                        case OP_BYTE_RANGE -> {
                            if (!matchesByteRange(id, c)) {
                                if ((outLast[id] & 1) != 0) {
                                    id = 0;
                                }
                                else {
                                    id = id + 1;
                                }
                                break;
                            }

                            q.setThreadAtPos(pos, startPos + 1);
                            int nextHint = hint[id];
                            if (nextHint == 0) {
                                id = 0;
                                break;
                            }
                            id = id + nextHint;
                        }
                        case OP_MATCH -> {
                            q.setThreadAtPos(pos, startPos + 1);
                            if ((outLast[id] & 1) != 0) {
                                id = 0;
                            }
                            else {
                                id = id + 1;
                            }
                        }
                        case OP_EMPTY_WIDTH -> {
                            int outLastValue = outLast[id];
                            if ((outLastValue & 1) == 0) {
                                pushAddState(id + 1, stackPointer++);
                            }

                            int emptyFlags = emptyFlags(position);
                            if ((empty[id] & ~emptyFlags) == 0) {
                                id = outLastValue >>> 1;
                            }
                            else {
                                id = 0;
                            }
                        }
                        case OP_ALT -> {
                            pushAddState(out1[id], stackPointer++);
                            id = out[id];
                        }
                    }
                    if (id == 0) {
                        break;
                    }
                }
            }
        }

        // Capture-heavy no-submatch path: preserve Inst semantics, but source opcode/state data
        // from flattened arrays to avoid repeated Inst object dereferences/getter calls.
        private void addToThreadqWithIndexedCaptureTables(NfaThreadQueue q, int id0, int c, int position, int startPos)
        {
            if (id0 == 0) {
                return;
            }

            int encodedStartPos = startPos + 1;
            int cachedEmptyFlags = Integer.MIN_VALUE;
            int stackPointer = 0;
            addIdStack[stackPointer] = id0;
            stackPointer++;

            while (stackPointer > 0) {
                stackPointer--;
                int id = addIdStack[stackPointer];

                for (; ; ) {
                    if (id == 0) {
                        break;
                    }

                    int pos = q.markVisitedIfAbsent(id);
                    if (pos < 0) {
                        break;
                    }

                    int outLastValue = outLast[id];
                    int outValue = outLastValue >>> 1;
                    boolean last = (outLastValue & 1) != 0;
                    switch (op[id]) {
                        case OP_FAIL -> id = 0;
                        case OP_ALT_MATCH -> {
                            q.setThreadAtPos(pos, encodedStartPos);
                            if (last) {
                                id = 0;
                                break;
                            }
                            id = id + 1;
                        }
                        case OP_NOP, OP_CAPTURE -> {
                            if (!last) {
                                pushAddState(id + 1, stackPointer++);
                            }
                            id = outValue;
                        }
                        case OP_BYTE_RANGE -> {
                            if (!matchesByteRange(id, c)) {
                                if (last) {
                                    id = 0;
                                }
                                else {
                                    id = id + 1;
                                }
                                break;
                            }

                            q.setThreadAtPos(pos, encodedStartPos);
                            int nextHint = hint[id];
                            if (nextHint == 0) {
                                id = 0;
                                break;
                            }
                            id = id + nextHint;
                        }
                        case OP_MATCH -> {
                            q.setThreadAtPos(pos, encodedStartPos);
                            if (last) {
                                id = 0;
                            }
                            else {
                                id = id + 1;
                            }
                        }
                        case OP_EMPTY_WIDTH -> {
                            if (!last) {
                                pushAddState(id + 1, stackPointer++);
                            }

                            if (cachedEmptyFlags == Integer.MIN_VALUE) {
                                cachedEmptyFlags = emptyFlags(position);
                            }
                            if ((empty[id] & ~cachedEmptyFlags) == 0) {
                                id = outValue;
                            }
                            else {
                                id = 0;
                            }
                        }
                        case OP_ALT -> {
                            pushAddState(out1[id], stackPointer++);
                            id = outValue;
                        }
                    }
                    if (id == 0) {
                        break;
                    }
                }
            }
        }

        private boolean matchesByteRange(int id, int c)
        {
            if (c < 0 || c > 0xFF) {
                return false;
            }
            int b = c;
            if (foldCase[id] && 'A' <= b && b <= 'Z') {
                b += 'a' - 'A';
            }
            return lo[id] <= b && b <= hi[id];
        }

        private void pushAddState(int id, int pos)
        {
            if (pos >= addIdStack.length) {
                throw new IllegalStateException("AddToThreadq stack overflow (pos=" + pos + " capacity=" + addIdStack.length + ")");
            }
            addIdStack[pos] = id;
        }

        private int emptyFlags(int position)
        {
            int flags = 0;

            if (position == ctxBegin) {
                flags |= EmptyOp.EMPTY_BEGIN_TEXT | EmptyOp.EMPTY_BEGIN_LINE;
            }
            else if (bytes[position - 1] == '\n') {
                flags |= EmptyOp.EMPTY_BEGIN_LINE;
            }

            if (position == ctxEnd) {
                flags |= EmptyOp.EMPTY_END_TEXT | EmptyOp.EMPTY_END_LINE;
            }
            else if (position < ctxEnd && bytes[position] == '\n') {
                flags |= EmptyOp.EMPTY_END_LINE;
            }

            // A word boundary exists when left/right word-char classification differs.
            // Boundaries at context edges treat the missing side as non-word.
            boolean leftWord = position > ctxBegin && isWordChar(bytes[position - 1]);
            boolean rightWord = position < ctxEnd && isWordChar(bytes[position]);
            boolean wordBoundary = leftWord != rightWord;

            if (wordBoundary) {
                flags |= EmptyOp.EMPTY_WORD_BOUNDARY;
            }
            else {
                flags |= EmptyOp.EMPTY_NO_WORD_BOUNDARY;
            }

            return flags;
        }

        private static boolean isWordChar(byte b)
        {
            return WORD_CHAR_TABLE[b & 0xFF];
        }

        private static boolean[] buildWordCharTable()
        {
            boolean[] table = new boolean[256];
            for (int c = 0; c <= 0xFF; c++) {
                table[c] = ('A' <= c && c <= 'Z') ||
                        ('a' <= c && c <= 'z') ||
                        ('0' <= c && c <= '9') ||
                        c == '_';
            }
            return table;
        }
    }

    private static final class CaptureThreadQueue
    {
        private int size;
        private final int[] sparsePosition;
        private final int[] denseInstructionId;
        private final int[] denseThread;

        private CaptureThreadQueue(int programSize)
        {
            sparsePosition = new int[programSize];
            denseInstructionId = new int[programSize];
            denseThread = new int[programSize];
        }

        private void clear()
        {
            size = 0;
        }

        private boolean isEmpty()
        {
            return size == 0;
        }

        private int size()
        {
            return size;
        }

        private int instructionIdAt(int position)
        {
            return denseInstructionId[position];
        }

        private int threadAt(int position)
        {
            return denseThread[position];
        }

        private int markVisitedIfAbsent(int instructionId)
        {
            int position = sparsePosition[instructionId];
            if (position < size && denseInstructionId[position] == instructionId) {
                return -1;
            }

            position = size;
            sparsePosition[instructionId] = position;
            denseInstructionId[position] = instructionId;
            denseThread[position] = 0;
            size = position + 1;
            return position;
        }

        private void setThreadAt(int position, int thread)
        {
            denseThread[position] = thread;
        }
    }

    static final class Workspace
    {
        private Prog program;
        private int captureSlotCount;
        private CaptureThreadQueue firstQueue;
        private CaptureThreadQueue secondQueue;
        private int[] addInstructionIdStack;
        private int[] addRestoreThreadStack;
        private int[] threadReferences;
        private int[] threadNext;
        private int[][] threadCaptures;
        private int[] match;
        private int threadCount;
        private int freeList;

        private void prepare(Prog program, int captureSlotCount)
        {
            if (this.program == program && this.captureSlotCount == captureSlotCount) {
                firstQueue.clear();
                secondQueue.clear();
                Arrays.fill(match, -1);
                return;
            }

            int programSize = program.size();
            CaptureThreadQueue firstQueue = new CaptureThreadQueue(programSize);
            CaptureThreadQueue secondQueue = new CaptureThreadQueue(programSize);
            int stackSize = captureStackSize(program);
            int[] addInstructionIdStack = new int[stackSize];
            int[] addRestoreThreadStack = new int[stackSize];
            int[] threadReferences = new int[16];
            int[] threadNext = new int[16];
            int[][] threadCaptures = new int[16][];
            int[] match = new int[captureSlotCount];
            Arrays.fill(match, -1);

            // Publish a complete workspace only after every allocation succeeds.
            this.program = program;
            this.captureSlotCount = captureSlotCount;
            this.firstQueue = firstQueue;
            this.secondQueue = secondQueue;
            this.addInstructionIdStack = addInstructionIdStack;
            this.addRestoreThreadStack = addRestoreThreadStack;
            this.threadReferences = threadReferences;
            this.threadNext = threadNext;
            this.threadCaptures = threadCaptures;
            this.match = match;
            this.threadCount = 0;
            this.freeList = 0;
        }

        private void updateArena(NfaImpl nfa)
        {
            threadReferences = nfa.threadRef;
            threadNext = nfa.threadNext;
            threadCaptures = nfa.threadCapture;
            threadCount = nfa.threadCount;
            freeList = nfa.freeList;
        }

        private void invalidate()
        {
            program = null;
            captureSlotCount = 0;
            firstQueue = null;
            secondQueue = null;
            addInstructionIdStack = null;
            addRestoreThreadStack = null;
            threadReferences = null;
            threadNext = null;
            threadCaptures = null;
            match = null;
            threadCount = 0;
            freeList = 0;
        }
    }

    private static final class NfaImpl
    {
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
        private final long[] instructionWords;
        private final byte[] bytes;

        private final int start;

        private final int ctxBegin;
        private final int ctxEnd;

        private final int textBegin;
        private final int textEnd;

        private final int submatchCount;
        private final int ncapture;
        private final boolean anchored;
        private final boolean longest;
        private final boolean endMatch;

        private final CaptureThreadQueue q0;
        private final CaptureThreadQueue q1;

        // AddToThreadq stack.
        private final int[] addIdStack;
        private final int[] addRestoreThreadStack;

        // Thread arena/pool (1-based ids; 0 means null).
        private int threadCount;
        private int freeList;
        private int[] threadRef;
        private int[] threadNext;
        private int[][] threadCapture;

        private final int[] match; // best match so far (absolute byte[] indices)
        private boolean matched;
        private final Workspace workspace;

        private NfaImpl(
                byte[] bytes,
                Prog prog,
                int start,
                int textBegin,
                int textEnd,
                int ctxBegin,
                int ctxEnd,
                int submatchCount,
                boolean anchored,
                boolean longest,
                boolean endMatch,
                Workspace workspace)
        {
            this.bytes = requireNonNull(bytes, "bytes is null");
            this.prog = requireNonNull(prog, "prog is null");
            this.instructionWords = prog.getOrCreateCaptureInstructionWords();

            this.start = start;
            this.textBegin = textBegin;
            this.textEnd = textEnd;
            this.ctxBegin = ctxBegin;
            this.ctxEnd = ctxEnd;
            this.submatchCount = submatchCount;
            this.anchored = anchored;
            this.longest = longest;
            this.endMatch = endMatch;

            // We always track at least $0 (two slots), even if the caller requested no submatches.
            this.ncapture = (submatchCount == 0) ? 2 : 2 * submatchCount;
            this.workspace = workspace;
            this.matched = false;

            if (workspace == null) {
                this.match = new int[ncapture];
                Arrays.fill(this.match, -1);
                int programSize = prog.size();
                this.q0 = new CaptureThreadQueue(programSize);
                this.q1 = new CaptureThreadQueue(programSize);
                int stackSize = captureStackSize(prog);
                this.addIdStack = new int[stackSize];
                this.addRestoreThreadStack = new int[stackSize];
                this.threadRef = new int[16];
                this.threadNext = new int[16];
                this.threadCapture = new int[16][];
                this.threadCount = 0;
                this.freeList = 0;
            }
            else {
                workspace.prepare(prog, ncapture);
                this.match = workspace.match;
                this.q0 = workspace.firstQueue;
                this.q1 = workspace.secondQueue;
                this.addIdStack = workspace.addInstructionIdStack;
                this.addRestoreThreadStack = workspace.addRestoreThreadStack;
                this.threadRef = workspace.threadReferences;
                this.threadNext = workspace.threadNext;
                this.threadCapture = workspace.threadCaptures;
                this.threadCount = workspace.threadCount;
                this.freeList = workspace.freeList;
            }
        }

        private void releaseWorkspace()
        {
            if (workspace != null) {
                workspace.updateArena(this);
            }
        }

        private boolean search()
        {
            CaptureThreadQueue runQueue = q0;
            CaptureThreadQueue nextQueue = q1;
            runQueue.clear();
            nextQueue.clear();

            // Loop over the text, stepping the machine.
            // position is an absolute index into the underlying byte[]; like upstream, we run one step past textEnd.
            for (int position = textBegin; ; position++) {
                int c = (position < textEnd) ? (bytes[position] & 0xFF) : -1;

                int shortcut = step(runQueue, nextQueue, c, position);

                // swap(runQueue, nextQueue)
                CaptureThreadQueue temporary = runQueue;
                runQueue = nextQueue;
                nextQueue = temporary;
                nextQueue.clear();

                if (shortcut != 0) {
                    // We're done: full match ahead.
                    position = textEnd;
                    int id = shortcut;
                    for (; ; ) {
                        long instructionWord = instructionWords[id];
                        switch ((int) (instructionWord & 0b111)) {
                            case OP_CAPTURE -> {
                                int captureIndex = (int) (instructionWord >>> 32);
                                if (captureIndex < ncapture) {
                                    match[captureIndex] = position;
                                }
                                id = (int) ((instructionWord >>> 4) & OUTPUT_MASK);
                            }
                            case OP_NOP -> id = (int) ((instructionWord >>> 4) & OUTPUT_MASK);
                            case OP_MATCH -> {
                                match[1] = position;
                                matched = true;
                                id = 0;
                            }
                            default -> {
                                // Unexpected opcode in short-circuit path.
                                id = 0;
                            }
                        }
                        if (id == 0) {
                            break;
                        }
                    }
                    break;
                }

                if (position > textEnd) {
                    break;
                }

                // Start a new thread if there have not been any matches.
                if (!matched && (!anchored || position == textBegin)) {
                    if (!anchored && runQueue.isEmpty() && position < textEnd && prog.canPrefixAccel()) {
                        int found = prog.prefixAccel(bytes, position, textEnd - position);
                        if (found < 0) {
                            position = textEnd;
                        }
                        else {
                            position = found;
                        }
                        c = (position < textEnd) ? (bytes[position] & 0xFF) : -1;
                    }

                    int t = allocThread();
                    copyCapture(threadCapture[t], match);
                    threadCapture[t][0] = position;
                    addToThreadq(runQueue, start, c, position, t);
                    decref(t);
                }

                if (runQueue.isEmpty()) {
                    break;
                }
            }

            // Free remaining threads.
            for (int i = 0; i < runQueue.size(); i++) {
                int t = runQueue.threadAt(i);
                if (t != 0) {
                    decref(t);
                }
            }
            runQueue.clear();

            return matched;
        }

        private void writeSubmatch(int[] out)
        {
            for (int i = 0; i < submatchCount; i++) {
                int a = match[2 * i];
                int b = match[2 * i + 1];
                int o = 2 * i;
                if (a < 0 || b < 0) {
                    out[o] = -1;
                    out[o + 1] = -1;
                }
                else {
                    out[o] = a - textBegin;
                    out[o + 1] = b - textBegin;
                }
            }
        }

        private int step(CaptureThreadQueue runQueue, CaptureThreadQueue nextQueue, int c, int position)
        {
            nextQueue.clear();

            for (int it = 0; it < runQueue.size(); it++) {
                int t = runQueue.threadAt(it);
                if (t == 0) {
                    continue;
                }

                if (longest) {
                    if (matched && match[0] >= 0 && match[0] < threadCapture[t][0]) {
                        decref(t);
                        continue;
                    }
                }

                int id = runQueue.instructionIdAt(it);
                long instructionWord = instructionWords[id];
                switch ((int) (instructionWord & 0b111)) {
                    case OP_BYTE_RANGE -> addToThreadq(nextQueue, (int) ((instructionWord >>> 4) & OUTPUT_MASK), c, position, t);
                    case OP_ALT_MATCH -> {
                        if (it != 0) {
                            break;
                        }
                        int payload = (int) (instructionWord >>> 32);
                        boolean greedy = payload < 0;
                        if (greedy || longest) {
                            copyCapture(match, threadCapture[t]);
                            matched = true;

                            decref(t);
                            for (int j = it + 1; j < runQueue.size(); j++) {
                                int other = runQueue.threadAt(j);
                                if (other != 0) {
                                    decref(other);
                                }
                            }
                            runQueue.clear();
                            if (greedy) {
                                return payload & (int) OUTPUT_MASK;
                            }
                            return (int) ((instructionWord >>> 4) & OUTPUT_MASK);
                        }
                    }
                    case OP_MATCH -> {
                        if (endMatch && (position - 1) != textEnd) {
                            break;
                        }

                        if (longest) {
                            int startPos = threadCapture[t][0];
                            int endPos = position - 1;
                            if (!matched ||
                                    startPos < match[0] ||
                                    (startPos == match[0] && endPos > match[1])) {
                                copyCapture(match, threadCapture[t]);
                                match[1] = endPos;
                                matched = true;
                            }
                        }
                        else {
                            copyCapture(match, threadCapture[t]);
                            match[1] = position - 1;
                            matched = true;

                            decref(t);
                            for (int j = it + 1; j < runQueue.size(); j++) {
                                int other = runQueue.threadAt(j);
                                if (other != 0) {
                                    decref(other);
                                }
                            }
                            runQueue.clear();
                            return 0;
                        }
                    }
                    default -> {
                        // Unexpected in flattened programs (runQueue should contain only ByteRange/AltMatch/Match).
                    }
                }

                decref(t);
            }
            runQueue.clear();
            return 0;
        }

        private void addToThreadq(CaptureThreadQueue queue, int id0, int c, int position, int t0)
        {
            if (id0 == 0) {
                return;
            }

            int stackPointer = 0;
            addIdStack[stackPointer] = id0;
            addRestoreThreadStack[stackPointer] = 0;
            stackPointer++;

            while (stackPointer > 0) {
                stackPointer--;
                int id = addIdStack[stackPointer];
                int restore = addRestoreThreadStack[stackPointer];

                if (restore != 0) {
                    decref(t0);
                    t0 = restore;
                }

                for (; ; ) {
                    if (id == 0) {
                        break;
                    }

                    int queuePosition = queue.markVisitedIfAbsent(id);
                    if (queuePosition < 0) {
                        break;
                    }
                    long instructionWord = instructionWords[id];
                    int output = (int) ((instructionWord >>> 4) & OUTPUT_MASK);
                    boolean last = (instructionWord & (1L << 3)) != 0;
                    int payload = (int) (instructionWord >>> 32);

                    switch ((int) (instructionWord & 0b111)) {
                        case OP_FAIL -> {
                            id = 0;
                        }
                        case OP_ALT_MATCH -> {
                            queue.setThreadAt(queuePosition, incref(t0));
                            if (last) {
                                id = 0;
                                break;
                            }
                            id = id + 1;
                        }
                        case OP_NOP -> {
                            if (!last) {
                                pushAddState(id + 1, 0, stackPointer++);
                            }
                            id = output;
                        }
                        case OP_CAPTURE -> {
                            if (!last) {
                                pushAddState(id + 1, 0, stackPointer++);
                            }

                            if (payload < ncapture) {
                                // Push a dummy whose only job is to restore t0.
                                pushAddState(0, t0, stackPointer++);

                                int t = allocThread();
                                copyCapture(threadCapture[t], threadCapture[t0]);
                                threadCapture[t][payload] = position;
                                t0 = t;
                            }

                            id = output;
                        }
                        case OP_BYTE_RANGE -> {
                            int value = c;
                            if (payload < 0 && 'A' <= value && value <= 'Z') {
                                value += 'a' - 'A';
                            }
                            int lowerBound = payload & 0xFF;
                            int upperBound = (payload >>> 8) & 0xFF;
                            if (value < lowerBound || value > upperBound) {
                                // Does not match; fall through to next in the list.
                                if (last) {
                                    id = 0;
                                }
                                else {
                                    id = id + 1;
                                }
                                break;
                            }

                            queue.setThreadAt(queuePosition, incref(t0));
                            int hint = (payload >>> 16) & 0x7FFF;
                            if (hint == 0) {
                                id = 0;
                                break;
                            }
                            id = id + hint;
                        }
                        case OP_MATCH -> {
                            queue.setThreadAt(queuePosition, incref(t0));
                            if (last) {
                                id = 0;
                            }
                            else {
                                id = id + 1;
                            }
                        }
                        case OP_EMPTY_WIDTH -> {
                            if (!last) {
                                pushAddState(id + 1, 0, stackPointer++);
                            }

                            int empty = emptyFlags(position);
                            if ((payload & ~empty) == 0) {
                                id = output;
                            }
                            else {
                                id = 0;
                            }
                        }
                        case OP_ALT -> {
                            // Unreachable in flattened programs but safe to handle.
                            pushAddState(payload & (int) OUTPUT_MASK, 0, stackPointer++);
                            id = output;
                        }
                    }
                    if (id == 0) {
                        break;
                    }
                }
            }
        }

        private void pushAddState(int id, int restoreThread, int pos)
        {
            if (pos >= addIdStack.length) {
                throw new IllegalStateException("AddToThreadq stack overflow (pos=" + pos + " capacity=" + addIdStack.length + ")");
            }
            addIdStack[pos] = id;
            addRestoreThreadStack[pos] = restoreThread;
        }

        private int allocThread()
        {
            int id = freeList;
            if (id != 0) {
                freeList = threadNext[id];
                threadRef[id] = 1;
                return id;
            }

            id = ++threadCount;
            ensureThreadCapacity(id);
            threadRef[id] = 1;
            if (threadCapture[id] == null || threadCapture[id].length != ncapture) {
                threadCapture[id] = new int[ncapture];
            }
            return id;
        }

        private void ensureThreadCapacity(int id)
        {
            if (id < threadRef.length) {
                return;
            }
            int newLen = threadRef.length;
            while (newLen <= id) {
                newLen *= 2;
            }
            threadRef = Arrays.copyOf(threadRef, newLen);
            threadNext = Arrays.copyOf(threadNext, newLen);
            threadCapture = Arrays.copyOf(threadCapture, newLen);
        }

        private int incref(int t)
        {
            threadRef[t]++;
            return t;
        }

        private void decref(int t)
        {
            threadRef[t]--;
            if (threadRef[t] > 0) {
                return;
            }
            threadNext[t] = freeList;
            freeList = t;
        }

        private void copyCapture(int[] dst, int[] src)
        {
            System.arraycopy(src, 0, dst, 0, ncapture);
        }

        private int emptyFlags(int position)
        {
            int flags = 0;

            // ^ and \A
            if (position == ctxBegin) {
                flags |= EmptyOp.EMPTY_BEGIN_TEXT | EmptyOp.EMPTY_BEGIN_LINE;
            }
            else if (bytes[position - 1] == '\n') {
                flags |= EmptyOp.EMPTY_BEGIN_LINE;
            }

            // $ and \z
            if (position == ctxEnd) {
                flags |= EmptyOp.EMPTY_END_TEXT | EmptyOp.EMPTY_END_LINE;
            }
            else if (position < ctxEnd && bytes[position] == '\n') {
                flags |= EmptyOp.EMPTY_END_LINE;
            }

            // \b and \B
            boolean wordBoundary = false;
            if (position == ctxBegin && position == ctxEnd) {
                wordBoundary = false;
            }
            else if (position == ctxBegin) {
                if (isWordChar(bytes[position])) {
                    wordBoundary = true;
                }
            }
            else if (position == ctxEnd) {
                if (isWordChar(bytes[position - 1])) {
                    wordBoundary = true;
                }
            }
            else {
                if (isWordChar(bytes[position - 1]) != isWordChar(bytes[position])) {
                    wordBoundary = true;
                }
            }

            if (wordBoundary) {
                flags |= EmptyOp.EMPTY_WORD_BOUNDARY;
            }
            else {
                flags |= EmptyOp.EMPTY_NO_WORD_BOUNDARY;
            }

            return flags;
        }

        private static boolean isWordChar(byte b)
        {
            int c = b & 0xFF;
            return ('A' <= c && c <= 'Z') ||
                    ('a' <= c && c <= 'z') ||
                    ('0' <= c && c <= '9') ||
                    c == '_';
        }
    }
}
