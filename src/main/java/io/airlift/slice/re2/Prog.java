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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

final class Prog
{
    private static final long BIT_STATE_BITMAP_MAXIMUM_BYTES = 256L * 1024L;
    private static final int FUSED_PREFIX_VECTOR_MINIMUM_BYTES = 1024;

    public enum MatchKind
    {
        FIRST_MATCH,
        LONGEST_MATCH,
        FULL_MATCH,
        MANY_MATCH,
    }

    enum PrefixAccelStrategy
    {
        REPEATED_BYTE,
        FUSED_SWAR,
        FUSED_VECTOR,
    }

    // VarHandles for SWAR byte scanning (8-byte and 4-byte at a time)
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
    private static final int MAXIMUM_FIXED_DISTANCE_BYTE_OFFSET = 16;

    private static final int[] WORD_RANGES = {48, 57, 65, 90, 95, 95, 97, 122};
    private static final int[] NON_WORD_RANGES = {0, 47, 58, 64, 91, 94, 96, 96, 123, 255};

    public record PossibleMatchRangeResult(Slice min, Slice max) {}

    record FixedDistanceByteCandidates(byte[] candidates, int offset) {}

    // Single instruction in regexp program.
    public static final class Inst
    {
        private InstOp opcode;

        private int out;
        private int out1;

        private int lo;
        private int hi;
        private int hintFoldCase;

        private int cap;
        private int empty;
        private int matchId;

        private boolean last;

        private Inst(InstOp opcode)
        {
            this.opcode = requireNonNull(opcode, "opcode is null");
        }

        public static Inst createAlt(int out, int out1)
        {
            Inst inst = new Inst(InstOp.ALT);
            inst.out = out;
            inst.out1 = out1;
            return inst;
        }

        public static Inst createAltMatch(int out, int out1)
        {
            Inst inst = new Inst(InstOp.ALT_MATCH);
            inst.out = out;
            inst.out1 = out1;
            return inst;
        }

        public static Inst createByteRange(int lo, int hi, boolean foldCase, int out)
        {
            Inst inst = new Inst(InstOp.BYTE_RANGE);
            inst.lo = lo & 0xFF;
            inst.hi = hi & 0xFF;
            inst.hintFoldCase = foldCase ? 1 : 0;
            inst.out = out;
            return inst;
        }

        public static Inst createCapture(int cap, int out)
        {
            Inst inst = new Inst(InstOp.CAPTURE);
            inst.cap = cap;
            inst.out = out;
            return inst;
        }

        public static Inst createEmptyWidth(int empty, int out)
        {
            Inst inst = new Inst(InstOp.EMPTY_WIDTH);
            inst.empty = empty;
            inst.out = out;
            return inst;
        }

        public static Inst createMatch(int matchId)
        {
            Inst inst = new Inst(InstOp.MATCH);
            inst.matchId = matchId;
            return inst;
        }

        public static Inst createNop(int out)
        {
            Inst inst = new Inst(InstOp.NOP);
            inst.out = out;
            return inst;
        }

        public static Inst createFail()
        {
            return new Inst(InstOp.FAIL);
        }

        public InstOp opcode()
        {
            return opcode;
        }

        public int out()
        {
            return out;
        }

        public void setOut(int out)
        {
            this.out = out;
        }

        public int out1()
        {
            return out1;
        }

        public void setOut1(int out1)
        {
            this.out1 = out1;
        }

        public void setOpcode(InstOp opcode)
        {
            this.opcode = requireNonNull(opcode, "opcode is null");
        }

        Inst copy()
        {
            Inst inst = new Inst(opcode);
            inst.out = out;
            inst.out1 = out1;
            inst.lo = lo;
            inst.hi = hi;
            inst.hintFoldCase = hintFoldCase;
            inst.cap = cap;
            inst.empty = empty;
            inst.matchId = matchId;
            inst.last = last;
            return inst;
        }

        public int lo()
        {
            return lo;
        }

        public int hi()
        {
            return hi;
        }

        public boolean foldCase()
        {
            return (hintFoldCase & 1) != 0;
        }

        public int hint()
        {
            return hintFoldCase >>> 1;
        }

        public int cap()
        {
            return cap;
        }

        public int empty()
        {
            return empty;
        }

        public int matchId()
        {
            return matchId;
        }

        public boolean last()
        {
            return last;
        }

        public void setLast()
        {
            this.last = true;
        }

        public boolean matches(int c)
        {
            if (opcode != InstOp.BYTE_RANGE) {
                throw new IllegalStateException("not a byte range inst: " + opcode);
            }
            // Upstream Inst::Matches() expects an int byte value. The DFA uses a sentinel
            // "end text" byte (256) and NFA-style engines use -1 for "no byte".
            // Both must never match a byte range.
            if (c < 0 || c > 0xFF) {
                return false;
            }
            int b = c;
            if (foldCase() && 'A' <= b && b <= 'Z') {
                b += 'a' - 'A';
            }
            return lo <= b && b <= hi;
        }

        public boolean greedy(Prog p)
        {
            requireNonNull(p, "p is null");
            if (opcode != InstOp.ALT_MATCH) {
                throw new IllegalStateException("not an altmatch inst: " + opcode);
            }

            Inst outInst = p.inst(out());
            if (outInst.opcode() == InstOp.BYTE_RANGE) {
                return true;
            }
            return outInst.opcode() == InstOp.NOP &&
                    p.inst(outInst.out()).opcode() == InstOp.BYTE_RANGE;
        }

        public String dump()
        {
            return switch (opcode) {
                case ALT -> "alt -> " + out + " | " + out1;
                case ALT_MATCH -> "altmatch -> " + out + " | " + out1;
                case BYTE_RANGE -> "byte" +
                        (foldCase() ? "/i" : "") +
                        " [" + hex2(lo) + "-" + hex2(hi) + "] " +
                        hint() + " -> " + out;
                case CAPTURE -> "capture " + cap + " -> " + out;
                case EMPTY_WIDTH -> "emptywidth " + hashHex(empty) + " -> " + out;
                case MATCH -> "match! " + matchId;
                case NOP -> "nop -> " + out;
                case FAIL -> "fail";
            };
        }

        private static String hex2(int v)
        {
            return String.format("%02x", v & 0xFF);
        }

        private static String hashHex(int v)
        {
            // C's "%#x" formatting: 0 => "0", otherwise "0x..." (lowercase).
            if (v == 0) {
                return "0";
            }
            return "0x" + Integer.toHexString(v);
        }
    }

    private final List<Inst> insts = new ArrayList<>();

    private boolean didFlatten;
    private int start;
    private int startUnanchored;

    private boolean anchorStart;
    private boolean anchorEnd;
    private boolean canMatchEmpty;

    /**
     * Whether this program is compiled for reverse (right-to-left) search.
     * <p>
     * When {@code true}:
     * <ul>
     * <li>{@link io.airlift.slice.re2.Dfa#search} scans bytes right-to-left (backward)
     * <li>Anchor flags are swapped from pattern semantics: {@code ^} becomes {@code $}, {@code $} becomes {@code ^}
     * <li>Used in Phase 2 of two-phase search to find match start after forward DFA found end
     * <li>Memory budget typically 1/3 of total (vs 2/3 for forward)
     * </ul>
     * <p><b>Two-Phase Search Pattern:</b>
     * <ol>
     * <li>Forward program ({@code reversed=false}) finds match end
     * <li>Reverse program ({@code reversed=true}) finds match start by searching backward
     * </ol>
     * <p>See {@code REVERSE_DFA.md} for usage examples and complete documentation.
     * <p>
     *
     * @see io.airlift.slice.re2.Compiler#compile for reverse compilation
     * @see io.airlift.slice.re2.Dfa#search for search execution
     */
    private boolean reversed;

    // Enables a full-match shortcut only when invalid UTF-8 is also accepted.
    private boolean matchesAnyByteString;

    // Prepared once for the OnePass engine.
    private boolean onePassPrepared;
    private int onePassStateCount;
    private int[] onePassMatchCond;
    private int[] onePassAction; // flattened [stateCount * bytemapRange] actions, encoded like upstream
    // Only programs exceeding upstream's capture-bit capacity allocate these parallel masks.
    private long[] onePassMatchCapture;
    private long[] onePassActionCapture;

    // Prepared by flatten() for BitState execution.
    private int listCount;
    private short[] listHeads; // maps list-head instruction index to list ID; other entries contain -1
    private int bitStateTextMaxSize;

    // Instruction counts by opcode, populated by flatten().
    private final int[] instCount = new int[InstOp.values().length];

    private int bytemapRange;
    private final byte[] bytemap = new byte[256];

    // Prefix acceleration configuration.
    private boolean prefixFoldCase;
    private int prefixSize;
    private byte[] prefix;
    private long prefixFrontBroadcastMask;  // Pre-computed SWAR broadcast mask for first prefix byte
    private static final int MAX_FOLD_CASE_PREFIX_BYTES = 9;

    // Required prefix for instant rejection (extracted via requiredPrefix()).
    // If pattern is "^literal..." and text doesn't contain the literal, reject immediately.
    private boolean requiredPrefixFoldCase;
    private byte[] requiredPrefix;

    // DFA caching with memory budgets (like upstream RE2).
    private long dfaMemory;  // Total DFA memory budget
    private final AtomicReference<Dfa.DfaInstance> dfaFirstMatch = new AtomicReference<>();
    private final AtomicReference<Dfa.DfaInstance> dfaLongestMatch = new AtomicReference<>();
    private final AtomicReference<Dfa.DfaInstance> dfaManyMatch = new AtomicReference<>();
    // Immutable flattened instruction tables for NFA no-submatch fast path.
    private final AtomicReference<NoSubmatchTables> noSubmatchNfaTables = new AtomicReference<>();
    // Compact immutable instructions built lazily after flattening and shared by capture engines.
    private volatile long[] captureInstructionWords;

    public Prog()
    {
        // Instruction #0 is always the fail instruction in upstream.
        add(Inst.createFail());
    }

    public int size()
    {
        return insts.size();
    }

    /**
     * For each instruction reachable from start, computes the number of byte-range
     * instructions reachable by following only empty transitions.
     */
    public void fanout(SparseIntArray fanout)
    {
        requireNonNull(fanout, "fanout is null");
        if (fanout.maxSize() != size()) {
            fanout.resize(size());
        }

        SparseSet reachable = new SparseSet(size());
        fanout.clear();
        fanout.setNew(start(), 0);

        for (int fanoutIndex = 0; fanoutIndex < fanout.size(); fanoutIndex++) {
            int id = fanout.denseIndexAt(fanoutIndex);
            int count = fanout.denseValueAt(fanoutIndex);
            reachable.clear();
            reachable.insert(id);
            for (int reachableIndex = 0; reachableIndex < reachable.size(); reachableIndex++) {
                int instructionId = reachable.denseAt(reachableIndex);
                Inst instruction = inst(instructionId);
                switch (instruction.opcode()) {
                    case ALT -> throw new IllegalStateException("unhandled ALT in Prog.fanout");
                    case BYTE_RANGE -> {
                        if (!instruction.last()) {
                            reachable.insert(instructionId + 1);
                        }
                        count++;
                        if (!fanout.hasIndex(instruction.out())) {
                            fanout.setNew(instruction.out(), 0);
                        }
                    }
                    case ALT_MATCH -> {
                        if (!instruction.last()) {
                            reachable.insert(instructionId + 1);
                        }
                    }
                    case CAPTURE, EMPTY_WIDTH, NOP -> {
                        if (!instruction.last()) {
                            reachable.insert(instructionId + 1);
                        }
                        reachable.insert(instruction.out());
                    }
                    case MATCH -> {
                        if (!instruction.last()) {
                            reachable.insert(instructionId + 1);
                        }
                    }
                    case FAIL -> {}
                }
            }
            fanout.setExisting(id, count);
        }
    }

    /**
     * Removes the most recently added instruction.
     * <p>
     * This is an internal compiler helper used during UTF-8 rune range trie factoring,
     * mirroring upstream RE2 behavior that frees an instruction when it would otherwise
     * become unreachable.
     */
    public void removeLastInst(int expectedId)
    {
        int last = insts.size() - 1;
        if (expectedId != last) {
            throw new IllegalArgumentException("expected last id " + last + " but got " + expectedId);
        }
        insts.remove(last);
    }

    public int start()
    {
        return start;
    }

    public void setStart(int start)
    {
        this.start = start;
    }

    public int startUnanchored()
    {
        return startUnanchored;
    }

    public void setStartUnanchored(int startUnanchored)
    {
        this.startUnanchored = startUnanchored;
    }

    public boolean anchorStart()
    {
        return anchorStart;
    }

    public void setAnchorStart(boolean anchorStart)
    {
        this.anchorStart = anchorStart;
    }

    public boolean anchorEnd()
    {
        return anchorEnd;
    }

    public void setAnchorEnd(boolean anchorEnd)
    {
        this.anchorEnd = anchorEnd;
    }

    public boolean canMatchEmpty()
    {
        return canMatchEmpty;
    }

    public void setCanMatchEmpty(boolean canMatchEmpty)
    {
        this.canMatchEmpty = canMatchEmpty;
    }

    /**
     * Returns whether this program is compiled for reverse (right-to-left) search.
     * <p>
     *
     * @return true if program scans backward, false if forward
     * @see #reversed field for complete documentation
     */
    public boolean reversed()
    {
        return reversed;
    }

    /**
     * Sets whether this program should execute reverse (right-to-left) search.
     * <p>
     * <b>NOTE:</b> Typically set during compilation by {@link io.airlift.slice.re2.Compiler#compile}.
     * Manually changing this after compilation may produce incorrect results if
     * anchor flags weren't swapped appropriately.
     * <p>
     *
     * @param reversed true for backward search, false for forward
     */
    public void setReversed(boolean reversed)
    {
        this.reversed = reversed;
    }

    /**
     * Returns whether this pattern matches every byte sequence, including invalid UTF-8.
     */
    public boolean matchesAnyByteString()
    {
        return matchesAnyByteString;
    }

    /**
     * Sets whether this pattern matches every byte sequence.
     */
    public void setMatchesAnyByteString(boolean matchesAnyByteString)
    {
        this.matchesAnyByteString = matchesAnyByteString;
    }

    public boolean isOnePass()
    {
        return onePassMatchCond != null;
    }

    void prepareOnePass()
    {
        if (onePassPrepared) {
            return;
        }
        onePassPrepared = true;
        computeOnePass();
    }

    public int onePassStateCount()
    {
        return onePassStateCount;
    }

    public int[] onePassMatchCond()
    {
        return onePassMatchCond;
    }

    public int[] onePassAction()
    {
        return onePassAction;
    }

    public boolean supportsOnePassCaptureSlots(int captureSlotCount)
    {
        return captureSlotCount <= ONEPASS_MAX_CAP ||
                (captureSlotCount <= Long.SIZE && onePassActionCapture != null);
    }

    public long[] onePassMatchCapture()
    {
        return onePassMatchCapture;
    }

    public long[] onePassActionCapture()
    {
        return onePassActionCapture;
    }

    public boolean canBitState()
    {
        return listHeads != null;
    }

    public int listCount()
    {
        return listCount;
    }

    public short[] listHeads()
    {
        return listHeads;
    }

    public int bitStateTextMaxSize()
    {
        return bitStateTextMaxSize;
    }

    /**
     * Returns instruction counts by opcode, indexed by {@link InstOp#ordinal()}.
     * Only valid after {@link #flatten()} has been called.
     */
    public int[] getInstCount()
    {
        return instCount.clone();
    }

    /**
     * Returns instruction count for a specific opcode.
     * Only valid after {@link #flatten()} has been called.
     */
    public int getInstCount(InstOp op)
    {
        return instCount[op.ordinal()];
    }

    public boolean canPrefixAccel()
    {
        return prefixSize != 0;
    }

    /**
     * Builds a conservative set of bytes that can begin a non-empty match.
     * Non-nullable searches may scan past other bytes while the DFA remains in
     * its start state. Nullable searches may return the current empty match when
     * the current byte is outside the set. Assertions and dense sets deliberately
     * use the normal DFA.
     */
    byte[] buildStartByteCandidates(int[] stack)
    {
        boolean nullable = canMatchEmpty;
        if (reversed || anchorStart || (nullable && anchorEnd) || canPrefixAccel() || start() == 0) {
            return null;
        }

        byte[] candidates = new byte[256];
        boolean[] visited = new boolean[size()];
        int stackPointer = 0;
        stack[stackPointer++] = start();
        int candidateCount = 0;

        while (stackPointer > 0) {
            int instructionId = stack[--stackPointer];
            if (instructionId <= 0 || instructionId >= size() || visited[instructionId]) {
                continue;
            }
            visited[instructionId] = true;

            Inst instruction = inst(instructionId);
            switch (instruction.opcode()) {
                case BYTE_RANGE -> {
                    candidateCount += addStartByteRange(candidates, instruction);
                    if (!instruction.last()) {
                        stack[stackPointer++] = instructionId + 1;
                    }
                }
                case CAPTURE, NOP -> {
                    if (!instruction.last()) {
                        stack[stackPointer++] = instructionId + 1;
                    }
                    stack[stackPointer++] = instruction.out();
                }
                case EMPTY_WIDTH -> {
                    // Skipping input would lose the previous-byte context needed by the assertion.
                    return null;
                }
                case ALT, ALT_MATCH -> {
                    if (!instruction.last()) {
                        stack[stackPointer++] = instructionId + 1;
                    }
                    stack[stackPointer++] = instruction.out1();
                    stack[stackPointer++] = instruction.out();
                }
                case MATCH -> {
                    if (!nullable) {
                        return null;
                    }
                }
                case FAIL -> {
                    if (!instruction.last()) {
                        stack[stackPointer++] = instructionId + 1;
                    }
                }
            }
        }

        // Dense candidate sets spend more time handing off to the DFA than they save scanning.
        if (candidateCount <= 64 && (candidateCount > 0 || nullable)) {
            return candidates;
        }
        return null;
    }

    /**
     * Finds a selective byte set at one fixed offset from every possible match start.
     * The bounded walk deliberately rejects assertions and variable-distance candidates.
     */
    FixedDistanceByteCandidates buildFixedDistanceByteCandidates(int[] stack, byte[] startByteCandidates)
    {
        if (reversed || anchorStart || canMatchEmpty || canPrefixAccel() || startByteCandidates == null || start() == 0) {
            return null;
        }

        int bestCandidateCount = countCandidates(startByteCandidates);
        byte[] bestCandidates = null;
        int bestOffset = -1;

        int[] currentInstructions = new int[size()];
        int currentInstructionCount = 1;
        currentInstructions[0] = start();
        int[] byteInstructions = new int[size()];
        boolean[] visited = new boolean[size()];

        for (int offset = 0; offset <= MAXIMUM_FIXED_DISTANCE_BYTE_OFFSET && currentInstructionCount > 0; offset++) {
            Arrays.fill(visited, false);
            byte[] candidates = new byte[256];
            int candidateCount = 0;
            int byteInstructionCount = 0;
            boolean canMatchAtCurrentOffset = false;
            for (int currentInstructionIndex = 0; currentInstructionIndex < currentInstructionCount; currentInstructionIndex++) {
                int stackPointer = 0;
                stack[stackPointer++] = currentInstructions[currentInstructionIndex];

                while (stackPointer > 0) {
                    int instructionId = stack[--stackPointer];
                    if (instructionId <= 0 || instructionId >= size() || visited[instructionId]) {
                        continue;
                    }
                    visited[instructionId] = true;

                    Inst instruction = inst(instructionId);
                    switch (instruction.opcode()) {
                        case BYTE_RANGE -> {
                            candidateCount += addStartByteRange(candidates, instruction);
                            byteInstructions[byteInstructionCount++] = instructionId;
                            if (!instruction.last()) {
                                stack[stackPointer++] = instructionId + 1;
                            }
                        }
                        case CAPTURE, NOP -> {
                            if (!instruction.last()) {
                                stack[stackPointer++] = instructionId + 1;
                            }
                            stack[stackPointer++] = instruction.out();
                        }
                        case EMPTY_WIDTH -> {
                            return null;
                        }
                        case ALT, ALT_MATCH -> {
                            if (!instruction.last()) {
                                stack[stackPointer++] = instructionId + 1;
                            }
                            stack[stackPointer++] = instruction.out1();
                            stack[stackPointer++] = instruction.out();
                        }
                        case MATCH -> {
                            canMatchAtCurrentOffset = true;
                            if (!instruction.last()) {
                                stack[stackPointer++] = instructionId + 1;
                            }
                        }
                        case FAIL -> {
                            if (!instruction.last()) {
                                stack[stackPointer++] = instructionId + 1;
                            }
                        }
                    }
                }
            }

            if (canMatchAtCurrentOffset) {
                break;
            }
            if (offset > 0 && candidateCount > 0 && candidateCount < bestCandidateCount) {
                bestCandidateCount = candidateCount;
                bestCandidates = candidates;
                bestOffset = offset;
            }

            currentInstructionCount = 0;
            for (int index = 0; index < byteInstructionCount; index++) {
                currentInstructions[currentInstructionCount++] = inst(byteInstructions[index]).out();
            }
        }

        if (bestCandidates == null || bestCandidateCount > 64) {
            return null;
        }
        return new FixedDistanceByteCandidates(bestCandidates, bestOffset);
    }

    private static int countCandidates(byte[] candidates)
    {
        if (candidates == null) {
            return Integer.MAX_VALUE;
        }
        int count = 0;
        for (byte candidate : candidates) {
            if (candidate != 0) {
                count++;
            }
        }
        return count;
    }

    private static int addStartByteRange(byte[] candidates, Inst instruction)
    {
        int added = 0;
        for (int value = instruction.lo(); value <= instruction.hi(); value++) {
            if ((!instruction.foldCase() || value < 'A' || value > 'Z') && candidates[value] == 0) {
                candidates[value] = 1;
                added++;
            }
        }
        if (instruction.foldCase()) {
            for (int value = 'A'; value <= 'Z'; value++) {
                int foldedValue = value + ('a' - 'A');
                if (instruction.lo() <= foldedValue && foldedValue <= instruction.hi() && candidates[value] == 0) {
                    candidates[value] = 1;
                    added++;
                }
            }
        }
        return added;
    }

    /**
     * True when prefix acceleration is a single-byte, case-sensitive search.
     * This is the highest-frequency false-positive regime and can use a
     * dedicated DFA path without affecting other prefix strategies.
     */
    boolean canUseSingleBytePrefixAccelFastPath()
    {
        return prefixSize == 1 && !prefixFoldCase;
    }

    boolean usesFoldCasePrefixCandidateScan()
    {
        return prefixFoldCase;
    }

    PrefixAccelStrategy prefixAccelStrategy(int length)
    {
        if (prefixFoldCase || prefixSize <= 1 || prefix[0] >= 0) {
            return PrefixAccelStrategy.REPEATED_BYTE;
        }
        if (VectorSupport.isAvailable() && length >= FUSED_PREFIX_VECTOR_MINIMUM_BYTES) {
            return PrefixAccelStrategy.FUSED_VECTOR;
        }
        return PrefixAccelStrategy.FUSED_SWAR;
    }

    /**
     * Specialized prefix scan for single-byte, case-sensitive prefix acceleration.
     * Keeps the scan path branch-free with respect to foldCase/prefix mode.
     */
    int prefixAccelSingleByteNoFoldCase(byte[] data, int offset, int length)
    {
        if (length <= 0) {
            return -1;
        }
        if (!canUseSingleBytePrefixAccelFastPath()) {
            throw new IllegalStateException("single-byte prefix accel fast path not configured");
        }
        return indexOfSWAR(data, offset, length, prefixFrontBroadcastMask);
    }

    public void configurePrefixAccel(Slice prefix, boolean foldCase)
    {
        if (prefix == null || prefix.length() == 0) {
            this.prefix = null;
            this.prefixSize = 0;
            this.prefixFoldCase = false;
            this.prefixFrontBroadcastMask = 0;
            return;
        }

        byte[] bytes = new byte[prefix.length()];
        System.arraycopy(prefix.byteArray(), prefix.byteArrayOffset(), bytes, 0, prefix.length());

        this.prefix = bytes;
        this.prefixFoldCase = foldCase;

        if (foldCase) {
            // Limit to the same nine-byte prefix used by upstream ShiftDFA.
            this.prefixSize = Math.min(bytes.length, MAX_FOLD_CASE_PREFIX_BYTES);
            this.prefixFrontBroadcastMask = 0;
        }
        else if (bytes.length != 1) {
            this.prefixSize = bytes.length;
            this.prefixFrontBroadcastMask = (bytes[0] & 0xFFL) * 0x0101010101010101L;
        }
        else {
            // For single-byte non-foldCase, use simple memchr-style search
            this.prefixSize = 1;
            this.prefixFrontBroadcastMask = (bytes[0] & 0xFFL) * 0x0101010101010101L;
        }
    }

    /**
     * Returns the absolute index in {@code data} of the first possible prefix match,
     * or {@code -1} if no match is found.
     * <p>
     * Uses different strategies depending on configuration:
     * <ul>
     * <li>Parallel front-and-back candidate scanning for folded prefixes
     * <li>FrontAndBack for non-folded multi-byte prefixes (like C++ {@code Prog::PrefixAccel_FrontAndBack()})
     * <li>Simple byte search for single-byte prefix
     * </ul>
     */
    public int prefixAccel(byte[] data, int offset, int length)
    {
        if (!canPrefixAccel()) {
            throw new IllegalStateException("prefix acceleration not configured");
        }

        // Handle edge cases
        if (length < prefixSize) {
            return -1;
        }

        if (prefixFoldCase) {
            return prefixAccelFoldCaseFrontAndBack(data, offset, length);
        }

        // For single-byte prefix, just scan for that byte
        if (prefixSize == 1) {
            return indexOf(data, offset, length, prefix[0], prefixFoldCase);
        }

        PrefixAccelStrategy strategy = prefixAccelStrategy(length);
        if (strategy == PrefixAccelStrategy.FUSED_VECTOR) {
            return VectorPrefixScanner.find(
                    data,
                    offset,
                    length,
                    prefix,
                    0,
                    prefixSize - 1);
        }
        if (strategy == PrefixAccelStrategy.FUSED_SWAR) {
            return prefixAccelNoFoldCaseFrontAndBackSwar(data, offset, length);
        }

        // FrontAndBack optimization: use first and last bytes to filter candidates
        byte prefixFront = prefix[0];
        byte prefixBack = prefix[prefixSize - 1];
        int end = offset + length;
        int lastStart = end - prefixSize;

        // Search for prefix_front, then check prefix_back before full match
        int position = offset;
        while (position <= lastStart) {
            // Find next occurrence of first byte
            int next = indexOf(data, position, (lastStart + 1) - position, prefixFront, prefixFoldCase);
            if (next < 0) {
                return -1;  // First byte not found
            }

            position = next;

            // Check last byte before checking middle bytes
            if (byteMatches(data[position + prefixSize - 1], prefixBack, prefixFoldCase)) {
                // Front and back match - check full prefix
                if (prefixMatchesAt(data, position)) {
                    return position;
                }
            }

            position++;  // Move to next position
        }

        return -1;
    }

    // PERFORMANCE-SENSITIVE HOT LOOP: changes here require target-host assembly
    // and benchmark evidence. The two loads dominate this loop.
    private int prefixAccelNoFoldCaseFrontAndBackSwar(byte[] data, int offset, int length)
    {
        long lowBits = 0x0101010101010101L;
        long highBits = 0x8080808080808080L;
        int candidateCount = length - prefixSize + 1;
        int wordEnd = offset + (candidateCount / Long.BYTES) * Long.BYTES;
        int primaryOffset = 0;
        int secondaryOffset = prefixSize - 1;
        long primaryBroadcast = (prefix[primaryOffset] & 0xFFL) * lowBits;
        long secondaryBroadcast = (prefix[secondaryOffset] & 0xFFL) * lowBits;
        int position = offset;

        for (; position < wordEnd; position += Long.BYTES) {
            long primaryDifference = ((long) LONG_HANDLE.get(data, position + primaryOffset)) ^ primaryBroadcast;
            long secondaryDifference = ((long) LONG_HANDLE.get(data, position + secondaryOffset)) ^ secondaryBroadcast;
            long candidates = ((primaryDifference - lowBits) & ~primaryDifference & highBits) &
                    ((secondaryDifference - lowBits) & ~secondaryDifference & highBits);
            while (candidates != 0) {
                int candidate = position + (Long.numberOfTrailingZeros(candidates) >>> 3);
                if (prefixMatchesAt(data, candidate)) {
                    return candidate;
                }
                candidates &= candidates - 1;
            }
        }

        int end = offset + candidateCount;
        for (; position < end; position++) {
            if (data[position + primaryOffset] == prefix[primaryOffset] &&
                    data[position + secondaryOffset] == prefix[secondaryOffset] &&
                    prefixMatchesAt(data, position)) {
                return position;
            }
        }
        return -1;
    }

    private int prefixAccelFoldCaseFrontAndBack(byte[] data, int offset, int length)
    {
        if (prefixSize == 1) {
            return indexOfFoldCase(data, offset, length, prefix[0]);
        }
        return prefixAccelFoldCaseFrontAndBackSWAR(data, offset, length);
    }

    private int prefixAccelFoldCaseFrontAndBackSWAR(byte[] data, int offset, int length)
    {
        int candidateCount = length - prefixSize + 1;
        int wordEnd = offset + ((candidateCount / Long.BYTES) * Long.BYTES);
        int backOffset = prefixSize - 1;
        int position = offset;

        for (; position < wordEnd; position += Long.BYTES) {
            long frontBytes = (long) LONG_HANDLE.get(data, position);
            long backBytes = (long) LONG_HANDLE.get(data, position + backOffset);
            long candidates = foldCaseMatches(frontBytes, prefix[0]) &
                    foldCaseMatches(backBytes, prefix[backOffset]);
            while (candidates != 0) {
                int candidate = position + (Long.numberOfTrailingZeros(candidates) >>> 3);
                if (prefixMatchesAt(data, candidate)) {
                    return candidate;
                }
                candidates &= candidates - 1;
            }
        }

        int end = offset + candidateCount;
        for (; position < end; position++) {
            if (byteMatches(data[position], prefix[0], true) &&
                    byteMatches(data[position + backOffset], prefix[backOffset], true) &&
                    prefixMatchesAt(data, position)) {
                return position;
            }
        }
        return -1;
    }

    private static long foldCaseMatches(long data, byte expected)
    {
        long lowBits = 0x0101010101010101L;
        long highBits = 0x8080808080808080L;
        int lowerCaseByte = asciiLower(expected & 0xFF);
        long lowerCaseBroadcast = (lowerCaseByte & 0xFFL) * lowBits;
        long xor = data ^ lowerCaseBroadcast;
        long matches = (xor - lowBits) & ~xor & highBits;
        if (lowerCaseByte >= 'a' && lowerCaseByte <= 'z') {
            long upperCaseBroadcast = (lowerCaseByte - ('a' - 'A')) * lowBits;
            xor = data ^ upperCaseBroadcast;
            matches |= (xor - lowBits) & ~xor & highBits;
        }
        return matches;
    }

    /**
     * Returns absolute index or -1 if not found.
     */
    private int indexOf(byte[] data, int offset, int length, byte b, boolean foldCase)
    {
        if (length <= 0) {
            return -1;
        }

        // Case-insensitive is complex for vectorization, fall back to byte-by-byte
        if (foldCase) {
            return indexOfFoldCase(data, offset, length, b);
        }

        // Use pre-computed broadcast mask when searching for the prefix front byte
        long broadcastMask = (prefix != null && b == prefix[0]) ? prefixFrontBroadcastMask : (b & 0xFFL) * 0x0101010101010101L;

        return indexOfSWAR(data, offset, length, broadcastMask);
    }

    /**
     * Case-insensitive byte search using the SWAR path.
     */
    private int indexOfFoldCase(byte[] data, int offset, int length, byte b)
    {
        int lowerCaseByte = asciiLower(b & 0xFF);
        if (lowerCaseByte < 'a' || lowerCaseByte > 'z') {
            long broadcastMask = (lowerCaseByte & 0xFFL) * 0x0101010101010101L;
            return indexOfSWAR(data, offset, length, broadcastMask);
        }

        byte lowerCase = (byte) lowerCaseByte;
        byte upperCase = (byte) (lowerCaseByte - ('a' - 'A'));
        return indexOfEitherSWAR(data, offset, length, lowerCase, upperCase);
    }

    private int indexOfEitherSWAR(byte[] data, int offset, int length, byte first, byte second)
    {
        long lowBits = 0x0101010101010101L;
        long highBits = 0x8080808080808080L;
        long firstBroadcast = (first & 0xFFL) * lowBits;
        long secondBroadcast = (second & 0xFFL) * lowBits;
        int end = offset + length;
        int vectorEnd = offset + ((length / 16) * 16);

        for (int position = offset; position < vectorEnd; position += 16) {
            long firstWord = (long) LONG_HANDLE.get(data, position);
            long secondWord = (long) LONG_HANDLE.get(data, position + Long.BYTES);
            long firstXor = firstWord ^ firstBroadcast;
            long secondXor = firstWord ^ secondBroadcast;
            long firstMatches = ((firstXor - lowBits) & ~firstXor & highBits) |
                    ((secondXor - lowBits) & ~secondXor & highBits);

            firstXor = secondWord ^ firstBroadcast;
            secondXor = secondWord ^ secondBroadcast;
            long secondMatches = ((firstXor - lowBits) & ~firstXor & highBits) |
                    ((secondXor - lowBits) & ~secondXor & highBits);

            if ((firstMatches | secondMatches) != 0) {
                if (firstMatches != 0) {
                    return position + (Long.numberOfTrailingZeros(firstMatches) >>> 3);
                }
                return position + Long.BYTES + (Long.numberOfTrailingZeros(secondMatches) >>> 3);
            }
        }

        for (int position = vectorEnd; position < end; position++) {
            byte value = data[position];
            if (value == first || value == second) {
                return position;
            }
        }
        return -1;
    }

    /**
     * SWAR (SIMD Within A Register) byte scanning using Long operations.
     * Processes 16 bytes (2 longs) per iteration with minimal branching.
     * Throughput: ~33.7 GB/s on 16MB buffers (5.5x faster than byte-by-byte).
     */
    private int indexOfSWAR(byte[] data, int offset, int length, long broadcastMask)
    {
        final long loMask = 0x0101010101010101L;
        final long hiMask = 0x8080808080808080L;

        int end = offset + length;
        // Process 16 bytes (2 longs) at a time - reduces branch frequency
        int end16 = offset + ((length / 16) * 16);

        for (int i = offset; i < end16; i += 16) {
            // Load 2 longs (16 bytes) - no branches in load/compute
            long c0 = (long) LONG_HANDLE.get(data, i);
            long c1 = (long) LONG_HANDLE.get(data, i + 8);

            // XOR with broadcast mask - matching bytes become 0x00
            long x0 = c0 ^ broadcastMask;
            long x1 = c1 ^ broadcastMask;

            // Detect zero bytes using SWAR technique
            long z0 = (x0 - loMask) & ~x0 & hiMask;
            long z1 = (x1 - loMask) & ~x1 & hiMask;

            // Combine - only ONE branch per 16 bytes
            long anyMatch = z0 | z1;

            if (anyMatch != 0) {
                // Find which chunk and byte position
                if (z0 != 0) {
                    return i + (Long.numberOfTrailingZeros(z0) >>> 3);
                }
                if (z1 != 0) {
                    return i + 8 + (Long.numberOfTrailingZeros(z1) >>> 3);
                }
            }
        }

        // Tail: byte-by-byte for remaining bytes
        byte b = (byte) broadcastMask;
        for (int i = end16; i < end; i++) {
            if (data[i] == b) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Check if two bytes match, accounting for optional case folding.
     */
    private boolean byteMatches(byte a, byte b, boolean foldCase)
    {
        if (!foldCase) {
            return a == b;
        }
        return asciiLower(a & 0xFF) == asciiLower(b & 0xFF);
    }

    private boolean prefixMatchesAt(byte[] data, int i)
    {
        if (!prefixFoldCase) {
            for (int j = 0; j < prefixSize; j++) {
                if (data[i + j] != prefix[j]) {
                    return false;
                }
            }
            return true;
        }

        // ASCII case-insensitive comparison (matches RE2's byte-oriented foldCase hint).
        for (int j = 0; j < prefixSize; j++) {
            int a = asciiLower(data[i + j] & 0xFF);
            int b = asciiLower(prefix[j] & 0xFF);
            if (a != b) {
                return false;
            }
        }
        return true;
    }

    private static int asciiLower(int b)
    {
        if ('A' <= b && b <= 'Z') {
            return b + ('a' - 'A');
        }
        return b;
    }

    /**
     * Check if pattern has a required prefix (for instant rejection).
     * Required prefix means pattern is "^literal..." and can be rejected if text doesn't match.
     */
    public boolean hasRequiredPrefix()
    {
        return requiredPrefix != null && requiredPrefix.length > 0;
    }

    /**
     * Get the required prefix bytes (null if none).
     */
    public byte[] getRequiredPrefix()
    {
        return requiredPrefix;
    }

    /**
     * Check if required prefix uses case-insensitive matching.
     */
    public boolean isRequiredPrefixFoldCase()
    {
        return requiredPrefixFoldCase;
    }

    /**
     * Configure the required prefix extracted from the regexp AST.
     * Called by Compiler after compilation.
     */
    public void configureRequiredPrefix(Slice prefix, boolean foldCase)
    {
        if (prefix == null || prefix.length() == 0) {
            this.requiredPrefix = null;
            this.requiredPrefixFoldCase = false;
            return;
        }

        byte[] bytes = new byte[prefix.length()];
        System.arraycopy(prefix.byteArray(), prefix.byteArrayOffset(), bytes, 0, prefix.length());

        this.requiredPrefix = bytes;
        this.requiredPrefixFoldCase = foldCase;
    }

    /**
     * Check if text starts with the required prefix.
     * Used for instant rejection in Re2.matchInto() before engine cascade.
     */
    public boolean textMatchesRequiredPrefix(byte[] data, int offset, int length)
    {
        if (!hasRequiredPrefix()) {
            throw new IllegalStateException("required prefix not configured");
        }

        // If text is shorter than prefix, can't match
        if (length < requiredPrefix.length) {
            return false;
        }

        if (!requiredPrefixFoldCase) {
            for (int j = 0; j < requiredPrefix.length; j++) {
                if (data[offset + j] != requiredPrefix[j]) {
                    return false;
                }
            }
            return true;
        }

        // ASCII case-insensitive comparison
        for (int j = 0; j < requiredPrefix.length; j++) {
            int a = asciiLower(data[offset + j] & 0xFF);
            int b = asciiLower(requiredPrefix[j] & 0xFF);
            if (a != b) {
                return false;
            }
        }
        return true;
    }

    /**
     * Set the DFA memory budget (called from Re2.build()).
     * Like upstream Prog::set_dfa_mem().
     */
    public void setDfaMemory(long dfaMemory)
    {
        this.dfaMemory = dfaMemory;
    }

    /**
     * Get the DFA memory budget.
     * Like upstream Prog::dfa_mem().
     */
    public long dfaMemory()
    {
        return dfaMemory;
    }

    /**
     * Get or create cached DFA with memory budget.
     * Like upstream Prog::GetDFA().
     */
    public Dfa.DfaInstance getCachedDfa(Dfa.DfaInstance.Kind kind)
    {
        requireNonNull(kind, "kind is null");
        AtomicReference<Dfa.DfaInstance> ref = dfaReference(kind);

        // Fast path - already cached
        Dfa.DfaInstance cached = ref.get();
        if (cached != null) {
            return cached;
        }

        // Lazy initialization with memory budget
        long budget = switch (kind) {
            case MANY_MATCH -> dfaMemory;
            case FIRST_MATCH -> dfaMemory / 2;
            case LONGEST_MATCH -> reversed ? dfaMemory : dfaMemory / 2;
        };
        if (budget <= 0) {
            return null;
        }
        Dfa.DfaInstance newDfa = new Dfa.DfaInstance(this, kind, budget);

        if (!newDfa.ok()) {
            // Budget too small to even initialize DFA
            return null;
        }

        // CAS to cache
        if (ref.compareAndSet(null, newDfa)) {
            return newDfa;
        }
        // Lost race - use winner's DFA
        return ref.get();
    }

    Dfa.DfaInstance cachedDfaIfPresent(Dfa.DfaInstance.Kind kind)
    {
        requireNonNull(kind, "kind is null");
        return dfaReference(kind).get();
    }

    private AtomicReference<Dfa.DfaInstance> dfaReference(Dfa.DfaInstance.Kind kind)
    {
        return switch (kind) {
            case FIRST_MATCH -> dfaFirstMatch;
            case LONGEST_MATCH -> dfaLongestMatch;
            case MANY_MATCH -> dfaManyMatch;
        };
    }

    NoSubmatchTables getOrCreateNoSubmatchTables()
    {
        NoSubmatchTables cached = noSubmatchNfaTables.get();
        if (cached != null) {
            return cached;
        }

        NoSubmatchTables created = NoSubmatchTables.build(this);
        if (noSubmatchNfaTables.compareAndSet(null, created)) {
            return created;
        }
        return noSubmatchNfaTables.get();
    }

    long[] getOrCreateCaptureInstructionWords()
    {
        long[] cached = captureInstructionWords;
        if (cached != null) {
            return cached;
        }
        return createCaptureInstructionWords();
    }

    private synchronized long[] createCaptureInstructionWords()
    {
        if (captureInstructionWords != null) {
            return captureInstructionWords;
        }

        if (!didFlatten) {
            throw new IllegalStateException("program must be flattened before packing capture instructions");
        }

        captureInstructionWords = buildCaptureInstructionWords();
        return captureInstructionWords;
    }

    private long[] buildCaptureInstructionWords()
    {
        // [0..2] opcode, [3] last, [4..31] output, [32..63] opcode-specific payload.
        // ALT_MATCH uses payload bit 31 for greedy and BYTE_RANGE uses it for fold-case.
        long[] words = new long[size()];
        int outputMask = (1 << 28) - 1;
        for (int instructionId = 0; instructionId < words.length; instructionId++) {
            Inst instruction = inst(instructionId);
            checkPackedInstructionOutput(instruction.out(), outputMask);
            long payload = switch (instruction.opcode()) {
                case ALT -> {
                    checkPackedInstructionOutput(instruction.out1(), outputMask);
                    yield instruction.out1();
                }
                case ALT_MATCH -> {
                    checkPackedInstructionOutput(instruction.out1(), outputMask);
                    yield instruction.out1() |
                            (instruction.greedy(this) ? 1L << 31 : 0);
                }
                case BYTE_RANGE -> instruction.lo() |
                        (long) instruction.hi() << 8 |
                        (long) instruction.hint() << 16 |
                        (instruction.foldCase() ? 1L << 31 : 0);
                case CAPTURE -> Integer.toUnsignedLong(instruction.cap());
                case EMPTY_WIDTH -> Integer.toUnsignedLong(instruction.empty());
                case MATCH -> Integer.toUnsignedLong(instruction.matchId());
                case NOP, FAIL -> 0;
            };
            words[instructionId] = instruction.opcode().ordinal() |
                    (instruction.last() ? 1L << 3 : 0) |
                    (long) instruction.out() << 4 |
                    payload << 32;
        }
        return words;
    }

    private static void checkPackedInstructionOutput(int output, int outputMask)
    {
        if (output < 0 || output > outputMask) {
            throw new IllegalStateException("output exceeds packed instruction limit: " + output);
        }
    }

    static final class NoSubmatchTables
    {
        private static final int CLOSURE_CONTEXT_COUNT = 64;
        private static final int CLOSURE_MEMORY_CAP_BYTES = 64 * 1024;

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
        private final int addStackSize;
        private final boolean hasEpsilonClosures;
        private final int closureContextCount;
        private final int[] closureOffsets;
        private final int[] closureIds;
        private final int closureBytes;

        private NoSubmatchTables(
                byte[] op,
                int[] out,
                int[] out1,
                int[] hint,
                int[] lo,
                int[] hi,
                int[] empty,
                int[] outLast,
                boolean[] foldCase,
                boolean[] altMatchGreedy,
                boolean hasCaptureOps,
                int addStackSize,
                boolean hasEpsilonClosures,
                int closureContextCount,
                int[] closureOffsets,
                int[] closureIds,
                int closureBytes)
        {
            this.op = op;
            this.out = out;
            this.out1 = out1;
            this.hint = hint;
            this.lo = lo;
            this.hi = hi;
            this.empty = empty;
            this.outLast = outLast;
            this.foldCase = foldCase;
            this.altMatchGreedy = altMatchGreedy;
            this.hasCaptureOps = hasCaptureOps;
            this.addStackSize = addStackSize;
            this.hasEpsilonClosures = hasEpsilonClosures;
            this.closureContextCount = closureContextCount;
            this.closureOffsets = closureOffsets;
            this.closureIds = closureIds;
            this.closureBytes = closureBytes;
        }

        private static NoSubmatchTables build(Prog prog)
        {
            int progSize = prog.size();
            byte[] op = new byte[progSize];
            int[] out = new int[progSize];
            int[] out1 = new int[progSize];
            int[] hint = new int[progSize];
            int[] lo = new int[progSize];
            int[] hi = new int[progSize];
            int[] empty = new int[progSize];
            int[] outLast = new int[progSize];
            boolean[] foldCase = new boolean[progSize];
            boolean[] altMatchGreedy = new boolean[progSize];

            int captures = 0;
            int emptyWidths = 0;
            int nops = 0;
            for (int id = 0; id < progSize; id++) {
                Inst inst = prog.inst(id);
                InstOp opcode = inst.opcode();
                op[id] = (byte) opcode.ordinal();
                int outValue = inst.out();
                out[id] = outValue;
                out1[id] = inst.out1();
                hint[id] = inst.hint();
                lo[id] = inst.lo();
                hi[id] = inst.hi();
                empty[id] = inst.empty();
                outLast[id] = (outValue << 1) | (inst.last() ? 1 : 0);
                foldCase[id] = inst.foldCase();

                switch (opcode) {
                    case CAPTURE -> captures++;
                    case EMPTY_WIDTH -> emptyWidths++;
                    case NOP -> nops++;
                    default -> {}
                }
            }

            byte opAltMatch = (byte) InstOp.ALT_MATCH.ordinal();
            byte opByteRange = (byte) InstOp.BYTE_RANGE.ordinal();
            byte opNop = (byte) InstOp.NOP.ordinal();
            for (int id = 0; id < progSize; id++) {
                if (op[id] != opAltMatch) {
                    continue;
                }
                int outId = out[id];
                byte outOp = op[outId];
                altMatchGreedy[id] = outOp == opByteRange ||
                        (outOp == opNop && op[out[outId]] == opByteRange);
            }

            boolean hasCaptureOps = captures != 0;
            int addStackSize = Math.max(8, captures + emptyWidths + nops + 1);

            // A1 scaffolding: reserve shape for precomputed epsilon closures and disable
            // automatically when projected closure metadata exceeds the memory budget.
            int closureContextCount = CLOSURE_CONTEXT_COUNT;
            int closureSlots = Math.multiplyExact(progSize, closureContextCount);
            int closureOffsetBytes;
            boolean hasEpsilonClosures;
            try {
                closureOffsetBytes = Math.multiplyExact(closureSlots + 1, Integer.BYTES);
                hasEpsilonClosures = closureOffsetBytes <= CLOSURE_MEMORY_CAP_BYTES;
            }
            catch (ArithmeticException e) {
                closureOffsetBytes = Integer.MAX_VALUE;
                hasEpsilonClosures = false;
            }
            int[] closureOffsets = hasEpsilonClosures ? new int[closureSlots + 1] : new int[0];
            int[] closureIds = new int[0];
            int closureBytes = hasEpsilonClosures ? closureOffsetBytes : 0;

            return new NoSubmatchTables(
                    op,
                    out,
                    out1,
                    hint,
                    lo,
                    hi,
                    empty,
                    outLast,
                    foldCase,
                    altMatchGreedy,
                    hasCaptureOps,
                    addStackSize,
                    hasEpsilonClosures,
                    closureContextCount,
                    closureOffsets,
                    closureIds,
                    closureBytes);
        }

        byte[] op()
        {
            return op;
        }

        int[] out()
        {
            return out;
        }

        int[] out1()
        {
            return out1;
        }

        int[] hint()
        {
            return hint;
        }

        int[] lo()
        {
            return lo;
        }

        int[] hi()
        {
            return hi;
        }

        int[] empty()
        {
            return empty;
        }

        int[] outLast()
        {
            return outLast;
        }

        boolean[] foldCase()
        {
            return foldCase;
        }

        boolean[] altMatchGreedy()
        {
            return altMatchGreedy;
        }

        boolean hasCaptureOps()
        {
            return hasCaptureOps;
        }

        int addStackSize()
        {
            return addStackSize;
        }

        boolean hasEpsilonClosures()
        {
            return hasEpsilonClosures;
        }

        int closureContextCount()
        {
            return closureContextCount;
        }

        int[] closureOffsets()
        {
            return closureOffsets;
        }

        int[] closureIds()
        {
            return closureIds;
        }

        int closureBytes()
        {
            return closureBytes;
        }
    }

    public boolean didFlatten()
    {
        return didFlatten;
    }

    public void setDidFlatten(boolean didFlatten)
    {
        this.didFlatten = didFlatten;
    }

    public int bytemapRange()
    {
        return bytemapRange;
    }

    public void setBytemapRange(int bytemapRange)
    {
        this.bytemapRange = bytemapRange;
    }

    public void setBytemap(int c, int cls)
    {
        if (c < 0 || c > 255) {
            throw new IllegalArgumentException("byte out of range: " + c);
        }
        bytemap[c] = (byte) cls;
    }

    public int bytemap(int c)
    {
        if (c < 0 || c > 255) {
            throw new IllegalArgumentException("byte out of range: " + c);
        }
        return bytemap[c] & 0xFF;
    }

    public byte[] bytemapArray()
    {
        return bytemap;
    }

    public int add(Inst instruction)
    {
        int instructionId = insts.size();
        insts.add(requireNonNull(instruction, "instruction is null"));
        return instructionId;
    }

    public Inst inst(int instructionId)
    {
        if (instructionId < 0 || instructionId >= insts.size()) {
            throw new IllegalArgumentException("instructionId out of range: " + instructionId);
        }
        return insts.get(instructionId);
    }

    public List<Inst> insts()
    {
        return List.copyOf(insts);
    }

    public String dump()
    {
        if (didFlatten) {
            return flattenedProgToString(start);
        }

        SparseSet queue = new SparseSet(size() + 1);
        addToQueue(queue, start);
        return progToString(queue);
    }

    public String dumpUnanchored()
    {
        if (didFlatten) {
            return flattenedProgToString(startUnanchored);
        }

        SparseSet queue = new SparseSet(size() + 1);
        addToQueue(queue, startUnanchored);
        return progToString(queue);
    }

    public String dumpByteMap()
    {
        StringBuilder builder = new StringBuilder();
        for (int c = 0; c < 256; c++) {
            int byteClass = bytemap(c);
            int low = c;
            while (c < 255 && bytemap(c + 1) == byteClass) {
                c++;
            }
            int high = c;
            builder.append(String.format("[%02x-%02x] -> %d%n", low, high, byteClass));
        }
        return builder.toString();
    }

    public void computeByteMap()
    {
        // Fill in bytemap with byte classes for the program.
        // Ranges of bytes that are treated indistinguishably
        // will be mapped to a single byte class.
        ByteMapBuilder builder = new ByteMapBuilder();

        boolean markedLineBoundaries = false;
        boolean markedWordBoundaries = false;

        for (int id = 0; id < size(); id++) {
            Inst instruction = inst(id);
            if (instruction.opcode() == InstOp.BYTE_RANGE) {
                int lo = instruction.lo();
                int hi = instruction.hi();
                builder.mark(lo, hi);
                if (instruction.foldCase() && lo <= 'z' && hi >= 'a') {
                    int foldlo = lo;
                    int foldhi = hi;
                    if (foldlo < 'a') {
                        foldlo = 'a';
                    }
                    if (foldhi > 'z') {
                        foldhi = 'z';
                    }
                    if (foldlo <= foldhi) {
                        foldlo += 'A' - 'a';
                        foldhi += 'A' - 'a';
                        builder.mark(foldlo, foldhi);
                    }
                }

                // If this Inst is not the last Inst in its list AND the next Inst is
                // also a ByteRange AND the Insts have the same out, defer the merge.
                if (!instruction.last() &&
                        id + 1 < size() &&
                        inst(id + 1).opcode() == InstOp.BYTE_RANGE &&
                        instruction.out() == inst(id + 1).out()) {
                    continue;
                }
                builder.merge();
            }
            else if (instruction.opcode() == InstOp.EMPTY_WIDTH) {
                int empty = instruction.empty();
                if ((empty & (EmptyOp.EMPTY_BEGIN_LINE | EmptyOp.EMPTY_END_LINE)) != 0 && !markedLineBoundaries) {
                    builder.mark('\n', '\n');
                    builder.merge();
                    markedLineBoundaries = true;
                }
                if ((empty & (EmptyOp.EMPTY_WORD_BOUNDARY | EmptyOp.EMPTY_NO_WORD_BOUNDARY)) != 0 && !markedWordBoundaries) {
                    // Keep the original two-phase order:
                    // first mark word ranges, then mark non-word ranges.
                    markRanges(builder, WORD_RANGES);
                    builder.merge();
                    markRanges(builder, NON_WORD_RANGES);
                    builder.merge();
                    markedWordBoundaries = true;
                }
            }
        }

        int[] outRange = new int[1];
        builder.build(bytemap, outRange);
        bytemapRange = outRange[0];
    }

    /**
     * Computes a lexicographic range {@code [min, max]} such that any string {@code s} that is a
     * full (anchored) match for this program satisfies {@code min <= s && s <= max}.
     * <p>
     * The returned {@code min}/{@code max} are truncated to at most {@code maxLength} bytes. When the
     * underlying language has no maximum element (e.g., matches all non-empty strings in Latin-1),
     * this method returns {@code null}.
     * <p>
     * Note: Like upstream, this only considers the first copy of an infinitely repeated element.
     */
    public PossibleMatchRangeResult possibleMatchRange(int maxLength)
    {
        if (maxLength < 0) {
            throw new IllegalArgumentException("maxLength must be >= 0");
        }

        // Start "before" the first byte: no previous byte (-1).
        int[] startInstructionIds = normalizeStartInstructions(start(), EmptyOp.EMPTY_BEGIN_TEXT | EmptyOp.EMPTY_BEGIN_LINE);
        DfaState startState = new DfaState(startInstructionIds, -1, false, stateNeedsContext(startInstructionIds));

        // Build minimum string.
        StateVisitMap seen = new StateVisitMap();
        byte[] minimumBuffer = new byte[maxLength];
        int minimumLength = 0;
        DfaState state = startState;
        for (int i = 0; i < maxLength; i++) {
            if (seen.get(state) > 0) {
                break;
            }
            seen.increment(state);

            if (endTextIsMatch(state)) {
                break;
            }

            boolean extended = false;
            for (int byteValue = 0; byteValue < 256; byteValue++) {
                NextStep step = runOnByte(state, byteValue);
                if (step != null) {
                    DfaState nextState = new DfaState(
                            step.instructionIds(),
                            step.previousByte(),
                            step.match(),
                            stateNeedsContext(step.instructionIds()));
                    minimumBuffer[minimumLength++] = (byte) byteValue;
                    state = nextState;
                    extended = true;
                    break;
                }
            }
            if (!extended) {
                break;
            }
        }

        // Build maximum string.
        seen.clear();
        byte[] maximumBuffer = new byte[maxLength];
        int maximumLength = 0;
        state = startState;
        for (int i = 0; i < maxLength; i++) {
            if (seen.get(state) > 0) {
                break;
            }
            seen.increment(state);

            boolean extended = false;
            for (int byteValue = 255; byteValue >= 0; byteValue--) {
                NextStep step = runOnByte(state, byteValue);
                if (step != null) {
                    DfaState nextState = new DfaState(
                            step.instructionIds(),
                            step.previousByte(),
                            step.match(),
                            stateNeedsContext(step.instructionIds()));
                    maximumBuffer[maximumLength++] = (byte) byteValue;
                    state = nextState;
                    extended = true;
                    break;
                }
            }
            if (!extended) {
                return new PossibleMatchRangeResult(
                        Slices.wrappedBuffer(copyOfBytes(minimumBuffer, minimumLength)),
                        Slices.wrappedBuffer(copyOfBytes(maximumBuffer, maximumLength)));
            }
        }

        // Stopped while still adding to max - round up to the next prefix successor.
        maximumLength = prefixSuccessor(maximumBuffer, maximumLength);
        if (maximumLength == 0) {
            return null;
        }

        return new PossibleMatchRangeResult(
                Slices.wrappedBuffer(copyOfBytes(minimumBuffer, minimumLength)),
                Slices.wrappedBuffer(copyOfBytes(maximumBuffer, maximumLength)));
    }

    private static byte[] copyOfBytes(byte[] buffer, int length)
    {
        if (length == 0) {
            return new byte[0];
        }
        byte[] result = new byte[length];
        System.arraycopy(buffer, 0, result, 0, length);
        return result;
    }

    private static int[] copyOfInts(int[] buffer, int length)
    {
        if (length == 0) {
            return new int[0];
        }
        int[] result = new int[length];
        System.arraycopy(buffer, 0, result, 0, length);
        return result;
    }

    private boolean endTextIsMatch(DfaState state)
    {
        EpsilonClosure closure = epsilonClosure(state, -1);
        return closure.match;
    }

    private NextStep runOnByte(DfaState state, int byteValue)
    {
        EpsilonClosure closure = epsilonClosure(state, byteValue);
        if (closure.byteRangeInstructionIds.length == 0) {
            return null;
        }

        int[] nextInstructionIds = new int[closure.byteRangeInstructionIds.length];
        int nextInstructionCount = 0;
        for (int instructionId : closure.byteRangeInstructionIds) {
            Inst instruction = inst(instructionId);
            if (instruction.matches(byteValue)) {
                nextInstructionIds[nextInstructionCount++] = instruction.out();
            }
        }
        if (nextInstructionCount == 0) {
            return null;
        }

        nextInstructionIds = uniqueSorted(nextInstructionIds, nextInstructionCount);
        // Match is delayed by one byte, so this flag represents a match ending immediately
        // before the byte currently being processed.
        return new NextStep(nextInstructionIds, byteValue & 0xFF, closure.match);
    }

    private EpsilonClosure epsilonClosure(DfaState state, int nextByte)
    {
        // Evaluate empty-width operations at the boundary around nextByte.
        int emptyFlags = state.needsContext ? emptyFlags(state.previousByte, nextByte) : 0;

        boolean[] visited = new boolean[size()];
        int[] stack = new int[Math.max(16, state.instructionIds.length)];
        int stackPointer = 0;
        for (int instructionId : state.instructionIds) {
            if (instructionId <= 0 || instructionId >= size()) {
                // Instruction zero is FAIL; out-of-range values are treated as failures.
                continue;
            }
            stack = ensureCapacity(stack, stackPointer + 1);
            stack[stackPointer++] = instructionId;
        }

        int[] byteRangeInstructionIds = new int[16];
        int byteRangeCount = 0;
        boolean match = false;

        while (stackPointer != 0) {
            int instructionId = stack[--stackPointer];
            if (instructionId <= 0 || instructionId >= size()) {
                continue;
            }
            if (visited[instructionId]) {
                continue;
            }
            visited[instructionId] = true;

            Inst instruction = inst(instructionId);
            switch (instruction.opcode()) {
                case ALT, ALT_MATCH -> {
                    stack = ensureCapacity(stack, stackPointer + 2);
                    stack[stackPointer++] = instruction.out();
                    stack[stackPointer++] = instruction.out1();
                    if (!instruction.last()) {
                        stack = ensureCapacity(stack, stackPointer + 1);
                        stack[stackPointer++] = instructionId + 1;
                    }
                }
                case NOP, CAPTURE -> {
                    stack = ensureCapacity(stack, stackPointer + 1);
                    stack[stackPointer++] = instruction.out();
                    if (!instruction.last()) {
                        stack = ensureCapacity(stack, stackPointer + 1);
                        stack[stackPointer++] = instructionId + 1;
                    }
                }
                case EMPTY_WIDTH -> {
                    if ((instruction.empty() & emptyFlags) == instruction.empty()) {
                        stack = ensureCapacity(stack, stackPointer + 1);
                        stack[stackPointer++] = instruction.out();
                    }
                    if (!instruction.last()) {
                        stack = ensureCapacity(stack, stackPointer + 1);
                        stack[stackPointer++] = instructionId + 1;
                    }
                }
                case BYTE_RANGE -> {
                    if (byteRangeCount == byteRangeInstructionIds.length) {
                        int[] expandedInstructionIds = new int[byteRangeInstructionIds.length * 2];
                        System.arraycopy(byteRangeInstructionIds, 0, expandedInstructionIds, 0, byteRangeInstructionIds.length);
                        byteRangeInstructionIds = expandedInstructionIds;
                    }
                    byteRangeInstructionIds[byteRangeCount++] = instructionId;
                    if (!instruction.last()) {
                        stack = ensureCapacity(stack, stackPointer + 1);
                        stack[stackPointer++] = instructionId + 1;
                    }
                }
                case MATCH -> {
                    match = true;
                    if (!instruction.last()) {
                        stack = ensureCapacity(stack, stackPointer + 1);
                        stack[stackPointer++] = instructionId + 1;
                    }
                }
                case FAIL -> {
                    if (!instruction.last()) {
                        stack = ensureCapacity(stack, stackPointer + 1);
                        stack[stackPointer++] = instructionId + 1;
                    }
                }
            }
        }

        return new EpsilonClosure(copyOfInts(byteRangeInstructionIds, byteRangeCount), match);
    }

    private static int[] ensureCapacity(int[] stack, int requiredCapacity)
    {
        if (requiredCapacity <= stack.length) {
            return stack;
        }
        int newLength = stack.length;
        while (newLength < requiredCapacity) {
            newLength *= 2;
        }
        int[] expandedStack = new int[newLength];
        System.arraycopy(stack, 0, expandedStack, 0, stack.length);
        return expandedStack;
    }

    private static int[] uniqueSorted(int[] values, int count)
    {
        java.util.Arrays.sort(values, 0, count);
        int writeIndex = 0;
        int lastValue = Integer.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            int value = values[i];
            if (value != lastValue) {
                values[writeIndex++] = value;
                lastValue = value;
            }
        }
        if (writeIndex == count) {
            int[] result = new int[count];
            System.arraycopy(values, 0, result, 0, count);
            return result;
        }
        int[] result = new int[writeIndex];
        System.arraycopy(values, 0, result, 0, writeIndex);
        return result;
    }

    private static int emptyFlags(int previousByte, int nextByte)
    {
        boolean atBeginText = previousByte < 0;
        boolean atEndText = nextByte < 0;

        boolean atBeginLine = atBeginText || previousByte == '\n';
        boolean atEndLine = atEndText || nextByte == '\n';

        boolean previousIsWord = !atBeginText && isWordChar(previousByte);
        boolean nextIsWord = !atEndText && isWordChar(nextByte);
        boolean boundary = previousIsWord != nextIsWord;

        int flags = 0;
        if (atBeginText) {
            flags |= EmptyOp.EMPTY_BEGIN_TEXT;
        }
        if (atEndText) {
            flags |= EmptyOp.EMPTY_END_TEXT;
        }
        if (atBeginLine) {
            flags |= EmptyOp.EMPTY_BEGIN_LINE;
        }
        if (atEndLine) {
            flags |= EmptyOp.EMPTY_END_LINE;
        }
        if (boundary) {
            flags |= EmptyOp.EMPTY_WORD_BOUNDARY;
        }
        else {
            flags |= EmptyOp.EMPTY_NO_WORD_BOUNDARY;
        }
        return flags;
    }

    private static int prefixSuccessor(byte[] buffer, int length)
    {
        int i = length - 1;
        while (i >= 0 && (buffer[i] & 0xFF) == 0xFF) {
            i--;
        }
        if (i < 0) {
            return 0;
        }
        buffer[i] = (byte) ((buffer[i] & 0xFF) + 1);
        return i + 1;
    }

    private record EpsilonClosure(int[] byteRangeInstructionIds, boolean match) {}

    private record NextStep(int[] instructionIds, int previousByte, boolean match) {}

    private static final class DfaState
    {
        final int[] instructionIds;
        final int previousByte; // -1 means beginning-of-text
        final boolean match;
        final boolean needsContext;

        DfaState(int[] instructionIds, int previousByte, boolean match, boolean needsContext)
        {
            this.instructionIds = requireNonNull(instructionIds, "instructionIds is null");
            this.previousByte = previousByte;
            this.match = match;
            this.needsContext = needsContext;
        }

        boolean hasInstructions()
        {
            return instructionIds.length != 0;
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) {
                return true;
            }
            if (!(object instanceof DfaState other)) {
                return false;
            }
            if (!needsContext && !other.needsContext) {
                return match == other.match && java.util.Arrays.equals(instructionIds, other.instructionIds);
            }
            return needsContext == other.needsContext &&
                    previousByte == other.previousByte &&
                    match == other.match &&
                    java.util.Arrays.equals(instructionIds, other.instructionIds);
        }

        @Override
        public int hashCode()
        {
            int hash = java.util.Arrays.hashCode(instructionIds);
            hash = 31 * hash + (match ? 1 : 0);
            if (needsContext) {
                hash = 31 * hash + previousByte;
                hash = 31 * hash + 1;
            }
            return hash;
        }
    }

    private boolean stateNeedsContext(int[] instructionIds)
    {
        // Context flags matter when the state has an empty-width instruction waiting to execute.
        boolean[] visited = new boolean[size()];
        int[] stack = new int[Math.max(16, instructionIds.length)];
        int stackPointer = 0;
        for (int instructionId : instructionIds) {
            if (instructionId <= 0 || instructionId >= size()) {
                continue;
            }
            stack = ensureCapacity(stack, stackPointer + 1);
            stack[stackPointer++] = instructionId;
        }

        while (stackPointer != 0) {
            int instructionId = stack[--stackPointer];
            if (instructionId <= 0 || instructionId >= size()) {
                continue;
            }
            if (visited[instructionId]) {
                continue;
            }
            visited[instructionId] = true;

            Inst instruction = inst(instructionId);
            if (instruction.opcode() == InstOp.EMPTY_WIDTH) {
                return true;
            }

            // Follow epsilon transitions irrespective of whether EMPTY_WIDTH would pass;
            // we're only interested in whether the instruction is present.
            switch (instruction.opcode()) {
                case ALT, ALT_MATCH -> {
                    stack = ensureCapacity(stack, stackPointer + 2);
                    stack[stackPointer++] = instruction.out();
                    stack[stackPointer++] = instruction.out1();
                }
                case NOP, CAPTURE -> {
                    stack = ensureCapacity(stack, stackPointer + 1);
                    stack[stackPointer++] = instruction.out();
                }
                case BYTE_RANGE, MATCH, FAIL, EMPTY_WIDTH -> {
                    // no-op
                }
            }

            if (!instruction.last()) {
                stack = ensureCapacity(stack, stackPointer + 1);
                stack[stackPointer++] = instructionId + 1;
            }
        }
        return false;
    }

    private int[] normalizeStartInstructions(int startInstructionId, int initialFlags)
    {
        // Build the initial work queue by following epsilon edges that are valid under the
        // initial begin-text and begin-line flags. This consumes anchors such as \A at the
        // starting position, matching AnalyzeSearchHelper/AddToQueue.
        if (startInstructionId <= 0 || startInstructionId >= size()) {
            return new int[] {startInstructionId};
        }

        boolean[] visited = new boolean[size()];
        int[] stack = new int[32];
        int stackPointer = 0;
        stack[stackPointer++] = startInstructionId;

        int[] normalizedInstructionIds = new int[32];
        int normalizedInstructionCount = 0;

        while (stackPointer != 0) {
            int instructionId = stack[--stackPointer];
            if (instructionId <= 0 || instructionId >= size()) {
                continue;
            }
            if (visited[instructionId]) {
                continue;
            }
            visited[instructionId] = true;

            Inst instruction = inst(instructionId);

            switch (instruction.opcode()) {
                case ALT, ALT_MATCH -> {
                    stack = ensureCapacity(stack, stackPointer + 2);
                    stack[stackPointer++] = instruction.out();
                    stack[stackPointer++] = instruction.out1();
                    if (!instruction.last()) {
                        stack = ensureCapacity(stack, stackPointer + 1);
                        stack[stackPointer++] = instructionId + 1;
                    }
                }
                case NOP, CAPTURE -> {
                    stack = ensureCapacity(stack, stackPointer + 1);
                    stack[stackPointer++] = instruction.out();
                    if (!instruction.last()) {
                        stack = ensureCapacity(stack, stackPointer + 1);
                        stack[stackPointer++] = instructionId + 1;
                    }
                }
                case EMPTY_WIDTH -> {
                    // Only "consume" EMPTY_WIDTH if it is satisfied by the initial flags.
                    if ((instruction.empty() & ~initialFlags) == 0) {
                        stack = ensureCapacity(stack, stackPointer + 1);
                        stack[stackPointer++] = instruction.out();
                    }
                    else {
                        normalizedInstructionIds = ensureCapacity(normalizedInstructionIds, normalizedInstructionCount + 1);
                        normalizedInstructionIds[normalizedInstructionCount++] = instructionId;
                    }
                    if (!instruction.last()) {
                        stack = ensureCapacity(stack, stackPointer + 1);
                        stack[stackPointer++] = instructionId + 1;
                    }
                }
                case BYTE_RANGE, MATCH, FAIL -> {
                    normalizedInstructionIds = ensureCapacity(normalizedInstructionIds, normalizedInstructionCount + 1);
                    normalizedInstructionIds[normalizedInstructionCount++] = instructionId;
                }
            }
        }

        return uniqueSorted(normalizedInstructionIds, normalizedInstructionCount);
    }

    private static final class StateVisitMap
    {
        private final java.util.HashMap<DfaState, Integer> visitCounts = new java.util.HashMap<>();

        int get(DfaState key)
        {
            Integer count = visitCounts.get(key);
            return count == null ? 0 : count;
        }

        void increment(DfaState key)
        {
            visitCounts.put(key, get(key) + 1);
        }

        void clear()
        {
            visitCounts.clear();
        }
    }

    public void optimize()
    {
        SparseSet queue = new SparseSet(size() + 1);

        // Eliminate nops. Most are taken out during compilation
        // but a few are hard to avoid.
        queue.clear();
        addToQueue(queue, start);
        for (int queueIndex = 0; queueIndex < queue.size(); queueIndex++) {
            int id = queue.denseAt(queueIndex);

            Inst instruction = inst(id);
            int outputId = instruction.out();
            while (outputId != 0 && inst(outputId).opcode() == InstOp.NOP) {
                outputId = inst(outputId).out();
            }
            instruction.setOut(outputId);
            addToQueue(queue, instruction.out());

            if (instruction.opcode() == InstOp.ALT) {
                int alternateOutputId = instruction.out1();
                while (alternateOutputId != 0 && inst(alternateOutputId).opcode() == InstOp.NOP) {
                    alternateOutputId = inst(alternateOutputId).out();
                }
                instruction.setOut1(alternateOutputId);
                addToQueue(queue, instruction.out1());
            }
        }

        // Insert ALT_MATCH instructions.
        queue.clear();
        addToQueue(queue, start);
        for (int queueIndex = 0; queueIndex < queue.size(); queueIndex++) {
            int id = queue.denseAt(queueIndex);
            Inst instruction = inst(id);
            addToQueue(queue, instruction.out());
            if (instruction.opcode() == InstOp.ALT) {
                addToQueue(queue, instruction.out1());
            }

            if (instruction.opcode() == InstOp.ALT) {
                Inst firstBranch = inst(instruction.out());
                Inst secondBranch = inst(instruction.out1());
                if (firstBranch.opcode() == InstOp.BYTE_RANGE && firstBranch.out() == id &&
                        firstBranch.lo() == 0x00 && firstBranch.hi() == 0xFF &&
                        isMatch(secondBranch)) {
                    instruction.setOpcode(InstOp.ALT_MATCH);
                    continue;
                }
                if (isMatch(firstBranch) &&
                        secondBranch.opcode() == InstOp.BYTE_RANGE && secondBranch.out() == id &&
                        secondBranch.lo() == 0x00 && secondBranch.hi() == 0xFF) {
                    instruction.setOpcode(InstOp.ALT_MATCH);
                }
            }
        }
    }

    public void flatten()
    {
        if (didFlatten) {
            return;
        }
        didFlatten = true;

        // Scratch structures reused by helper routines to avoid heap churn.
        SparseSet reachable = new SparseSet(size());
        IntStack stack = new IntStack(size());

        // First pass: mark successor roots and predecessors.
        SparseIntArray rootIdsByInstruction = new SparseIntArray(size());
        SparseIntArray predecessorListIdsByInstruction = new SparseIntArray(size());
        IntListTable predecessorLists = new IntListTable(size());
        markSuccessors(rootIdsByInstruction, predecessorListIdsByInstruction, predecessorLists, reachable, stack);

        // Second pass: mark dominator roots.
        SparseIntArray sortedRoots = new SparseIntArray(rootIdsByInstruction);
        sortedRoots.sortByIndex();
        for (int rootIndex = sortedRoots.size() - 1; rootIndex > 0; rootIndex--) {
            int id = sortedRoots.denseIndexAt(rootIndex);
            if (id != startUnanchored() && id != start()) {
                markDominator(id, rootIdsByInstruction, predecessorListIdsByInstruction, predecessorLists, reachable, stack);
            }
        }

        // Third pass: emit lists and map each root to its flattened instruction index.
        int[] flattenedIndexesByRoot = new int[rootIdsByInstruction.size()];
        List<Inst> flattenedInstructions = new ArrayList<>(size());
        for (int rootIndex = 0; rootIndex < rootIdsByInstruction.size(); rootIndex++) {
            int rootInstructionId = rootIdsByInstruction.denseIndexAt(rootIndex);
            int rootId = rootIdsByInstruction.denseValueAt(rootIndex);
            flattenedIndexesByRoot[rootId] = flattenedInstructions.size();
            emitList(rootInstructionId, rootIdsByInstruction, flattenedInstructions, reachable, stack);
            flattenedInstructions.getLast().setLast();
            computeHints(flattenedInstructions, flattenedIndexesByRoot[rootId], flattenedInstructions.size());
        }

        // Reset instruction counts before fourth pass.
        java.util.Arrays.fill(instCount, 0);

        // Fourth pass: remap outputs to flattened indexes and count instructions by opcode.
        for (int id = 0; id < flattenedInstructions.size(); id++) {
            Inst instruction = flattenedInstructions.get(id);
            if (instruction.opcode() != InstOp.ALT_MATCH) {
                instruction.setOut(flattenedIndexesByRoot[instruction.out()]);
            }
            instCount[instruction.opcode().ordinal()]++;
        }

        // Remap start_unanchored and start.
        if (startUnanchored() == 0) {
            // Leave both as zero.
        }
        else if (startUnanchored() == start()) {
            setStartUnanchored(flattenedIndexesByRoot[1]);
            setStart(flattenedIndexesByRoot[1]);
        }
        else {
            setStartUnanchored(flattenedIndexesByRoot[1]);
            setStart(flattenedIndexesByRoot[2]);
        }

        // Replace old instructions with flattened ones.
        insts.clear();
        insts.addAll(flattenedInstructions);

        // Populate the list-head mapping used by the BitState engine.
        // Upstream limits this to <= 512 instructions to keep the bitmap small.
        listCount = rootIdsByInstruction.size();
        listHeads = null;
        bitStateTextMaxSize = 0;
        if (insts.size() <= 512) {
            short[] heads = new short[insts.size()];
            java.util.Arrays.fill(heads, (short) -1);
            for (int i = 0; i < listCount; i++) {
                int headInstructionIndex = flattenedIndexesByRoot[i];
                if (headInstructionIndex >= 0 && headInstructionIndex < heads.length) {
                    heads[headInstructionIndex] = (short) i;
                }
            }
            listHeads = heads;

            // BitState allocates a bitmap of size listCount * (text.size()+1). The Java server
            // implementation accepts a larger fixed bound than upstream when that avoids three
            // complete capture passes over a long input.
            long maximumBitmapBits = BIT_STATE_BITMAP_MAXIMUM_BYTES * Byte.SIZE;
            bitStateTextMaxSize = (int) (maximumBitmapBits / Math.max(1, listCount) - 1);
        }
    }

    // The base action encoding remains compatible with RE2's onepass.cc layout.
    private static final int ONEPASS_INDEX_SHIFT = 16; // bits below next-state index
    private static final int ONEPASS_EMPTY_SHIFT = 6;  // number of empty flags (kEmptyAllFlags is (1<<6)-1)
    private static final int ONEPASS_REAL_CAP_SHIFT = ONEPASS_EMPTY_SHIFT + 1;
    private static final int ONEPASS_REAL_MAX_CAP = ((ONEPASS_INDEX_SHIFT - ONEPASS_REAL_CAP_SHIFT) / 2) * 2;
    private static final int ONEPASS_CAP_SHIFT = ONEPASS_REAL_CAP_SHIFT - 2; // skip cap[0], cap[1]
    private static final int ONEPASS_MAX_CAP = ONEPASS_REAL_MAX_CAP + 2;

    private static final int ONEPASS_MATCH_WINS = 1 << ONEPASS_EMPTY_SHIFT;
    private static final int ONEPASS_CAP_MASK = ((1 << ONEPASS_REAL_MAX_CAP) - 1) << ONEPASS_REAL_CAP_SHIFT;
    private static final int ONEPASS_EMPTY_ALL_FLAGS = (1 << ONEPASS_EMPTY_SHIFT) - 1;
    private static final int ONEPASS_IMPOSSIBLE = EmptyOp.EMPTY_WORD_BOUNDARY | EmptyOp.EMPTY_NO_WORD_BOUNDARY;

    private boolean computeOnePass()
    {
        // This computation assumes the program is in flattened list form.
        flatten();
        if (!didFlatten) {
            return false;
        }
        if (start() == 0) {
            return false;
        }
        if (bytemapRange() <= 0) {
            return false;
        }

        int byteRangeCount = 0;
        int captureCount = 0;
        int maximumCaptureSlot = -1;
        int emptyWidthCount = 0;
        int noOpCount = 0;
        for (int id = 0; id < size(); id++) {
            switch (inst(id).opcode()) {
                case BYTE_RANGE -> byteRangeCount++;
                case CAPTURE -> {
                    captureCount++;
                    maximumCaptureSlot = Math.max(maximumCaptureSlot, inst(id).cap());
                }
                case EMPTY_WIDTH -> emptyWidthCount++;
                case NOP -> noOpCount++;
                default -> {}
            }
        }

        // Match upstream limits: reserve at most one quarter of the DFA budget and avoid
        // overflowing the 16-bit node index used by the OnePass action encoding.
        int maximumNodeCount = 2 + byteRangeCount;
        boolean hasExtendedCaptures = maximumCaptureSlot >= ONEPASS_MAX_CAP && maximumCaptureSlot < Long.SIZE;
        long stateSize = Integer.BYTES + ((long) bytemapRange() * Integer.BYTES);
        if (hasExtendedCaptures) {
            stateSize += Long.BYTES + ((long) bytemapRange() * Long.BYTES);
        }
        if (maximumNodeCount >= 65000 || dfaMemory / 4 / stateSize < maximumNodeCount) {
            return false;
        }

        int maximumStackSize = captureCount + emptyWidthCount + noOpCount + 1; // +1 for start instruction
        int[] stackInstructionIds = new int[Math.max(8, maximumStackSize)];
        int[] stackConditions = new int[Math.max(8, maximumStackSize)];
        long[] stackCaptureMasks = hasExtendedCaptures ? new long[Math.max(8, maximumStackSize)] : null;

        int[] nodeById = new int[size()];
        java.util.Arrays.fill(nodeById, -1);

        int byteClassCount = bytemapRange();
        int[] nodeMatchCondition = new int[maximumNodeCount];
        int[] nodeAction = new int[maximumNodeCount * byteClassCount];
        long[] nodeMatchCapture = hasExtendedCaptures ? new long[maximumNodeCount] : null;
        long[] nodeActionCapture = hasExtendedCaptures ? new long[maximumNodeCount * byteClassCount] : null;

        SparseSet toVisit = new SparseSet(size());
        SparseSet workQueue = new SparseSet(size());

        addOnePassQueue(toVisit, start());
        nodeById[start()] = 0;

        int allocatedNodeCount = 1;
        for (int visitIndex = 0; visitIndex < toVisit.size(); visitIndex++) {
            int startInstructionId = toVisit.denseAt(visitIndex);
            int nodeIndex = nodeById[startInstructionId];
            if (nodeIndex < 0) {
                continue;
            }

            int actionBase = nodeIndex * byteClassCount;
            java.util.Arrays.fill(nodeAction, actionBase, actionBase + byteClassCount, ONEPASS_IMPOSSIBLE);
            nodeMatchCondition[nodeIndex] = ONEPASS_IMPOSSIBLE;

            workQueue.clear();
            boolean foundMatch = false;
            int stackSize = 0;
            stackInstructionIds[stackSize] = startInstructionId;
            stackConditions[stackSize] = 0;
            if (hasExtendedCaptures) {
                stackCaptureMasks[stackSize] = 0;
            }
            stackSize++;

            while (stackSize > 0) {
                stackSize--;
                int id = stackInstructionIds[stackSize];
                int condition = stackConditions[stackSize];
                long captureMask = hasExtendedCaptures ? stackCaptureMasks[stackSize] : 0;

                while (id != 0) {
                    Inst instruction = inst(id);
                    switch (instruction.opcode()) {
                        case ALT_MATCH -> {
                            // Upstream ignores ALT_MATCH's subtle semantics in IsOnePass; we do the same.
                            if (instruction.last()) {
                                return false;
                            }
                            if (!addOnePassQueue(workQueue, id + 1)) {
                                return false;
                            }
                            id = id + 1;
                        }

                        case BYTE_RANGE -> {
                            int nextInstructionId = instruction.out();
                            int nextIndex = nodeById[nextInstructionId];
                            if (nextIndex == -1) {
                                if (allocatedNodeCount >= maximumNodeCount) {
                                    return false;
                                }
                                nextIndex = allocatedNodeCount++;
                                addOnePassQueue(toVisit, nextInstructionId);
                                nodeById[nextInstructionId] = nextIndex;
                            }

                            // Fill each action for the byte classes touched by this instruction.
                            int lo = instruction.lo();
                            int hi = instruction.hi();
                            for (int c = lo; c <= hi; c++) {
                                int byteClass = bytemap(c);
                                while (c < 255 && bytemap(c + 1) == byteClass) {
                                    c++;
                                }
                                int action = nodeAction[actionBase + byteClass];
                                int newAction = (nextIndex << ONEPASS_INDEX_SHIFT) | condition;
                                if (foundMatch) {
                                    newAction |= ONEPASS_MATCH_WINS;
                                }
                                if (action == ONEPASS_IMPOSSIBLE) {
                                    nodeAction[actionBase + byteClass] = newAction;
                                    if (hasExtendedCaptures) {
                                        nodeActionCapture[actionBase + byteClass] = captureMask;
                                    }
                                }
                                else if (action != newAction ||
                                        (hasExtendedCaptures && nodeActionCapture[actionBase + byteClass] != captureMask)) {
                                    return false;
                                }
                            }

                            if (instruction.foldCase()) {
                                int uppercaseLow = Math.max(lo, 'a') + ('A' - 'a');
                                int uppercaseHigh = Math.min(hi, 'z') + ('A' - 'a');
                                for (int c = uppercaseLow; c <= uppercaseHigh; c++) {
                                    int byteClass = bytemap(c);
                                    while (c < 255 && bytemap(c + 1) == byteClass) {
                                        c++;
                                    }
                                    int action = nodeAction[actionBase + byteClass];
                                    int newAction = (nextIndex << ONEPASS_INDEX_SHIFT) | condition;
                                    if (foundMatch) {
                                        newAction |= ONEPASS_MATCH_WINS;
                                    }
                                    if (action == ONEPASS_IMPOSSIBLE) {
                                        nodeAction[actionBase + byteClass] = newAction;
                                        if (hasExtendedCaptures) {
                                            nodeActionCapture[actionBase + byteClass] = captureMask;
                                        }
                                    }
                                    else if (action != newAction ||
                                            (hasExtendedCaptures && nodeActionCapture[actionBase + byteClass] != captureMask)) {
                                        return false;
                                    }
                                }
                            }

                            if (instruction.last()) {
                                id = 0;
                                break;
                            }
                            if (!addOnePassQueue(workQueue, id + 1)) {
                                return false;
                            }
                            id = id + 1;
                        }

                        case CAPTURE, EMPTY_WIDTH, NOP -> {
                            if (!instruction.last()) {
                                if (!addOnePassQueue(workQueue, id + 1)) {
                                    return false;
                                }
                                stackInstructionIds[stackSize] = id + 1;
                                stackConditions[stackSize] = condition;
                                if (hasExtendedCaptures) {
                                    stackCaptureMasks[stackSize] = captureMask;
                                }
                                stackSize++;
                            }

                            if (instruction.opcode() == InstOp.CAPTURE && instruction.cap() < ONEPASS_MAX_CAP) {
                                condition |= 1 << (ONEPASS_CAP_SHIFT + instruction.cap());
                            }
                            if (hasExtendedCaptures && instruction.opcode() == InstOp.CAPTURE) {
                                captureMask |= 1L << instruction.cap();
                            }
                            if (instruction.opcode() == InstOp.EMPTY_WIDTH) {
                                condition |= instruction.empty();
                            }

                            if (!addOnePassQueue(workQueue, instruction.out())) {
                                return false;
                            }
                            id = instruction.out();
                        }

                        case MATCH -> {
                            if (foundMatch) {
                                return false;
                            }
                            foundMatch = true;
                            nodeMatchCondition[nodeIndex] = condition;
                            if (hasExtendedCaptures) {
                                nodeMatchCapture[nodeIndex] = captureMask;
                            }

                            if (instruction.last()) {
                                id = 0;
                                break;
                            }
                            if (!addOnePassQueue(workQueue, id + 1)) {
                                return false;
                            }
                            id = id + 1;
                        }

                        case FAIL -> id = 0;

                        case ALT -> {
                            // ALT instructions do not appear in flattened list form.
                            return false;
                        }
                    }
                }
            }
        }

        onePassStateCount = allocatedNodeCount;
        onePassMatchCond = java.util.Arrays.copyOf(nodeMatchCondition, allocatedNodeCount);
        onePassAction = java.util.Arrays.copyOf(nodeAction, allocatedNodeCount * byteClassCount);
        if (hasExtendedCaptures) {
            onePassMatchCapture = java.util.Arrays.copyOf(nodeMatchCapture, allocatedNodeCount);
            onePassActionCapture = java.util.Arrays.copyOf(nodeActionCapture, allocatedNodeCount * byteClassCount);
        }
        dfaMemory -= allocatedNodeCount * stateSize;
        return true;
    }

    private static boolean addOnePassQueue(SparseSet queue, int id)
    {
        if (id == 0) {
            return true;
        }
        if (queue.contains(id)) {
            return false;
        }
        queue.insertNew(id);
        return true;
    }

    private static boolean isWordChar(int c)
    {
        int b = c & 0xFF;
        return ('A' <= b && b <= 'Z') ||
                ('a' <= b && b <= 'z') ||
                ('0' <= b && b <= '9') ||
                b == '_';
    }

    private static void markRanges(ByteMapBuilder builder, int[] ranges)
    {
        for (int i = 0; i < ranges.length; i += 2) {
            builder.mark(ranges[i], ranges[i + 1]);
        }
    }

    // Computes the empty-width assertions at a position in the logical Slice.
    public static int emptyFlags(Slice text, int p)
    {
        requireNonNull(text, "text is null");

        int begin = text.byteArrayOffset();
        int end = begin + text.length();
        if (p < begin || p > end) {
            throw new IllegalArgumentException("p out of range for text slice: p=" + p + " begin=" + begin + " end=" + end);
        }

        byte[] bytes = text.byteArray();
        int flags = 0;

        // ^ and \A
        if (p == begin) {
            flags |= EmptyOp.EMPTY_BEGIN_TEXT | EmptyOp.EMPTY_BEGIN_LINE;
        }
        else if (bytes[p - 1] == '\n') {
            flags |= EmptyOp.EMPTY_BEGIN_LINE;
        }

        // $ and \z
        if (p == end) {
            flags |= EmptyOp.EMPTY_END_TEXT | EmptyOp.EMPTY_END_LINE;
        }
        else if (p < end && bytes[p] == '\n') {
            flags |= EmptyOp.EMPTY_END_LINE;
        }

        // \b and \B
        boolean boundary = false;
        if (p == begin && p == end) {
            boundary = false;
        }
        else if (p == begin) {
            if (isWordChar(bytes[p] & 0xFF)) {
                boundary = true;
            }
        }
        else if (p == end) {
            if (isWordChar(bytes[p - 1] & 0xFF)) {
                boundary = true;
            }
        }
        else {
            if (isWordChar(bytes[p - 1] & 0xFF) != isWordChar(bytes[p] & 0xFF)) {
                boundary = true;
            }
        }

        if (boundary) {
            flags |= EmptyOp.EMPTY_WORD_BOUNDARY;
        }
        else {
            flags |= EmptyOp.EMPTY_NO_WORD_BOUNDARY;
        }

        return flags;
    }

    private boolean isMatch(Inst instruction)
    {
        while (true) {
            switch (instruction.opcode()) {
                case ALT, ALT_MATCH, BYTE_RANGE, FAIL, EMPTY_WIDTH -> {
                    return false;
                }
                case CAPTURE, NOP -> instruction = inst(instruction.out());
                case MATCH -> {
                    return true;
                }
            }
        }
    }

    private void markSuccessors(SparseIntArray rootIdsByInstruction,
            SparseIntArray predecessorListIdsByInstruction,
            IntListTable predecessorLists,
            SparseSet reachable,
            IntStack stack)
    {
        // Mark the FAIL instruction.
        rootIdsByInstruction.setNew(0, rootIdsByInstruction.size());

        // Mark the start_unanchored and start instructions.
        if (!rootIdsByInstruction.hasIndex(startUnanchored())) {
            rootIdsByInstruction.setNew(startUnanchored(), rootIdsByInstruction.size());
        }
        if (!rootIdsByInstruction.hasIndex(start())) {
            rootIdsByInstruction.setNew(start(), rootIdsByInstruction.size());
        }

        reachable.clear();
        stack.clear();
        stack.push(startUnanchored());
        while (!stack.isEmpty()) {
            int id = stack.pop();
            while (true) {
                if (reachable.contains(id)) {
                    break;
                }
                reachable.insertNew(id);

                Inst instruction = inst(id);
                switch (instruction.opcode()) {
                    case ALT_MATCH, ALT -> {
                        // Mark this instruction as a predecessor of each out.
                        addPredecessor(predecessorListIdsByInstruction, predecessorLists, id, instruction.out());
                        addPredecessor(predecessorListIdsByInstruction, predecessorLists, id, instruction.out1());
                        stack.push(instruction.out1());
                        id = instruction.out();
                    }
                    case BYTE_RANGE, CAPTURE, EMPTY_WIDTH -> {
                        // Mark the out of this instruction as a "root".
                        if (!rootIdsByInstruction.hasIndex(instruction.out())) {
                            rootIdsByInstruction.setNew(instruction.out(), rootIdsByInstruction.size());
                        }
                        id = instruction.out();
                    }
                    case NOP -> id = instruction.out();
                    case MATCH, FAIL -> {
                        id = 0;
                    }
                }
                if (id == 0) {
                    break;
                }
            }
        }
    }

    private static void addPredecessor(
            SparseIntArray predecessorListIdsByInstruction,
            IntListTable predecessorLists,
            int predecessorInstructionId,
            int outputInstructionId)
    {
        if (!predecessorListIdsByInstruction.hasIndex(outputInstructionId)) {
            predecessorListIdsByInstruction.setNew(outputInstructionId, predecessorLists.addBucket());
        }
        predecessorLists.add(
                predecessorListIdsByInstruction.getExisting(outputInstructionId),
                predecessorInstructionId);
    }

    private void markDominator(int rootInstructionId,
            SparseIntArray rootIdsByInstruction,
            SparseIntArray predecessorListIdsByInstruction,
            IntListTable predecessorLists,
            SparseSet reachable,
            IntStack stack)
    {
        reachable.clear();
        stack.clear();
        stack.push(rootInstructionId);
        while (!stack.isEmpty()) {
            int id = stack.pop();
            while (true) {
                if (reachable.contains(id)) {
                    break;
                }
                reachable.insertNew(id);

                if (id != rootInstructionId && rootIdsByInstruction.hasIndex(id)) {
                    // We reached another "tree" via epsilon transition.
                    break;
                }

                Inst instruction = inst(id);
                switch (instruction.opcode()) {
                    case ALT_MATCH, ALT -> {
                        stack.push(instruction.out1());
                        id = instruction.out();
                    }
                    case BYTE_RANGE, CAPTURE, EMPTY_WIDTH, MATCH, FAIL -> {
                        id = 0;
                    }
                    case NOP -> id = instruction.out();
                }
                if (id == 0) {
                    break;
                }
            }
        }

        for (int reachableIndex = 0; reachableIndex < reachable.size(); reachableIndex++) {
            int id = reachable.denseAt(reachableIndex);
            if (predecessorListIdsByInstruction.hasIndex(id)) {
                int predecessorListId = predecessorListIdsByInstruction.getExisting(id);
                for (int predecessorIndex = 0; predecessorIndex < predecessorLists.size(predecessorListId); predecessorIndex++) {
                    int predecessorInstructionId = predecessorLists.get(predecessorListId, predecessorIndex);
                    if (!reachable.contains(predecessorInstructionId)) {
                        // id has a predecessor that cannot be reached from root!
                        // Therefore, id must be a "root" too - mark it as such.
                        if (!rootIdsByInstruction.hasIndex(id)) {
                            rootIdsByInstruction.setNew(id, rootIdsByInstruction.size());
                        }
                    }
                }
            }
        }
    }

    private void emitList(int rootInstructionId,
            SparseIntArray rootIdsByInstruction,
            List<Inst> flattenedInstructions,
            SparseSet reachable,
            IntStack stack)
    {
        reachable.clear();
        stack.clear();
        stack.push(rootInstructionId);
        while (!stack.isEmpty()) {
            int id = stack.pop();
            while (true) {
                if (reachable.contains(id)) {
                    break;
                }
                reachable.insertNew(id);

                if (id != rootInstructionId && rootIdsByInstruction.hasIndex(id)) {
                    // We reached another "tree" via epsilon transition. Emit a NOP
                    // instruction so that the Prog does not become quadratically larger.
                    Inst noOp = new Inst(InstOp.NOP);
                    noOp.setOut(rootIdsByInstruction.getExisting(id));
                    flattenedInstructions.add(noOp);
                    break;
                }

                Inst instruction = inst(id);
                switch (instruction.opcode()) {
                    case ALT_MATCH -> {
                        Inst altMatch = new Inst(InstOp.ALT_MATCH);
                        flattenedInstructions.add(altMatch);
                        int next = flattenedInstructions.size();
                        altMatch.setOut(next);
                        altMatch.setOut1(next + 1);
                        // fall through to ALT handling
                        stack.push(instruction.out1());
                        id = instruction.out();
                    }
                    case ALT -> {
                        stack.push(instruction.out1());
                        id = instruction.out();
                    }
                    case BYTE_RANGE, CAPTURE, EMPTY_WIDTH -> {
                        Inst copy = instruction.copy();
                        copy.setOut(rootIdsByInstruction.getExisting(instruction.out()));
                        flattenedInstructions.add(copy);
                        id = 0;
                    }
                    case NOP -> id = instruction.out();
                    case MATCH, FAIL -> {
                        flattenedInstructions.add(instruction.copy());
                        id = 0;
                    }
                }
                if (id == 0) {
                    break;
                }
            }
        }
    }

    private static void computeHints(List<Inst> flattenedInstructions, int begin, int end)
    {
        Bitmap256 splits = new Bitmap256();
        int[] colors = new int[256];

        boolean dirty = false;
        for (int id = end; id >= begin; id--) {
            if (id == end || flattenedInstructions.get(id).opcode() != InstOp.BYTE_RANGE) {
                if (dirty) {
                    dirty = false;
                    splits.clear();
                }
                splits.set(255);
                colors[255] = id;
                continue;
            }
            dirty = true;

            int first = end;

            Inst instruction = flattenedInstructions.get(id);
            int lo = instruction.lo();
            int hi = instruction.hi();
            first = recolorHints(splits, colors, id, lo, hi, first);
            if (instruction.foldCase() && lo <= 'z' && hi >= 'a') {
                int foldlo = lo;
                int foldhi = hi;
                if (foldlo < 'a') {
                    foldlo = 'a';
                }
                if (foldhi > 'z') {
                    foldhi = 'z';
                }
                if (foldlo <= foldhi) {
                    foldlo += 'A' - 'a';
                    foldhi += 'A' - 'a';
                    first = recolorHints(splits, colors, id, foldlo, foldhi, first);
                }
            }

            if (first != end) {
                int hint = Math.min(first - id, 32767);
                instruction.hintFoldCase |= hint << 1;
            }
        }
    }

    private static int recolorHints(Bitmap256 splits, int[] colors, int id, int lo, int hi, int first)
    {
        // Like ByteMapBuilder, split at lo-1 and at hi.
        lo--;

        if (0 <= lo && !splits.test(lo)) {
            splits.set(lo);
            int next = splits.findNextSetBit(lo + 1);
            colors[lo] = colors[next];
        }
        if (!splits.test(hi)) {
            splits.set(hi);
            int next = splits.findNextSetBit(hi + 1);
            colors[hi] = colors[next];
        }

        int c = lo + 1;
        while (c < 256) {
            int next = splits.findNextSetBit(c);
            first = Math.min(first, colors[next]);
            colors[next] = id;
            if (next == hi) {
                break;
            }
            c = next + 1;
        }
        return first;
    }

    // Builds the equivalence classes used to reduce the DFA alphabet.
    static final class ByteMapBuilder
    {
        private final Bitmap256 splits = new Bitmap256();
        private final int[] colors = new int[256];
        private int nextColor = 257;

        private int[] colorRemap = new int[512];
        private int[] colorRemapGeneration = new int[512];
        private int remapGeneration = 1;

        private int[] rangeLo = new int[16];
        private int[] rangeHi = new int[16];
        private int rangeSize;

        ByteMapBuilder()
        {
            // Initial state: the [0-255] range has color 256.
            // This will avoid problems during the second phase,
            // in which we assign byte classes numbered from 0.
            splits.set(255);
            colors[255] = 256;
        }

        void mark(int lo, int hi)
        {
            if (lo < 0 || lo > 255 || hi < 0 || hi > 255 || lo > hi) {
                throw new IllegalArgumentException("range must be within [0,255]: [" + lo + "," + hi + "]");
            }

            // Ignore any [0-255] ranges.
            if (lo == 0 && hi == 255) {
                return;
            }

            if (rangeSize == rangeLo.length) {
                rangeLo = java.util.Arrays.copyOf(rangeLo, rangeLo.length * 2);
                rangeHi = java.util.Arrays.copyOf(rangeHi, rangeHi.length * 2);
            }
            rangeLo[rangeSize] = lo;
            rangeHi[rangeSize] = hi;
            rangeSize++;
        }

        void merge()
        {
            for (int i = 0; i < rangeSize; i++) {
                int lo = rangeLo[i] - 1;
                int hi = rangeHi[i];

                if (0 <= lo && !splits.test(lo)) {
                    splits.set(lo);
                    int next = splits.findNextSetBit(lo + 1);
                    colors[lo] = colors[next];
                }
                if (!splits.test(hi)) {
                    splits.set(hi);
                    int next = splits.findNextSetBit(hi + 1);
                    colors[hi] = colors[next];
                }

                int c = lo + 1;
                while (c < 256) {
                    int next = splits.findNextSetBit(c);
                    colors[next] = recolor(colors[next]);
                    if (next == hi) {
                        break;
                    }
                    c = next + 1;
                }
            }
            remapGeneration++;
            if (remapGeneration == 0) {
                java.util.Arrays.fill(colorRemapGeneration, 0);
                remapGeneration = 1;
            }
            rangeSize = 0;
        }

        void build(byte[] bytemap, int[] bytemapRangeOut)
        {
            requireNonNull(bytemap, "bytemap is null");
            if (bytemap.length != 256) {
                throw new IllegalArgumentException("bytemap must have length 256");
            }
            requireNonNull(bytemapRangeOut, "bytemapRangeOut is null");
            if (bytemapRangeOut.length != 1) {
                throw new IllegalArgumentException("bytemapRangeOut must have length 1");
            }

            // Assign byte classes numbered from 0.
            nextColor = 0;

            int c = 0;
            while (c < 256) {
                int next = splits.findNextSetBit(c);
                int b = recolor(colors[next]);
                while (c <= next) {
                    bytemap[c] = (byte) b;
                    c++;
                }
            }

            bytemapRangeOut[0] = nextColor;
        }

        private int recolor(int oldColor)
        {
            if (oldColor >= colorRemap.length) {
                int newSize = Math.max(colorRemap.length * 2, oldColor + 1);
                colorRemap = java.util.Arrays.copyOf(colorRemap, newSize);
                colorRemapGeneration = java.util.Arrays.copyOf(colorRemapGeneration, newSize);
            }
            if (colorRemapGeneration[oldColor] == remapGeneration) {
                return colorRemap[oldColor];
            }

            int newColor = nextColor;
            nextColor++;

            if (newColor >= colorRemap.length) {
                int newSize = Math.max(colorRemap.length * 2, newColor + 1);
                colorRemap = java.util.Arrays.copyOf(colorRemap, newSize);
                colorRemapGeneration = java.util.Arrays.copyOf(colorRemapGeneration, newSize);
            }

            colorRemap[oldColor] = newColor;
            colorRemapGeneration[oldColor] = remapGeneration;
            // Preserve the old transitive rule where matching the mapped value
            // also returned that value within the same merge batch.
            colorRemap[newColor] = newColor;
            colorRemapGeneration[newColor] = remapGeneration;
            return newColor;
        }
    }

    private static final class IntStack
    {
        private int[] values;
        private int size;

        IntStack(int capacity)
        {
            values = new int[Math.max(1, capacity)];
        }

        void clear()
        {
            size = 0;
        }

        boolean isEmpty()
        {
            return size == 0;
        }

        void push(int v)
        {
            if (size >= values.length) {
                values = java.util.Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = v;
        }

        int pop()
        {
            return values[--size];
        }
    }

    private static final class IntListTable
    {
        private int[][] values;
        private int[] sizes;
        private int count;

        IntListTable(int capacity)
        {
            int cap = Math.max(4, capacity);
            values = new int[cap][];
            sizes = new int[cap];
        }

        int addBucket()
        {
            if (count == values.length) {
                int newSize = values.length * 2;
                values = java.util.Arrays.copyOf(values, newSize);
                sizes = java.util.Arrays.copyOf(sizes, newSize);
            }
            values[count] = new int[4];
            sizes[count] = 0;
            return count++;
        }

        void add(int bucket, int value)
        {
            int[] bucketValues = values[bucket];
            int size = sizes[bucket];
            if (size == bucketValues.length) {
                bucketValues = java.util.Arrays.copyOf(bucketValues, bucketValues.length * 2);
                values[bucket] = bucketValues;
            }
            bucketValues[size] = value;
            sizes[bucket] = size + 1;
        }

        int size(int bucket)
        {
            return sizes[bucket];
        }

        int get(int bucket, int index)
        {
            return values[bucket][index];
        }
    }

    private String progToString(SparseSet queue)
    {
        StringBuilder builder = new StringBuilder();
        for (int queueIndex = 0; queueIndex < queue.size(); queueIndex++) {
            int id = queue.denseAt(queueIndex);
            Inst instruction = inst(id);
            builder.append(id).append(". ").append(instruction.dump()).append('\n');

            addToQueue(queue, instruction.out());
            if (instruction.opcode() == InstOp.ALT || instruction.opcode() == InstOp.ALT_MATCH) {
                addToQueue(queue, instruction.out1());
            }
        }
        return builder.toString();
    }

    private String flattenedProgToString(int start)
    {
        StringBuilder builder = new StringBuilder();
        for (int id = start; id < size(); id++) {
            Inst instruction = inst(id);
            if (instruction.last()) {
                builder.append(id).append(". ").append(instruction.dump()).append('\n');
            }
            else {
                builder.append(id).append("+ ").append(instruction.dump()).append('\n');
            }
        }
        return builder.toString();
    }

    private static void addToQueue(SparseSet queue, int id)
    {
        if (id != 0) {
            queue.insert(id);
        }
    }
}
