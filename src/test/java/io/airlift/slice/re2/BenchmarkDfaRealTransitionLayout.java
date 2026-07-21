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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static io.airlift.slice.re2.Re2BenchmarkRunner.randomText;

/** Measures compact and paired rows over the actual warmed benchmark DFA graph. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDfaRealTransitionLayout
{
    private static final int TEXT_LENGTH = 16 * 1024 * 1024;
    private static final int SELF_LOOP_SAMPLE_SIZE = 64;
    private static final int SELF_LOOP_SAMPLE_PERCENTAGE = 90;
    private static final String HARD = "[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$";
    private static final String PARENS = "([ -~])*(A)(B)(C)(D)(E)(F)(G)(H)(I)(J)(K)(L)(M)(N)(O)(P)(Q)(R)(S)(T)(U)(V)(W)(X)(Y)(Z)$";
    private static final String SELF_LOOP = ".*$";
    private static final String LATE_TRANSITION = "^a*bc$";
    private static final String ALTERNATING = "^(?:ab|ba)*z$";
    private static final String MULTIBYTE = "^(?:\uD83D\uDCB0|\u20AC|\u00E9)+Z$";
    private static final byte[] MULTIBYTE_SEQUENCE = "\uD83D\uDCB0\u20AC\u00E9".getBytes(StandardCharsets.UTF_8);

    @Param({"HARD", "PARENS", "SELF_LOOP", "LATE_TRANSITION", "ALTERNATING", "MULTIBYTE"})
    String pattern;

    private byte[] text;
    private byte[] byteMap;
    private int classCount;
    private Object[][] stateRows;
    private Object[] initialState;
    private Object[] initialPairState;
    private Object[] expectedState;
    private Object[] expectedPairState;
    private int[] exactStrideTransitions;
    private int[] preShiftedTransitions;
    private int paddedClassCount;
    private int rowShift;
    private int abnormalStateIndex;
    private int abnormalByteClass;
    private int stateCount;
    private long compactTransitionBytes;
    private long pairedTransitionBytes;
    private long selfLoopTransitionCount;
    private long stateChangeTransitionCount;

    @Setup(Level.Trial)
    public void setup()
    {
        String expression;
        switch (pattern) {
            case "HARD" -> {
                expression = HARD;
                text = randomText(TEXT_LENGTH);
            }
            case "PARENS" -> {
                expression = PARENS;
                text = randomText(TEXT_LENGTH);
            }
            case "SELF_LOOP" -> {
                expression = SELF_LOOP;
                text = randomText(TEXT_LENGTH);
            }
            case "LATE_TRANSITION" -> {
                expression = LATE_TRANSITION;
                text = new byte[TEXT_LENGTH];
                Arrays.fill(text, (byte) 'a');
            }
            case "ALTERNATING" -> {
                expression = ALTERNATING;
                text = new byte[TEXT_LENGTH];
                for (int index = 0; index < text.length; index += 2) {
                    text[index] = 'a';
                    text[index + 1] = 'b';
                }
            }
            case "MULTIBYTE" -> {
                expression = MULTIBYTE;
                text = repeatSequence(MULTIBYTE_SEQUENCE, TEXT_LENGTH);
            }
            default -> throw new IllegalArgumentException("Unknown pattern: " + pattern);
        }
        Slice input = Slices.wrappedBuffer(text);
        Prog program = compileProg(expression);

        Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true);
        Dfa.DfaInstance dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);

        byteMap = dfa.bytemap;
        classCount = dfa.nextSize;
        stateRows = dfa.stateRefArrays;
        // Offset zero is reserved, so the first allocated row is this DFA's start state.
        int startStateIndex = 1;
        initialState = stateRows[startStateIndex];

        IdentityHashMap<Object[], Integer> stateIndexes = new IdentityHashMap<>();
        for (int index = 0; index < stateRows.length; index++) {
            if (stateRows[index] != null) {
                stateIndexes.put(stateRows[index], index);
            }
        }
        stateCount = stateIndexes.size();

        Object[] topologyState = initialState;
        for (byte value : text) {
            Object nextState = topologyState[byteMap[value & 0xFF] & 0xFF];
            if (!(nextState instanceof Object[] nextStateRow)) {
                throw new IllegalStateException("Actual benchmark path contains an abnormal transition");
            }
            if (nextStateRow == topologyState) {
                selfLoopTransitionCount++;
            }
            else {
                stateChangeTransitionCount++;
                topologyState = nextStateRow;
            }
        }

        paddedClassCount = nextPowerOfTwo(classCount);
        rowShift = Integer.numberOfTrailingZeros(paddedClassCount);
        exactStrideTransitions = new int[Math.multiplyExact(stateRows.length, classCount)];
        preShiftedTransitions = new int[Math.multiplyExact(stateRows.length, paddedClassCount)];

        abnormalStateIndex = -1;
        abnormalByteClass = -1;
        for (Object[] source : stateIndexes.keySet()) {
            int sourceIndex = stateIndexes.get(source);
            for (int byteClass = 0; byteClass < classCount; byteClass++) {
                if (!(source[byteClass] instanceof Object[] target)) {
                    if (abnormalStateIndex < 0) {
                        abnormalStateIndex = sourceIndex;
                        abnormalByteClass = byteClass;
                    }
                    continue;
                }

                int targetIndex = stateIndexes.get(target);
                exactStrideTransitions[(sourceIndex * classCount) + byteClass] = targetIndex * classCount;
                preShiftedTransitions[(sourceIndex << rowShift) | byteClass] = targetIndex << rowShift;
            }
        }
        if (abnormalStateIndex < 0) {
            throw new IllegalStateException("Actual warmed DFA graph contains no abnormal transition");
        }

        Object[][] pairRows = new Object[stateRows.length][];
        int pairClassCount = classCount * classCount;
        for (int index : stateIndexes.values()) {
            pairRows[index] = new Object[pairClassCount];
        }
        for (Object[] source : stateIndexes.keySet()) {
            int sourceIndex = stateIndexes.get(source);
            Object[] sourcePairs = pairRows[sourceIndex];
            for (int firstClass = 0; firstClass < classCount; firstClass++) {
                if (!(source[firstClass] instanceof Object[] intermediate)) {
                    continue;
                }
                for (int secondClass = 0; secondClass < classCount; secondClass++) {
                    if (intermediate[secondClass] instanceof Object[] target) {
                        sourcePairs[(firstClass * classCount) + secondClass] = pairRows[stateIndexes.get(target)];
                    }
                }
            }
        }

        initialPairState = pairRows[startStateIndex];
        expectedState = scanCompact();
        expectedPairState = pairRows[stateIndexes.get(expectedState)];
        compactTransitionBytes = (long) stateCount * classCount * Integer.BYTES;
        pairedTransitionBytes = (long) stateCount * pairClassCount * Integer.BYTES;
        if (scanPairs() != expectedPairState) {
            throw new IllegalStateException("Paired transitions do not match compact transitions");
        }
    }

    @Benchmark
    public Object compactObjectReferences()
    {
        return scanCompact();
    }

    @Benchmark
    public Object selfLoopObjectReferences()
    {
        Object[] state = initialState;
        for (byte value : text) {
            Object nextState = state[byteMap[value & 0xFF] & 0xFF];
            if (nextState == null) {
                throw new IllegalStateException("Actual benchmark path contains an abnormal transition");
            }
            if (nextState != state) {
                state = (Object[]) nextState;
            }
        }
        return state;
    }

    @Benchmark
    public Object selfLoopFirstObjectReferences()
    {
        Object[] state = initialState;
        for (byte value : text) {
            Object nextState = state[byteMap[value & 0xFF] & 0xFF];
            if (nextState == state) {
                continue;
            }
            if (nextState == null) {
                throw new IllegalStateException("Actual benchmark path contains an abnormal transition");
            }
            state = (Object[]) nextState;
        }
        return state;
    }

    @Benchmark
    public Object sampledSelfLoopObjectReferences()
    {
        Object[] state = initialState;
        int sampleEnd = Math.min(SELF_LOOP_SAMPLE_SIZE, text.length);
        int selfLoopCount = 0;
        int position = 0;
        for (; position < sampleEnd; position++) {
            Object nextState = state[byteMap[text[position] & 0xFF] & 0xFF];
            if (nextState == state) {
                selfLoopCount++;
                continue;
            }
            if (nextState == null) {
                throw new IllegalStateException("Actual benchmark path contains an abnormal transition");
            }
            state = (Object[]) nextState;
        }

        if (selfLoopCount * 100 >= sampleEnd * SELF_LOOP_SAMPLE_PERCENTAGE) {
            return scanSelfLoopTail(state, position);
        }
        for (; position < text.length; position++) {
            Object nextState = state[byteMap[text[position] & 0xFF] & 0xFF];
            if (nextState == null) {
                throw new IllegalStateException("Actual benchmark path contains an abnormal transition");
            }
            state = (Object[]) nextState;
        }
        return state;
    }

    private Object[] scanSelfLoopTail(Object[] state, int position)
    {
        for (; position < text.length; position++) {
            Object nextState = state[byteMap[text[position] & 0xFF] & 0xFF];
            if (nextState == state) {
                continue;
            }
            if (nextState == null) {
                throw new IllegalStateException("Actual benchmark path contains an abnormal transition");
            }
            state = (Object[]) nextState;
        }
        return state;
    }

    @Benchmark
    public Object pairedObjectReferences()
    {
        return scanPairs();
    }

    @Benchmark
    public int exactStrideHeapRowIndexes()
    {
        int rowIndex = classCount;
        for (byte value : text) {
            int nextRowIndex = exactStrideTransitions[rowIndex + (byteMap[value & 0xFF] & 0xFF)];
            if (nextRowIndex == 0) {
                throw new IllegalStateException("Actual benchmark path contains an abnormal exact-stride transition");
            }
            rowIndex = nextRowIndex;
        }
        return rowIndex;
    }

    @Benchmark
    public int preShiftedPowerOfTwoHeapRowIndexes()
    {
        int rowIndex = paddedClassCount;
        for (byte value : text) {
            int nextRowIndex = preShiftedTransitions[rowIndex | (byteMap[value & 0xFF] & 0xFF)];
            if (nextRowIndex == 0) {
                throw new IllegalStateException("Actual benchmark path contains an abnormal pre-shifted transition");
            }
            rowIndex = nextRowIndex;
        }
        return rowIndex;
    }

    private Object[] scanCompact()
    {
        Object[] state = initialState;
        for (byte value : text) {
            Object nextState = state[byteMap[value & 0xFF] & 0xFF];
            if (nextState == null) {
                throw new IllegalStateException("Actual benchmark path contains an abnormal transition");
            }
            state = (Object[]) nextState;
        }
        return state;
    }

    private Object[] scanPairs()
    {
        Object[] state = initialPairState;
        for (int position = 0; position < text.length; position += 2) {
            int pairClass = ((byteMap[text[position] & 0xFF] & 0xFF) * classCount) +
                    (byteMap[text[position + 1] & 0xFF] & 0xFF);
            Object nextState = state[pairClass];
            if (nextState == null) {
                throw new IllegalStateException("Actual benchmark path contains an abnormal pair transition");
            }
            state = (Object[]) nextState;
        }
        return state;
    }

    Object expectedState()
    {
        return expectedState;
    }

    Object expectedPairState()
    {
        return expectedPairState;
    }

    int stateCount()
    {
        return stateCount;
    }

    int classCount()
    {
        return classCount;
    }

    long compactTransitionBytes()
    {
        return compactTransitionBytes;
    }

    long pairedTransitionBytes()
    {
        return pairedTransitionBytes;
    }

    long selfLoopTransitionCount()
    {
        return selfLoopTransitionCount;
    }

    long stateChangeTransitionCount()
    {
        return stateChangeTransitionCount;
    }

    int textLength()
    {
        return text.length;
    }

    Object[] stateAtExactStrideRowIndex(int rowIndex)
    {
        return stateRows[rowIndex / classCount];
    }

    Object[] stateAtPreShiftedRowIndex(int rowIndex)
    {
        return stateRows[rowIndex >> rowShift];
    }

    int abnormalExactStrideTransition()
    {
        return exactStrideTransitions[(abnormalStateIndex * classCount) + abnormalByteClass];
    }

    int abnormalPreShiftedTransition()
    {
        return preShiftedTransitions[(abnormalStateIndex << rowShift) | abnormalByteClass];
    }

    private static int nextPowerOfTwo(int value)
    {
        return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(value - 1));
    }

    private static byte[] repeatSequence(byte[] sequence, int maximumLength)
    {
        int sequenceCount = maximumLength / sequence.length;
        sequenceCount -= sequenceCount & 1;
        byte[] repeated = new byte[sequenceCount * sequence.length];
        for (int offset = 0; offset < repeated.length; offset += sequence.length) {
            System.arraycopy(sequence, 0, repeated, offset, sequence.length);
        }
        return repeated;
    }
}
