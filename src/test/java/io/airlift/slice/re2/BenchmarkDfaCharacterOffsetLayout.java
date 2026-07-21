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

import java.util.concurrent.TimeUnit;

import static java.util.Arrays.fill;

/**
 * Isolates pre-shifted unsigned row offsets whose type range makes every transition index valid.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDfaCharacterOffsetLayout
{
    private static final int ROW_SHIFT = 6;
    private static final int STATE_CAPACITY = 1 << (Character.SIZE - ROW_SHIFT);
    private static final int TRANSITION_COUNT = 1 << Character.SIZE;
    private static final char ABNORMAL_TRANSITION = Character.MAX_VALUE;

    @Param({"32", "128", "512"})
    int stateCount;

    @Param({"8", "29", "64"})
    int classCount;

    @Param("16777216")
    int textLength;

    private byte[] text;
    private byte[] byteMap;
    private char[] transitions;
    private int expectedRowOffset;

    @Setup(Level.Trial)
    public void setup()
    {
        if (Integer.bitCount(stateCount) != 1 || stateCount > STATE_CAPACITY) {
            throw new IllegalArgumentException("stateCount must be a power of two no greater than " + STATE_CAPACITY + ": " + stateCount);
        }
        if (classCount < 1 || classCount > (1 << ROW_SHIFT)) {
            throw new IllegalArgumentException("classCount must be between 1 and " + (1 << ROW_SHIFT) + ": " + classCount);
        }

        text = new byte[textLength];
        long randomState = 1;
        for (int position = 0; position < text.length; position++) {
            randomState ^= randomState << 13;
            randomState ^= randomState >>> 7;
            randomState ^= randomState << 17;
            text[position] = (byte) randomState;
        }

        byteMap = new byte[256];
        for (int value = 0; value < byteMap.length; value++) {
            byteMap[value] = (byte) (value % classCount);
        }

        transitions = new char[TRANSITION_COUNT];
        fill(transitions, ABNORMAL_TRANSITION);
        int stateMask = stateCount - 1;
        for (int stateIndex = 0; stateIndex < stateCount; stateIndex++) {
            int rowOffset = stateIndex << ROW_SHIFT;
            for (int byteClass = 0; byteClass < classCount; byteClass++) {
                int nextStateIndex = ((stateIndex * 33) + byteClass + 1) & stateMask;
                transitions[rowOffset | byteClass] = (char) (nextStateIndex << ROW_SHIFT);
            }
        }

        int stateIndex = 0;
        for (byte value : text) {
            stateIndex = ((stateIndex * 33) + (byteMap[value & 0xFF] & 0xFF) + 1) & stateMask;
        }
        expectedRowOffset = stateIndex << ROW_SHIFT;
    }

    @Benchmark
    public int preShiftedCharacterRowOffsets()
    {
        char[] transitions = this.transitions;
        if (transitions.length < TRANSITION_COUNT) {
            throw new IllegalStateException("Transition table is smaller than the character offset range");
        }

        int rowOffset = 0;
        for (byte value : text) {
            int nextRowOffset = transitions[rowOffset | (byteMap[value & 0xFF] & 0xFF)];
            if (nextRowOffset == ABNORMAL_TRANSITION) {
                break;
            }
            rowOffset = nextRowOffset;
        }
        return rowOffset;
    }

    int expectedRowOffset()
    {
        return expectedRowOffset;
    }

    int initialRowOffset()
    {
        return 0;
    }

    void makeInitialTransitionAbnormal()
    {
        transitions[byteMap[text[0] & 0xFF] & 0xFF] = ABNORMAL_TRANSITION;
    }
}
