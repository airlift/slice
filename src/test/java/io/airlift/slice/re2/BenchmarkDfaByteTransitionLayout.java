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
 * Isolates compact one-byte state identities for DFAs small enough to reserve zero as a sentinel.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDfaByteTransitionLayout
{
    @Param({"32", "128"})
    int stateCount;

    @Param({"8", "29", "64"})
    int classCount;

    @Param("16777216")
    int textLength;

    private byte[] text;
    private byte[] byteMap;
    private byte[] rowMajorTransitions;
    private byte[] classMajorTransitions;
    private byte[] directByteTransitions;
    private byte[] compactDirectByteTransitions;
    private int compactDirectByteTransitionMask;
    private int[] classMajorTransitionOffsets;
    private int stateMask;
    private int paddedClassCount;
    private int rowShift;
    private int expectedStateId;

    @Setup(Level.Trial)
    public void setup()
    {
        if (Integer.bitCount(stateCount) != 1 || stateCount > 254) {
            throw new IllegalArgumentException("stateCount must be a power of two no greater than 254: " + stateCount);
        }
        if (classCount < 1 || classCount > 256) {
            throw new IllegalArgumentException("classCount must be between 1 and 256: " + classCount);
        }

        stateMask = stateCount - 1;
        paddedClassCount = nextPowerOfTwo(classCount);
        rowShift = Integer.numberOfTrailingZeros(paddedClassCount);
        text = new byte[textLength];
        long randomState = 1;
        for (int position = 0; position < text.length; position++) {
            randomState ^= randomState << 13;
            randomState ^= randomState >>> 7;
            randomState ^= randomState << 17;
            text[position] = (byte) randomState;
        }

        byteMap = new byte[256];
        classMajorTransitionOffsets = new int[256];
        for (int value = 0; value < byteMap.length; value++) {
            int byteClass = value % classCount;
            byteMap[value] = (byte) byteClass;
            classMajorTransitionOffsets[value] = byteClass * (stateCount + 1);
        }

        rowMajorTransitions = new byte[(stateCount + 1) * paddedClassCount];
        classMajorTransitions = new byte[classCount * (stateCount + 1)];
        directByteTransitions = new byte[1 << 16];
        fill(directByteTransitions, (byte) 0xFF);
        compactDirectByteTransitions = new byte[stateCount << 8];
        compactDirectByteTransitionMask = compactDirectByteTransitions.length - 1;
        fill(compactDirectByteTransitions, (byte) 0xFF);
        for (int stateIndex = 0; stateIndex < stateCount; stateIndex++) {
            int stateId = stateIndex + 1;
            for (int byteClass = 0; byteClass < classCount; byteClass++) {
                int nextStateId = nextStateIndex(stateIndex, byteClass) + 1;
                rowMajorTransitions[(stateId << rowShift) | byteClass] = (byte) nextStateId;
                classMajorTransitions[(byteClass * (stateCount + 1)) + stateId] = (byte) nextStateId;
            }
            for (int value = 0; value < 256; value++) {
                int byteClass = byteMap[value] & 0xFF;
                byte nextStateIndex = (byte) nextStateIndex(stateIndex, byteClass);
                directByteTransitions[(stateIndex << 8) | value] = nextStateIndex;
                compactDirectByteTransitions[(stateIndex << 8) | value] = nextStateIndex;
            }
        }

        int stateIndex = 0;
        for (byte value : text) {
            stateIndex = nextStateIndex(stateIndex, byteMap[value & 0xFF] & 0xFF);
        }
        expectedStateId = stateIndex + 1;
    }

    @Benchmark
    public int rowMajorStateIds()
    {
        int stateId = 1;
        for (byte value : text) {
            int nextStateId = rowMajorTransitions[(stateId << rowShift) | (byteMap[value & 0xFF] & 0xFF)] & 0xFF;
            if (nextStateId == 0) {
                break;
            }
            stateId = nextStateId;
        }
        return stateId;
    }

    @Benchmark
    public int classMajorStateIds()
    {
        int stateId = 1;
        for (byte value : text) {
            int nextStateId = classMajorTransitions[classMajorTransitionOffsets[value & 0xFF] + stateId] & 0xFF;
            if (nextStateId == 0) {
                break;
            }
            stateId = nextStateId;
        }
        return stateId;
    }

    @Benchmark
    public int directByteStateIds()
    {
        int stateIndex = 0;
        for (byte value : text) {
            int nextStateIndex = directByteTransitions[(stateIndex << 8) | (value & 0xFF)] & 0xFF;
            if (nextStateIndex == 0xFF) {
                break;
            }
            stateIndex = nextStateIndex;
        }
        return stateIndex + 1;
    }

    @Benchmark
    public int compactDirectByteStateIds()
    {
        int stateIndex = 0;
        for (byte value : text) {
            int transitionIndex = ((stateIndex << 8) | (value & 0xFF)) & compactDirectByteTransitionMask;
            int nextStateIndex = compactDirectByteTransitions[transitionIndex] & 0xFF;
            if (nextStateIndex == 0xFF) {
                break;
            }
            stateIndex = nextStateIndex;
        }
        return stateIndex + 1;
    }

    int expectedStateId()
    {
        return expectedStateId;
    }

    boolean hasZeroAbnormalSentinels()
    {
        return rowMajorTransitions[0] == 0 && classMajorTransitions[0] == 0;
    }

    boolean hasDirectByteAbnormalSentinels()
    {
        return directByteTransitions[(255 << 8) | (text[0] & 0xFF)] == (byte) 0xFF;
    }

    void makeInitialTransitionAbnormal()
    {
        int byteClass = byteMap[text[0] & 0xFF] & 0xFF;
        rowMajorTransitions[paddedClassCount | byteClass] = 0;
        classMajorTransitions[classMajorTransitionOffsets[text[0] & 0xFF] + 1] = 0;
        directByteTransitions[text[0] & 0xFF] = (byte) 0xFF;
        compactDirectByteTransitions[text[0] & 0xFF] = (byte) 0xFF;
    }

    int initialStateId()
    {
        return 1;
    }

    private int nextStateIndex(int stateIndex, int byteClass)
    {
        return ((stateIndex * 33) + byteClass + 1) & stateMask;
    }

    private static int nextPowerOfTwo(int value)
    {
        return value == 1 ? 1 : Integer.highestOneBit(value - 1) << 1;
    }
}
