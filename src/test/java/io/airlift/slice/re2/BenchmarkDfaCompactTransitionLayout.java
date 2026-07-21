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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.TimeUnit;

/**
 * Compares compact one-byte DFA layouts with the proposed zero-abnormal-transition contract.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDfaCompactTransitionLayout
{
    @Param({"32", "64", "96", "128", "160", "192", "224", "256", "320", "384", "448", "512"})
    int stateCount;

    @Param({"8", "29", "64"})
    int classCount;

    @Param("16777216")
    int textLength;

    private byte[] text;
    private byte[] byteMap;
    private short[] transitionByteOffsets;
    private Object[][] objectTransitionRows;
    private int[] exactStrideTransitions;
    private int[] powerOfTwoStateIdTransitions;
    private int[] preShiftedRowTransitions;
    private char[] rowMajorCharacterStateTransitions;
    private char[] classMajorCharacterStateTransitions;
    private int[] classMajorTransitionOffsets;
    private Arena foreignArena;
    private MemorySegment foreignPointerTransitions;
    private AddressLayout transitionRowPointerLayout;
    private long foreignRowSize;
    private long expectedForeignRowAddress;
    private int paddedClassCount;
    private int rowShift;
    private int expectedStateId;

    @Setup(Level.Trial)
    public void setup()
    {
        if (stateCount < 1 || stateCount >= Character.MAX_VALUE) {
            throw new IllegalArgumentException("stateCount must be between 1 and " + (Character.MAX_VALUE - 1) + ": " + stateCount);
        }
        if (classCount < 1 || classCount > 256) {
            throw new IllegalArgumentException("classCount must be between 1 and 256: " + classCount);
        }

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
        transitionByteOffsets = new short[256];
        for (int value = 0; value < byteMap.length; value++) {
            byteMap[value] = (byte) (value % classCount);
            transitionByteOffsets[value] = (short) ((byteMap[value] & 0xFF) * Long.BYTES);
        }

        // Row zero is reserved so every valid row identity is nonzero.
        objectTransitionRows = new Object[stateCount + 1][];
        for (int stateId = 1; stateId <= stateCount; stateId++) {
            objectTransitionRows[stateId] = new Object[classCount + 1];
        }
        exactStrideTransitions = new int[(stateCount + 1) * classCount];
        powerOfTwoStateIdTransitions = new int[(stateCount + 1) * paddedClassCount];
        preShiftedRowTransitions = new int[(stateCount + 1) * paddedClassCount];
        rowMajorCharacterStateTransitions = new char[(stateCount + 1) * paddedClassCount];
        classMajorCharacterStateTransitions = new char[classCount * (stateCount + 1)];
        classMajorTransitionOffsets = new int[256];
        for (int value = 0; value < classMajorTransitionOffsets.length; value++) {
            classMajorTransitionOffsets[value] = (byteMap[value] & 0xFF) * (stateCount + 1);
        }

        for (int stateIndex = 0; stateIndex < stateCount; stateIndex++) {
            int stateId = stateIndex + 1;
            for (int byteClass = 0; byteClass < classCount; byteClass++) {
                int nextStateIndex = nextStateIndex(stateIndex, byteClass);
                int nextStateId = nextStateIndex + 1;

                objectTransitionRows[stateId][byteClass] = objectTransitionRows[nextStateId];
                exactStrideTransitions[(stateId * classCount) + byteClass] = nextStateId * classCount;
                powerOfTwoStateIdTransitions[(stateId << rowShift) | byteClass] = nextStateId;
                preShiftedRowTransitions[(stateId << rowShift) | byteClass] = nextStateId << rowShift;
                rowMajorCharacterStateTransitions[(stateId << rowShift) | byteClass] = (char) nextStateId;
                classMajorCharacterStateTransitions[(byteClass * (stateCount + 1)) + stateId] = (char) nextStateId;
            }
        }

        if (Dfa.nativeAccessEnabled()) {
            foreignArena = Arena.ofConfined();
            foreignRowSize = (long) classCount * Long.BYTES;
            transitionRowPointerLayout = ValueLayout.ADDRESS.withTargetLayout(
                    MemoryLayout.sequenceLayout(classCount, ValueLayout.JAVA_LONG));
            foreignPointerTransitions = foreignArena.allocate((stateCount + 1L) * foreignRowSize, Long.BYTES);
            long foreignBaseAddress = foreignPointerTransitions.address();
            for (int stateIndex = 0; stateIndex < stateCount; stateIndex++) {
                int stateId = stateIndex + 1;
                for (int byteClass = 0; byteClass < classCount; byteClass++) {
                    int nextStateId = nextStateIndex(stateIndex, byteClass) + 1;
                    foreignPointerTransitions.set(
                            ValueLayout.JAVA_LONG,
                            (stateId * foreignRowSize) + ((long) byteClass * Long.BYTES),
                            foreignBaseAddress + (nextStateId * foreignRowSize));
                }
            }
        }

        int stateIndex = 0;
        for (byte value : text) {
            stateIndex = nextStateIndex(stateIndex, byteMap[value & 0xFF] & 0xFF);
        }
        expectedStateId = stateIndex + 1;
        if (foreignPointerTransitions != null) {
            expectedForeignRowAddress = foreignPointerTransitions.address() + (expectedStateId * foreignRowSize);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown()
    {
        if (foreignArena != null) {
            foreignArena.close();
        }
    }

    @Benchmark
    public Object objectReferences()
    {
        Object[] transitionRow = objectTransitionRows[1];
        for (byte value : text) {
            Object nextTransitionRow = transitionRow[byteMap[value & 0xFF] & 0xFF];
            if (nextTransitionRow == null) {
                break;
            }
            transitionRow = (Object[]) nextTransitionRow;
        }
        return transitionRow;
    }

    @Benchmark
    public Object selfLoopAwareObjectReferences()
    {
        Object[] transitionRow = objectTransitionRows[1];
        for (byte value : text) {
            Object nextTransitionRow = transitionRow[byteMap[value & 0xFF] & 0xFF];
            if (nextTransitionRow == null) {
                break;
            }
            if (nextTransitionRow != transitionRow) {
                transitionRow = (Object[]) nextTransitionRow;
            }
        }
        return transitionRow;
    }

    @Benchmark
    public Object selfLoopFirstObjectReferences()
    {
        Object[] transitionRow = objectTransitionRows[1];
        for (byte value : text) {
            Object nextTransitionRow = transitionRow[byteMap[value & 0xFF] & 0xFF];
            if (nextTransitionRow == transitionRow) {
                continue;
            }
            if (nextTransitionRow == null) {
                break;
            }
            transitionRow = (Object[]) nextTransitionRow;
        }
        return transitionRow;
    }

    @Benchmark
    public int exactStrideHeapIntegers()
    {
        int rowIndex = classCount;
        for (byte value : text) {
            int nextRowIndex = exactStrideTransitions[rowIndex + (byteMap[value & 0xFF] & 0xFF)];
            if (nextRowIndex == 0) {
                break;
            }
            rowIndex = nextRowIndex;
        }
        return rowIndex;
    }

    @Benchmark
    public int powerOfTwoStateIds()
    {
        int stateId = 1;
        for (byte value : text) {
            int nextStateId = powerOfTwoStateIdTransitions[(stateId << rowShift) | (byteMap[value & 0xFF] & 0xFF)];
            if (nextStateId == 0) {
                break;
            }
            stateId = nextStateId;
        }
        return stateId;
    }

    @Benchmark
    public int preShiftedHeapRowIndexes()
    {
        int rowIndex = paddedClassCount;
        for (byte value : text) {
            int nextRowIndex = preShiftedRowTransitions[rowIndex | (byteMap[value & 0xFF] & 0xFF)];
            if (nextRowIndex == 0) {
                break;
            }
            rowIndex = nextRowIndex;
        }
        return rowIndex;
    }

    @Benchmark
    public int rowMajorCharacterStateIds()
    {
        int stateId = 1;
        for (byte value : text) {
            int nextStateId = rowMajorCharacterStateTransitions[(stateId << rowShift) | (byteMap[value & 0xFF] & 0xFF)];
            if (nextStateId == 0) {
                break;
            }
            stateId = nextStateId;
        }
        return stateId;
    }

    @Benchmark
    public int classMajorCharacterStateIds()
    {
        int stateId = 1;
        for (byte value : text) {
            int nextStateId = classMajorCharacterStateTransitions[classMajorTransitionOffsets[value & 0xFF] + stateId];
            if (nextStateId == 0) {
                break;
            }
            stateId = nextStateId;
        }
        return stateId;
    }

    @Benchmark
    public long everythingSegmentPointers()
    {
        long transitionRowAddress = foreignPointerTransitions.address() + foreignRowSize;
        for (byte value : text) {
            long nextTransitionRowAddress = AbsoluteAddressSpace.EVERYTHING.get(
                    ValueLayout.JAVA_LONG,
                    transitionRowAddress + ((long) (byteMap[value & 0xFF] & 0xFF) * Long.BYTES));
            if (nextTransitionRowAddress == 0) {
                break;
            }
            transitionRowAddress = nextTransitionRowAddress;
        }
        return transitionRowAddress;
    }

    @Benchmark
    public long everythingSegmentUnalignedPointers()
    {
        long transitionRowAddress = foreignPointerTransitions.address() + foreignRowSize;
        for (byte value : text) {
            long nextTransitionRowAddress = AbsoluteAddressSpace.EVERYTHING.get(
                    ValueLayout.JAVA_LONG_UNALIGNED,
                    transitionRowAddress + ((long) (byteMap[value & 0xFF] & 0xFF) * Long.BYTES));
            if (nextTransitionRowAddress == 0) {
                break;
            }
            transitionRowAddress = nextTransitionRowAddress;
        }
        return transitionRowAddress;
    }

    @Benchmark
    public long everythingSegmentPreScaledPointers()
    {
        long transitionRowAddress = foreignPointerTransitions.address() + foreignRowSize;
        for (byte value : text) {
            long nextTransitionRowAddress = AbsoluteAddressSpace.EVERYTHING.get(
                    ValueLayout.JAVA_LONG_UNALIGNED,
                    transitionRowAddress + transitionByteOffsets[value & 0xFF]);
            if (nextTransitionRowAddress == 0) {
                break;
            }
            transitionRowAddress = nextTransitionRowAddress;
        }
        return transitionRowAddress;
    }

    @Benchmark
    public long everythingSegmentIndexedPointers()
    {
        long transitionRowAddress = foreignPointerTransitions.address() + foreignRowSize;
        for (byte value : text) {
            long nextTransitionRowAddress = AbsoluteAddressSpace.EVERYTHING.getAtIndex(
                    ValueLayout.JAVA_LONG_UNALIGNED,
                    (transitionRowAddress >>> 3) + (byteMap[value & 0xFF] & 0xFF));
            if (nextTransitionRowAddress == 0) {
                break;
            }
            transitionRowAddress = nextTransitionRowAddress;
        }
        return transitionRowAddress;
    }

    private static final class AbsoluteAddressSpace
    {
        // The static final shape lets C2 reduce FFM bounds checking to a non-negative address check.
        private static final MemorySegment EVERYTHING = MemorySegment.NULL.reinterpret(Long.MAX_VALUE);

        private AbsoluteAddressSpace() {}
    }

    @Benchmark
    public long rowSegmentPointers()
    {
        MemorySegment transitionRow = foreignPointerTransitions.asSlice(foreignRowSize, foreignRowSize);
        for (byte value : text) {
            MemorySegment nextTransitionRow = transitionRow.getAtIndex(
                    transitionRowPointerLayout,
                    byteMap[value & 0xFF] & 0xFF);
            if (nextTransitionRow.address() == 0) {
                break;
            }
            transitionRow = nextTransitionRow;
        }
        return transitionRow.address();
    }

    Object[] expectedObjectTransitionRow()
    {
        return objectTransitionRows[expectedStateId];
    }

    int expectedExactStrideRowIndex()
    {
        return expectedStateId * classCount;
    }

    int expectedStateId()
    {
        return expectedStateId;
    }

    int expectedPreShiftedRowIndex()
    {
        return expectedStateId << rowShift;
    }

    long expectedForeignRowAddress()
    {
        return expectedForeignRowAddress;
    }

    boolean hasZeroAbnormalSentinels()
    {
        return exactStrideTransitions[0] == 0 &&
                powerOfTwoStateIdTransitions[0] == 0 &&
                preShiftedRowTransitions[0] == 0 &&
                rowMajorCharacterStateTransitions[0] == 0 &&
                classMajorCharacterStateTransitions[0] == 0 &&
                (foreignPointerTransitions == null || foreignPointerTransitions.get(ValueLayout.JAVA_LONG, 0) == 0);
    }

    void makeInitialTransitionAbnormal()
    {
        int byteClass = byteMap[text[0] & 0xFF] & 0xFF;
        objectTransitionRows[1][byteClass] = null;
        exactStrideTransitions[classCount + byteClass] = 0;
        powerOfTwoStateIdTransitions[paddedClassCount | byteClass] = 0;
        preShiftedRowTransitions[paddedClassCount | byteClass] = 0;
        rowMajorCharacterStateTransitions[paddedClassCount | byteClass] = 0;
        classMajorCharacterStateTransitions[classMajorTransitionOffsets[text[0] & 0xFF] + 1] = 0;
        if (foreignPointerTransitions != null) {
            foreignPointerTransitions.set(
                    ValueLayout.JAVA_LONG,
                    foreignRowSize + ((long) byteClass * Long.BYTES),
                    0);
        }
    }

    Object[] initialObjectTransitionRow()
    {
        return objectTransitionRows[1];
    }

    int initialExactStrideRowIndex()
    {
        return classCount;
    }

    int initialStateId()
    {
        return 1;
    }

    int initialPreShiftedRowIndex()
    {
        return paddedClassCount;
    }

    long initialForeignRowAddress()
    {
        return foreignPointerTransitions.address() + foreignRowSize;
    }

    private int nextStateIndex(int stateIndex, int byteClass)
    {
        return ((stateIndex * 33) + byteClass + 1) % stateCount;
    }

    private static int nextPowerOfTwo(int value)
    {
        return value == 1 ? 1 : Integer.highestOneBit(value - 1) << 1;
    }
}
