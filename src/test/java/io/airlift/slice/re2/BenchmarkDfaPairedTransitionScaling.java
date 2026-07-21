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

import java.util.IdentityHashMap;
import java.util.concurrent.TimeUnit;

/** Measures the cache-footprint crossover for compact and depth-two DFA rows. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDfaPairedTransitionScaling
{
    private static final int TEXT_LENGTH = 16 * 1024 * 1024;

    @Param({"4", "8", "16", "32", "64", "128", "256"})
    int stateCount;

    @Param({"16", "29", "64"})
    int classCount;

    private byte[] text;
    private byte[] byteMap;
    private Object[] initialCompactState;
    private Object[] initialPairedState;
    private Object[] expectedCompactState;
    private Object[] expectedPairedState;
    private int visitedStateCount;

    @Setup(Level.Trial)
    public void setup()
    {
        if (Integer.bitCount(stateCount) != 1) {
            throw new IllegalArgumentException("stateCount must be a power of two");
        }

        text = new byte[TEXT_LENGTH];
        long randomState = 1;
        for (int index = 0; index < text.length; index++) {
            randomState ^= randomState << 13;
            randomState ^= randomState >>> 7;
            randomState ^= randomState << 17;
            text[index] = (byte) randomState;
        }

        byteMap = new byte[256];
        for (int value = 0; value < byteMap.length; value++) {
            byteMap[value] = (byte) (value % classCount);
        }

        Object[][] compactRows = new Object[stateCount][classCount];
        int pairedClassCount = classCount * classCount;
        Object[][] pairedRows = new Object[stateCount][pairedClassCount];
        for (int state = 0; state < stateCount; state++) {
            for (int byteClass = 0; byteClass < classCount; byteClass++) {
                compactRows[state][byteClass] = compactRows[nextState(state, byteClass)];
            }
            for (int firstByteClass = 0; firstByteClass < classCount; firstByteClass++) {
                int intermediateState = nextState(state, firstByteClass);
                for (int secondByteClass = 0; secondByteClass < classCount; secondByteClass++) {
                    int pairedClass = (firstByteClass * classCount) + secondByteClass;
                    pairedRows[state][pairedClass] = pairedRows[nextState(intermediateState, secondByteClass)];
                }
            }
        }

        initialCompactState = compactRows[0];
        initialPairedState = pairedRows[0];
        expectedCompactState = scanCompact();
        int finalState = findState(compactRows, expectedCompactState);
        expectedPairedState = pairedRows[finalState];
        if (scanPaired() != expectedPairedState) {
            throw new IllegalStateException("Paired transitions do not match compact transitions");
        }

        IdentityHashMap<Object[], Boolean> visitedStates = new IdentityHashMap<>();
        Object[] state = initialCompactState;
        visitedStates.put(state, true);
        for (byte value : text) {
            state = (Object[]) state[byteMap[value & 0xFF] & 0xFF];
            visitedStates.put(state, true);
        }
        visitedStateCount = visitedStates.size();
    }

    @Benchmark
    public Object compactObjectReferences()
    {
        return scanCompact();
    }

    @Benchmark
    public Object pairedObjectReferences()
    {
        return scanPaired();
    }

    private Object[] scanCompact()
    {
        Object[] state = initialCompactState;
        for (byte value : text) {
            Object nextState = state[byteMap[value & 0xFF] & 0xFF];
            if (nextState == null) {
                throw new IllegalStateException("Synthetic graph contains an abnormal transition");
            }
            state = (Object[]) nextState;
        }
        return state;
    }

    private Object[] scanPaired()
    {
        Object[] state = initialPairedState;
        for (int position = 0; position < text.length; position += 2) {
            int pairedClass = ((byteMap[text[position] & 0xFF] & 0xFF) * classCount) +
                    (byteMap[text[position + 1] & 0xFF] & 0xFF);
            Object nextState = state[pairedClass];
            if (nextState == null) {
                throw new IllegalStateException("Synthetic graph contains an abnormal paired transition");
            }
            state = (Object[]) nextState;
        }
        return state;
    }

    private int nextState(int state, int byteClass)
    {
        return ((state * 33) + byteClass + 1) & (stateCount - 1);
    }

    private static int findState(Object[][] rows, Object[] target)
    {
        for (int state = 0; state < rows.length; state++) {
            if (rows[state] == target) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown state row");
    }

    Object expectedCompactState()
    {
        return expectedCompactState;
    }

    Object expectedPairedState()
    {
        return expectedPairedState;
    }

    int visitedStateCount()
    {
        return visitedStateCount;
    }

    long compactReferenceBytes()
    {
        return (long) stateCount * classCount * Integer.BYTES;
    }

    long pairedReferenceBytes()
    {
        return (long) stateCount * classCount * classCount * Integer.BYTES;
    }
}
