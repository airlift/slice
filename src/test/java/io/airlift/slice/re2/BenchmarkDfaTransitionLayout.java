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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

/**
 * Isolates the loop-carried dependency of alternative DFA transition layouts.
 * The 29-class case matches the Hard and Parens benchmark programs.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDfaTransitionLayout
{
    private static final int STATE_COUNT = 32;
    private static final int STATE_MASK = STATE_COUNT - 1;
    private static final int TEXT_LENGTH = 16 * 1024 * 1024;
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
    @Param({"16", "29", "64"})
    int classCount;

    private byte[] text;
    private byte[] byteMap;
    private byte[] scaledByteMap;
    private Object[][] objectTransitions;
    private Object[][] directObjectTransitions;
    private Object[][] pairedObjectTransitions;
    private int[] flatTransitions;
    private int[] pairedFlatTransitions;
    private byte[] byteTransitions;
    private byte[] directByteTransitions;
    private char[] directCharacterTransitions;
    private int[] directIntegerTransitions;
    private Slice transitionSlice;
    private int expectedState;

    @Setup(Level.Trial)
    public void setup()
    {
        text = new byte[TEXT_LENGTH];
        long randomState = 1;
        for (int index = 0; index < text.length; index++) {
            randomState ^= randomState << 13;
            randomState ^= randomState >>> 7;
            randomState ^= randomState << 17;
            text[index] = (byte) randomState;
        }

        byteMap = new byte[256];
        scaledByteMap = new byte[256];
        for (int value = 0; value < 256; value++) {
            int byteClass = value % classCount;
            byteMap[value] = (byte) byteClass;
            scaledByteMap[value] = (byte) (byteClass * Integer.BYTES);
        }

        objectTransitions = new Object[STATE_COUNT][classCount];
        directObjectTransitions = new Object[STATE_COUNT][256];
        int pairClassCount = classCount * classCount;
        pairedObjectTransitions = new Object[STATE_COUNT][pairClassCount];
        flatTransitions = new int[STATE_COUNT * classCount];
        pairedFlatTransitions = new int[STATE_COUNT * pairClassCount];
        byteTransitions = new byte[STATE_COUNT * classCount * Integer.BYTES];
        directByteTransitions = new byte[STATE_COUNT * 256];
        directCharacterTransitions = new char[STATE_COUNT * 256];
        directIntegerTransitions = new int[STATE_COUNT * 256];
        for (int state = 0; state < STATE_COUNT; state++) {
            for (int byteClass = 0; byteClass < classCount; byteClass++) {
                int nextState = ((state * 33) + byteClass + 1) & STATE_MASK;
                objectTransitions[state][byteClass] = objectTransitions[nextState];

                int transitionIndex = (state * classCount) + byteClass;
                int nextElementOffset = nextState * classCount;
                int nextByteOffset = nextElementOffset * Integer.BYTES;
                flatTransitions[transitionIndex] = nextElementOffset;
                INT_HANDLE.set(byteTransitions, transitionIndex * Integer.BYTES, nextByteOffset);
            }
        }
        for (int state = 0; state < STATE_COUNT; state++) {
            for (int firstByteClass = 0; firstByteClass < classCount; firstByteClass++) {
                int intermediateState = ((state * 33) + firstByteClass + 1) & STATE_MASK;
                for (int secondByteClass = 0; secondByteClass < classCount; secondByteClass++) {
                    int nextState = ((intermediateState * 33) + secondByteClass + 1) & STATE_MASK;
                    int pairClass = (firstByteClass * classCount) + secondByteClass;
                    pairedObjectTransitions[state][pairClass] = pairedObjectTransitions[nextState];
                    pairedFlatTransitions[(state * pairClassCount) + pairClass] = nextState * pairClassCount;
                }
            }
        }
        for (int state = 0; state < STATE_COUNT; state++) {
            for (int value = 0; value < 256; value++) {
                int nextState = ((state * 33) + (byteMap[value] & 0xFF) + 1) & STATE_MASK;
                int transitionIndex = (state << 8) + value;
                directByteTransitions[transitionIndex] = (byte) nextState;
                directCharacterTransitions[transitionIndex] = (char) (nextState << 8);
                directIntegerTransitions[transitionIndex] = nextState << 8;
                directObjectTransitions[state][value] = directObjectTransitions[nextState];
            }
        }
        transitionSlice = Slices.wrappedBuffer(byteTransitions);

        int state = 0;
        for (byte value : text) {
            state = ((state * 33) + (byteMap[value & 0xFF] & 0xFF) + 1) & STATE_MASK;
        }
        expectedState = state;
    }

    @Benchmark
    public Object objectReferences()
    {
        Object[] state = objectTransitions[0];
        for (byte value : text) {
            Object nextState = state[byteMap[value & 0xFF] & 0xFF];
            if (nextState == null) {
                break;
            }
            state = (Object[]) nextState;
        }
        return state;
    }

    @Benchmark
    public Object directObjectReferences()
    {
        Object[] state = directObjectTransitions[0];
        for (byte value : text) {
            Object nextState = state[value & 0xFF];
            if (nextState == null) {
                break;
            }
            state = (Object[]) nextState;
        }
        return state;
    }

    @Benchmark
    public Object pairedObjectReferences()
    {
        Object[] state = pairedObjectTransitions[0];
        for (int position = 0; position < text.length; position += 2) {
            int pairClass = ((byteMap[text[position] & 0xFF] & 0xFF) * classCount) +
                    (byteMap[text[position + 1] & 0xFF] & 0xFF);
            Object nextState = state[pairClass];
            if (nextState == null) {
                break;
            }
            state = (Object[]) nextState;
        }
        return state;
    }

    @Benchmark
    public int flatIntegers()
    {
        int stateOffset = 0;
        for (byte value : text) {
            int nextState = flatTransitions[stateOffset + (byteMap[value & 0xFF] & 0xFF)];
            if (nextState < 0) {
                break;
            }
            stateOffset = nextState;
        }
        return stateOffset;
    }

    @Benchmark
    public int pairedFlatIntegers()
    {
        int stateOffset = 0;
        for (int position = 0; position < text.length; position += 2) {
            int pairClass = ((byteMap[text[position] & 0xFF] & 0xFF) * classCount) +
                    (byteMap[text[position + 1] & 0xFF] & 0xFF);
            int nextState = pairedFlatTransitions[stateOffset + pairClass];
            if (nextState < 0) {
                break;
            }
            stateOffset = nextState;
        }
        return stateOffset;
    }

    @Benchmark
    public int directBytes()
    {
        int state = 0;
        for (byte value : text) {
            int nextState = directByteTransitions[(state << 8) + (value & 0xFF)] & 0xFF;
            if (nextState == 0xFF) {
                break;
            }
            state = nextState;
        }
        return state;
    }

    @Benchmark
    public int directCharacters()
    {
        int stateOffset = 0;
        for (byte value : text) {
            int nextState = directCharacterTransitions[stateOffset + (value & 0xFF)];
            if (nextState == 0xFFFF) {
                break;
            }
            stateOffset = nextState;
        }
        return stateOffset;
    }

    @Benchmark
    public int directIntegers()
    {
        int stateOffset = 0;
        for (byte value : text) {
            int nextState = directIntegerTransitions[stateOffset + (value & 0xFF)];
            if (nextState < 0) {
                break;
            }
            stateOffset = nextState;
        }
        return stateOffset;
    }

    @Benchmark
    public int byteArrayOffsets()
    {
        int stateByteOffset = 0;
        for (byte value : text) {
            int nextState = (int) INT_HANDLE.get(
                    byteTransitions,
                    stateByteOffset + (scaledByteMap[value & 0xFF] & 0xFF));
            if (nextState < 0) {
                break;
            }
            stateByteOffset = nextState;
        }
        return stateByteOffset;
    }

    @Benchmark
    public int sliceOffsets()
    {
        int stateByteOffset = 0;
        for (byte value : text) {
            int nextState = transitionSlice.getIntUnchecked(
                    stateByteOffset + (scaledByteMap[value & 0xFF] & 0xFF));
            if (nextState < 0) {
                break;
            }
            stateByteOffset = nextState;
        }
        return stateByteOffset;
    }

    Object[] expectedObjectState()
    {
        return objectTransitions[expectedState];
    }

    Object[] expectedDirectObjectState()
    {
        return directObjectTransitions[expectedState];
    }

    int expectedElementOffset()
    {
        return expectedState * classCount;
    }

    Object[] expectedPairedObjectState()
    {
        return pairedObjectTransitions[expectedState];
    }

    int expectedPairedElementOffset()
    {
        return expectedState * classCount * classCount;
    }

    int expectedState()
    {
        return expectedState;
    }

    int expectedDirectOffset()
    {
        return expectedState << 8;
    }

    int expectedByteOffset()
    {
        return expectedElementOffset() * Integer.BYTES;
    }
}
