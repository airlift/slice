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
package io.airlift.slice;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;

/**
 * Benchmarks the substring-search scan used by {@link Slice#indexOf(Slice, int)}.
 *
 * <ul>
 *   <li>{@code indexOfInline} is the baseline: a local copy of the pre-optimization scan that reads
 *       the haystack through the {@link Slice} instance-field accessors ({@code getIntUnchecked} /
 *       {@code equalsUnchecked}), so it reflects the throughput C2 achieves when the backing array
 *       is reached through instance fields.</li>
 *   <li>{@code indexOfHelper} is the optimization: the same algorithm as a static method over the
 *       backing array passed as a parameter, which keeps C2 range-check elimination in effect for
 *       long scans.</li>
 *   <li>{@code indexOfDispatched} exercises the shipped {@link Slice#indexOf(Slice)}, which picks the
 *       inline scan below {@code INDEX_OF_SCAN_THRESHOLD} and the helper at or above it; comparing it
 *       against the two above shows the dispatch overhead and that the right path is chosen.</li>
 * </ul>
 *
 * <p>Note: comparing {@code indexOfDispatched} against {@code indexOfHelper} at sizes at or above the
 * threshold measures the optimized helper against itself (plus dispatch); use {@code indexOfInline}
 * vs {@code indexOfHelper} to measure the optimization itself.
 *
 * <p>Worst case: the pattern's first byte does not occur in the haystack until the pattern itself,
 * which sits at the very end, so the whole haystack is scanned through the fast-skip path.
 */
@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(2)
@Warmup(iterations = 4, time = 400, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 6, time = 400, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkSearch
{
    @Benchmark
    public int indexOfInline(SearchData data)
    {
        return indexOfInline(data.haystack, data.pattern);
    }

    @Benchmark
    public int indexOfHelper(SearchData data)
    {
        return indexOfHelper(data.haystackBytes, 0, data.haystackBytes.length, data.patternBytes);
    }

    @Benchmark
    public int indexOfDispatched(SearchData data)
    {
        return data.haystack.indexOf(data.pattern);
    }

    private static final java.lang.invoke.VarHandle INT_HANDLE =
            java.lang.invoke.MethodHandles.byteArrayViewVarHandle(int[].class, java.nio.ByteOrder.LITTLE_ENDIAN);

    // Baseline: a local copy of the pre-optimization Slice.indexOf fast-skip scan. It reads the
    // haystack through the Slice instance-field accessors, so it does not get the parameter-array
    // range-check elimination that the static helper below enjoys.
    static int indexOfInline(Slice haystack, Slice pattern)
    {
        int size = haystack.length();
        int patternLength = pattern.length();
        int head = pattern.getIntUnchecked(0);
        int firstByteMask = head & 0xff;
        firstByteMask |= firstByteMask << 8;
        firstByteMask |= firstByteMask << 16;

        int lastValidIndex = size - patternLength;
        int index = 0;
        while (index <= lastValidIndex) {
            int value = haystack.getIntUnchecked(index);
            int valueXor = value ^ firstByteMask;
            int hasZeroBytes = (valueXor - 0x01010101) & ~valueXor & 0x80808080;
            if (hasZeroBytes == 0) {
                index += SIZE_OF_INT;
                continue;
            }
            if (value == head && haystack.equalsUnchecked(index, pattern.byteArray(), pattern.byteArrayOffset(), patternLength)) {
                return index;
            }
            index++;
        }
        return -1;
    }

    // Mirror of Slice.indexOf's fast-skip scan, but as a static method over a parameter array.
    static int indexOfHelper(byte[] base, int start, int end, byte[] pattern)
    {
        int patternLength = pattern.length;
        int head = (int) INT_HANDLE.get(pattern, 0);
        int firstByteMask = head & 0xff;
        firstByteMask |= firstByteMask << 8;
        firstByteMask |= firstByteMask << 16;

        int lastValidIndex = end - patternLength;
        int index = start;
        while (index <= lastValidIndex) {
            int value = (int) INT_HANDLE.get(base, index);
            int valueXor = value ^ firstByteMask;
            int hasZeroBytes = (valueXor - 0x01010101) & ~valueXor & 0x80808080;
            if (hasZeroBytes == 0) {
                index += SIZE_OF_INT;
                continue;
            }
            if (value == head && Arrays.equals(base, index, index + patternLength, pattern, 0, patternLength)) {
                return index;
            }
            index++;
        }
        return -1;
    }

    @State(Scope.Thread)
    public static class SearchData
    {
        @Param({"127", "1024", "8192", "32779"})
        private int size;

        @Param("8")
        private int patternLength;

        private byte[] haystackBytes;
        private byte[] patternBytes;
        private Slice haystack;
        private Slice pattern;

        @Setup(Level.Iteration)
        public void setup()
        {
            haystackBytes = new byte[size];
            // haystack body is all zero; the pattern (all 0x01) only matches at the very end
            patternBytes = new byte[patternLength];
            Arrays.fill(patternBytes, (byte) 0x01);
            System.arraycopy(patternBytes, 0, haystackBytes, size - patternLength, patternLength);

            haystack = Slices.wrappedBuffer(haystackBytes);
            pattern = Slices.wrappedBuffer(patternBytes);
        }
    }

    static void main()
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSearch.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
