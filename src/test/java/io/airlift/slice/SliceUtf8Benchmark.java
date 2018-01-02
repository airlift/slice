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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.leftTrim;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.lengthOfCodePointFromStartByte;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.airlift.slice.SliceUtf8.reverse;
import static io.airlift.slice.SliceUtf8.rightTrim;
import static io.airlift.slice.SliceUtf8.substring;
import static io.airlift.slice.SliceUtf8.toLowerCase;
import static io.airlift.slice.SliceUtf8.toUpperCase;
import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.SURROGATE;
import static java.lang.Character.getType;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 4, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = MILLISECONDS)
public class SliceUtf8Benchmark
{
    @Benchmark
    public int benchmarkLengthOfCodePointFromStartByte(BenchmarkData data)
    {
        Slice slice = data.getSlice();
        int i = 0;
        int codePoints = 0;
        while (i < slice.length()) {
            i += lengthOfCodePointFromStartByte(slice.getByte(i));
            codePoints++;
        }
        if (codePoints != data.getLength()) {
            throw new AssertionError();
        }
        return codePoints;
    }

    @Benchmark
    public int benchmarkCountCodePoints(BenchmarkData data)
    {
        int codePoints = countCodePoints(data.getSlice());
        if (codePoints != data.getLength()) {
            throw new AssertionError();
        }
        return codePoints;
    }

    @Benchmark
    public int benchmarkOffsetByCodePoints(BenchmarkData data)
    {
        Slice slice = data.getSlice();
        int offset = offsetOfCodePoint(slice, data.getLength() - 1);
        if (offset + lengthOfCodePoint(slice, offset) != slice.length()) {
            throw new AssertionError();
        }
        return offset;
    }

    @Benchmark
    public Slice benchmarkSubstring(BenchmarkData data)
    {
        Slice slice = data.getSlice();
        int length = data.getLength();
        return substring(slice, (length / 2) - 1, length / 2);
    }

    @Benchmark
    public Slice benchmarkReverse(BenchmarkData data)
    {
        return reverse(data.getSlice());
    }

    @Benchmark
    public Slice benchmarkToLowerCase(BenchmarkData data)
    {
        return toLowerCase(data.getSlice());
    }

    @Benchmark
    public Slice benchmarkToUpperCase(BenchmarkData data)
    {
        return toUpperCase(data.getSlice());
    }

    @Benchmark
    public Slice benchmarkLeftTrim(WhitespaceData data)
    {
        return leftTrim(data.getLeftWhitespace());
    }

    @Benchmark
    public Slice benchmarkRightTrim(WhitespaceData data)
    {
        return rightTrim(data.getRightWhitespace());
    }

    @Benchmark
    public Slice benchmarkTrim(WhitespaceData data)
    {
        return trim(data.getBothWhitespace());
    }

    @State(Thread)
    public static class BenchmarkData
    {
        private static final int[] ASCII_CODE_POINTS;
        private static final int[] ALL_CODE_POINTS;

        static {
            ASCII_CODE_POINTS = IntStream.rangeClosed(0, 0x7F)
                    .toArray();
            ALL_CODE_POINTS = IntStream.rangeClosed(0, MAX_CODE_POINT)
                    .filter(codePoint -> getType(codePoint) != SURROGATE)
                    .toArray();
        }

        @Param({"2", "5", "10", "100", "1000", "10000"})
        private int length;

        @Param({"true", "false"})
        private boolean ascii;

        private Slice slice;
        private int[] codePoints;

        @Setup
        public void setup()
        {
            int[] codePointSet = ascii ? ASCII_CODE_POINTS : ALL_CODE_POINTS;
            ThreadLocalRandom random = ThreadLocalRandom.current();

            codePoints = new int[length];
            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(length * 4);
            for (int i = 0; i < codePoints.length; i++) {
                int codePoint = codePointSet[random.nextInt(codePointSet.length)];
                codePoints[i] = codePoint;
                sliceOutput.appendBytes(new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8));
            }
            slice = sliceOutput.slice();
        }

        public Slice getSlice()
        {
            return slice;
        }

        public int getLength()
        {
            return length;
        }
    }

    @State(Thread)
    public static class WhitespaceData
    {
        private static final int[] ASCII_WHITESPACE;
        private static final int[] ALL_WHITESPACE;

        static {
            ASCII_WHITESPACE = IntStream.rangeClosed(0, 0x7F)
                    .filter(Character::isWhitespace)
                    .toArray();
            ALL_WHITESPACE = IntStream.rangeClosed(0, MAX_CODE_POINT)
                    .filter(Character::isWhitespace)
                    .toArray();
        }

        @Param({"2", "5", "10", "100", "1000", "10000"})
        private int length;

        @Param({"true", "false"})
        private boolean ascii;

        private Slice leftWhitespace;
        private Slice rightWhitespace;
        private Slice bothWhitespace;

        @Setup
        public void setup()
        {
            Slice whitespace = createRandomUtf8Slice(ascii ? ASCII_WHITESPACE : ALL_WHITESPACE, length + 1);
            leftWhitespace = Slices.copyOf(whitespace);
            leftWhitespace.setByte(leftWhitespace.length() - 1, 'X');
            rightWhitespace = Slices.copyOf(whitespace);
            rightWhitespace.setByte(0, 'X');
            bothWhitespace = Slices.copyOf(whitespace);
            bothWhitespace.setByte(length / 2, 'X');
        }

        private static Slice createRandomUtf8Slice(int[] codePointSet, int length)
        {
            int[] codePoints = new int[length];
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0; i < codePoints.length; i++) {
                int codePoint = codePointSet[random.nextInt(codePointSet.length)];
                codePoints[i] = codePoint;
            }
            return utf8Slice(new String(codePoints, 0, codePoints.length));
        }

        public int getLength()
        {
            return length;
        }

        public Slice getLeftWhitespace()
        {
            return leftWhitespace;
        }

        public Slice getRightWhitespace()
        {
            return rightWhitespace;
        }

        public Slice getBothWhitespace()
        {
            return bothWhitespace;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + SliceUtf8Benchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
