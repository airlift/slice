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
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.airlift.slice.SliceUtf8.codePointByteLengths;
import static io.airlift.slice.SliceUtf8.codePointToUtf8;
import static io.airlift.slice.SliceUtf8.compareUtf16BE;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.fixInvalidUtf8;
import static io.airlift.slice.SliceUtf8.fromCodePoints;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.leftTrim;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.lengthOfCodePointFromStartByte;
import static io.airlift.slice.SliceUtf8.lengthOfCodePointSafe;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.airlift.slice.SliceUtf8.reverse;
import static io.airlift.slice.SliceUtf8.rightTrim;
import static io.airlift.slice.SliceUtf8.setCodePointAt;
import static io.airlift.slice.SliceUtf8.substring;
import static io.airlift.slice.SliceUtf8.toCodePoints;
import static io.airlift.slice.SliceUtf8.toLowerCase;
import static io.airlift.slice.SliceUtf8.toUpperCase;
import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.SliceUtf8.tryGetCodePointAt;
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
        byte[] utf8 = data.getUtf8();
        int baseOffset = data.getOffset();
        int byteLength = data.getByteLength();

        int i = 0;
        int codePoints = 0;
        while (i < byteLength) {
            i += lengthOfCodePointFromStartByte(utf8[baseOffset + i]);
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
        int codePoints = countCodePoints(data.getUtf8(), data.getOffset(), data.getByteLength());
        if (codePoints != data.getLength()) {
            throw new AssertionError();
        }
        return codePoints;
    }

    @Benchmark
    public int benchmarkCountCodePointsRange(RangeCountData data)
    {
        int codePoints = countCodePoints(data.getUtf8(), data.getOffset(), data.getRangeLength());
        if (codePoints != data.getExpectedCodePoints()) {
            throw new AssertionError();
        }
        return codePoints;
    }

    @Benchmark
    public int benchmarkOffsetByCodePoints(BenchmarkData data)
    {
        int index = offsetOfCodePoint(data.getUtf8(), data.getOffset(), data.getByteLength(), data.getLength() - 1);
        if (index + lengthOfCodePoint(data.getUtf8(), data.getOffset(), data.getByteLength(), index) != data.getByteLength()) {
            throw new AssertionError();
        }
        return index;
    }

    @Benchmark
    public int benchmarkTryGetCodePointAt(BenchmarkData data)
    {
        byte[] utf8 = data.getUtf8();
        int baseOffset = data.getOffset();
        int byteLength = data.getByteLength();

        int offset = 0;
        int codePoints = 0;
        while (offset < byteLength) {
            int codePoint = tryGetCodePointAt(utf8, baseOffset, byteLength, offset);
            if (codePoint < 0) {
                throw new AssertionError();
            }
            offset += lengthOfCodePoint(codePoint);
            codePoints++;
        }
        if (codePoints != data.getLength()) {
            throw new AssertionError();
        }
        return codePoints;
    }

    @Benchmark
    public int benchmarkGetCodePointAt(BenchmarkData data)
    {
        Slice utf8 = data.getSlice();
        int offset = 0;
        int checksum = 0;
        while (offset < utf8.length()) {
            checksum ^= getCodePointAt(utf8, offset);
            offset += lengthOfCodePoint(utf8, offset);
        }
        return checksum;
    }

    @Benchmark
    public int benchmarkLengthOfCodePointSafe(BenchmarkData data)
    {
        Slice utf8 = data.getSlice();
        int offset = 0;
        int consumed = 0;
        while (offset < utf8.length()) {
            int codePointLength = lengthOfCodePointSafe(utf8, offset);
            offset += codePointLength;
            consumed += codePointLength;
        }
        if (consumed != utf8.length()) {
            throw new AssertionError();
        }
        return consumed;
    }

    @Benchmark
    public int[] benchmarkToCodePointsApi(BenchmarkData data)
    {
        int[] codePoints = toCodePoints(data.getUtf8(), data.getOffset(), data.getByteLength());
        if (codePoints.length != data.getLength()) {
            throw new AssertionError();
        }
        return codePoints;
    }

    @Benchmark
    public int[] benchmarkTrinoCastToCodePointsTwoPass(BenchmarkData data)
    {
        Slice utf8 = data.getSlice();

        int codePointCount = 0;
        for (int position = 0; position < utf8.length(); ) {
            int codePoint = tryGetCodePointAt(utf8, position);
            if (codePoint < 0) {
                throw new AssertionError();
            }
            position += lengthOfCodePoint(codePoint);
            codePointCount++;
        }

        int[] codePoints = new int[codePointCount];
        int position = 0;
        for (int index = 0; index < codePoints.length; index++) {
            codePoints[index] = getCodePointAt(utf8, position);
            position += lengthOfCodePoint(utf8, position);
        }
        return codePoints;
    }

    @Benchmark
    public int benchmarkTrinoToCodePoints(TrinoCodePointData data)
    {
        Slice utf8 = data.getSlice();
        int offset = 0;
        int checksum = 0;
        while (offset < utf8.length()) {
            int codePoint = getCodePointAt(utf8, offset);
            offset += lengthOfCodePoint(utf8, offset);
            checksum ^= codePoint;
        }
        return checksum;
    }

    @Benchmark
    public int benchmarkTrinoToCodePointsLengthFromDecoded(TrinoCodePointData data)
    {
        Slice utf8 = data.getSlice();
        int offset = 0;
        int checksum = 0;
        while (offset < utf8.length()) {
            int codePoint = getCodePointAt(utf8, offset);
            offset += lengthOfCodePoint(codePoint);
            checksum ^= codePoint;
        }
        return checksum;
    }

    @Benchmark
    public int benchmarkTrinoToCodePointsByteArray(TrinoCodePointData data)
    {
        byte[] utf8 = data.getUtf8();
        int baseOffset = data.getOffset();
        int byteLength = data.getByteLength();

        int offset = 0;
        int checksum = 0;
        while (offset < byteLength) {
            int codePoint = getCodePointAt(utf8, baseOffset, byteLength, offset);
            offset += lengthOfCodePoint(codePoint);
            checksum ^= codePoint;
        }
        return checksum;
    }

    @Benchmark
    public Slice benchmarkFromCodePointsApi(CodePointWriteData data)
    {
        Slice result = fromCodePoints(data.getCodePoints());
        if (result.length() != data.getExpectedBytes()) {
            throw new AssertionError();
        }
        return result;
    }

    @Benchmark
    public Slice benchmarkTrinoCodePointsToSliceUtf8Baseline(CodePointWriteData data)
    {
        int[] codePoints = data.getCodePoints();

        int bufferLength = 0;
        for (int codePoint : codePoints) {
            bufferLength += lengthOfCodePoint(codePoint);
        }

        Slice result = Slices.wrappedBuffer(new byte[bufferLength]);
        int offset = 0;
        for (int codePoint : codePoints) {
            offset += setCodePointAt(codePoint, result, offset);
        }
        if (offset != bufferLength) {
            throw new AssertionError();
        }
        return result;
    }

    @Benchmark
    public int benchmarkTrinoPatternConstantPrefixBytes(LikePatternData data)
    {
        Slice pattern = data.getPattern();
        int escapeChar = data.getEscapeChar();

        boolean escaped = false;
        int position = 0;
        while (position < pattern.length()) {
            int currentChar = getCodePointAt(pattern, position);
            if (!escaped && (currentChar == escapeChar)) {
                escaped = true;
            }
            else if (escaped) {
                escaped = false;
            }
            else if ((currentChar == '%') || (currentChar == '_')) {
                return position;
            }
            position += lengthOfCodePoint(currentChar);
        }
        if (escaped) {
            throw new AssertionError();
        }
        return position;
    }

    @Benchmark
    public int benchmarkTrinoPatternConstantPrefixBytesByteArray(LikePatternData data)
    {
        Slice pattern = data.getPattern();
        byte[] utf8 = pattern.byteArray();
        int baseOffset = pattern.byteArrayOffset();
        int byteLength = pattern.length();
        int escapeChar = data.getEscapeChar();

        boolean escaped = false;
        int position = 0;
        while (position < byteLength) {
            int currentChar = getCodePointAt(utf8, baseOffset, byteLength, position);
            if (!escaped && (currentChar == escapeChar)) {
                escaped = true;
            }
            else if (escaped) {
                escaped = false;
            }
            else if ((currentChar == '%') || (currentChar == '_')) {
                return position;
            }
            position += lengthOfCodePoint(currentChar);
        }
        if (escaped) {
            throw new AssertionError();
        }
        return position;
    }

    @Benchmark
    public int benchmarkTrinoPadStringCodePointLengths(TrinoPadData data)
    {
        Slice padString = data.getPadString();
        int padStringLength = countCodePoints(padString);
        int[] padStringCounts = new int[padStringLength];
        for (int index = 0; index < padStringLength; index++) {
            padStringCounts[index] = lengthOfCodePointSafe(padString, offsetOfCodePoint(padString, index));
        }
        return checksum(padStringCounts);
    }

    @Benchmark
    public int benchmarkTrinoPadStringCodePointLengthsSinglePass(TrinoPadData data)
    {
        Slice padString = data.getPadString();
        int[] padStringCounts = new int[countCodePoints(padString)];
        int position = 0;
        int index = 0;
        while (position < padString.length()) {
            int codePoint = getCodePointAt(padString, position);
            int codePointLength = lengthOfCodePoint(codePoint);
            padStringCounts[index] = codePointLength;
            index++;
            position += codePointLength;
        }
        if (index != padStringCounts.length) {
            throw new AssertionError();
        }
        return checksum(padStringCounts);
    }

    @Benchmark
    public int benchmarkTrinoPadStringCodePointLengthsByteArray(TrinoPadData data)
    {
        byte[] utf8 = data.getUtf8();
        int baseOffset = data.getOffset();
        int byteLength = data.getByteLength();
        int[] padStringCounts = new int[countCodePoints(utf8, baseOffset, byteLength)];
        int position = 0;
        int index = 0;
        while (position < byteLength) {
            int codePoint = getCodePointAt(utf8, baseOffset, byteLength, position);
            int codePointLength = lengthOfCodePoint(codePoint);
            padStringCounts[index] = codePointLength;
            index++;
            position += codePointLength;
        }
        if (index != padStringCounts.length) {
            throw new AssertionError();
        }
        return checksum(padStringCounts);
    }

    @Benchmark
    public int benchmarkTrinoPadStringCodePointLengthsSliceUtf8Helper(TrinoPadData data)
    {
        return checksum(codePointByteLengths(data.getPadString()));
    }

    @Benchmark
    public int benchmarkTrinoPadStringCodePointLengthsSliceUtf8HelperByteArray(TrinoPadData data)
    {
        return checksum(codePointByteLengths(data.getUtf8(), data.getOffset(), data.getByteLength()));
    }

    @Benchmark
    public Slice benchmarkTrinoDomainTranslatorPrefixRange(TrinoPrefixRangeData data)
    {
        Slice constantPrefix = data.getConstantPrefix();

        int lastIncrementable = -1;
        for (int position = 0; position < constantPrefix.length(); position += lengthOfCodePoint(constantPrefix, position)) {
            if (getCodePointAt(constantPrefix, position) < 127) {
                lastIncrementable = position;
            }
        }

        if (lastIncrementable == -1) {
            return Slices.EMPTY_SLICE;
        }

        Slice upperBound = constantPrefix.slice(0, lastIncrementable + lengthOfCodePoint(constantPrefix, lastIncrementable)).copy();
        setCodePointAt(getCodePointAt(constantPrefix, lastIncrementable) + 1, upperBound, lastIncrementable);
        return upperBound;
    }

    @Benchmark
    public Slice benchmarkTrinoDomainTranslatorPrefixRangeSingleDecode(TrinoPrefixRangeData data)
    {
        byte[] utf8 = data.getUtf8();
        int baseOffset = data.getOffset();
        int byteLength = data.getByteLength();
        Slice constantPrefix = data.getConstantPrefix();

        int lastIncrementableOffset = -1;
        int lastIncrementableCodePoint = -1;
        int lastIncrementableLength = 0;
        int position = 0;
        while (position < byteLength) {
            int codePoint = getCodePointAt(utf8, baseOffset, byteLength, position);
            int codePointLength = lengthOfCodePoint(codePoint);
            if (codePoint < 127) {
                lastIncrementableOffset = position;
                lastIncrementableCodePoint = codePoint;
                lastIncrementableLength = codePointLength;
            }
            position += codePointLength;
        }

        if (lastIncrementableOffset == -1) {
            return Slices.EMPTY_SLICE;
        }

        Slice upperBound = constantPrefix.slice(0, lastIncrementableOffset + lastIncrementableLength).copy();
        setCodePointAt(lastIncrementableCodePoint + 1, upperBound, lastIncrementableOffset);
        return upperBound;
    }

    @Benchmark
    public int benchmarkCompareUtf16BE(CompareData data)
    {
        int result = compareUtf16BE(
                data.getUtf8(), data.getOffset(), data.getByteLength(),
                data.getRightUtf8(), data.getRightOffset(), data.getRightByteLength());
        if (result != 0) {
            throw new AssertionError();
        }
        return result;
    }

    @Benchmark
    public Slice benchmarkSubstring(BenchmarkData data)
    {
        int length = data.getLength();
        return substring(data.getUtf8(), data.getOffset(), data.getByteLength(), (length / 2) - 1, length / 2);
    }

    @Benchmark
    public Slice benchmarkReverse(BenchmarkData data)
    {
        return reverse(data.getUtf8(), data.getOffset(), data.getByteLength());
    }

    @Benchmark
    public Slice benchmarkToLowerCase(BenchmarkData data)
    {
        return toLowerCase(data.getUtf8(), data.getOffset(), data.getByteLength());
    }

    @Benchmark
    public Slice benchmarkToUpperCase(BenchmarkData data)
    {
        return toUpperCase(data.getUtf8(), data.getOffset(), data.getByteLength());
    }

    @Benchmark
    public Slice benchmarkLeftTrim(WhitespaceData data)
    {
        return leftTrim(data.getLeftWhitespace(), 0, data.getLeftWhitespace().length);
    }

    @Benchmark
    public Slice benchmarkLeftTrimCustom(WhitespaceData data)
    {
        return leftTrim(data.getLeftWhitespace(), 0, data.getLeftWhitespace().length, data.getTrimCodePoints());
    }

    @Benchmark
    public Slice benchmarkRightTrim(WhitespaceData data)
    {
        return rightTrim(data.getRightWhitespace(), 0, data.getRightWhitespace().length);
    }

    @Benchmark
    public Slice benchmarkRightTrimCustom(WhitespaceData data)
    {
        return rightTrim(data.getRightWhitespace(), 0, data.getRightWhitespace().length, data.getTrimCodePoints());
    }

    @Benchmark
    public Slice benchmarkTrim(WhitespaceData data)
    {
        return trim(data.getBothWhitespace(), 0, data.getBothWhitespace().length);
    }

    @Benchmark
    public Slice benchmarkTrimCustom(WhitespaceData data)
    {
        return trim(data.getBothWhitespace(), 0, data.getBothWhitespace().length, data.getTrimCodePoints());
    }

    @Benchmark
    public Slice benchmarkFixInvalidUtf8WithReplacement(FixInvalidUtf8Data data)
    {
        return fixInvalidUtf8(data.getUtf8(), data.getOffset(), data.getLength());
    }

    @Benchmark
    public Slice benchmarkFixInvalidUtf8WithoutReplacement(FixInvalidUtf8Data data)
    {
        return fixInvalidUtf8(data.getUtf8(), data.getOffset(), data.getLength(), OptionalInt.empty());
    }

    @Benchmark
    public int benchmarkSetCodePointAt(CodePointWriteData data)
    {
        Slice output = data.getOutput();
        int position = 0;
        int[] codePoints = data.getCodePoints();
        for (int codePoint : codePoints) {
            position += setCodePointAt(codePoint, output, position);
        }
        if (position != data.getExpectedBytes()) {
            throw new AssertionError();
        }
        return position;
    }

    @Benchmark
    public int benchmarkCodePointToUtf8(CodePointWriteData data)
    {
        int totalBytes = 0;
        for (int codePoint : data.getCodePoints()) {
            totalBytes += codePointToUtf8(codePoint).length();
        }
        if (totalBytes != data.getExpectedBytes()) {
            throw new AssertionError();
        }
        return totalBytes;
    }

    private static int checksum(int[] values)
    {
        int checksum = 1;
        for (int value : values) {
            checksum = (31 * checksum) ^ value;
        }
        return checksum;
    }

    private static int checksum(byte[] values)
    {
        int checksum = 1;
        for (byte value : values) {
            checksum = (31 * checksum) ^ value;
        }
        return checksum;
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

        private byte[] utf8;
        private int offset;
        private int byteLength;
        private Slice slice;

        @Setup
        public void setup()
        {
            int[] codePointSet = ascii ? ASCII_CODE_POINTS : ALL_CODE_POINTS;
            ThreadLocalRandom random = ThreadLocalRandom.current();

            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(length * 4);
            for (int i = 0; i < length; i++) {
                int codePoint = codePointSet[random.nextInt(codePointSet.length)];
                sliceOutput.appendBytes(new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8));
            }

            byte[] data = sliceOutput.slice().getBytes();
            offset = 7;
            utf8 = new byte[offset + data.length + 3];
            System.arraycopy(data, 0, utf8, offset, data.length);
            byteLength = data.length;
            slice = Slices.wrappedBuffer(utf8, offset, byteLength);
        }

        public byte[] getUtf8()
        {
            return utf8;
        }

        public int getOffset()
        {
            return offset;
        }

        public int getByteLength()
        {
            return byteLength;
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
    public static class CompareData
            extends BenchmarkData
    {
        private byte[] rightUtf8;
        private int rightOffset;
        private int rightByteLength;

        @Override
        @Setup
        public void setup()
        {
            super.setup();
            rightOffset = 11;
            rightByteLength = getByteLength();
            rightUtf8 = new byte[rightOffset + rightByteLength + 5];
            System.arraycopy(getUtf8(), getOffset(), rightUtf8, rightOffset, rightByteLength);
        }

        public byte[] getRightUtf8()
        {
            return rightUtf8;
        }

        public int getRightOffset()
        {
            return rightOffset;
        }

        public int getRightByteLength()
        {
            return rightByteLength;
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

        private byte[] leftWhitespace;
        private byte[] rightWhitespace;
        private byte[] bothWhitespace;
        private int[] trimCodePoints;

        @Setup
        public void setup()
        {
            trimCodePoints = ascii ? ASCII_WHITESPACE : ALL_WHITESPACE;
            byte[] whitespace = createRandomUtf8Bytes(trimCodePoints, length + 1);

            leftWhitespace = whitespace.clone();
            leftWhitespace[leftWhitespace.length - 1] = 'X';

            rightWhitespace = whitespace.clone();
            rightWhitespace[0] = 'X';

            bothWhitespace = whitespace.clone();
            bothWhitespace[bothWhitespace.length / 2] = 'X';
        }

        private static byte[] createRandomUtf8Bytes(int[] codePointSet, int length)
        {
            int[] codePoints = new int[length];
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0; i < codePoints.length; i++) {
                int codePoint = codePointSet[random.nextInt(codePointSet.length)];
                codePoints[i] = codePoint;
            }
            return new String(codePoints, 0, codePoints.length).getBytes(StandardCharsets.UTF_8);
        }

        public byte[] getLeftWhitespace()
        {
            return leftWhitespace;
        }

        public byte[] getRightWhitespace()
        {
            return rightWhitespace;
        }

        public byte[] getBothWhitespace()
        {
            return bothWhitespace;
        }

        public int[] getTrimCodePoints()
        {
            return trimCodePoints;
        }
    }

    @State(Thread)
    public static class RangeCountData
    {
        @Param({"true", "false"})
        private boolean ascii;

        @Param("1000")
        private int rangeLengthCodePoints;

        @Param({"1", "7", "31"})
        private int offsetBytes;

        private byte[] utf8;
        private int offset;
        private int rangeLength;
        private int expectedCodePoints;

        @Setup
        public void setup()
        {
            int[] codePointSet = ascii ? BenchmarkData.ASCII_CODE_POINTS : BenchmarkData.ALL_CODE_POINTS;
            ThreadLocalRandom random = ThreadLocalRandom.current();

            DynamicSliceOutput sliceOutput = new DynamicSliceOutput((rangeLengthCodePoints * 4) + offsetBytes);

            // Fixed-width ASCII prefix guarantees a non-zero, deterministic byte offset.
            for (int i = 0; i < offsetBytes; i++) {
                sliceOutput.appendByte('x');
            }

            offset = offsetBytes;

            for (int i = 0; i < rangeLengthCodePoints; i++) {
                int codePoint = codePointSet[random.nextInt(codePointSet.length)];
                sliceOutput.appendBytes(new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8));
            }

            utf8 = sliceOutput.slice().getBytes();
            rangeLength = utf8.length - offset;
            expectedCodePoints = rangeLengthCodePoints;
        }

        public byte[] getUtf8()
        {
            return utf8;
        }

        public int getOffset()
        {
            return offset;
        }

        public int getRangeLength()
        {
            return rangeLength;
        }

        public int getExpectedCodePoints()
        {
            return expectedCodePoints;
        }
    }

    @State(Thread)
    public static class FixInvalidUtf8Data
    {
        @Param({"ascii", "valid_non_ascii", "invalid_non_ascii"})
        private String inputKind;

        @Param("1024")
        private int inputLength;

        private byte[] utf8;
        private int offset;
        private int length;

        @Setup
        public void setup()
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            if (inputKind.equals("ascii")) {
                int[] asciiCodePoints = BenchmarkData.ASCII_CODE_POINTS;
                int[] codePoints = new int[inputLength];
                for (int i = 0; i < codePoints.length; i++) {
                    codePoints[i] = asciiCodePoints[random.nextInt(asciiCodePoints.length)];
                }
                setPaddedInput(new String(codePoints, 0, codePoints.length).getBytes(StandardCharsets.UTF_8));
                return;
            }

            DynamicSliceOutput out = new DynamicSliceOutput(inputLength * 4);
            int[] allCodePoints = BenchmarkData.ALL_CODE_POINTS;
            for (int i = 0; i < inputLength; i++) {
                int codePoint = allCodePoints[random.nextInt(allCodePoints.length)];
                out.appendBytes(new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8));
            }
            byte[] input = out.slice().getBytes();

            if (inputKind.equals("invalid_non_ascii") && input.length > 8) {
                // Insert an illegal byte to force invalid UTF-8 handling.
                input[input.length / 2] = (byte) 0xFF;
            }

            setPaddedInput(input);
        }

        private void setPaddedInput(byte[] input)
        {
            offset = 5;
            utf8 = new byte[offset + input.length + 3];
            System.arraycopy(input, 0, utf8, offset, input.length);
            length = input.length;
        }

        public byte[] getUtf8()
        {
            return utf8;
        }

        public int getOffset()
        {
            return offset;
        }

        public int getLength()
        {
            return length;
        }
    }

    @State(Thread)
    public static class TrinoCodePointData
            extends BenchmarkData
    {
        // Uses BenchmarkData setup and exposes the Slice-based access pattern used in Trino loops.
    }

    @State(Thread)
    public static class LikePatternData
    {
        @Param("1000")
        private int length;

        @Param({"true", "false"})
        private boolean ascii;

        private Slice pattern;

        @Setup
        public void setup()
        {
            int[] codePointSet = ascii ? BenchmarkData.ASCII_CODE_POINTS : BenchmarkData.ALL_CODE_POINTS;
            ThreadLocalRandom random = ThreadLocalRandom.current();
            DynamicSliceOutput out = new DynamicSliceOutput((length * 4) + 1);

            for (int i = 0; i < length - 1; i++) {
                int codePoint;
                do {
                    codePoint = codePointSet[random.nextInt(codePointSet.length)];
                }
                while (codePoint == '%' || codePoint == '_' || codePoint == '\\');
                out.appendBytes(new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8));
            }
            out.appendByte('%');

            byte[] encoded = out.slice().getBytes();
            byte[] padded = new byte[3 + encoded.length + 2];
            System.arraycopy(encoded, 0, padded, 3, encoded.length);
            pattern = Slices.wrappedBuffer(padded, 3, encoded.length);
        }

        public Slice getPattern()
        {
            return pattern;
        }

        public int getEscapeChar()
        {
            return -1;
        }
    }

    @State(Thread)
    public static class TrinoPadData
    {
        @Param("128")
        private int length;

        @Param({"true", "false"})
        private boolean ascii;

        private byte[] utf8;
        private int offset;
        private int byteLength;
        private Slice padString;

        @Setup
        public void setup()
        {
            int[] codePointSet = ascii ? BenchmarkData.ASCII_CODE_POINTS : BenchmarkData.ALL_CODE_POINTS;
            ThreadLocalRandom random = ThreadLocalRandom.current();
            DynamicSliceOutput out = new DynamicSliceOutput(length * 4);
            for (int index = 0; index < length; index++) {
                int codePoint = codePointSet[random.nextInt(codePointSet.length)];
                out.appendBytes(new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8));
            }

            byte[] encoded = out.slice().getBytes();
            offset = 9;
            utf8 = new byte[offset + encoded.length + 3];
            System.arraycopy(encoded, 0, utf8, offset, encoded.length);
            byteLength = encoded.length;
            padString = Slices.wrappedBuffer(utf8, offset, byteLength);
        }

        public byte[] getUtf8()
        {
            return utf8;
        }

        public int getOffset()
        {
            return offset;
        }

        public int getByteLength()
        {
            return byteLength;
        }

        public Slice getPadString()
        {
            return padString;
        }
    }

    @State(Thread)
    public static class TrinoPrefixRangeData
    {
        @Param("256")
        private int length;

        @Param({"true", "false"})
        private boolean ascii;

        private byte[] utf8;
        private int offset;
        private int byteLength;
        private Slice constantPrefix;

        @Setup
        public void setup()
        {
            int[] codePointSet = ascii ? BenchmarkData.ASCII_CODE_POINTS : BenchmarkData.ALL_CODE_POINTS;
            ThreadLocalRandom random = ThreadLocalRandom.current();

            int[] codePoints = new int[length];
            codePoints[0] = 'a';
            for (int index = 1; index < codePoints.length; index++) {
                codePoints[index] = codePointSet[random.nextInt(codePointSet.length)];
            }

            DynamicSliceOutput out = new DynamicSliceOutput(length * 4);
            for (int codePoint : codePoints) {
                out.appendBytes(new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8));
            }

            byte[] encoded = out.slice().getBytes();
            offset = 13;
            utf8 = new byte[offset + encoded.length + 5];
            System.arraycopy(encoded, 0, utf8, offset, encoded.length);
            byteLength = encoded.length;
            constantPrefix = Slices.wrappedBuffer(utf8, offset, byteLength);
        }

        public byte[] getUtf8()
        {
            return utf8;
        }

        public int getOffset()
        {
            return offset;
        }

        public int getByteLength()
        {
            return byteLength;
        }

        public Slice getConstantPrefix()
        {
            return constantPrefix;
        }
    }

    @State(Thread)
    public static class CodePointWriteData
    {
        @Param("1000")
        private int length;

        @Param({"true", "false"})
        private boolean ascii;

        private int[] codePoints;
        private Slice output;
        private int expectedBytes;

        @Setup
        public void setup()
        {
            int[] codePointSet = ascii ? BenchmarkData.ASCII_CODE_POINTS : BenchmarkData.ALL_CODE_POINTS;
            ThreadLocalRandom random = ThreadLocalRandom.current();

            codePoints = new int[length];
            expectedBytes = 0;
            for (int i = 0; i < codePoints.length; i++) {
                int codePoint = codePointSet[random.nextInt(codePointSet.length)];
                codePoints[i] = codePoint;
                expectedBytes += lengthOfCodePoint(codePoint);
            }

            byte[] buffer = new byte[11 + expectedBytes + 5];
            output = Slices.wrappedBuffer(buffer, 11, expectedBytes);
        }

        public int[] getCodePoints()
        {
            return codePoints;
        }

        public Slice getOutput()
        {
            return output;
        }

        public int getExpectedBytes()
        {
            return expectedBytes;
        }
    }

    static void main()
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + SliceUtf8Benchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
