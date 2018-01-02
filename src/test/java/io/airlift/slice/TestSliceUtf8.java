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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static com.google.common.primitives.Bytes.concat;
import static io.airlift.slice.SliceUtf8.codePointToUtf8;
import static io.airlift.slice.SliceUtf8.compareUtf16BE;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.fixInvalidUtf8;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.getCodePointBefore;
import static io.airlift.slice.SliceUtf8.isAscii;
import static io.airlift.slice.SliceUtf8.leftTrim;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.lengthOfCodePointFromStartByte;
import static io.airlift.slice.SliceUtf8.lengthOfCodePointSafe;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.airlift.slice.SliceUtf8.reverse;
import static io.airlift.slice.SliceUtf8.rightTrim;
import static io.airlift.slice.SliceUtf8.setCodePointAt;
import static io.airlift.slice.SliceUtf8.substring;
import static io.airlift.slice.SliceUtf8.toLowerCase;
import static io.airlift.slice.SliceUtf8.toUpperCase;
import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MIN_SURROGATE;
import static java.lang.Character.SURROGATE;
import static java.lang.Character.getType;
import static java.lang.Integer.signum;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.IntStream.concat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSliceUtf8
{
    private static final int[] ASCII_CODE_POINTS;
    private static final String STRING_ASCII_CODE_POINTS;
    private static final int[] ALL_CODE_POINTS;
    private static final String STRING_ALL_CODE_POINTS;
    private static final int[] WHITESPACE_CODE_POINTS;
    private static final int[] ALL_CODE_POINTS_RANDOM;
    private static final String STRING_ALL_CODE_POINTS_RANDOM;

    private static final byte START_1_BYTE = (byte) 0b0111_1111;
    private static final byte CONTINUATION_BYTE = (byte) 0b1011_1111;
    private static final byte START_2_BYTE = (byte) 0b1101_1111;
    private static final byte START_3_BYTE = (byte) 0b1110_1111;
    private static final byte START_4_BYTE = (byte) 0b1111_0111;
    private static final byte START_5_BYTE = (byte) 0b1111_1011;
    private static final byte START_6_BYTE = (byte) 0b1111_1101;
    private static final byte INVALID_FE_BYTE = (byte) 0b11111110;
    private static final byte INVALID_FF_BYTE = (byte) 0b11111111;
    private static final byte X_CHAR = (byte) 'X';

    private static final List<byte[]> INVALID_SEQUENCES;

    static {
        ASCII_CODE_POINTS = IntStream.rangeClosed(0, 0x7F)
                .toArray();
        STRING_ASCII_CODE_POINTS = new String(ASCII_CODE_POINTS, 0, ASCII_CODE_POINTS.length);

        ALL_CODE_POINTS = IntStream.rangeClosed(0, MAX_CODE_POINT)
                .filter(codePoint -> getType(codePoint) != SURROGATE)
                .toArray();
        STRING_ALL_CODE_POINTS = new String(ALL_CODE_POINTS, 0, ALL_CODE_POINTS.length);
        WHITESPACE_CODE_POINTS = IntStream.rangeClosed(0, MAX_CODE_POINT)
                .filter(Character::isWhitespace)
                .toArray();

        ALL_CODE_POINTS_RANDOM = Arrays.copyOf(ALL_CODE_POINTS, ALL_CODE_POINTS.length);
        Collections.shuffle(Arrays.asList(ALL_CODE_POINTS_RANDOM));
        STRING_ALL_CODE_POINTS_RANDOM = new String(ALL_CODE_POINTS_RANDOM, 0, ALL_CODE_POINTS_RANDOM.length);

        ImmutableList.Builder<byte[]> invalidSequences = ImmutableList.builder();
        invalidSequences.add(new byte[] {CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {START_2_BYTE});
        invalidSequences.add(new byte[] {START_3_BYTE});
        invalidSequences.add(new byte[] {START_3_BYTE, CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {START_4_BYTE});
        invalidSequences.add(new byte[] {START_4_BYTE, CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {START_4_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE});
        // 4 byte sequence is limited to 10FFFF
        invalidSequences.add(new byte[] {START_4_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {START_5_BYTE});
        invalidSequences.add(new byte[] {START_5_BYTE, CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {START_5_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {START_5_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {START_5_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {START_6_BYTE});
        invalidSequences.add(new byte[] {START_6_BYTE, CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {START_6_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {START_6_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {START_6_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {START_6_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE});
        invalidSequences.add(new byte[] {INVALID_FF_BYTE});

        // min and max surrogate characters
        invalidSequences.add(new byte[] {(byte) 0b11101101, (byte) 0xA0, (byte) 0x80});
        invalidSequences.add(new byte[] {(byte) 0b11101101, (byte) 0xBF, (byte) 0xBF});

        // check overlong encoding
        invalidSequences.add(new byte[] {(byte) 0b11000000, (byte) 0xAF}); // 2-byte encoding of 0x2F
        invalidSequences.add(new byte[] {(byte) 0b11000001, (byte) 0xBF}); // 2-byte encoding of 0x7F
        invalidSequences.add(new byte[] {(byte) 0b11100000, (byte) 0x81, (byte) 0xBF}); // 3-byte encoding of 0x7F
        invalidSequences.add(new byte[] {(byte) 0b11100000, (byte) 0x90, (byte) 0x80}); // 3-byte encoding of 0x400
        invalidSequences.add(new byte[] {(byte) 0b11100000, (byte) 0x9F, (byte) 0xBF}); // 3-byte encoding of 0x7FF
        invalidSequences.add(new byte[] {(byte) 0b11110000, (byte) 0x8D, (byte) 0xA0, (byte) 0x80}); // 4-byte encoding of 0xD800
        invalidSequences.add(new byte[] {(byte) 0b11110000, (byte) 0x8D, (byte) 0xBF, (byte) 0xBF}); // 4-byte encoding of 0xDFFF
        invalidSequences.add(new byte[] {(byte) 0b11110000, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF}); // 4-byte encoding of 0xFFFF

        INVALID_SEQUENCES = invalidSequences.build();
    }

    private static final int[] UTF_16_BOUNDARY_CODE_POINTS = new int[] {0x0000, 0xD7FF, 0xE000, 0xFFFF, 0x10000, 0x10FFFF};
    private static final int[] UTF_16_TESTING_CODE_POINTS;

    static {
        Random random = new Random(0);
        IntStream utf16TestCodePoints = IntStream.of(UTF_16_BOUNDARY_CODE_POINTS);
        utf16TestCodePoints = concat(utf16TestCodePoints, random.ints(200, 0x0000, 0xD800));
        utf16TestCodePoints = concat(utf16TestCodePoints, random.ints(200, 0xE000, 0x10000));
        utf16TestCodePoints = concat(utf16TestCodePoints, random.ints(200, 0x10000, 0x110000));
        UTF_16_TESTING_CODE_POINTS = utf16TestCodePoints.toArray();
    }

    private static final String STRING_EMPTY = "";
    private static final String STRING_HELLO = "hello";
    private static final String STRING_QUADRATICALLY = "Quadratically";
    private static final String STRING_OESTERREICH = "\u00D6sterreich";
    private static final String STRING_DULIOE_DULIOE = "Duli\u00F6 duli\u00F6";
    private static final String STRING_FAITH_HOPE_LOVE = "\u4FE1\u5FF5,\u7231,\u5E0C\u671B";
    private static final String STRING_NAIVE = "na\u00EFve";
    private static final String STRING_OO = "\uD801\uDC2Dend";
    // length increase when cast to lower case, and ends with invalid character
    private static final byte[] INVALID_SEQUENCE_TO_LOWER_EXPANDS = new byte[] {(byte) 0xC8, (byte) 0xBA, (byte) 0xFF};

    private static final byte[] INVALID_UTF8_1 = new byte[] {-127};
    private static final byte[] INVALID_UTF8_2 = new byte[] {50, -127, 52, 50};

    private static final byte[] EM_SPACE_SURROUNDED_BY_CONTINUATION_BYTE =
            new byte[] {CONTINUATION_BYTE, (byte) 0xE2, (byte) 0x80, (byte) 0x83, CONTINUATION_BYTE};

    @Test
    public void testCodePointCount()
    {
        assertCodePointCount(STRING_EMPTY);
        assertCodePointCount(STRING_HELLO);
        assertCodePointCount(STRING_QUADRATICALLY);
        assertCodePointCount(STRING_OESTERREICH);
        assertCodePointCount(STRING_DULIOE_DULIOE);
        assertCodePointCount(STRING_FAITH_HOPE_LOVE);
        assertCodePointCount(STRING_NAIVE);
        assertCodePointCount(STRING_OO);
        assertCodePointCount(STRING_ASCII_CODE_POINTS);
        assertCodePointCount(STRING_ALL_CODE_POINTS);
        assertCodePointCount(STRING_ALL_CODE_POINTS_RANDOM);

        assertEquals(countCodePoints(wrappedBuffer(START_1_BYTE)), 1);
        assertEquals(countCodePoints(wrappedBuffer(START_2_BYTE)), 1);
        assertEquals(countCodePoints(wrappedBuffer(START_3_BYTE)), 1);
        assertEquals(countCodePoints(wrappedBuffer(START_4_BYTE)), 1);
        assertEquals(countCodePoints(wrappedBuffer(START_5_BYTE)), 1);
        assertEquals(countCodePoints(wrappedBuffer(START_6_BYTE)), 1);
        assertEquals(countCodePoints(wrappedBuffer(INVALID_FE_BYTE)), 1);
        assertEquals(countCodePoints(wrappedBuffer(INVALID_FF_BYTE)), 1);
        assertEquals(countCodePoints(wrappedBuffer(CONTINUATION_BYTE)), 0);
    }

    private static void assertCodePointCount(String string)
    {
        assertEquals(countCodePoints(utf8Slice(string)), string.codePoints().count());
    }

    @Test
    public void testOffsetByCodePoints()
    {
        assertEquals(offsetOfCodePoint(EMPTY_SLICE, 0), -1);
        assertOffsetByCodePoints(STRING_HELLO);
        assertOffsetByCodePoints(STRING_QUADRATICALLY);
        assertOffsetByCodePoints(STRING_OESTERREICH);
        assertOffsetByCodePoints(STRING_DULIOE_DULIOE);
        assertOffsetByCodePoints(STRING_FAITH_HOPE_LOVE);
        assertOffsetByCodePoints(STRING_NAIVE);
        assertOffsetByCodePoints(STRING_OO);
        assertOffsetByCodePoints(STRING_ASCII_CODE_POINTS);
        assertOffsetByCodePoints(STRING_ALL_CODE_POINTS);
        assertOffsetByCodePoints(STRING_ALL_CODE_POINTS_RANDOM);
    }

    private static void assertOffsetByCodePoints(String string)
    {
        Slice utf8 = utf8Slice(string);

        int codePoints = (int) string.codePoints().count();
        int lastIndex = 0;
        int characterIndex = 0;
        for (int codePointIndex = 0; codePointIndex < codePoints; codePointIndex++) {
            int expectedIndex = 0;

            // calculate the expected index by searching forward from the last index
            if (codePointIndex > 0) {
                expectedIndex = lastIndex + lengthOfCodePoint(string.codePointAt(characterIndex));
                characterIndex = string.offsetByCodePoints(characterIndex, 1);
            }
            // avoid n^2 performance for large test string
            if (codePointIndex < 10000) {
                assertEquals(offsetOfCodePoint(utf8, codePointIndex), expectedIndex);
            }

            if (codePointIndex > 0) {
                assertEquals(offsetOfCodePoint(utf8, lastIndex, 1), expectedIndex);
            }
            lastIndex = expectedIndex;
        }
        assertEquals(offsetOfCodePoint(utf8Slice(string), codePoints), -1);
    }

    @Test
    public void testSubstring()
    {
        assertSubstring(STRING_HELLO);
        assertSubstring(STRING_QUADRATICALLY);
        assertSubstring(STRING_OESTERREICH);
        assertSubstring(STRING_DULIOE_DULIOE);
        assertSubstring(STRING_FAITH_HOPE_LOVE);
        assertSubstring(STRING_NAIVE);
        assertSubstring(STRING_OO);
        assertSubstring(STRING_ASCII_CODE_POINTS);
        // substring test over all code points takes too long, so only run it on the tail
        // that has the largest code points
        assertSubstring(new String(ALL_CODE_POINTS, ALL_CODE_POINTS.length - 500, 500));
    }

    private static void assertSubstring(String string)
    {
        Slice utf8 = utf8Slice(string);

        int[] codePoints = string.codePoints().toArray();
        for (int start = 0; start < codePoints.length / 2; start++) {
            int count = Math.min(20, codePoints.length - start - start - 1);
            Slice actual = substring(utf8, start, count);
            Slice expected = wrappedBuffer(new String(codePoints, start, count).getBytes(UTF_8));
            assertEquals(actual, expected);
        }
        assertEquals(substring(utf8, 0, codePoints.length), utf8);
        assertEquals(substring(utf8, 0, 0), EMPTY_SLICE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "UTF-8 does not contain 10 code points")
    public void testSubstringInvalidStart()
    {
        substring(utf8Slice(STRING_HELLO), 10, 2);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "UTF-8 does not contain 7 code points")
    public void testSubstringInvalidLength()
    {
        substring(utf8Slice(STRING_HELLO), 0, 7);
    }

    @Test(expectedExceptions = InvalidUtf8Exception.class, expectedExceptionsMessageRegExp = "UTF-8 is not well formed")
    public void testSubstringInvalidUtf8()
    {
        substring(wrappedBuffer((byte) 'f', (byte) 'o', (byte) 'o', START_3_BYTE, CONTINUATION_BYTE), 0, 4);
    }

    @Test
    public void testReverse()
    {
        assertReverse(STRING_HELLO);
        assertReverse(STRING_QUADRATICALLY);
        assertReverse(STRING_OESTERREICH);
        assertReverse(STRING_DULIOE_DULIOE);
        assertReverse(STRING_FAITH_HOPE_LOVE);
        assertReverse(STRING_NAIVE);
        assertReverse(STRING_OO);
        assertReverse(STRING_ASCII_CODE_POINTS);
        assertReverse(STRING_ALL_CODE_POINTS);

        INVALID_SEQUENCES.forEach(TestSliceUtf8::assertReverseWithInvalidSequence);
    }

    private static void assertReverse(String string)
    {
        Slice actualReverse = reverse(utf8Slice(string));

        int[] codePoints = string.codePoints().toArray();
        codePoints = Ints.toArray(Lists.reverse(Ints.asList(codePoints)));
        Slice expectedReverse = wrappedBuffer(new String(codePoints, 0, codePoints.length).getBytes(UTF_8));

        assertEquals(actualReverse, expectedReverse);
    }

    private static void assertReverseWithInvalidSequence(byte[] invalidSequence)
    {
        assertEquals(
                reverse(wrappedBuffer(invalidSequence)),
                wrappedBuffer(invalidSequence));
        assertEquals(
                reverse(wrappedBuffer(concat(new byte[] {'a', 'b', 'c'}, invalidSequence))),
                wrappedBuffer(concat(invalidSequence, new byte[] {'c', 'b', 'a'})));
        assertEquals(
                reverse(wrappedBuffer(concat(invalidSequence, new byte[] {'x', 'y', 'z'}))),
                wrappedBuffer(concat(new byte[] {'z', 'y', 'x'}, invalidSequence)));
        assertEquals(
                reverse(wrappedBuffer(concat(new byte[] {'a', 'b', 'c'}, invalidSequence, new byte[] {'x', 'y', 'z'}))),
                wrappedBuffer(concat(new byte[] {'z', 'y', 'x'}, invalidSequence, new byte[] {'c', 'b', 'a'})));
    }

    @Test
    public void testCompareUtf16BE()
    {
        testCompareUtf16BESequence(STRING_HELLO);
        testCompareUtf16BESequence(STRING_OESTERREICH);
        testCompareUtf16BESequence(STRING_DULIOE_DULIOE);
        testCompareUtf16BESequence(STRING_FAITH_HOPE_LOVE);
        testCompareUtf16BESequence(STRING_NAIVE);
        testCompareUtf16BESequence(STRING_OO);
    }

    private static void testCompareUtf16BESequence(String prefix)
    {
        for (String leftSuffix : ImmutableList.of("", "a", "\uD801\uDC2D")) {
            for (String rightSuffix : ImmutableList.of("", "a", "\uD801\uDC2D")) {
                for (int leftCodePoint : UTF_16_BOUNDARY_CODE_POINTS) {
                    String leftString = prefix + new String(Character.toChars(leftCodePoint)) + leftSuffix;
                    Slice leftSlice = utf8Slice(leftString);

                    // try the UTF-16 code points at the boundaries
                    for (int rightCodePoint : UTF_16_BOUNDARY_CODE_POINTS) {
                        String rightString = prefix + new String(Character.toChars(rightCodePoint)) + rightSuffix;
                        Slice rightSlice = utf8Slice(rightString);

                        int expected = signum(leftString.compareTo(rightString));

                        int actual = compareUtf16BE(leftSlice, rightSlice);
                        assertEquals(actual, expected, String.format("left: 0x%x right: 0x%x", leftCodePoint, rightCodePoint));
                        actual = compareUtf16BE(rightSlice, leftSlice);
                        assertEquals(actual, -expected, String.format("left: 0x%x right: 0x%x", leftCodePoint, rightCodePoint));
                    }
                }
            }
        }
    }

    @Test
    public void testCompareUtf16BEIllegalSequences()
    {
        for (byte[] invalidSequence : INVALID_SEQUENCES) {
            Slice leftSlice = new Slice(concat(new byte[] {'F', 'O', 'O'}, invalidSequence));
            Slice rightSlice = new Slice(new byte[] {'F', 'O', 'O', 'B', 'a', 'r'});
            try {
                compareUtf16BE(leftSlice, rightSlice);
                fail("Expected exception for invalid UTF-8");
            }
            catch (InvalidUtf8Exception e) {
            }
            try {
                compareUtf16BE(rightSlice, leftSlice);
                fail("Expected exception for invalid UTF-8");
            }
            catch (InvalidUtf8Exception e) {
            }
        }
    }

    @Test
    public void testCompareUtf16BECodePoint()
    {
        for (int leftCodePoint : UTF_16_TESTING_CODE_POINTS) {
            String leftString = new String(Character.toChars(leftCodePoint));

            // try the UTF-16 code points at the boundaries
            for (int rightCodePoint : UTF_16_TESTING_CODE_POINTS) {
                String rightString = new String(Character.toChars(rightCodePoint));
                int expected = signum(leftString.compareTo(rightString));

                int actual = compareUtf16BE(leftCodePoint, rightCodePoint);
                assertEquals(actual, expected, String.format("left: 0x%x right: 0x%x", leftCodePoint, rightCodePoint));
                actual = compareUtf16BE(rightCodePoint, leftCodePoint);
                assertEquals(actual, -expected, String.format("left: 0x%x right: 0x%x", leftCodePoint, rightCodePoint));
            }
        }
    }

    @Test
    public void testIsAscii()
    {
        assertTrue(isAscii(utf8Slice(STRING_HELLO)));
        assertTrue(isAscii(utf8Slice(STRING_QUADRATICALLY)));
        assertFalse(isAscii(utf8Slice(STRING_OESTERREICH)));
        assertFalse(isAscii(utf8Slice(STRING_DULIOE_DULIOE)));
        assertFalse(isAscii(utf8Slice(STRING_FAITH_HOPE_LOVE)));
        assertFalse(isAscii(utf8Slice(STRING_NAIVE)));
        assertFalse(isAscii(utf8Slice(STRING_OO)));
        assertTrue(isAscii(utf8Slice(STRING_ASCII_CODE_POINTS)));
        assertFalse(isAscii(utf8Slice(STRING_ALL_CODE_POINTS)));
    }

    @Test
    public void testFixInvalidUtf8()
    {
        assertFixInvalidUtf8(utf8Slice(STRING_OESTERREICH), utf8Slice(STRING_OESTERREICH));
        assertFixInvalidUtf8(utf8Slice(STRING_HELLO), utf8Slice(STRING_HELLO));
        assertFixInvalidUtf8(utf8Slice(STRING_QUADRATICALLY), utf8Slice(STRING_QUADRATICALLY));
        assertFixInvalidUtf8(utf8Slice(STRING_OESTERREICH), utf8Slice(STRING_OESTERREICH));
        assertFixInvalidUtf8(utf8Slice(STRING_DULIOE_DULIOE), utf8Slice(STRING_DULIOE_DULIOE));
        assertFixInvalidUtf8(utf8Slice(STRING_FAITH_HOPE_LOVE), utf8Slice(STRING_FAITH_HOPE_LOVE));
        assertFixInvalidUtf8(utf8Slice(STRING_NAIVE), utf8Slice(STRING_NAIVE));
        assertFixInvalidUtf8(utf8Slice(STRING_OO), utf8Slice(STRING_OO));
        assertFixInvalidUtf8(utf8Slice(STRING_ASCII_CODE_POINTS), utf8Slice(STRING_ASCII_CODE_POINTS));
        assertFixInvalidUtf8(utf8Slice(STRING_ALL_CODE_POINTS), utf8Slice(STRING_ALL_CODE_POINTS));

        // max valid value for 2, 3, and 4 byte sequences
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_2_BYTE, CONTINUATION_BYTE), utf8Slice("X\u07FF"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_3_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFF"));
        // 4 byte sequence is limited to U+10FFFF by RFC 3629
        assertFixInvalidUtf8(
                wrappedBuffer(X_CHAR, (byte) 0xF4, (byte) 0x8F, CONTINUATION_BYTE, CONTINUATION_BYTE),
                wrappedBuffer(X_CHAR, (byte) 0xF4, (byte) 0x8F, CONTINUATION_BYTE, CONTINUATION_BYTE));

        // 4 byte sequence is limited to 10FFFF
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_4_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));

        // 5 and 6 byte sequences are always invalid
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_5_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_6_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));

        // continuation byte alone is invalid
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));

        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, INVALID_FE_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, INVALID_FF_BYTE), utf8Slice("X\uFFFD"));

        // sequences with not enough continuation bytes, but enough bytes
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_2_BYTE, X_CHAR), utf8Slice("X\uFFFDX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_3_BYTE, X_CHAR, X_CHAR), utf8Slice("X\uFFFDXX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_3_BYTE, CONTINUATION_BYTE, X_CHAR), utf8Slice("X\uFFFDX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_4_BYTE, X_CHAR, X_CHAR, X_CHAR), utf8Slice("X\uFFFDXXX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_4_BYTE, CONTINUATION_BYTE, X_CHAR, X_CHAR), utf8Slice("X\uFFFDXX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_4_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, X_CHAR), utf8Slice("X\uFFFDX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_5_BYTE, X_CHAR, X_CHAR, X_CHAR, X_CHAR), utf8Slice("X\uFFFDXXXX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_5_BYTE, CONTINUATION_BYTE, X_CHAR, X_CHAR, X_CHAR), utf8Slice("X\uFFFDXXX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_5_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, X_CHAR, X_CHAR), utf8Slice("X\uFFFDXX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_5_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, X_CHAR), utf8Slice("X\uFFFDX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_6_BYTE, X_CHAR, X_CHAR, X_CHAR, X_CHAR, X_CHAR), utf8Slice("X\uFFFDXXXXX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_6_BYTE, CONTINUATION_BYTE, X_CHAR, X_CHAR, X_CHAR, X_CHAR), utf8Slice("X\uFFFDXXXX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_6_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, X_CHAR, X_CHAR, X_CHAR), utf8Slice("X\uFFFDXXX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_6_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, X_CHAR, X_CHAR), utf8Slice("X\uFFFDXX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_6_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, X_CHAR), utf8Slice("X\uFFFDX"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, INVALID_FE_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, INVALID_FF_BYTE), utf8Slice("X\uFFFD"));

        // truncated sequences
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_2_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_3_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_3_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_4_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_4_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_4_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_5_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_5_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_5_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_5_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_6_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_6_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_6_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_6_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_6_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, START_6_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, INVALID_FE_BYTE), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, INVALID_FF_BYTE), utf8Slice("X\uFFFD"));
        // min and max surrogate characters
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, (byte) 0b11101101, (byte) 0xA0, (byte) 0x80), utf8Slice("X\uFFFD"));
        assertFixInvalidUtf8(wrappedBuffer(X_CHAR, (byte) 0b11101101, (byte) 0xBF, (byte) 0xBF), utf8Slice("X\uFFFD"));
    }

    private static void assertFixInvalidUtf8(Slice testSlice, Slice expectedSlice)
    {
        assertEquals(fixInvalidUtf8(testSlice), expectedSlice);
    }

    @Test
    public void testCaseChange()
    {
        assertCaseChange(STRING_ALL_CODE_POINTS);
        assertCaseChange(STRING_FAITH_HOPE_LOVE);
        assertCaseChange(STRING_HELLO);
        assertCaseChange(STRING_QUADRATICALLY);
        assertCaseChange(STRING_OESTERREICH);
        assertCaseChange(STRING_DULIOE_DULIOE);
        assertCaseChange(STRING_FAITH_HOPE_LOVE);
        assertCaseChange(STRING_NAIVE);
        assertCaseChange(STRING_OO);
        assertCaseChange(STRING_ASCII_CODE_POINTS);
        assertCaseChange(STRING_ALL_CODE_POINTS);
        assertCaseChange(STRING_ALL_CODE_POINTS_RANDOM);

        toLowerCase(Slices.wrappedBuffer(INVALID_SEQUENCE_TO_LOWER_EXPANDS));

        INVALID_SEQUENCES.forEach(TestSliceUtf8::assertCaseChangeWithInvalidSequence);
    }

    private static void assertCaseChangeWithInvalidSequence(byte[] invalidSequence)
    {
        assertEquals(
                toLowerCase(wrappedBuffer(invalidSequence)),
                wrappedBuffer(invalidSequence));
        assertEquals(
                toUpperCase(wrappedBuffer(invalidSequence)),
                wrappedBuffer(invalidSequence));

        assertEquals(
                toLowerCase(wrappedBuffer(concat(new byte[] {'F', 'O', 'O'}, invalidSequence))),
                wrappedBuffer(concat(new byte[] {'f', 'o', 'o'}, invalidSequence)));
        assertEquals(
                toUpperCase(wrappedBuffer(concat(new byte[] {'f', 'o', 'o'}, invalidSequence))),
                wrappedBuffer(concat(new byte[] {'F', 'O', 'O'}, invalidSequence)));

        assertEquals(
                toLowerCase(wrappedBuffer(concat(invalidSequence, new byte[] {'F', 'O', 'O'}))),
                wrappedBuffer(concat(invalidSequence, new byte[] {'f', 'o', 'o'})));
        assertEquals(
                toUpperCase(wrappedBuffer(concat(invalidSequence, new byte[] {'f', 'o', 'o'}))),
                wrappedBuffer(concat(invalidSequence, new byte[] {'F', 'O', 'O'})));

        assertEquals(
                toLowerCase(wrappedBuffer(concat(new byte[] {'F', 'O', 'O'}, invalidSequence, new byte[] {'B', 'A', 'R'}))),
                wrappedBuffer(concat(new byte[] {'f', 'o', 'o'}, invalidSequence, new byte[] {'b', 'a', 'r'})));
        assertEquals(
                toUpperCase(wrappedBuffer(concat(new byte[] {'f', 'o', 'o'}, invalidSequence, new byte[] {'b', 'a', 'r'}))),
                wrappedBuffer(concat(new byte[] {'F', 'O', 'O'}, invalidSequence, new byte[] {'B', 'A', 'R'})));
    }

    private static void assertCaseChange(String string)
    {
        String expectedLower = lowerByCodePoint(string);
        Slice actualLower = toLowerCase(utf8Slice(string));
        assertEquals(actualLower, wrappedBuffer(expectedLower.getBytes(UTF_8)));

        String expectedUpper = upperByCodePoint(string);
        Slice actualUpper = toUpperCase(utf8Slice(string));
        assertEquals(actualUpper, wrappedBuffer(expectedUpper.getBytes(UTF_8)));

        // lower the upper and upper the lower
        // NOTE: not all code points roundtrip, so calculate the expected
        assertEquals(toLowerCase(actualUpper), wrappedBuffer(lowerByCodePoint(expectedUpper).getBytes(UTF_8)));
        assertEquals(toUpperCase(actualLower), wrappedBuffer(upperByCodePoint(expectedLower).getBytes(UTF_8)));
    }

    private static String lowerByCodePoint(String string)
    {
        int[] upperCodePoints = string.codePoints().map(Character::toLowerCase).toArray();
        return new String(upperCodePoints, 0, upperCodePoints.length);
    }

    private static String upperByCodePoint(String string)
    {
        int[] upperCodePoints = string.codePoints().map(Character::toUpperCase).toArray();
        return new String(upperCodePoints, 0, upperCodePoints.length);
    }

    @Test
    public void testLeftTrim()
    {
        assertLeftTrim("");
        assertLeftTrim("hello");
        assertLeftTrim("hello world");
        assertLeftTrim("hello world  ");
        assertLeftTrim(EM_SPACE_SURROUNDED_BY_CONTINUATION_BYTE);

        INVALID_SEQUENCES.forEach(TestSliceUtf8::assertLeftTrim);
    }

    private static void assertLeftTrim(String string)
    {
        assertLeftTrim(string.getBytes(UTF_8));
    }

    private static void assertLeftTrim(byte[] sequence)
    {
        assertEquals(leftTrim(wrappedBuffer(sequence)), wrappedBuffer(sequence));
        assertEquals(leftTrim(wrappedBuffer(sequence), WHITESPACE_CODE_POINTS), wrappedBuffer(sequence));
        assertEquals(leftTrim(wrappedBuffer(concat(new byte[] {'@'}, sequence)), new int[] {'@'}), wrappedBuffer(sequence));
        for (int codePoint : ALL_CODE_POINTS) {
            if (Character.isWhitespace(codePoint)) {
                byte[] whitespace = new String(new int[] {codePoint}, 0, 1).getBytes(UTF_8);
                assertEquals(leftTrim(wrappedBuffer(concat(whitespace, sequence))), wrappedBuffer(sequence));
                assertEquals(leftTrim(wrappedBuffer(concat(whitespace, sequence)), WHITESPACE_CODE_POINTS), wrappedBuffer(sequence));
                assertEquals(leftTrim(wrappedBuffer(concat(whitespace, new byte[] {'\r', '\n', '\t', ' '}, whitespace, sequence)), WHITESPACE_CODE_POINTS), wrappedBuffer(sequence));
            }
        }
    }

    @Test
    public void testRightTrim()
    {
        assertRightTrim("");
        assertRightTrim("hello");
        assertRightTrim("hello world");
        assertRightTrim("  hello world");
        assertRightTrim(EM_SPACE_SURROUNDED_BY_CONTINUATION_BYTE);

        INVALID_SEQUENCES.forEach(TestSliceUtf8::assertRightTrim);
    }

    private static void assertRightTrim(String string)
    {
        assertRightTrim(string.getBytes(UTF_8));
    }

    private static void assertRightTrim(byte[] sequence)
    {
        assertEquals(rightTrim(wrappedBuffer(sequence)), wrappedBuffer(sequence));
        assertEquals(rightTrim(wrappedBuffer(sequence), WHITESPACE_CODE_POINTS), wrappedBuffer(sequence));
        assertEquals(rightTrim(wrappedBuffer(concat(sequence, new byte[] {'@'})), new int[] {'@'}), wrappedBuffer(sequence));
        for (int codePoint : ALL_CODE_POINTS) {
            if (Character.isWhitespace(codePoint)) {
                byte[] whitespace = new String(new int[] {codePoint}, 0, 1).getBytes(UTF_8);
                assertEquals(rightTrim(wrappedBuffer(concat(sequence, whitespace))), wrappedBuffer(sequence));
                assertEquals(rightTrim(wrappedBuffer(concat(sequence, whitespace)), WHITESPACE_CODE_POINTS), wrappedBuffer(sequence));
                assertEquals(rightTrim(wrappedBuffer(concat(sequence, whitespace, new byte[] {'\r', '\n', '\t', ' '}, whitespace))), wrappedBuffer(sequence));
                assertEquals(rightTrim(wrappedBuffer(concat(sequence, whitespace, new byte[] {'\r', '\n', '\t', ' '}, whitespace)), WHITESPACE_CODE_POINTS), wrappedBuffer(sequence));
            }
        }
    }

    @Test
    public void testTrim()
    {
        assertTrim("");
        assertTrim("hello");
        assertTrim("hello world");
        assertTrim(EM_SPACE_SURROUNDED_BY_CONTINUATION_BYTE);

        INVALID_SEQUENCES.forEach(TestSliceUtf8::assertTrim);
    }

    private static void assertTrim(String string)
    {
        assertTrim(string.getBytes(UTF_8));
    }

    private static void assertTrim(byte[] sequence)
    {
        assertEquals(trim(wrappedBuffer(sequence)), wrappedBuffer(sequence));
        assertEquals(trim(wrappedBuffer(sequence), WHITESPACE_CODE_POINTS), wrappedBuffer(sequence));
        assertEquals(trim(wrappedBuffer(concat(new byte[] {'@'}, sequence, new byte[] {'@'})), new int[] {'@'}), wrappedBuffer(sequence));
        for (int codePoint : ALL_CODE_POINTS) {
            if (Character.isWhitespace(codePoint)) {
                byte[] whitespace = new String(new int[] {codePoint}, 0, 1).getBytes(UTF_8);
                assertEquals(trim(wrappedBuffer(concat(whitespace, sequence, whitespace))), wrappedBuffer(sequence));
                assertEquals(trim(wrappedBuffer(concat(whitespace, sequence, whitespace)), WHITESPACE_CODE_POINTS), wrappedBuffer(sequence));
                assertEquals(
                        trim(wrappedBuffer(concat(whitespace, new byte[] {'\r', '\n', '\t', ' '}, whitespace, sequence, whitespace, new byte[] {'\r', '\n', '\t', ' '}, whitespace))),
                        wrappedBuffer(sequence));
                assertEquals(
                        trim(
                                wrappedBuffer(concat(whitespace, new byte[] {'\r', '\n', '\t', ' '}, whitespace, sequence, whitespace, new byte[] {'\r', '\n', '\t', ' '}, whitespace)),
                                WHITESPACE_CODE_POINTS),
                        wrappedBuffer(sequence));
            }
        }
    }

    /**
     * Test invalid UTF8 encodings. We do not expect a 'correct' but none harmful result.
     */
    @Test
    public void testInvalidUtf8()
    {
        assertEquals(countCodePoints(wrappedBuffer(INVALID_UTF8_1)), 0);
        assertEquals(countCodePoints(wrappedBuffer(INVALID_UTF8_2)), 3);

        assertEquals(offsetOfCodePoint(wrappedBuffer(INVALID_UTF8_1), 0), 0);
        assertEquals(offsetOfCodePoint(wrappedBuffer(INVALID_UTF8_1), 1), -1);

        assertEquals(offsetOfCodePoint(wrappedBuffer(INVALID_UTF8_2), 0), 0);
        assertEquals(offsetOfCodePoint(wrappedBuffer(INVALID_UTF8_2), 1), 2);
        assertEquals(offsetOfCodePoint(wrappedBuffer(INVALID_UTF8_2), 2), 3);
        assertEquals(offsetOfCodePoint(wrappedBuffer(INVALID_UTF8_2), 3), -1);
    }

    @Test
    public void testLengthOfCodePoint()
    {
        assertEquals(lengthOfCodePointFromStartByte(START_1_BYTE), 1);
        assertEquals(lengthOfCodePointFromStartByte(START_2_BYTE), 2);
        assertEquals(lengthOfCodePointFromStartByte(START_3_BYTE), 3);
        assertEquals(lengthOfCodePointFromStartByte(START_4_BYTE), 4);

        for (int codePoint : ALL_CODE_POINTS) {
            String string = new String(new int[] {codePoint}, 0, 1);
            assertEquals(string.codePoints().count(), 1);

            Slice utf8 = wrappedBuffer(string.getBytes(UTF_8));
            assertEquals(lengthOfCodePoint(codePoint), utf8.length());
            assertEquals(lengthOfCodePoint(utf8, 0), utf8.length());
            assertEquals(lengthOfCodePointSafe(utf8, 0), utf8.length());
            assertEquals(lengthOfCodePointFromStartByte(utf8.getByte(0)), utf8.length());

            assertEquals(getCodePointAt(utf8, 0), codePoint);
            assertEquals(getCodePointBefore(utf8, utf8.length()), codePoint);

            assertEquals(codePointToUtf8(codePoint), utf8);
        }

        for (byte[] sequence : INVALID_SEQUENCES) {
            assertEquals(lengthOfCodePointSafe(wrappedBuffer(sequence), 0), sequence.length);
            assertEquals(lengthOfCodePointSafe(wrappedBuffer(concat(new byte[] {'x'}, sequence)), 1), sequence.length);
            assertEquals(lengthOfCodePointSafe(wrappedBuffer(concat(sequence, new byte[] {'x'})), 0), sequence.length);
        }
    }

    @Test(expectedExceptions = InvalidCodePointException.class, expectedExceptionsMessageRegExp = "Invalid code point 0xFFFFFFFF")
    public void testLengthOfNegativeCodePoint()
    {
        lengthOfCodePoint(-1);
    }

    @Test(expectedExceptions = InvalidCodePointException.class, expectedExceptionsMessageRegExp = "Invalid code point 0x110000")
    public void testLengthOfOutOfRangeCodePoint()
    {
        lengthOfCodePoint(MAX_CODE_POINT + 1);
    }

    @Test(expectedExceptions = InvalidUtf8Exception.class, expectedExceptionsMessageRegExp = "Illegal start 0xBF of code point")
    public void testLengthOfCodePointContinuationByte()
    {
        lengthOfCodePointFromStartByte(CONTINUATION_BYTE);
    }

    @Test(expectedExceptions = InvalidUtf8Exception.class, expectedExceptionsMessageRegExp = "Illegal start 0xFB of code point")
    public void testLengthOfCodePoint5ByteSequence()
    {
        lengthOfCodePointFromStartByte(START_5_BYTE);
    }

    @Test(expectedExceptions = InvalidUtf8Exception.class, expectedExceptionsMessageRegExp = "Illegal start 0xFD of code point")
    public void testLengthOfCodePoint6ByteByte()
    {
        lengthOfCodePointFromStartByte(START_6_BYTE);
    }

    @Test(expectedExceptions = InvalidUtf8Exception.class, expectedExceptionsMessageRegExp = "Illegal start 0xFE of code point")
    public void testLengthOfCodePointFEByte()
    {
        lengthOfCodePointFromStartByte(INVALID_FE_BYTE);
    }

    @Test(expectedExceptions = InvalidUtf8Exception.class, expectedExceptionsMessageRegExp = "Illegal start 0xFF of code point")
    public void testLengthOfCodePointFFByte()
    {
        lengthOfCodePointFromStartByte(INVALID_FF_BYTE);
    }

    @Test(expectedExceptions = InvalidUtf8Exception.class, expectedExceptionsMessageRegExp = "UTF-8 sequence truncated")
    public void testCodePointAtTruncated2()
    {
        getCodePointAt(wrappedBuffer((byte) 'x', START_2_BYTE), 1);
    }

    @Test(expectedExceptions = InvalidUtf8Exception.class, expectedExceptionsMessageRegExp = "UTF-8 sequence truncated")
    public void testCodePointAtTruncated3()
    {
        getCodePointAt(wrappedBuffer((byte) 'x', START_3_BYTE, CONTINUATION_BYTE), 1);
    }

    @Test(expectedExceptions = InvalidUtf8Exception.class, expectedExceptionsMessageRegExp = "UTF-8 sequence truncated")
    public void testCodePointAtTruncated4()
    {
        getCodePointAt(wrappedBuffer((byte) 'x', START_4_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), 1);
    }

    @Test(expectedExceptions = InvalidUtf8Exception.class, expectedExceptionsMessageRegExp = "Illegal start 0xFB of code point")
    public void testCodePointAt5ByteSequence()
    {
        getCodePointAt(wrappedBuffer((byte) 'x', START_5_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), 1);
    }

    @Test(expectedExceptions = InvalidUtf8Exception.class, expectedExceptionsMessageRegExp = "UTF-8 is not well formed")
    public void testCodePointBefore5ByteSequence()
    {
        getCodePointBefore(wrappedBuffer((byte) 'x', START_5_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE, CONTINUATION_BYTE), 6);
    }

    @Test(expectedExceptions = InvalidCodePointException.class, expectedExceptionsMessageRegExp = "Invalid code point 0xFFFFFFFF")
    public void testSetNegativeCodePoint()
    {
        setCodePointAt(-1, Slices.allocate(8), 0);
    }

    @Test(expectedExceptions = InvalidCodePointException.class, expectedExceptionsMessageRegExp = "Invalid code point 0xD800")
    public void testSetSurrogateCodePoint()
    {
        setCodePointAt(MIN_SURROGATE, Slices.allocate(8), 0);
    }

    @Test(expectedExceptions = InvalidCodePointException.class, expectedExceptionsMessageRegExp = "Invalid code point 0x110000")
    public void testSetOutOfRangeCodePoint()
    {
        setCodePointAt(MAX_CODE_POINT + 1, Slices.allocate(8), 0);
    }

    @Test(expectedExceptions = InvalidCodePointException.class, expectedExceptionsMessageRegExp = "Invalid code point 0xFFFFFFBF")
    public void testSetCodePointContinuationByte()
    {
        setCodePointAt(CONTINUATION_BYTE, Slices.allocate(8), 0);
    }
}
