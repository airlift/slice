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

import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.Preconditions.checkPositionIndex;
import static io.airlift.slice.Preconditions.checkPositionIndexes;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_SURROGATE;
import static java.lang.Integer.toHexString;

/**
 * Utility methods for UTF-8 encoded slices.
 */
public final class SliceUtf8
{
    private SliceUtf8() {}

    private static final int REPLACEMENT_CODE_POINT = 0xFFFD;

    private static final int TOP_MASK32 = 0x8080_8080;
    private static final long TOP_MASK64 = 0x8080_8080_8080_8080L;

    private static final int[] LOWER_CODE_POINTS;
    private static final int[] UPPER_CODE_POINTS;
    private static final boolean[] WHITESPACE_CODE_POINTS;

    static {
        LOWER_CODE_POINTS = new int[MAX_CODE_POINT];
        UPPER_CODE_POINTS = new int[MAX_CODE_POINT];
        WHITESPACE_CODE_POINTS = new boolean[MAX_CODE_POINT];
        for (int codePoint = 0; codePoint < MAX_CODE_POINT; codePoint++) {
            int type = Character.getType(codePoint);
            if (type != Character.SURROGATE) {
                LOWER_CODE_POINTS[codePoint] = Character.toLowerCase(codePoint);
                UPPER_CODE_POINTS[codePoint] = Character.toUpperCase(codePoint);
                WHITESPACE_CODE_POINTS[codePoint] = Character.isWhitespace(codePoint);
            }
            else {
                LOWER_CODE_POINTS[codePoint] = REPLACEMENT_CODE_POINT;
                UPPER_CODE_POINTS[codePoint] = REPLACEMENT_CODE_POINT;
                WHITESPACE_CODE_POINTS[codePoint] = false;
            }
        }
    }

    /**
     * Counts the code points within UTF-8 encoded slice.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static int countCodePoints(Slice utf8)
    {
        return countCodePoints(utf8, 0, utf8.length());
    }

    /**
     * Counts the code points within UTF-8 encoded slice up to {@code length}.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static int countCodePoints(Slice utf8, int offset, int length)
    {
        checkPositionIndexes(offset, offset + length, utf8.length());

        // Quick exit if empty string
        if (length == 0) {
            return 0;
        }

        int continuationBytesCount = 0;
        // Length rounded to 8 bytes
        int length8 = length & 0x7FFF_FFF8;
        for (; offset < length8; offset += 8) {
            // Count bytes which are NOT the start of a code point
            continuationBytesCount += countContinuationBytes(utf8.getLongUnchecked(offset));
        }
        // Enough bytes left for 32 bits?
        if (offset + 4 < length) {
            // Count bytes which are NOT the start of a code point
            continuationBytesCount += countContinuationBytes(utf8.getIntUnchecked(offset));

            offset += 4;
        }
        // Do the rest one by one
        for (; offset < length; offset++) {
            // Count bytes which are NOT the start of a code point
            continuationBytesCount += countContinuationBytes(utf8.getByteUnchecked(offset));
        }

        assert continuationBytesCount <= length;
        return length - continuationBytesCount;
    }

    /**
     * Gets the substring starting at {@code codePointStart} and extending for
     * {@code codePointLength} code points.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static Slice substring(Slice utf8, int codePointStart, int codePointLength)
    {
        checkArgument(codePointStart >= 0, "codePointStart is negative");
        checkArgument(codePointLength >= 0, "codePointLength is negative");

        int indexStart = offsetOfCodePoint(utf8, codePointStart);
        if (indexStart < 0) {
            throw new IllegalArgumentException("UTF-8 does not contain " + codePointStart + " code points");
        }
        if (codePointLength == 0) {
            return Slices.EMPTY_SLICE;
        }
        int indexEnd = offsetOfCodePoint(utf8, indexStart, codePointLength - 1);
        if (indexEnd < 0) {
            throw new IllegalArgumentException("UTF-8 does not contain " + (codePointStart + codePointLength) + " code points");
        }
        indexEnd += lengthOfCodePoint(utf8, indexEnd);
        if (indexEnd > utf8.length()) {
            throw new InvalidUtf8Exception("UTF-8 is not well formed");
        }
        return utf8.slice(indexStart, indexEnd - indexStart);
    }

    /**
     * Reverses the slice code point by code point.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static Slice reverse(Slice utf8)
    {
        int length = utf8.length();
        Slice reverse = Slices.allocate(length);

        int forwardPosition = 0;
        int reversePosition = length;
        while (forwardPosition < length) {
            int codePointLength = lengthOfCodePointFromStartByte(utf8.getByteUnchecked(forwardPosition));

            // backup the reverse pointer
            reversePosition -= codePointLength;
            if (reversePosition < 0) {
                throw new InvalidUtf8Exception("UTF-8 is not well formed");
            }
            // copy the character
            switch (codePointLength) {
                case 1:
                    reverse.setByteUnchecked(reversePosition, utf8.getByteUnchecked(forwardPosition));
                    break;
                case 2:
                    reverse.setShortUnchecked(reversePosition, utf8.getShortUnchecked(forwardPosition));
                    break;
                case 3:
                    reverse.setByteUnchecked(reversePosition, utf8.getByteUnchecked(forwardPosition));
                    reverse.setShortUnchecked(reversePosition + 1, utf8.getShortUnchecked(forwardPosition + 1));
                    break;
                case 4:
                    reverse.setIntUnchecked(reversePosition, utf8.getIntUnchecked(forwardPosition));
                    break;
                default:
                    throw new IllegalStateException("Invalid code point length " + codePointLength);
            }

            forwardPosition += codePointLength;
        }
        return reverse;
    }

    /**
     * Converts slice to upper case code point by code point.  This method does
     * not perform perform locale-sensitive, context-sensitive, or one-to-many
     * mappings required for some languages.  Specifically, this will return
     * incorrect results for Lithuanian, Turkish, and Azeri.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static Slice toUpperCase(Slice utf8)
    {
        int length = utf8.length();
        Slice upperUtf8 = Slices.allocate(length);

        int position = 0;
        int upperPosition = 0;
        while (position < length) {
            int codePoint = getCodePointAt(utf8, position);
            int upperCodePoint = UPPER_CODE_POINTS[codePoint];

            // grow slice if necessary
            int nextUpperPosition = upperPosition + lengthOfCodePoint(upperCodePoint);
            if (nextUpperPosition > length) {
                upperUtf8 = Slices.ensureSize(upperUtf8, nextUpperPosition);
            }

            // write new byte
            setCodePointAt(upperCodePoint, upperUtf8, upperPosition);

            position += lengthOfCodePoint(codePoint);
            upperPosition = nextUpperPosition;
        }
        return upperUtf8.slice(0, upperPosition);
    }

    /**
     * Converts slice to lower case code point by code point.  This method does
     * not perform perform locale-sensitive, context-sensitive, or one-to-many
     * mappings required for some languages.  Specifically, this will return
     * incorrect results for Lithuanian, Turkish, and Azeri.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static Slice toLowerCase(Slice utf8)
    {
        int length = utf8.length();
        Slice lowerUtf8 = Slices.allocate(length);

        int position = 0;
        int lowerPosition = 0;
        while (position < length) {
            int codePoint = getCodePointAt(utf8, position);
            int lowerCodePoint = LOWER_CODE_POINTS[codePoint];

            // grow slice if necessary
            int nextLowerPosition = lowerPosition + lengthOfCodePoint(lowerCodePoint);
            if (nextLowerPosition > length) {
                lowerUtf8 = Slices.ensureSize(lowerUtf8, nextLowerPosition);
            }

            // write new byte
            setCodePointAt(lowerCodePoint, lowerUtf8, lowerPosition);

            position += lengthOfCodePoint(codePoint);
            lowerPosition = nextLowerPosition;
        }
        return lowerUtf8.slice(0, lowerPosition);
    }

    /**
     * Removes all white space characters from the left string of the string.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static Slice leftTrim(Slice utf8)
    {
        int length = utf8.length();

        int position = firstNonWhitespacePosition(utf8);
        return utf8.slice(position, length - position);
    }

    private static int firstNonWhitespacePosition(Slice utf8)
    {
        int length = utf8.length();

        int position = 0;
        while (position < length) {
            int codePoint = getCodePointAt(utf8, position);
            if (!WHITESPACE_CODE_POINTS[codePoint]) {
                break;
            }
            position += lengthOfCodePoint(codePoint);
        }
        return position;
    }

    /**
     * Removes all white space characters from the right side of the string.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static Slice rightTrim(Slice utf8)
    {
        int position = lastNonWhitespacePosition(utf8, 0);
        return utf8.slice(0, position);
    }

    private static int lastNonWhitespacePosition(Slice utf8, int minPosition)
    {
        int length = utf8.length();

        int position = length;
        while (minPosition < position) {
            int codePoint = getCodePointBefore(utf8, position);
            if (!WHITESPACE_CODE_POINTS[codePoint]) {
                break;
            }
            position -= lengthOfCodePoint(codePoint);
        }
        return position;
    }

    /**
     * Removes all white space characters from the left and right side of the string.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static Slice trim(Slice utf8)
    {
        int start = firstNonWhitespacePosition(utf8);
        int end = lastNonWhitespacePosition(utf8, start);
        return utf8.slice(start, end - start);
    }

    /**
     * Finds the index of the first byte of the code point at a position, or
     * {@code -1} if the position is not withing the slice.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static int offsetOfCodePoint(Slice utf8, int codePointCount)
    {
        return offsetOfCodePoint(utf8, 0, codePointCount);
    }

    /**
     * Starting from {@code position} bytes in {@code utf8}, finds the
     * index of the first byte of the code point {@code codePointCount}
     * in the slice.  If the slice does not contain
     * {@code codePointCount} code points after {@code position}, {@code -1}
     * is returned.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static int offsetOfCodePoint(Slice utf8, int position, int codePointCount)
    {
        checkPositionIndex(position, utf8.length());
        checkArgument(codePointCount >= 0, "codePointPosition is negative");

        // Quick exit if we are sure that the position is after the end
        if (utf8.length() - position <= codePointCount) {
            return -1;
        }
        if (codePointCount == 0) {
            return position;
        }

        int correctIndex = codePointCount + position;
        // Length rounded to 8 bytes
        int length8 = utf8.length() & 0x7FFF_FFF8;
        // While we have enough bytes left and we need at least 8 characters process 8 bytes at once
        while (position < length8 && correctIndex >= position + 8) {
            // Count bytes which are NOT the start of a code point
            correctIndex += countContinuationBytes(utf8.getLongUnchecked(position));

            position += 8;
        }
        // Length rounded to 4 bytes
        int length4 = utf8.length() & 0x7FFF_FFFC;
        // While we have enough bytes left and we need at least 4 characters process 4 bytes at once
        while (position < length4 && correctIndex >= position + 4) {
            // Count bytes which are NOT the start of a code point
            correctIndex += countContinuationBytes(utf8.getIntUnchecked(position));

            position += 4;
        }
        // Do the rest one by one, always check the last byte to find the end of the code point
        while (position < utf8.length()) {
            // Count bytes which are NOT the start of a code point
            correctIndex += countContinuationBytes(utf8.getByteUnchecked(position));
            if (position == correctIndex) {
                break;
            }

            position++;
        }

        if (position == correctIndex && correctIndex < utf8.length()) {
            return correctIndex;
        }
        return -1;
    }

    /**
     * Gets the UTF-8 sequence length of the code point at {@code position}.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static int lengthOfCodePoint(Slice utf8, int position)
    {
        return lengthOfCodePointFromStartByte(utf8.getByte(position));
    }

    /**
     * Gets the UTF-8 sequence length of the code point.
     *
     * @throws InvalidCodePointException if code point is not within a valid range
     */
    public static int lengthOfCodePoint(int codePoint)
    {
        if (codePoint < 0) {
            throw new InvalidCodePointException(codePoint);
        }
        if (codePoint < 0x80) {
            // normal ASCII
            // 0xxx_xxxx
            return 1;
        }
        if (codePoint < 0x800) {
            return 2;
        }
        if (codePoint < 0x1_0000) {
            return 3;
        }
        if (codePoint < 0x11_0000) {
            return 4;
        }
        // Per RFC3629, UTF-8 is limited to 4 bytes, so more bytes are illegal
        throw new InvalidCodePointException(codePoint);
    }

    /**
     * Gets the UTF-8 sequence length using the sequence start byte.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static int lengthOfCodePointFromStartByte(byte startByte)
    {
        int unsignedStartByte = startByte & 0xFF;
        if (unsignedStartByte < 0x80) {
            // normal ASCII
            // 0xxx_xxxx
            return 1;
        }
        if (unsignedStartByte < 0xc0) {
            // illegal bytes
            // 10xx_xxxx
            throw new InvalidUtf8Exception("Illegal start 0x" + toHexString(unsignedStartByte).toUpperCase() + " of code point");
        }
        if (unsignedStartByte < 0xe0) {
            // 110x_xxxx 10xx_xxxx
            return 2;
        }
        if (unsignedStartByte < 0xf0) {
            // 1110_xxxx 10xx_xxxx 10xx_xxxx
            return 3;
        }
        if (unsignedStartByte < 0xf8) {
            // 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
            return 4;
        }
        // Per RFC3629, UTF-8 is limited to 4 bytes, so more bytes are illegal
        throw new InvalidUtf8Exception("Illegal start 0x" + toHexString(unsignedStartByte).toUpperCase() + " of code point");
    }

    /**
     * Gets the UTF-8 encoded code point at the {@code position}.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static int getCodePointAt(Slice utf8, int position)
    {
        int unsignedStartByte = utf8.getByte(position) & 0xFF;
        if (unsignedStartByte < 0x80) {
            // normal ASCII
            // 0xxx_xxxx
            return unsignedStartByte;
        }
        if (unsignedStartByte < 0xc0) {
            // illegal bytes
            // 10xx_xxxx
            throw new InvalidUtf8Exception("Illegal start 0x" + toHexString(unsignedStartByte).toUpperCase() + " of code point");
        }
        if (unsignedStartByte < 0xe0) {
            // 110x_xxxx 10xx_xxxx
            if (position + 1 >= utf8.length()) {
                throw new InvalidUtf8Exception("UTF-8 sequence truncated");
            }
            return ((unsignedStartByte & 0b0001_1111) << 6) |
                    (utf8.getByte(position + 1) & 0b0011_1111);
        }
        if (unsignedStartByte < 0xf0) {
            // 1110_xxxx 10xx_xxxx 10xx_xxxx
            if (position + 2 >= utf8.length()) {
                throw new InvalidUtf8Exception("UTF-8 sequence truncated");
            }
            return ((unsignedStartByte & 0b0000_1111) << 12) |
                    ((utf8.getByteUnchecked(position + 1) & 0b0011_1111) << 6) |
                    (utf8.getByteUnchecked(position + 2) & 0b0011_1111);
        }
        if (unsignedStartByte < 0xf8) {
            // 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
            if (position + 3 >= utf8.length()) {
                throw new InvalidUtf8Exception("UTF-8 sequence truncated");
            }
            return ((unsignedStartByte & 0b0000_0111) << 18) |
                    ((utf8.getByteUnchecked(position + 1) & 0b0011_1111) << 12) |
                    ((utf8.getByteUnchecked(position + 2) & 0b0011_1111) << 6) |
                    (utf8.getByteUnchecked(position + 3) & 0b0011_1111);
        }
        // Per RFC3629, UTF-8 is limited to 4 bytes, so more bytes are illegal
        throw new InvalidUtf8Exception("Illegal start 0x" + toHexString(unsignedStartByte).toUpperCase() + " of code point");
    }

    /**
     * Gets the UTF-8 encoded code point before the {@code position}.
     * </p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static int getCodePointBefore(Slice utf8, int position)
    {
        byte unsignedByte = utf8.getByte(position - 1);
        if (!isContinuationByte(unsignedByte)) {
            return unsignedByte & 0xFF;
        }
        if (!isContinuationByte(utf8.getByte(position - 2))) {
            return getCodePointAt(utf8, position - 2);
        }
        if (!isContinuationByte(utf8.getByte(position - 3))) {
            return getCodePointAt(utf8, position - 3);
        }
        if (!isContinuationByte(utf8.getByte(position - 4))) {
            return getCodePointAt(utf8, position - 4);
        }

        // Per RFC3629, UTF-8 is limited to 4 bytes, so more bytes are illegal
        throw new InvalidUtf8Exception("UTF-8 is not well formed");
    }

    private static boolean isContinuationByte(byte b)
    {
        return (b & 0b1100_0000) == 0b1000_0000;
    }

    /**
     * Convert the code point to UTF-8.
     * </p>
     *
     * @throws InvalidCodePointException if code point is not within a valid range
     */
    public static Slice codePointToUtf8(int codePoint)
    {
        Slice utf8 = Slices.allocate(lengthOfCodePoint(codePoint));
        setCodePointAt(codePoint, utf8, 0);
        return utf8;
    }

    /**
     * Sets the UTF-8 sequence for code point at the {@code position}.
     *
     * @throws InvalidCodePointException if code point is not within a valid range
     */
    public static int setCodePointAt(int codePoint, Slice utf8, int position)
    {
        if (codePoint < 0) {
            throw new InvalidCodePointException(codePoint);
        }
        if (codePoint < 0x80) {
            // normal ASCII
            // 0xxx_xxxx
            utf8.setByte(position, codePoint);
            return 1;
        }
        if (codePoint < 0x800) {
            // 110x_xxxx 10xx_xxxx
            utf8.setByte(position, 0b1100_0000 | (codePoint >>> 6));
            utf8.setByte(position + 1, 0b1000_0000 | (codePoint & 0b0011_1111));
            return 2;
        }
        if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
            throw new InvalidCodePointException(codePoint);
        }
        if (codePoint < 0x1_0000) {
            // 1110_xxxx 10xx_xxxx 10xx_xxxx
            utf8.setByte(position, 0b1110_0000 | ((codePoint >>> 12) & 0b0000_1111));
            utf8.setByte(position + 1, 0b1000_0000 | ((codePoint >>> 6) & 0b0011_1111));
            utf8.setByte(position + 2, 0b1000_0000 | (codePoint & 0b0011_1111));
            return 3;
        }
        if (codePoint < 0x11_0000) {
            // 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
            utf8.setByte(position, 0b1111_0000 | ((codePoint >>> 18) & 0b0000_0111));
            utf8.setByte(position + 1, 0b1000_0000 | ((codePoint >>> 12) & 0b0011_1111));
            utf8.setByte(position + 2, 0b1000_0000 | ((codePoint >>> 6) & 0b0011_1111));
            utf8.setByte(position + 3, 0b1000_0000 | (codePoint & 0b0011_1111));
            return 4;
        }
        // Per RFC3629, UTF-8 is limited to 4 bytes, so more bytes are illegal
        throw new InvalidCodePointException(codePoint);
    }

    private static int countContinuationBytes(byte i8)
    {
        // see below
        int value = i8 & 0xff;
        return (value >>> 7) & (~value >>> 6);
    }

    private static int countContinuationBytes(int i32)
    {
        // see below
        i32 = ((i32 & TOP_MASK32) >>> 1) & (~i32);
        return Integer.bitCount(i32);
    }

    private static int countContinuationBytes(long i64)
    {
        // Count the number of bytes that match 0b10xx_xxxx as follows:
        // 1. Mask off the 8th bit of every byte and shift it into the 7th position.
        // 2. Then invert the bytes, which turns the 0 in the 7th bit to a one.
        // 3. And together the restults of step 1 and 2, giving us a one in the 7th
        //    position if the byte matched.
        // 4. Count the number of bits in the result, which is the number of bytes
        //    that matched.
        i64 = ((i64 & TOP_MASK64) >>> 1) & (~i64);
        return Long.bitCount(i64);
    }
}
