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

import java.util.OptionalInt;

import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.Preconditions.checkPositionIndex;
import static io.airlift.slice.Preconditions.checkPositionIndexes;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_SUPPLEMENTARY_CODE_POINT;
import static java.lang.Character.MIN_SURROGATE;
import static java.lang.Integer.toHexString;

/**
 * Utility methods for UTF-8 encoded slices.
 */
public final class SliceUtf8
{
    private SliceUtf8() {}

    private static final int MIN_HIGH_SURROGATE_CODE_POINT = 0xD800;
    private static final int REPLACEMENT_CODE_POINT = 0xFFFD;

    private static final int TOP_MASK32 = 0x8080_8080;
    private static final long TOP_MASK64 = 0x8080_8080_8080_8080L;

    private static final int[] LOWER_CODE_POINTS;
    private static final int[] UPPER_CODE_POINTS;
    private static final boolean[] WHITESPACE_CODE_POINTS;

    static {
        LOWER_CODE_POINTS = new int[MAX_CODE_POINT + 1];
        UPPER_CODE_POINTS = new int[MAX_CODE_POINT + 1];
        WHITESPACE_CODE_POINTS = new boolean[MAX_CODE_POINT + 1];
        for (int codePoint = 0; codePoint <= MAX_CODE_POINT; codePoint++) {
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
     * Does the slice contain only 7-bit ASCII characters.
     */
    public static boolean isAscii(Slice utf8)
    {
        int length = utf8.length();
        int offset = 0;

        // Length rounded to 8 bytes
        int length8 = length & 0x7FFF_FFF8;
        for (; offset < length8; offset += 8) {
            if ((utf8.getLongUnchecked(offset) & TOP_MASK64) != 0) {
                return false;
            }
        }
        // Enough bytes left for 32 bits?
        if (offset + 4 < length) {
            if ((utf8.getIntUnchecked(offset) & TOP_MASK32) != 0) {
                return false;
            }

            offset += 4;
        }
        // Do the rest one by one
        for (; offset < length; offset++) {
            if ((utf8.getByteUnchecked(offset) & 0x80) != 0) {
                return false;
            }
        }

        return true;
    }

    /**
     * Counts the code points within UTF-8 encoded slice.
     * <p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static int countCodePoints(Slice utf8)
    {
        return countCodePoints(utf8, 0, utf8.length());
    }

    /**
     * Counts the code points within UTF-8 encoded slice up to {@code length}.
     * <p>
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
     * <p>
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
     * <p>
     * Note: Invalid UTF-8 sequences are copied directly to the output.
     */
    public static Slice reverse(Slice utf8)
    {
        int length = utf8.length();
        Slice reverse = Slices.allocate(length);

        int forwardPosition = 0;
        int reversePosition = length;
        while (forwardPosition < length) {
            int codePointLength = lengthOfCodePointSafe(utf8, forwardPosition);

            // backup the reverse pointer
            reversePosition -= codePointLength;
            if (reversePosition < 0) {
                // this should not happen
                throw new InvalidUtf8Exception("UTF-8 is not well formed");
            }
            // copy the character
            copyUtf8SequenceUnsafe(utf8, forwardPosition, reverse, reversePosition, codePointLength);

            forwardPosition += codePointLength;
        }
        return reverse;
    }

    /**
     * Compares to UTF-8 sequences using UTF-16 big endian semantics.  This is
     * equivalent to the {@link java.lang.String#compareTo(Object)}.
     * {@code java.lang.String}.
     * @throws InvalidUtf8Exception if the UTF-8 are invalid
     */
    public static int compareUtf16BE(Slice utf8Left, Slice utf8Right)
    {
        int leftLength = utf8Left.length();
        int rightLength = utf8Right.length();

        int offset = 0;
        while (offset < leftLength) {
            // if there are no more right code points, right is less
            if (offset >= rightLength) {
                return 1; // left.compare(right) > 0
            }

            int leftCodePoint = tryGetCodePointAt(utf8Left, offset);
            if (leftCodePoint < 0) {
                throw new InvalidUtf8Exception("Invalid UTF-8 sequence in utf8Left at " + offset);
            }

            int rightCodePoint = tryGetCodePointAt(utf8Right, offset);
            if (rightCodePoint < 0) {
                throw new InvalidUtf8Exception("Invalid UTF-8 sequence in utf8Right at " + offset);
            }

            int result = compareUtf16BE(leftCodePoint, rightCodePoint);
            if (result != 0) {
                return result;
            }

            // the code points are the same and non-canonical sequences are not allowed,
            // so we advance a single offset through both sequences
            offset += lengthOfCodePoint(leftCodePoint);
        }

        // there are no more left code points, so if there are more right code points,
        // left is less
        if (offset < rightLength) {
            return -1; // left.compare(right) < 0
        }

        return 0;
    }

    static int compareUtf16BE(int leftCodePoint, int rightCodePoint)
    {
        if (leftCodePoint < MIN_SUPPLEMENTARY_CODE_POINT) {
            if (rightCodePoint < MIN_SUPPLEMENTARY_CODE_POINT) {
                return Integer.compare(leftCodePoint, rightCodePoint);
            }
            else {
                // left simple, right complex
                return leftCodePoint < MIN_HIGH_SURROGATE_CODE_POINT ? -1 : 1;
            }
        }
        else {
            if (rightCodePoint >= MIN_SUPPLEMENTARY_CODE_POINT) {
                return Integer.compare(leftCodePoint, rightCodePoint);
            }
            else {
                // left complex, right simple
                return rightCodePoint < MIN_HIGH_SURROGATE_CODE_POINT ? 1 : -1;
            }
        }
    }

    /**
     * Converts slice to upper case code point by code point.  This method does
     * not perform perform locale-sensitive, context-sensitive, or one-to-many
     * mappings required for some languages.  Specifically, this will return
     * incorrect results for Lithuanian, Turkish, and Azeri.
     * <p>
     * Note: Invalid UTF-8 sequences are copied directly to the output.
     */
    public static Slice toUpperCase(Slice utf8)
    {
        return translateCodePoints(utf8, UPPER_CODE_POINTS);
    }

    /**
     * Converts slice to lower case code point by code point.  This method does
     * not perform perform locale-sensitive, context-sensitive, or one-to-many
     * mappings required for some languages.  Specifically, this will return
     * incorrect results for Lithuanian, Turkish, and Azeri.
     * <p>
     * Note: Invalid UTF-8 sequences are copied directly to the output.
     */
    public static Slice toLowerCase(Slice utf8)
    {
        return translateCodePoints(utf8, LOWER_CODE_POINTS);
    }

    private static Slice translateCodePoints(Slice utf8, int[] codePointTranslationMap)
    {
        int length = utf8.length();
        Slice newUtf8 = Slices.allocate(length);

        int position = 0;
        int upperPosition = 0;
        while (position < length) {
            int codePoint = tryGetCodePointAt(utf8, position);
            if (codePoint >= 0) {
                int upperCodePoint = codePointTranslationMap[codePoint];

                // grow slice if necessary
                int nextUpperPosition = upperPosition + lengthOfCodePoint(upperCodePoint);
                if (nextUpperPosition > length) {
                    newUtf8 = Slices.ensureSize(newUtf8, nextUpperPosition);
                }

                // write new byte
                setCodePointAt(upperCodePoint, newUtf8, upperPosition);

                position += lengthOfCodePoint(codePoint);
                upperPosition = nextUpperPosition;
            }
            else {
                int skipLength = -codePoint;

                // grow slice if necessary
                int nextUpperPosition = upperPosition + skipLength;
                if (nextUpperPosition > length) {
                    newUtf8 = Slices.ensureSize(newUtf8, nextUpperPosition);
                }

                copyUtf8SequenceUnsafe(utf8, position, newUtf8, upperPosition, skipLength);
                position += skipLength;
                upperPosition = nextUpperPosition;
            }
        }
        return newUtf8.slice(0, upperPosition);
    }

    private static void copyUtf8SequenceUnsafe(Slice source, int sourcePosition, Slice destination, int destinationPosition, int length)
    {
        switch (length) {
            case 1:
                destination.setByteUnchecked(destinationPosition, source.getByteUnchecked(sourcePosition));
                break;
            case 2:
                destination.setShortUnchecked(destinationPosition, source.getShortUnchecked(sourcePosition));
                break;
            case 3:
                destination.setShortUnchecked(destinationPosition, source.getShortUnchecked(sourcePosition));
                destination.setByteUnchecked(destinationPosition + 2, source.getByteUnchecked(sourcePosition + 2));
                break;
            case 4:
                destination.setIntUnchecked(destinationPosition, source.getIntUnchecked(sourcePosition));
                break;
            case 5:
                destination.setIntUnchecked(destinationPosition, source.getIntUnchecked(sourcePosition));
                destination.setByteUnchecked(destinationPosition + 4, source.getByteUnchecked(sourcePosition + 4));
                break;
            case 6:
                destination.setIntUnchecked(destinationPosition, source.getIntUnchecked(sourcePosition));
                destination.setShortUnchecked(destinationPosition + 4, source.getShortUnchecked(sourcePosition + 4));
                break;
            default:
                throw new IllegalStateException("Invalid code point length " + length);
        }
    }

    /**
     * Removes all white space characters from the left side of the string.
     * <p>
     * Note: Invalid UTF-8 sequences are not trimmed.
     */
    public static Slice leftTrim(Slice utf8)
    {
        int length = utf8.length();

        int position = firstNonWhitespacePosition(utf8);
        return utf8.slice(position, length - position);
    }

    /**
     * Removes all {@code whiteSpaceCodePoints} from the left side of the string.
     * <p>
     * Note: Invalid UTF-8 sequences are not trimmed.
     */
    public static Slice leftTrim(Slice utf8, int[] whiteSpaceCodePoints)
    {
        int length = utf8.length();

        int position = firstNonMatchPosition(utf8, whiteSpaceCodePoints);
        return utf8.slice(position, length - position);
    }

    private static int firstNonWhitespacePosition(Slice utf8)
    {
        int length = utf8.length();

        int position = 0;
        while (position < length) {
            int codePoint = tryGetCodePointAt(utf8, position);
            if (codePoint < 0) {
                break;
            }
            if (!WHITESPACE_CODE_POINTS[codePoint]) {
                break;
            }
            position += lengthOfCodePoint(codePoint);
        }
        return position;
    }

    // This function is an exact duplicate of firstNonWhitespacePosition(Slice) except for one line.
    private static int firstNonMatchPosition(Slice utf8, int[] codePointsToMatch)
    {
        int length = utf8.length();

        int position = 0;
        while (position < length) {
            int codePoint = tryGetCodePointAt(utf8, position);
            if (codePoint < 0) {
                break;
            }
            if (!matches(codePoint, codePointsToMatch)) {
                break;
            }
            position += lengthOfCodePoint(codePoint);
        }
        return position;
    }

    private static boolean matches(int codePoint, int[] codePoints)
    {
        for (int codePointToTrim : codePoints) {
            if (codePoint == codePointToTrim) {
                return true;
            }
        }
        return false;
    }

    /**
     * Removes all white space characters from the right side of the string.
     * <p>
     * Note: Invalid UTF-8 sequences are not trimmed.
     */
    public static Slice rightTrim(Slice utf8)
    {
        int position = lastNonWhitespacePosition(utf8, 0);
        return utf8.slice(0, position);
    }

    /**
     * Removes all white {@code whiteSpaceCodePoints} from the right side of the string.
     * <p>
     * Note: Invalid UTF-8 sequences are not trimmed.
     */
    public static Slice rightTrim(Slice utf8, int[] whiteSpaceCodePoints)
    {
        int position = lastNonMatchPosition(utf8, 0, whiteSpaceCodePoints);
        return utf8.slice(0, position);
    }

    private static int lastNonWhitespacePosition(Slice utf8, int minPosition)
    {
        int position = utf8.length();
        while (minPosition < position) {
            // decode the code point before position if possible
            int codePoint;
            int codePointLength;
            byte unsignedByte = utf8.getByte(position - 1);
            if (!isContinuationByte(unsignedByte)) {
                codePoint = unsignedByte & 0xFF;
                codePointLength = 1;
            }
            else if (minPosition <= position -2 && !isContinuationByte(utf8.getByte(position - 2))) {
                codePoint = tryGetCodePointAt(utf8, position - 2);
                codePointLength = 2;
            }
            else if (minPosition <= position -3 && !isContinuationByte(utf8.getByte(position - 3))) {
                codePoint = tryGetCodePointAt(utf8, position - 3);
                codePointLength = 3;
            }
            else if (minPosition <= position -4 && !isContinuationByte(utf8.getByte(position - 4))) {
                codePoint = tryGetCodePointAt(utf8, position - 4);
                codePointLength = 4;
            }
            else {
                break;
            }
            if (codePoint < 0 || codePointLength != lengthOfCodePoint(codePoint)) {
                break;
            }
            if (!WHITESPACE_CODE_POINTS[codePoint]) {
                break;
            }
            position -= codePointLength;
        }
        return position;
    }

    // This function is an exact duplicate of lastNonWhitespacePosition(Slice, int) except for one line.
    private static int lastNonMatchPosition(Slice utf8, int minPosition, int[] codePointsToMatch)
    {
        int position = utf8.length();
        while (position > minPosition) {
            // decode the code point before position if possible
            int codePoint;
            int codePointLength;
            byte unsignedByte = utf8.getByte(position - 1);
            if (!isContinuationByte(unsignedByte)) {
                codePoint = unsignedByte & 0xFF;
                codePointLength = 1;
            }
            else if (minPosition <= position - 2 && !isContinuationByte(utf8.getByte(position - 2))) {
                codePoint = tryGetCodePointAt(utf8, position - 2);
                codePointLength = 2;
            }
            else if (minPosition <= position - 3 && !isContinuationByte(utf8.getByte(position - 3))) {
                codePoint = tryGetCodePointAt(utf8, position - 3);
                codePointLength = 3;
            }
            else if (minPosition <= position - 4 && !isContinuationByte(utf8.getByte(position - 4))) {
                codePoint = tryGetCodePointAt(utf8, position - 4);
                codePointLength = 4;
            }
            else {
                break;
            }
            if (codePoint < 0 || codePointLength != lengthOfCodePoint(codePoint)) {
                break;
            }
            if (!matches(codePoint, codePointsToMatch)) {
                break;
            }
            position -= codePointLength;
        }
        return position;
    }

    /**
     * Removes all white space characters from the left and right side of the string.
     * <p>
     * Note: Invalid UTF-8 sequences are not trimmed.
     */
    public static Slice trim(Slice utf8)
    {
        int start = firstNonWhitespacePosition(utf8);
        int end = lastNonWhitespacePosition(utf8, start);
        return utf8.slice(start, end - start);
    }

    /**
     * Removes all white {@code whiteSpaceCodePoints} from the left and right side of the string.
     * <p>
     * Note: Invalid UTF-8 sequences are not trimmed.
     */
    public static Slice trim(Slice utf8, int[] whiteSpaceCodePoints)
    {
        int start = firstNonMatchPosition(utf8, whiteSpaceCodePoints);
        int end = lastNonMatchPosition(utf8, start, whiteSpaceCodePoints);
        return utf8.slice(start, end - start);
    }

    public static Slice fixInvalidUtf8(Slice slice)
    {
        return fixInvalidUtf8(slice, OptionalInt.of(REPLACEMENT_CODE_POINT));
    }

    public static Slice fixInvalidUtf8(Slice slice, OptionalInt replacementCodePoint)
    {
        if (isAscii(slice)) {
            return slice;
        }

        int replacementCodePointValue = -1;
        int replacementCodePointLength = 0;
        if (replacementCodePoint.isPresent()) {
            replacementCodePointValue = replacementCodePoint.getAsInt();
            replacementCodePointLength = lengthOfCodePoint(replacementCodePointValue);
        }

        int length = slice.length();
        Slice utf8 = Slices.allocate(length);

        int dataPosition = 0;
        int utf8Position = 0;
        while (dataPosition < length) {
            int codePoint = tryGetCodePointAt(slice, dataPosition);
            int codePointLength;
            if (codePoint >= 0) {
                codePointLength = lengthOfCodePoint(codePoint);
                dataPosition += codePointLength;
            }
            else {
                // negative number carries the number of invalid bytes
                dataPosition += (-codePoint);
                if (replacementCodePointValue < 0) {
                    continue;
                }
                codePoint = replacementCodePointValue;
                codePointLength = replacementCodePointLength;
            }
            utf8 = Slices.ensureSize(utf8, utf8Position + codePointLength);
            utf8Position += setCodePointAt(codePoint, utf8, utf8Position);
        }
        return utf8.slice(0, utf8Position);
    }

    /**
     * Tries to get the UTF-8 encoded code point at the {@code position}.  A positive
     * return value means the UTF-8 sequence at the position is valid, and the result
     * is the code point.  A negative return value means the UTF-8 sequence at the
     * position is invalid, and the length of the invalid sequence is the absolute
     * value of the result.
     * @return the code point or negative the number of bytes in the invalid UTF-8 sequence.
     */
    public static int tryGetCodePointAt(Slice utf8, int position)
    {
        //
        // Process first byte
        byte firstByte = utf8.getByte(position);

        int length = lengthOfCodePointFromStartByteSafe(firstByte);
        if (length < 0) {
            return length;
        }

        if (length == 1) {
            // normal ASCII
            // 0xxx_xxxx
            return firstByte;
        }

        //
        // Process second byte
        if (position + 1 >= utf8.length()) {
            return -1;
        }

        byte secondByte = utf8.getByteUnchecked(position + 1);
        if (!isContinuationByte(secondByte)) {
            return -1;
        }

        if (length == 2) {
            // 110x_xxxx 10xx_xxxx
            int codePoint = ((firstByte & 0b0001_1111) << 6) |
                    (secondByte & 0b0011_1111);
            // fail if overlong encoding
            return codePoint < 0x80 ? -2 : codePoint;
        }

        //
        // Process third byte
        if (position + 2 >= utf8.length()) {
            return -2;
        }

        byte thirdByte = utf8.getByteUnchecked(position + 2);
        if (!isContinuationByte(thirdByte)) {
            return -2;
        }

        if (length == 3) {
            // 1110_xxxx 10xx_xxxx 10xx_xxxx
            int codePoint = ((firstByte & 0b0000_1111) << 12) |
                    ((secondByte & 0b0011_1111) << 6) |
                    (thirdByte & 0b0011_1111);

            // surrogates are invalid
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                return -3;
            }
            // fail if overlong encoding
            return codePoint < 0x800 ? -3 : codePoint;
        }

        //
        // Process forth byte
        if (position + 3 >= utf8.length()) {
            return -3;
        }

        byte forthByte = utf8.getByteUnchecked(position + 3);
        if (!isContinuationByte(forthByte)) {
            return -3;
        }

        if (length == 4) {
            // 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
            int codePoint = ((firstByte & 0b0000_0111) << 18) |
                    ((secondByte & 0b0011_1111) << 12) |
                    ((thirdByte & 0b0011_1111) << 6) |
                    (forthByte & 0b0011_1111);
            // fail if overlong encoding or above upper bound of Unicode
            if (codePoint < 0x11_0000 && codePoint >= 0x1_0000) {
                return codePoint;
            }
            return -4;
        }

        //
        // Process fifth byte
        if (position + 4 >= utf8.length()) {
            return -4;
        }

        byte fifthByte = utf8.getByteUnchecked(position + 4);
        if (!isContinuationByte(fifthByte)) {
            return -4;
        }

        if (length == 5) {
            // Per RFC3629, UTF-8 is limited to 4 bytes, so more bytes are illegal
            return -5;
        }

        //
        // Process sixth byte
        if (position + 5 >= utf8.length()) {
            return -5;
        }

        byte sixthByte = utf8.getByteUnchecked(position + 5);
        if (!isContinuationByte(sixthByte)) {
            return -5;
        }

        if (length == 6) {
            // Per RFC3629, UTF-8 is limited to 4 bytes, so more bytes are illegal
            return -6;
        }

        // for longer sequence, which can't happen
        return -1;
    }

    static int lengthOfCodePointFromStartByteSafe(byte startByte)
    {
        int unsignedStartByte = startByte & 0xFF;
        if (unsignedStartByte < 0b1000_0000) {
            // normal ASCII
            // 0xxx_xxxx
            return 1;
        }
        if (unsignedStartByte < 0b1100_0000) {
            // illegal bytes
            // 10xx_xxxx
            return -1;
        }
        if (unsignedStartByte < 0b1110_0000) {
            // 110x_xxxx 10xx_xxxx
            return 2;
        }
        if (unsignedStartByte < 0b1111_0000) {
            // 1110_xxxx 10xx_xxxx 10xx_xxxx
            return 3;
        }
        if (unsignedStartByte < 0b1111_1000) {
            // 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
            return 4;
        }
        if (unsignedStartByte < 0b1111_1100) {
            // 1111_10xx 10xx_xxxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
            return 5;
        }
        if (unsignedStartByte < 0b1111_1110) {
            // 1111_110x 10xx_xxxx 10xx_xxxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
            return 6;
        }
        return -1;
    }

    /**
     * Finds the index of the first byte of the code point at a position, or
     * {@code -1} if the position is not within the slice.
     * <p>
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
     * <p>
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
     * <p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static int lengthOfCodePoint(Slice utf8, int position)
    {
        return lengthOfCodePointFromStartByte(utf8.getByte(position));
    }

    /**
     * Gets the UTF-8 sequence length of the code point at {@code position}.
     * <p>
     * Truncated UTF-8 sequences, 5 and 6 byte sequences, and invalid code points
     * are handled by this method without throwing an exception.
     */
    public static int lengthOfCodePointSafe(Slice utf8, int position)
    {
        int length = lengthOfCodePointFromStartByteSafe(utf8.getByte(position));
        if (length < 0) {
            return -length;
        }

        if (length == 1 || position + 1 >= utf8.length() || !isContinuationByte(utf8.getByteUnchecked(position + 1))) {
            return 1;
        }

        if (length == 2 || position + 2 >= utf8.length() || !isContinuationByte(utf8.getByteUnchecked(position + 2))) {
            return 2;
        }

        if (length == 3 || position + 3 >= utf8.length() || !isContinuationByte(utf8.getByteUnchecked(position + 3))) {
            return 3;
        }

        if (length == 4 || position + 4 >= utf8.length() || !isContinuationByte(utf8.getByteUnchecked(position + 4))) {
            return 4;
        }

        if (length == 5 || position + 5 >= utf8.length() || !isContinuationByte(utf8.getByteUnchecked(position + 5))) {
            return 5;
        }

        if (length == 6) {
            return 6;
        }

        return 1;
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
     * <p>
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
     * <p>
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
     * <p>
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
     * <p>
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
