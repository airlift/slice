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

import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.OptionalInt;

import static io.airlift.slice.Preconditions.checkArgument;
import static io.airlift.slice.Preconditions.verify;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_SUPPLEMENTARY_CODE_POINT;
import static java.lang.Character.MIN_SURROGATE;
import static java.lang.Integer.toHexString;
import static java.lang.invoke.MethodHandles.byteArrayViewVarHandle;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.checkIndex;

/**
 * Utility methods for UTF-8 encoded slices.
 */
public final class SliceUtf8
{
    private SliceUtf8() {}

    private static final int MIN_HIGH_SURROGATE_CODE_POINT = 0xD800;
    private static final int REPLACEMENT_CODE_POINT = 0xFFFD;

    private static final VarHandle SHORT_HANDLE = byteArrayViewVarHandle(short[].class, LITTLE_ENDIAN);
    private static final VarHandle INT_HANDLE = byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN);
    private static final VarHandle LONG_HANDLE = byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

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
        return isAscii(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length());
    }

    /**
     * Does the byte array range contain only 7-bit ASCII characters.
     */
    public static boolean isAscii(byte[] utf8, int offset, int length)
    {
        checkFromIndexSize(offset, length, utf8.length);
        return isAsciiRaw(utf8, offset, length);
    }

    private static boolean isAsciiRaw(byte[] utf8, int utf8Offset, int utf8Length)
    {
        int offset = 0;

        // Length rounded to 8 bytes
        int length8 = utf8Length & 0x7FFF_FFF8;
        for (; offset < length8; offset += 8) {
            if (((long) LONG_HANDLE.get(utf8, utf8Offset + offset) & TOP_MASK64) != 0) {
                return false;
            }
        }
        // Enough bytes left for 32 bits?
        if (offset <= utf8Length - Integer.BYTES) {
            if (((int) INT_HANDLE.get(utf8, utf8Offset + offset) & TOP_MASK32) != 0) {
                return false;
            }

            offset += 4;
        }
        // Do the rest one by one
        for (; offset < utf8Length; offset++) {
            if ((utf8[utf8Offset + offset] & 0x80) != 0) {
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
        return countCodePoints(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length());
    }

    /**
     * Counts the code points within UTF-8 encoded byte array range.
     * <p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static int countCodePoints(byte[] utf8, int offset, int length)
    {
        checkFromIndexSize(offset, length, utf8.length);
        return countCodePoints(utf8, offset, length, 0, length);
    }

    /**
     * Counts the code points within UTF-8 encoded slice up to {@code length}.
     * <p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static int countCodePoints(Slice utf8, int offset, int length)
    {
        return countCodePoints(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), offset, length);
    }

    private static int countCodePoints(byte[] utf8, int utf8Offset, int utf8Length, int offset, int length)
    {
        checkFromIndexSize(offset, length, utf8Length);

        // Quick exit if empty string
        if (length == 0) {
            return 0;
        }

        int end = offset + length;
        int continuationBytesCount = 0;
        int lastLongStart = end - 8;
        for (; offset <= lastLongStart; offset += 8) {
            // Count bytes which are NOT the start of a code point
            continuationBytesCount += countContinuationBytes((long) LONG_HANDLE.get(utf8, utf8Offset + offset));
        }
        // Enough bytes left for 32 bits?
        if (offset <= end - 4) {
            // Count bytes which are NOT the start of a code point
            continuationBytesCount += countContinuationBytes((int) INT_HANDLE.get(utf8, utf8Offset + offset));

            offset += 4;
        }
        // Do the rest one by one
        for (; offset < end; offset++) {
            // Count bytes which are NOT the start of a code point
            continuationBytesCount += countContinuationBytes(utf8[utf8Offset + offset]);
        }

        verify(continuationBytesCount <= length);
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
        return substring(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), codePointStart, codePointLength);
    }

    /**
     * Gets the substring within byte array range starting at {@code codePointStart}
     * and extending for {@code codePointLength} code points.
     * <p>
     * Note: This method does not explicitly check for valid UTF-8, and may
     * return incorrect results or throw an exception for invalid UTF-8.
     */
    public static Slice substring(byte[] utf8, int offset, int length, int codePointStart, int codePointLength)
    {
        checkFromIndexSize(offset, length, utf8.length);
        return substringRaw(utf8, offset, length, codePointStart, codePointLength);
    }

    private static Slice substringRaw(byte[] utf8, int utf8Offset, int utf8Length, int codePointStart, int codePointLength)
    {
        checkArgument(codePointStart >= 0, "codePointStart is negative");
        checkArgument(codePointLength >= 0, "codePointLength is negative");

        int indexStart = offsetOfCodePoint(utf8, utf8Offset, utf8Length, codePointStart);
        if (indexStart < 0) {
            throw new IllegalArgumentException("UTF-8 does not contain " + codePointStart + " code points");
        }
        if (codePointLength == 0) {
            return Slices.EMPTY_SLICE;
        }
        int indexEnd = offsetOfCodePoint(utf8, utf8Offset, utf8Length, indexStart, codePointLength - 1);
        if (indexEnd < 0) {
            throw new IllegalArgumentException("UTF-8 does not contain " + (codePointStart + codePointLength) + " code points");
        }
        indexEnd += lengthOfCodePoint(utf8, utf8Offset, utf8Length, indexEnd);
        if (indexEnd > utf8Length) {
            throw new InvalidUtf8Exception("UTF-8 is not well formed");
        }
        return Slices.wrappedBuffer(utf8, utf8Offset + indexStart, indexEnd - indexStart);
    }

    /**
     * Reverses the slice code point by code point.
     * <p>
     * Note: Invalid UTF-8 sequences are copied directly to the output.
     */
    public static Slice reverse(Slice utf8)
    {
        return reverse(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length());
    }

    /**
     * Reverses a UTF-8 byte array range code point by code point.
     * <p>
     * Note: Invalid UTF-8 sequences are copied directly to the output.
     */
    public static Slice reverse(byte[] utf8, int offset, int length)
    {
        checkFromIndexSize(offset, length, utf8.length);
        return reverseRaw(utf8, offset, length);
    }

    private static Slice reverseRaw(byte[] utf8, int utf8Offset, int utf8Length)
    {
        if (isAsciiRaw(utf8, utf8Offset, utf8Length)) {
            return reverseRawAscii(utf8, utf8Offset, utf8Length);
        }

        Slice reverse = Slices.allocate(utf8Length);

        int forwardPosition = 0;
        int reversePosition = utf8Length;
        while (forwardPosition < utf8Length) {
            int codePointLength = lengthOfCodePointSafeRaw(utf8, utf8Offset, utf8Length, forwardPosition);

            // backup the reverse pointer
            reversePosition -= codePointLength;
            if (reversePosition < 0) {
                // this should not happen
                throw new InvalidUtf8Exception("UTF-8 is not well formed");
            }
            // copy the character
            copyUtf8SequenceUnsafe(utf8, utf8Offset, forwardPosition, reverse, reversePosition, codePointLength);

            forwardPosition += codePointLength;
        }
        return reverse;
    }

    private static Slice reverseRawAscii(byte[] utf8, int utf8Offset, int utf8Length)
    {
        Slice reverse = Slices.allocate(utf8Length);
        int sourcePosition = utf8Length;
        int destinationPosition = 0;

        while (sourcePosition >= Long.BYTES) {
            sourcePosition -= Long.BYTES;
            long value = (long) LONG_HANDLE.get(utf8, utf8Offset + sourcePosition);
            reverse.setLongUnchecked(destinationPosition, Long.reverseBytes(value));
            destinationPosition += Long.BYTES;
        }

        if (sourcePosition >= Integer.BYTES) {
            sourcePosition -= Integer.BYTES;
            int value = (int) INT_HANDLE.get(utf8, utf8Offset + sourcePosition);
            reverse.setIntUnchecked(destinationPosition, Integer.reverseBytes(value));
            destinationPosition += Integer.BYTES;
        }

        if (sourcePosition >= Short.BYTES) {
            sourcePosition -= Short.BYTES;
            short value = (short) SHORT_HANDLE.get(utf8, utf8Offset + sourcePosition);
            reverse.setShortUnchecked(destinationPosition, Short.reverseBytes(value));
            destinationPosition += Short.BYTES;
        }

        if (sourcePosition == 1) {
            reverse.setByteUnchecked(destinationPosition, utf8[utf8Offset]);
        }
        return reverse;
    }

    /**
     * Compares to UTF-8 sequences using UTF-16 big endian semantics.  This is
     * equivalent to the {@link java.lang.String#compareTo(String)}.
     * {@code java.lang.String}.
     *
     * Note: this method validates UTF-8 only for byte regions it decodes during
     * comparison. Invalid UTF-8 in unvisited suffix regions may not be detected.
     *
     * @throws InvalidUtf8Exception if invalid UTF-8 is encountered while decoding
     */
    public static int compareUtf16BE(Slice utf8Left, Slice utf8Right)
    {
        return compareUtf16BE(
                utf8Left.byteArray(), utf8Left.byteArrayOffset(), utf8Left.length(),
                utf8Right.byteArray(), utf8Right.byteArrayOffset(), utf8Right.length());
    }

    /**
     * Compares two UTF-8 byte array ranges using UTF-16 big endian semantics.
     *
     * Note: this method validates UTF-8 only for byte regions it decodes during
     * comparison. Invalid UTF-8 in unvisited suffix regions may not be detected.
     *
     * @throws InvalidUtf8Exception if invalid UTF-8 is encountered while decoding
     */
    public static int compareUtf16BE(byte[] utf8Left, int leftOffset, int leftLength, byte[] utf8Right, int rightOffset, int rightLength)
    {
        checkFromIndexSize(leftOffset, leftLength, utf8Left.length);
        checkFromIndexSize(rightOffset, rightLength, utf8Right.length);
        return compareUtf16BERaw(utf8Left, leftOffset, leftLength, utf8Right, rightOffset, rightLength);
    }

    private static int compareUtf16BERaw(byte[] utf8Left, int leftOffset, int leftLength, byte[] utf8Right, int rightOffset, int rightLength)
    {
        int offset = 0;
        int equalPrefixLength = Math.min(leftLength, rightLength);
        int ascii64Limit = equalPrefixLength - Long.BYTES;
        int ascii32Limit = equalPrefixLength - Integer.BYTES;

        while (offset < leftLength) {
            while (offset <= ascii64Limit) {
                long leftLong = (long) LONG_HANDLE.get(utf8Left, leftOffset + offset);
                long rightLong = (long) LONG_HANDLE.get(utf8Right, rightOffset + offset);
                if ((((leftLong | rightLong) & TOP_MASK64) != 0) || leftLong != rightLong) {
                    break;
                }
                offset += Long.BYTES;
            }

            while (offset <= ascii32Limit) {
                int leftInt = (int) INT_HANDLE.get(utf8Left, leftOffset + offset);
                int rightInt = (int) INT_HANDLE.get(utf8Right, rightOffset + offset);
                if ((((leftInt | rightInt) & TOP_MASK32) != 0) || leftInt != rightInt) {
                    break;
                }
                offset += Integer.BYTES;
            }

            // chunk skipping can consume the full left range
            if (offset >= leftLength) {
                break;
            }

            // if there are no more right code points, right is less
            if (offset >= rightLength) {
                return 1; // left.compare(right) > 0
            }

            int leftByte = utf8Left[leftOffset + offset] & 0xFF;
            int rightByte = utf8Right[rightOffset + offset] & 0xFF;
            if ((leftByte | rightByte) < 0x80) {
                if (leftByte != rightByte) {
                    return Integer.compare(leftByte, rightByte);
                }
                offset++;
                continue;
            }

            if (leftByte == rightByte) {
                int leftCodePoint = tryGetCodePointAtRaw(utf8Left, leftOffset, leftLength, offset);
                if (leftCodePoint < 0) {
                    throw new InvalidUtf8Exception("Invalid UTF-8 sequence in utf8Left at " + offset);
                }

                int leftCodePointLength = lengthOfCodePoint(leftCodePoint);
                if (offset + leftCodePointLength <= rightLength && utf8SequencesEqual(utf8Left, leftOffset + offset, utf8Right, rightOffset + offset, leftCodePointLength)) {
                    offset += leftCodePointLength;
                    continue;
                }

                int rightCodePoint = tryGetCodePointAtRaw(utf8Right, rightOffset, rightLength, offset);
                if (rightCodePoint < 0) {
                    throw new InvalidUtf8Exception("Invalid UTF-8 sequence in utf8Right at " + offset);
                }

                int result = compareUtf16BE(leftCodePoint, rightCodePoint);
                if (result != 0) {
                    return result;
                }

                offset += leftCodePointLength;
                continue;
            }

            int leftCodePoint = tryGetCodePointAtRaw(utf8Left, leftOffset, leftLength, offset);
            if (leftCodePoint < 0) {
                throw new InvalidUtf8Exception("Invalid UTF-8 sequence in utf8Left at " + offset);
            }

            int rightCodePoint = tryGetCodePointAtRaw(utf8Right, rightOffset, rightLength, offset);
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

    private static boolean utf8SequencesEqual(byte[] left, int leftStart, byte[] right, int rightStart, int length)
    {
        switch (length) {
            case 1 -> {
                return left[leftStart] == right[rightStart];
            }
            case 2 -> {
                return (short) SHORT_HANDLE.get(left, leftStart) == (short) SHORT_HANDLE.get(right, rightStart);
            }
            case 3 -> {
                return (short) SHORT_HANDLE.get(left, leftStart) == (short) SHORT_HANDLE.get(right, rightStart) &&
                        left[leftStart + 2] == right[rightStart + 2];
            }
            case 4 -> {
                return (int) INT_HANDLE.get(left, leftStart) == (int) INT_HANDLE.get(right, rightStart);
            }
            default -> throw new IllegalArgumentException("Invalid UTF-8 sequence length: " + length);
        }
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
     * not perform locale-sensitive, context-sensitive, or one-to-many
     * mappings required for some languages.  Specifically, this will return
     * incorrect results for Lithuanian, Turkish, and Azeri.
     * <p>
     * Note: Invalid UTF-8 sequences are copied directly to the output.
     */
    public static Slice toUpperCase(Slice utf8)
    {
        return toUpperCase(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length());
    }

    /**
     * Converts byte array range to upper case code point by code point.
     */
    public static Slice toUpperCase(byte[] utf8, int offset, int length)
    {
        checkFromIndexSize(offset, length, utf8.length);
        return toUpperCaseAsciiOrCodePoints(utf8, offset, length);
    }

    /**
     * Converts slice to lower case code point by code point.  This method does
     * not perform locale-sensitive, context-sensitive, or one-to-many
     * mappings required for some languages.  Specifically, this will return
     * incorrect results for Lithuanian, Turkish, and Azeri.
     * <p>
     * Note: Invalid UTF-8 sequences are copied directly to the output.
     */
    public static Slice toLowerCase(Slice utf8)
    {
        return toLowerCase(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length());
    }

    /**
     * Converts byte array range to lower case code point by code point.
     */
    public static Slice toLowerCase(byte[] utf8, int offset, int length)
    {
        checkFromIndexSize(offset, length, utf8.length);
        return toLowerCaseAsciiOrCodePoints(utf8, offset, length);
    }

    private static Slice translateCodePoints(byte[] utf8, int utf8Offset, int utf8Length, int[] codePointTranslationMap)
    {
        return translateCodePoints(utf8, utf8Offset, utf8Length, 0, null, 0, codePointTranslationMap);
    }

    private static Slice translateCodePoints(byte[] utf8, int utf8Offset, int utf8Length, int position, Slice translatedUtf8, int translatedPosition, int[] codePointTranslationMap)
    {
        while (position < utf8Length) {
            int codePoint = tryGetCodePointAtRaw(utf8, utf8Offset, utf8Length, position);
            if (codePoint >= 0) {
                int translatedCodePoint = codePointTranslationMap[codePoint];
                int codePointLength = lengthOfCodePoint(codePoint);

                if (translatedCodePoint == codePoint) {
                    if (translatedUtf8 != null) {
                        int nextTranslatedPosition = translatedPosition + codePointLength;
                        if (nextTranslatedPosition > utf8Length) {
                            translatedUtf8 = Slices.ensureSize(translatedUtf8, nextTranslatedPosition);
                        }

                        copyUtf8SequenceUnsafe(utf8, utf8Offset, position, translatedUtf8, translatedPosition, codePointLength);
                        translatedPosition = nextTranslatedPosition;
                    }
                    position += codePointLength;
                    continue;
                }

                if (translatedUtf8 == null) {
                    translatedUtf8 = Slices.allocate(utf8Length);
                    translatedUtf8.setBytes(0, utf8, utf8Offset, position);
                    translatedPosition = position;
                }

                // grow slice if necessary
                int nextTranslatedPosition = translatedPosition + lengthOfCodePoint(translatedCodePoint);
                if (nextTranslatedPosition > utf8Length) {
                    translatedUtf8 = Slices.ensureSize(translatedUtf8, nextTranslatedPosition);
                }

                // write translated code point
                setCodePointAt(translatedCodePoint, translatedUtf8, translatedPosition);

                position += codePointLength;
                translatedPosition = nextTranslatedPosition;
            }
            else {
                int skipLength = -codePoint;

                if (translatedUtf8 != null) {
                    // grow slice if necessary
                    int nextTranslatedPosition = translatedPosition + skipLength;
                    if (nextTranslatedPosition > utf8Length) {
                        translatedUtf8 = Slices.ensureSize(translatedUtf8, nextTranslatedPosition);
                    }

                    copyUtf8SequenceUnsafe(utf8, utf8Offset, position, translatedUtf8, translatedPosition, skipLength);
                    translatedPosition = nextTranslatedPosition;
                }
                position += skipLength;
            }
        }
        if (translatedUtf8 == null) {
            return Slices.wrappedBuffer(utf8, utf8Offset, utf8Length);
        }
        return translatedUtf8.slice(0, translatedPosition);
    }

    private static Slice toUpperCaseAsciiOrCodePoints(byte[] utf8, int utf8Offset, int utf8Length)
    {
        Slice translated = Slices.allocate(utf8Length);
        int position = 0;
        while (position < utf8Length) {
            int value = utf8[utf8Offset + position] & 0xFF;
            if (value >= 0x80) {
                return translateCodePoints(utf8, utf8Offset, utf8Length, UPPER_CODE_POINTS);
            }

            if (value >= 'a' && value <= 'z') {
                translated.setByteUnchecked(position, value - ('a' - 'A'));
            }
            else {
                translated.setByteUnchecked(position, value);
            }
            position++;
        }
        return translated;
    }

    private static Slice toLowerCaseAsciiOrCodePoints(byte[] utf8, int utf8Offset, int utf8Length)
    {
        int position = 0;

        // Fast scan until the first ASCII byte that needs translation.
        while (position < utf8Length) {
            int value = utf8[utf8Offset + position] & 0xFF;
            if (value >= 0x80) {
                return translateCodePoints(utf8, utf8Offset, utf8Length, position, null, position, LOWER_CODE_POINTS);
            }

            if (value >= 'A' && value <= 'Z') {
                break;
            }
            position++;
        }

        // Nothing to translate in the entire input.
        if (position == utf8Length) {
            return Slices.wrappedBuffer(utf8, utf8Offset, utf8Length);
        }

        Slice translated = Slices.allocate(utf8Length);
        translated.setBytes(0, utf8, utf8Offset, position);

        // Continue with a single tight loop once output exists.
        while (position < utf8Length) {
            int value = utf8[utf8Offset + position] & 0xFF;
            if (value >= 0x80) {
                return translateCodePoints(utf8, utf8Offset, utf8Length, position, translated, position, LOWER_CODE_POINTS);
            }

            if (value >= 'A' && value <= 'Z') {
                translated.setByteUnchecked(position, value + ('a' - 'A'));
            }
            else {
                translated.setByteUnchecked(position, value);
            }
            position++;
        }

        return translated;
    }

    private static void copyUtf8SequenceUnsafe(byte[] source, int sourceOffset, int sourcePosition, Slice destination, int destinationPosition, int length)
    {
        switch (length) {
            case 1 -> destination.setByteUnchecked(destinationPosition, source[sourceOffset + sourcePosition]);
            case 2 -> destination.setShortUnchecked(destinationPosition, (short) SHORT_HANDLE.get(source, sourceOffset + sourcePosition));
            case 3 -> {
                destination.setShortUnchecked(destinationPosition, (short) SHORT_HANDLE.get(source, sourceOffset + sourcePosition));
                destination.setByteUnchecked(destinationPosition + 2, source[sourceOffset + sourcePosition + 2]);
            }
            case 4 -> destination.setIntUnchecked(destinationPosition, (int) INT_HANDLE.get(source, sourceOffset + sourcePosition));
            case 5 -> {
                destination.setIntUnchecked(destinationPosition, (int) INT_HANDLE.get(source, sourceOffset + sourcePosition));
                destination.setByteUnchecked(destinationPosition + 4, source[sourceOffset + sourcePosition + 4]);
            }
            case 6 -> {
                destination.setIntUnchecked(destinationPosition, (int) INT_HANDLE.get(source, sourceOffset + sourcePosition));
                destination.setShortUnchecked(destinationPosition + 4, (short) SHORT_HANDLE.get(source, sourceOffset + sourcePosition + 4));
            }
            default -> throw new IllegalStateException("Invalid code point length " + length);
        }
    }

    /**
     * Removes all white space characters from the left side of the string.
     * <p>
     * Note: Invalid UTF-8 sequences are not trimmed.
     */
    public static Slice leftTrim(Slice utf8)
    {
        return leftTrim(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length());
    }

    /**
     * Removes all white space characters from the left side of a byte array range.
     */
    public static Slice leftTrim(byte[] utf8, int offset, int length)
    {
        checkFromIndexSize(offset, length, utf8.length);
        int position = firstNonWhitespacePosition(utf8, offset, length);
        return Slices.wrappedBuffer(utf8, offset + position, length - position);
    }

    /**
     * Removes all {@code whiteSpaceCodePoints} from the left side of the string.
     * <p>
     * Note: Invalid UTF-8 sequences are not trimmed.
     */
    public static Slice leftTrim(Slice utf8, int[] whiteSpaceCodePoints)
    {
        return leftTrim(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), whiteSpaceCodePoints);
    }

    /**
     * Removes all {@code whiteSpaceCodePoints} from the left side of a byte array range.
     */
    public static Slice leftTrim(byte[] utf8, int offset, int length, int[] whiteSpaceCodePoints)
    {
        checkFromIndexSize(offset, length, utf8.length);
        int position = firstNonMatchPosition(utf8, offset, length, whiteSpaceCodePoints);
        return Slices.wrappedBuffer(utf8, offset + position, length - position);
    }

    private static int firstNonWhitespacePosition(byte[] utf8, int utf8Offset, int utf8Length)
    {
        int position = 0;
        while (position < utf8Length) {
            int value = utf8[utf8Offset + position] & 0xFF;
            if (value < 0x80) {
                if (!WHITESPACE_CODE_POINTS[value]) {
                    break;
                }
                position++;
                continue;
            }

            int codePoint = tryGetCodePointAtRaw(utf8, utf8Offset, utf8Length, position);
            if (codePoint < 0 || !WHITESPACE_CODE_POINTS[codePoint]) {
                break;
            }

            position += lengthOfCodePoint(codePoint);
        }
        return position;
    }

    // This function mirrors firstNonWhitespacePosition but uses a caller-provided match set.
    private static int firstNonMatchPosition(byte[] utf8, int utf8Offset, int utf8Length, int[] codePointsToMatch)
    {
        long asciiMatchMaskLow = asciiMatchMaskLow(codePointsToMatch);
        long asciiMatchMaskHigh = asciiMatchMaskHigh(codePointsToMatch);

        int position = 0;
        while (position < utf8Length) {
            int value = utf8[utf8Offset + position] & 0xFF;
            if (value < 0x80) {
                if (!matches(value, codePointsToMatch, asciiMatchMaskLow, asciiMatchMaskHigh)) {
                    break;
                }
                position++;
                continue;
            }

            int codePoint = tryGetCodePointAtRaw(utf8, utf8Offset, utf8Length, position);
            if (codePoint < 0 || !matches(codePoint, codePointsToMatch, asciiMatchMaskLow, asciiMatchMaskHigh)) {
                break;
            }

            position += lengthOfCodePoint(codePoint);
        }
        return position;
    }

    /**
     * Removes all white space characters from the right side of the string.
     * <p>
     * Note: Invalid UTF-8 sequences are not trimmed.
     */
    public static Slice rightTrim(Slice utf8)
    {
        return rightTrim(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length());
    }

    /**
     * Removes all white space characters from the right side of a byte array range.
     */
    public static Slice rightTrim(byte[] utf8, int offset, int length)
    {
        checkFromIndexSize(offset, length, utf8.length);
        int position = lastNonWhitespacePosition(utf8, offset, length, 0);
        return Slices.wrappedBuffer(utf8, offset, position);
    }

    /**
     * Removes all white {@code whiteSpaceCodePoints} from the right side of the string.
     * <p>
     * Note: Invalid UTF-8 sequences are not trimmed.
     */
    public static Slice rightTrim(Slice utf8, int[] whiteSpaceCodePoints)
    {
        return rightTrim(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), whiteSpaceCodePoints);
    }

    /**
     * Removes all {@code whiteSpaceCodePoints} from the right side of a byte array range.
     */
    public static Slice rightTrim(byte[] utf8, int offset, int length, int[] whiteSpaceCodePoints)
    {
        checkFromIndexSize(offset, length, utf8.length);
        int position = lastNonMatchPosition(utf8, offset, length, 0, whiteSpaceCodePoints);
        return Slices.wrappedBuffer(utf8, offset, position);
    }

    private static boolean matches(int codePoint, int[] codePoints, long asciiMatchMaskLow, long asciiMatchMaskHigh)
    {
        if (codePoint < Long.SIZE) {
            return ((asciiMatchMaskLow >>> codePoint) & 1) == 1;
        }
        if (codePoint < (Long.SIZE * 2)) {
            return ((asciiMatchMaskHigh >>> (codePoint - Long.SIZE)) & 1) == 1;
        }

        for (int codePointToTrim : codePoints) {
            if (codePoint == codePointToTrim) {
                return true;
            }
        }
        return false;
    }

    private static long asciiMatchMaskLow(int[] codePoints)
    {
        long asciiMatchMaskLow = 0;
        for (int codePoint : codePoints) {
            if (codePoint < Long.SIZE) {
                asciiMatchMaskLow |= (1L << codePoint);
            }
        }
        return asciiMatchMaskLow;
    }

    private static long asciiMatchMaskHigh(int[] codePoints)
    {
        long asciiMatchMaskHigh = 0;
        for (int codePoint : codePoints) {
            if (codePoint >= Long.SIZE && codePoint < (Long.SIZE * 2)) {
                asciiMatchMaskHigh |= (1L << (codePoint - Long.SIZE));
            }
        }
        return asciiMatchMaskHigh;
    }

    private static int lastNonWhitespacePosition(byte[] utf8, int utf8Offset, int utf8Length, int minPosition)
    {
        int position = utf8Length;
        while (minPosition < position) {
            int value = utf8[utf8Offset + position - 1] & 0xFF;
            if (value < 0x80) {
                if (!WHITESPACE_CODE_POINTS[value]) {
                    break;
                }
                position--;
                continue;
            }

            // decode the code point before position if possible
            int codePoint;
            int codePointLength;
            if (minPosition <= position - 2 && !isContinuationByte(utf8[utf8Offset + position - 2])) {
                codePoint = tryGetCodePointAtRaw(utf8, utf8Offset, utf8Length, position - 2);
                codePointLength = 2;
            }
            else if (minPosition <= position - 3 && !isContinuationByte(utf8[utf8Offset + position - 3])) {
                codePoint = tryGetCodePointAtRaw(utf8, utf8Offset, utf8Length, position - 3);
                codePointLength = 3;
            }
            else if (minPosition <= position - 4 && !isContinuationByte(utf8[utf8Offset + position - 4])) {
                codePoint = tryGetCodePointAtRaw(utf8, utf8Offset, utf8Length, position - 4);
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

    // This function mirrors lastNonWhitespacePosition but uses a caller-provided match set.
    private static int lastNonMatchPosition(byte[] utf8, int utf8Offset, int utf8Length, int minPosition, int[] codePointsToMatch)
    {
        long asciiMatchMaskLow = asciiMatchMaskLow(codePointsToMatch);
        long asciiMatchMaskHigh = asciiMatchMaskHigh(codePointsToMatch);

        int position = utf8Length;
        while (position > minPosition) {
            int value = utf8[utf8Offset + position - 1] & 0xFF;
            if (value < 0x80) {
                if (!matches(value, codePointsToMatch, asciiMatchMaskLow, asciiMatchMaskHigh)) {
                    break;
                }
                position--;
                continue;
            }

            // decode the code point before position if possible
            int codePoint;
            int codePointLength;
            if (minPosition <= position - 2 && !isContinuationByte(utf8[utf8Offset + position - 2])) {
                codePoint = tryGetCodePointAtRaw(utf8, utf8Offset, utf8Length, position - 2);
                codePointLength = 2;
            }
            else if (minPosition <= position - 3 && !isContinuationByte(utf8[utf8Offset + position - 3])) {
                codePoint = tryGetCodePointAtRaw(utf8, utf8Offset, utf8Length, position - 3);
                codePointLength = 3;
            }
            else if (minPosition <= position - 4 && !isContinuationByte(utf8[utf8Offset + position - 4])) {
                codePoint = tryGetCodePointAtRaw(utf8, utf8Offset, utf8Length, position - 4);
                codePointLength = 4;
            }
            else {
                break;
            }
            if (codePoint < 0 || codePointLength != lengthOfCodePoint(codePoint)) {
                break;
            }
            if (!matches(codePoint, codePointsToMatch, asciiMatchMaskLow, asciiMatchMaskHigh)) {
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
        return trim(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length());
    }

    /**
     * Removes all white space characters from the left and right side of a byte array range.
     */
    public static Slice trim(byte[] utf8, int offset, int length)
    {
        checkFromIndexSize(offset, length, utf8.length);
        int start = firstNonWhitespacePosition(utf8, offset, length);
        int end = lastNonWhitespacePosition(utf8, offset, length, start);
        return Slices.wrappedBuffer(utf8, offset + start, end - start);
    }

    /**
     * Removes all white {@code whiteSpaceCodePoints} from the left and right side of the string.
     * <p>
     * Note: Invalid UTF-8 sequences are not trimmed.
     */
    public static Slice trim(Slice utf8, int[] whiteSpaceCodePoints)
    {
        return trim(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), whiteSpaceCodePoints);
    }

    /**
     * Removes all {@code whiteSpaceCodePoints} from the left and right side of a byte array range.
     */
    public static Slice trim(byte[] utf8, int offset, int length, int[] whiteSpaceCodePoints)
    {
        checkFromIndexSize(offset, length, utf8.length);
        int start = firstNonMatchPosition(utf8, offset, length, whiteSpaceCodePoints);
        int end = lastNonMatchPosition(utf8, offset, length, start, whiteSpaceCodePoints);
        return Slices.wrappedBuffer(utf8, offset + start, end - start);
    }

    public static Slice fixInvalidUtf8(Slice slice)
    {
        return fixInvalidUtf8(slice, OptionalInt.of(REPLACEMENT_CODE_POINT));
    }

    public static Slice fixInvalidUtf8(byte[] utf8, int offset, int length)
    {
        return fixInvalidUtf8(utf8, offset, length, OptionalInt.of(REPLACEMENT_CODE_POINT));
    }

    public static Slice fixInvalidUtf8(Slice slice, OptionalInt replacementCodePoint)
    {
        if (isAscii(slice)) {
            return slice;
        }
        return fixInvalidUtf8(slice.byteArray(), slice.byteArrayOffset(), slice.length(), replacementCodePoint);
    }

    public static Slice fixInvalidUtf8(byte[] utf8, int offset, int length, OptionalInt replacementCodePoint)
    {
        checkFromIndexSize(offset, length, utf8.length);
        return fixInvalidUtf8Raw(utf8, offset, length, replacementCodePoint);
    }

    private static Slice fixInvalidUtf8Raw(byte[] utf8, int utf8Offset, int utf8Length, OptionalInt replacementCodePoint)
    {
        if (isAsciiRaw(utf8, utf8Offset, utf8Length)) {
            return Slices.wrappedBuffer(utf8, utf8Offset, utf8Length);
        }

        int replacementCodePointValue = -1;
        int replacementCodePointLength = 0;
        if (replacementCodePoint.isPresent()) {
            replacementCodePointValue = replacementCodePoint.getAsInt();
            replacementCodePointLength = lengthOfCodePoint(replacementCodePointValue);
        }

        int dataPosition = 0;
        int utf8Position = 0;
        Slice output = null;
        while (dataPosition < utf8Length) {
            int codePoint = tryGetCodePointAtRaw(utf8, utf8Offset, utf8Length, dataPosition);
            if (codePoint >= 0) {
                int codePointLength = lengthOfCodePoint(codePoint);
                if (output != null) {
                    int nextUtf8Position = utf8Position + codePointLength;
                    output = Slices.ensureSize(output, nextUtf8Position);
                    copyUtf8SequenceUnsafe(utf8, utf8Offset, dataPosition, output, utf8Position, codePointLength);
                    utf8Position = nextUtf8Position;
                }
                dataPosition += codePointLength;
            }
            else {
                if (output == null) {
                    output = Slices.allocate(utf8Length);
                    output.setBytes(0, utf8, utf8Offset, dataPosition);
                    utf8Position = dataPosition;
                }

                // negative number carries the number of invalid bytes
                dataPosition += (-codePoint);
                if (replacementCodePointValue < 0) {
                    continue;
                }
                output = Slices.ensureSize(output, utf8Position + replacementCodePointLength);
                utf8Position += setCodePointAt(replacementCodePointValue, output, utf8Position);
            }
        }

        if (output == null) {
            return Slices.wrappedBuffer(utf8, utf8Offset, utf8Length);
        }
        return output.slice(0, utf8Position);
    }

    /**
     * Tries to get the UTF-8 encoded code point at the {@code position}.  A positive
     * return value means the UTF-8 sequence at the position is valid, and the result
     * is the code point.  A negative return value means the UTF-8 sequence at the
     * position is invalid, and the length of the invalid sequence is the absolute
     * value of the result.
     *
     * @return the code point or negative the number of bytes in the invalid UTF-8 sequence.
     */
    public static int tryGetCodePointAt(Slice utf8, int position)
    {
        return tryGetCodePointAt(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), position);
    }

    /**
     * Tries to get the UTF-8 encoded code point at {@code position} in the byte array range.
     *
     * @return the code point or negative the number of bytes in the invalid UTF-8 sequence.
     */
    public static int tryGetCodePointAt(byte[] utf8, int offset, int length, int position)
    {
        checkFromIndexSize(offset, length, utf8.length);
        checkIndex(position, length);
        return tryGetCodePointAtRaw(utf8, offset, length, position);
    }

    private static int tryGetCodePointAtRaw(byte[] utf8, int utf8Offset, int utf8Length, int position)
    {
        //
        // Process first byte
        byte firstByte = utf8[utf8Offset + position];

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
        if (position + 1 >= utf8Length) {
            return -1;
        }

        byte secondByte = utf8[utf8Offset + position + 1];
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
        if (position + 2 >= utf8Length) {
            return -2;
        }

        byte thirdByte = utf8[utf8Offset + position + 2];
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
        if (position + 3 >= utf8Length) {
            return -3;
        }

        byte forthByte = utf8[utf8Offset + position + 3];
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
        if (position + 4 >= utf8Length) {
            return -4;
        }

        byte fifthByte = utf8[utf8Offset + position + 4];
        if (!isContinuationByte(fifthByte)) {
            return -4;
        }

        if (length == 5) {
            // Per RFC3629, UTF-8 is limited to 4 bytes, so more bytes are illegal
            return -5;
        }

        //
        // Process sixth byte
        if (position + 5 >= utf8Length) {
            return -5;
        }

        byte sixthByte = utf8[utf8Offset + position + 5];
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

    private static int lengthOfCodePointFromStartByteSafe(byte startByte)
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
        return offsetOfCodePoint(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), 0, codePointCount);
    }

    /**
     * Finds the index of the first byte of the code point at a position within
     * a UTF-8 byte array range, or {@code -1} if the position is not within the range.
     */
    public static int offsetOfCodePoint(byte[] utf8, int offset, int length, int codePointCount)
    {
        checkFromIndexSize(offset, length, utf8.length);
        return offsetOfCodePointRaw(utf8, offset, length, 0, codePointCount);
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
        return offsetOfCodePoint(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), position, codePointCount);
    }

    /**
     * Starting from {@code position} bytes in a UTF-8 byte array range, finds the
     * index of the first byte of the code point {@code codePointCount} in the range.
     * Returned position is relative to the provided range.
     */
    public static int offsetOfCodePoint(byte[] utf8, int offset, int length, int position, int codePointCount)
    {
        checkFromIndexSize(offset, length, utf8.length);
        return offsetOfCodePointRaw(utf8, offset, length, position, codePointCount);
    }

    private static int offsetOfCodePointRaw(byte[] utf8, int utf8Offset, int utf8Length, int position, int codePointCount)
    {
        // allow position to be at the end of the slice
        checkIndex(position, utf8Length + 1);

        // Quick exit if we are sure that the position is after the end
        if (utf8Length - position <= codePointCount) {
            return -1;
        }
        if (codePointCount == 0) {
            return position;
        }

        int correctIndex = codePointCount + position;
        // Length rounded to 8 bytes
        int length8 = (utf8Length & 0x7FFF_FFF8) - 8;
        // process 8 bytes at a time
        // at most this can find 8 code points (if they are all US_ASCII), so this
        // is only called if there are at least 8 more code points needed
        while (position < length8 && correctIndex >= position + 8) {
            // Count bytes which are NOT the start of a code point
            correctIndex += countContinuationBytes((long) LONG_HANDLE.get(utf8, utf8Offset + position));

            position += 8;
        }
        // Length rounded to 4 bytes
        int length4 = (utf8Length & 0x7FFF_FFFC) - 4;
        // While we have enough bytes left, and we need at least 4 characters process 4 bytes at once
        while (position < length4 && correctIndex >= position + 4) {
            // Count bytes which are NOT the start of a code point
            correctIndex += countContinuationBytes((int) INT_HANDLE.get(utf8, utf8Offset + position));

            position += 4;
        }
        // Do the rest one by one, always check the last byte to find the end of the code point
        while (position < utf8Length) {
            // Count bytes which are NOT the start of a code point
            correctIndex += countContinuationBytes(utf8[utf8Offset + position]);
            if (position == correctIndex) {
                break;
            }

            position++;
        }

        if (position == correctIndex && correctIndex < utf8Length) {
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
        return lengthOfCodePoint(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), position);
    }

    /**
     * Gets the UTF-8 sequence length of the code point at {@code position}
     * within a UTF-8 byte array range.
     */
    public static int lengthOfCodePoint(byte[] utf8, int offset, int length, int position)
    {
        checkFromIndexSize(offset, length, utf8.length);
        checkIndex(position, length);
        return lengthOfCodePointRaw(utf8, offset, length, position);
    }

    private static int lengthOfCodePointRaw(byte[] utf8, int utf8Offset, int utf8Length, int position)
    {
        return lengthOfCodePointFromStartByte(utf8[utf8Offset + position]);
    }

    /**
     * Gets the UTF-8 sequence length of the code point at {@code position}.
     * <p>
     * Truncated UTF-8 sequences, 5 and 6 byte sequences, and invalid code points
     * are handled by this method without throwing an exception.
     */
    public static int lengthOfCodePointSafe(Slice utf8, int position)
    {
        return lengthOfCodePointSafe(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), position);
    }

    /**
     * Gets the UTF-8 sequence length of the code point at {@code position}
     * within a UTF-8 byte array range. Invalid encodings are handled safely.
     */
    public static int lengthOfCodePointSafe(byte[] utf8, int offset, int length, int position)
    {
        checkFromIndexSize(offset, length, utf8.length);
        checkIndex(position, length);
        return lengthOfCodePointSafeRaw(utf8, offset, length, position);
    }

    private static int lengthOfCodePointSafeRaw(byte[] utf8, int utf8Offset, int utf8Length, int position)
    {
        int length = lengthOfCodePointFromStartByteSafe(utf8[utf8Offset + position]);
        if (length < 0) {
            return -length;
        }

        if (length == 1 || position + 1 >= utf8Length || !isContinuationByte(utf8[utf8Offset + position + 1])) {
            return 1;
        }

        if (length == 2 || position + 2 >= utf8Length || !isContinuationByte(utf8[utf8Offset + position + 2])) {
            return 2;
        }

        if (length == 3 || position + 3 >= utf8Length || !isContinuationByte(utf8[utf8Offset + position + 3])) {
            return 3;
        }

        if (length == 4 || position + 4 >= utf8Length || !isContinuationByte(utf8[utf8Offset + position + 4])) {
            return 4;
        }

        if (length == 5 || position + 5 >= utf8Length || !isContinuationByte(utf8[utf8Offset + position + 5])) {
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
        return getCodePointAt(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), position);
    }

    /**
     * Gets the UTF-8 encoded code point at {@code position} within a UTF-8 byte array range.
     */
    public static int getCodePointAt(byte[] utf8, int offset, int length, int position)
    {
        checkFromIndexSize(offset, length, utf8.length);
        checkIndex(position, length);
        return getCodePointAtRaw(utf8, offset, length, position);
    }

    private static int getCodePointAtRaw(byte[] utf8, int utf8Offset, int utf8Length, int position)
    {
        int unsignedStartByte = utf8[utf8Offset + position] & 0xFF;
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
            if (position + 1 >= utf8Length) {
                throw new InvalidUtf8Exception("UTF-8 sequence truncated");
            }
            return ((unsignedStartByte & 0b0001_1111) << 6) |
                    (utf8[utf8Offset + position + 1] & 0b0011_1111);
        }
        if (unsignedStartByte < 0xf0) {
            // 1110_xxxx 10xx_xxxx 10xx_xxxx
            if (position + 2 >= utf8Length) {
                throw new InvalidUtf8Exception("UTF-8 sequence truncated");
            }
            return ((unsignedStartByte & 0b0000_1111) << 12) |
                    ((utf8[utf8Offset + position + 1] & 0b0011_1111) << 6) |
                    (utf8[utf8Offset + position + 2] & 0b0011_1111);
        }
        if (unsignedStartByte < 0xf8) {
            // 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
            if (position + 3 >= utf8Length) {
                throw new InvalidUtf8Exception("UTF-8 sequence truncated");
            }
            return ((unsignedStartByte & 0b0000_0111) << 18) |
                    ((utf8[utf8Offset + position + 1] & 0b0011_1111) << 12) |
                    ((utf8[utf8Offset + position + 2] & 0b0011_1111) << 6) |
                    (utf8[utf8Offset + position + 3] & 0b0011_1111);
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
        return getCodePointBefore(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), position);
    }

    /**
     * Gets the UTF-8 encoded code point before {@code position} within a UTF-8 byte array range.
     */
    public static int getCodePointBefore(byte[] utf8, int offset, int length, int position)
    {
        checkFromIndexSize(offset, length, utf8.length);
        checkFromIndexSize(position - 1, 1, length);
        return getCodePointBeforeRaw(utf8, offset, length, position);
    }

    private static int getCodePointBeforeRaw(byte[] utf8, int utf8Offset, int utf8Length, int position)
    {
        byte unsignedByte = utf8[utf8Offset + position - 1];
        if (!isContinuationByte(unsignedByte)) {
            return unsignedByte & 0xFF;
        }
        if (position < 2) {
            throw new InvalidUtf8Exception("UTF-8 is not well formed");
        }
        if (!isContinuationByte(utf8[utf8Offset + position - 2])) {
            return getCodePointAtRaw(utf8, utf8Offset, utf8Length, position - 2);
        }
        if (position < 3) {
            throw new InvalidUtf8Exception("UTF-8 is not well formed");
        }
        if (!isContinuationByte(utf8[utf8Offset + position - 3])) {
            return getCodePointAtRaw(utf8, utf8Offset, utf8Length, position - 3);
        }
        if (position < 4) {
            throw new InvalidUtf8Exception("UTF-8 is not well formed");
        }
        if (!isContinuationByte(utf8[utf8Offset + position - 4])) {
            return getCodePointAtRaw(utf8, utf8Offset, utf8Length, position - 4);
        }

        // Per RFC3629, UTF-8 is limited to 4 bytes, so more bytes are illegal
        throw new InvalidUtf8Exception("UTF-8 is not well formed");
    }

    /**
     * Decodes a UTF-8 slice into Unicode code points.
     *
     * @throws InvalidUtf8Exception if the input contains invalid UTF-8
     */
    public static int[] toCodePoints(Slice utf8)
    {
        return toCodePoints(utf8.byteArray(), utf8.byteArrayOffset(), utf8.length());
    }

    /**
     * Decodes a UTF-8 byte array range into Unicode code points.
     *
     * @throws InvalidUtf8Exception if the input contains invalid UTF-8
     */
    public static int[] toCodePoints(byte[] utf8, int offset, int length)
    {
        checkFromIndexSize(offset, length, utf8.length);
        return toCodePointsRaw(utf8, offset, length);
    }

    private static int[] toCodePointsRaw(byte[] utf8, int utf8Offset, int utf8Length)
    {
        if (utf8Length == 0) {
            return new int[0];
        }

        if (isAsciiRaw(utf8, utf8Offset, utf8Length)) {
            int[] codePoints = new int[utf8Length];
            for (int index = 0; index < utf8Length; index++) {
                codePoints[index] = utf8[utf8Offset + index] & 0x7F;
            }
            return codePoints;
        }

        int[] codePoints = new int[Math.max(8, utf8Length >>> 1)];
        int codePointCount = 0;
        int position = 0;
        while (position < utf8Length) {
            int codePoint = tryGetCodePointAtRaw(utf8, utf8Offset, utf8Length, position);
            if (codePoint < 0) {
                throw new InvalidUtf8Exception("Invalid UTF-8 sequence at position " + position);
            }

            if (codePointCount == codePoints.length) {
                codePoints = Arrays.copyOf(codePoints, codePoints.length * 2);
            }
            codePoints[codePointCount] = codePoint;
            codePointCount++;

            if (codePoint < 0x80) {
                position++;
            }
            else if (codePoint < 0x800) {
                position += 2;
            }
            else if (codePoint < 0x1_0000) {
                position += 3;
            }
            else {
                position += 4;
            }
        }

        if (codePointCount == codePoints.length) {
            return codePoints;
        }
        return Arrays.copyOf(codePoints, codePointCount);
    }

    /**
     * Encodes Unicode code points into UTF-8.
     *
     * @throws InvalidCodePointException if any code point is invalid
     */
    public static Slice fromCodePoints(int[] codePoints)
    {
        return fromCodePoints(codePoints, 0, codePoints.length);
    }

    /**
     * Encodes a range of Unicode code points into UTF-8.
     *
     * @throws InvalidCodePointException if any code point is invalid
     */
    public static Slice fromCodePoints(int[] codePoints, int offset, int length)
    {
        checkFromIndexSize(offset, length, codePoints.length);
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        return fromCodePointsRaw(codePoints, offset, length);
    }

    private static Slice fromCodePointsRaw(int[] codePoints, int codePointsOffset, int codePointsLength)
    {
        int utf8Length = 0;
        boolean ascii = true;
        for (int index = 0; index < codePointsLength; index++) {
            int codePoint = codePoints[codePointsOffset + index];
            int codePointLength = lengthOfCodePoint(codePoint);
            if (codePointLength == 3 && MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                throw new InvalidCodePointException(codePoint);
            }
            utf8Length += codePointLength;
            ascii = ascii && (codePointLength == 1);
        }

        byte[] utf8 = new byte[utf8Length];
        if (ascii) {
            for (int index = 0; index < codePointsLength; index++) {
                utf8[index] = (byte) codePoints[codePointsOffset + index];
            }
            return Slices.wrappedBuffer(utf8);
        }

        int position = 0;
        for (int index = 0; index < codePointsLength; index++) {
            int codePoint = codePoints[codePointsOffset + index];
            if (codePoint < 0x80) {
                utf8[position] = (byte) codePoint;
                position++;
            }
            else if (codePoint < 0x800) {
                utf8[position] = (byte) (0b1100_0000 | (codePoint >>> 6));
                utf8[position + 1] = (byte) (0b1000_0000 | (codePoint & 0b0011_1111));
                position += 2;
            }
            else if (codePoint < 0x1_0000) {
                utf8[position] = (byte) (0b1110_0000 | ((codePoint >>> 12) & 0b0000_1111));
                utf8[position + 1] = (byte) (0b1000_0000 | ((codePoint >>> 6) & 0b0011_1111));
                utf8[position + 2] = (byte) (0b1000_0000 | (codePoint & 0b0011_1111));
                position += 3;
            }
            else {
                utf8[position] = (byte) (0b1111_0000 | ((codePoint >>> 18) & 0b0000_0111));
                utf8[position + 1] = (byte) (0b1000_0000 | ((codePoint >>> 12) & 0b0011_1111));
                utf8[position + 2] = (byte) (0b1000_0000 | ((codePoint >>> 6) & 0b0011_1111));
                utf8[position + 3] = (byte) (0b1000_0000 | (codePoint & 0b0011_1111));
                position += 4;
            }
        }

        return Slices.wrappedBuffer(utf8);
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
        return setCodePointAt(codePoint, utf8.byteArray(), utf8.byteArrayOffset(), utf8.length(), position);
    }

    /**
     * Sets the UTF-8 sequence for code point at {@code position} within a UTF-8 byte array range.
     *
     * @throws InvalidCodePointException if code point is not within a valid range
     */
    public static int setCodePointAt(int codePoint, byte[] utf8, int offset, int length, int position)
    {
        checkFromIndexSize(offset, length, utf8.length);
        checkIndex(position, length);
        int codePointLength = lengthOfCodePoint(codePoint);
        if (codePointLength == 3 && MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
            throw new InvalidCodePointException(codePoint);
        }
        checkFromIndexSize(position, codePointLength, length);
        int start = offset + position;

        switch (codePointLength) {
            case 1 -> {
                // 0xxx_xxxx
                utf8[start] = (byte) codePoint;
            }
            case 2 -> {
                // 110x_xxxx 10xx_xxxx
                utf8[start] = (byte) (0b1100_0000 | (codePoint >>> 6));
                utf8[start + 1] = (byte) (0b1000_0000 | (codePoint & 0b0011_1111));
            }
            case 3 -> {
                // 1110_xxxx 10xx_xxxx 10xx_xxxx
                utf8[start] = (byte) (0b1110_0000 | ((codePoint >>> 12) & 0b0000_1111));
                utf8[start + 1] = (byte) (0b1000_0000 | ((codePoint >>> 6) & 0b0011_1111));
                utf8[start + 2] = (byte) (0b1000_0000 | (codePoint & 0b0011_1111));
            }
            case 4 -> {
                // 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
                utf8[start] = (byte) (0b1111_0000 | ((codePoint >>> 18) & 0b0000_0111));
                utf8[start + 1] = (byte) (0b1000_0000 | ((codePoint >>> 12) & 0b0011_1111));
                utf8[start + 2] = (byte) (0b1000_0000 | ((codePoint >>> 6) & 0b0011_1111));
                utf8[start + 3] = (byte) (0b1000_0000 | (codePoint & 0b0011_1111));
            }
            default -> throw new InvalidCodePointException(codePoint);
        }
        return codePointLength;
    }

    private static int setCodePointAtUnchecked(int codePoint, byte[] utf8, int position)
    {
        if (codePoint < 0) {
            throw new InvalidCodePointException(codePoint);
        }
        if (codePoint < 0x80) {
            // normal ASCII
            // 0xxx_xxxx
            utf8[position] = (byte) codePoint;
            return 1;
        }
        if (codePoint < 0x800) {
            // 110x_xxxx 10xx_xxxx
            utf8[position] = (byte) (0b1100_0000 | (codePoint >>> 6));
            utf8[position + 1] = (byte) (0b1000_0000 | (codePoint & 0b0011_1111));
            return 2;
        }
        if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
            throw new InvalidCodePointException(codePoint);
        }
        if (codePoint < 0x1_0000) {
            // 1110_xxxx 10xx_xxxx 10xx_xxxx
            utf8[position] = (byte) (0b1110_0000 | ((codePoint >>> 12) & 0b0000_1111));
            utf8[position + 1] = (byte) (0b1000_0000 | ((codePoint >>> 6) & 0b0011_1111));
            utf8[position + 2] = (byte) (0b1000_0000 | (codePoint & 0b0011_1111));
            return 3;
        }
        if (codePoint < 0x11_0000) {
            // 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
            utf8[position] = (byte) (0b1111_0000 | ((codePoint >>> 18) & 0b0000_0111));
            utf8[position + 1] = (byte) (0b1000_0000 | ((codePoint >>> 12) & 0b0011_1111));
            utf8[position + 2] = (byte) (0b1000_0000 | ((codePoint >>> 6) & 0b0011_1111));
            utf8[position + 3] = (byte) (0b1000_0000 | (codePoint & 0b0011_1111));
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
        // 3. And together the results of step 1 and 2, giving us a one in the 7th
        //    position if the byte matched.
        // 4. Count the number of bits in the result, which is the number of bytes
        //    that matched.
        i64 = ((i64 & TOP_MASK64) >>> 1) & (~i64);
        return Long.bitCount(i64);
    }
}
