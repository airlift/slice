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

final class NumericParsers
{
    private NumericParsers() {}

    public static long parseSignedLong(byte[] bytes, int offset, int length, int radix)
    {
        if (bytes == null || length == 0) {
            throw new NumberFormatException("empty value");
        }
        checkRadix(radix);
        int index = offset;
        int end = offset + length;
        boolean negative = false;
        int first = bytes[index] & 0xFF;
        if (first == '-' || first == '+') {
            negative = (first == '-');
            index++;
            if (index >= end) {
                throw new NumberFormatException("sign without digits");
            }
        }

        int base = normalizeRadix(bytes, index, end, radix);
        if (base == 16 && index + 1 < end && bytes[index] == '0' && ((bytes[index + 1] & 0xFF) == 'x' || (bytes[index + 1] & 0xFF) == 'X')) {
            index += 2;
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / base;
        long result = 0;
        boolean any = false;
        for (; index < end; index++) {
            int digit = decodeDigit(bytes[index] & 0xFF);
            if (digit < 0 || digit >= base) {
                throw new NumberFormatException("invalid digit");
            }
            any = true;
            if (result < multMin) {
                throw new NumberFormatException("overflow");
            }
            result *= base;
            if (result < limit + digit) {
                throw new NumberFormatException("overflow");
            }
            result -= digit;
        }
        if (!any) {
            throw new NumberFormatException("empty value");
        }
        return negative ? result : -result;
    }

    public static long parseUnsignedLong(byte[] bytes, int offset, int length, int radix)
    {
        if (bytes == null || length == 0) {
            throw new NumberFormatException("empty value");
        }
        checkRadix(radix);
        int index = offset;
        int end = offset + length;
        int first = bytes[index] & 0xFF;
        if (first == '+') {
            index++;
            if (index >= end) {
                throw new NumberFormatException("sign without digits");
            }
        }
        else if (first == '-') {
            throw new NumberFormatException("negative value");
        }

        int base = normalizeRadix(bytes, index, end, radix);
        if (base == 16 && index + 1 < end && bytes[index] == '0' && ((bytes[index + 1] & 0xFF) == 'x' || (bytes[index + 1] & 0xFF) == 'X')) {
            index += 2;
        }

        long max = -1L;
        long maxDiv = Long.divideUnsigned(max, base);
        long maxRem = Long.remainderUnsigned(max, base);
        long result = 0;
        boolean any = false;
        for (; index < end; index++) {
            int digit = decodeDigit(bytes[index] & 0xFF);
            if (digit < 0 || digit >= base) {
                throw new NumberFormatException("invalid digit");
            }
            any = true;
            if (Long.compareUnsigned(result, maxDiv) > 0 ||
                    (Long.compareUnsigned(result, maxDiv) == 0 && digit > maxRem)) {
                throw new NumberFormatException("overflow");
            }
            result = result * base + digit;
        }
        if (!any) {
            throw new NumberFormatException("empty value");
        }
        return result;
    }

    private static int normalizeRadix(byte[] bytes, int index, int end, int radix)
    {
        int base = radix;
        if (base == 0 || base == 16) {
            if (index + 1 < end && bytes[index] == '0' && ((bytes[index + 1] & 0xFF) == 'x' || (bytes[index + 1] & 0xFF) == 'X')) {
                base = 16;
                if (index + 2 >= end) {
                    throw new NumberFormatException("0x without digits");
                }
            }
        }
        if (base == 0) {
            base = (bytes[index] == '0') ? 8 : 10;
        }
        return base;
    }

    private static void checkRadix(int radix)
    {
        if (radix != 0 && (radix < Character.MIN_RADIX || radix > Character.MAX_RADIX)) {
            throw new NumberFormatException("radix out of range: " + radix);
        }
    }

    private static int decodeDigit(int value)
    {
        if (value >= '0' && value <= '9') {
            return value - '0';
        }
        if (value >= 'a' && value <= 'z') {
            return value - 'a' + 10;
        }
        if (value >= 'A' && value <= 'Z') {
            return value - 'A' + 10;
        }
        return -1;
    }
}
