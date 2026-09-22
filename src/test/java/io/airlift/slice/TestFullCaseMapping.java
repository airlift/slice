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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static io.airlift.slice.SliceUtf8.toLowerCase;
import static io.airlift.slice.SliceUtf8.toLowerCaseFull;
import static io.airlift.slice.SliceUtf8.toUpperCase;
import static io.airlift.slice.SliceUtf8.toUpperCaseFull;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Character.MAX_CODE_POINT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ROOT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFullCaseMapping
{
    @Test
    public void testEveryCodePointAgainstRunningJava()
    {
        for (int codePoint = 0; codePoint <= MAX_CODE_POINT; codePoint++) {
            if (codePoint >= 0xD800 && codePoint <= 0xDFFF) {
                continue;
            }
            String input = new String(Character.toChars(codePoint));
            String expectedUpper = input.toUpperCase(ROOT);
            String expectedLower = input.toLowerCase(ROOT);
            assertThat(toUpperCaseFull(utf8Slice(input)).toStringUtf8())
                    .as("uppercase U+%04X", codePoint)
                    .isEqualTo(expectedUpper);
            assertThat(toLowerCaseFull(utf8Slice(input)).toStringUtf8())
                    .as("lowercase U+%04X", codePoint)
                    .isEqualTo(expectedLower);
        }
    }

    @Test
    public void testExpansionsAndGrowth()
    {
        assertUpper("straße", "STRASSE");
        assertUpper("ﬃ", "FFI");
        assertUpper("ΐ", "Ι\u0308\u0301");
        assertLower("İ", "i\u0307");
        assertUpper("ΐﬃßöxyz".repeat(1000), "Ι\u0308\u0301FFISSÖXYZ".repeat(1000));
        assertLower("İAΣ!".repeat(1000), "i\u0307aς!".repeat(1000));
        assertUpper("ıiIİ", "IIIİ");
        assertLower("ıiIİ", "ıiii\u0307");

        // Existing simple functions retain their behavior.
        assertThat(toUpperCase(utf8Slice("ß")).toStringUtf8()).isEqualTo("ß");
        assertThat(toLowerCase(utf8Slice("İAΣ")).toStringUtf8()).isEqualTo("iaσ");
    }

    @Test
    public void testFinalSigma()
    {
        assertLower("Σ", "σ");
        assertLower("ΣA", "σa");
        assertLower("ΟΣ", "ος");
        assertLower("ΟΣΑ", "οσα");
        assertLower("AΣ", "aς");
        assertLower("aΣ", "aς");
        assertLower("AΣA", "aσa");
        assertLower("A1Σ", "a1σ");
        assertLower("AΣ1A", "aς1a");
        assertLower("A'Σ", "a'ς");
        assertLower("AΣ'A", "aσ'a");
        assertLower("AΣ'", "aς'");
        assertLower("AΣ:A", "aσ:a");
        assertLower("AΣ-A", "aς-a");
        assertLower("ΣΣΣ", "σσς");
        assertLower("A\u0301Σ", "a\u0301ς");
        assertLower("AΣ\u0301A", "aσ\u0301a");
        assertLower("AΣ\u200D", "aς\u200D");
        assertLower("AΣ\u200DA", "aσ\u200Da");

        // U+0345 is both Cased and Case_Ignorable, so it must be ignored.
        assertLower("\u0345Σ", "\u0345σ");
        assertLower("A\u0345Σ", "a\u0345ς");
        assertLower("AΣ\u0345", "aς\u0345");
        assertLower("AΣ\u0345B", "aσ\u0345b");
        assertLower("ⅠΣ", "ⅰς");
        assertLower("ⒶΣ", "ⓐς");
        assertLower("𐐀Σ", "𐐨ς");
        assertLower("AΣ𐐀", "aσ𐐨");
        assertLower("İΣ", "i\u0307ς");
    }

    @Test
    public void testAscii()
    {
        Random random = new Random(831);
        for (int length : new int[] {0, 1, 7, 8, 15, 16, 127, 1024}) {
            byte[] input = new byte[length];
            for (int i = 0; i < length; i++) {
                input[i] = (byte) random.nextInt(128);
            }
            String text = new String(input, UTF_8);
            assertUpper(text, text.toUpperCase(ROOT));
            assertLower(text, text.toLowerCase(ROOT));
        }
    }

    @Test
    public void testAsciiWordBoundaries()
    {
        for (int value = 0; value < 128; value++) {
            for (int position = 0; position < 8; position++) {
                String text = "@AZ[az{".substring(0, position) + (char) value + "@AZ[az{".substring(position);
                assertUpper(text.repeat(3), text.repeat(3).toUpperCase(ROOT));
                assertLower(text.repeat(3), text.repeat(3).toLowerCase(ROOT));
                assertUpper("é" + text.repeat(3), ("é" + text.repeat(3)).toUpperCase(ROOT));
                assertLower("É" + text.repeat(3), ("É" + text.repeat(3)).toLowerCase(ROOT));
            }
        }
        for (int length = 0; length <= 24; length++) {
            String ignored = ".:'".repeat(length);
            assertLower("a" + ignored + "Σ", "a" + ignored + "ς");
            assertLower("A" + ignored + "Σ", "a" + ignored + "ς");
            assertLower("a1" + ignored + "Σ", "a1" + ignored + "σ");
            assertLower("éA" + ignored + "Σ", "éa" + ignored + "ς");
            assertLower("é1" + ignored + "Σ", "é1" + ignored + "σ");
        }
        for (int position = 0; position < 8; position++) {
            String text = "A".repeat(position) + "é" + "z".repeat(16);
            assertUpper(text, text.toUpperCase(ROOT));
            assertLower(text, text.toLowerCase(ROOT));
        }
        assertUpper("ß" + "a".repeat(32), "SS" + "A".repeat(32));
        assertUpper("ΐ" + "a".repeat(32), "Ι\u0308\u0301" + "A".repeat(32));
        assertLower("İ" + "A".repeat(32), "i\u0307" + "a".repeat(32));
    }

    @Test
    public void testFinalSigmaAcrossLongRuns()
    {
        String ignored = "\u0301\u0345\u200D".repeat(1000);
        assertLower("a".repeat(1000) + ignored + "Σ", "a".repeat(1000) + ignored + "ς");
        assertLower("A".repeat(1000) + ignored + "Σ", "a".repeat(1000) + ignored + "ς");
        assertLower("a1" + ignored + "Σ", "a1" + ignored + "σ");
        assertLower("A1" + ignored + "Σ", "a1" + ignored + "σ");
        assertLower("AΣ" + ignored, "aς" + ignored);
        assertLower("AΣ" + ignored + "B", "aσ" + ignored + "b");
        assertLower("AΣ" + ignored + "1B", "aς" + ignored + "1b");
        assertLower(("Σ" + ignored).repeat(100), ("σ" + ignored).repeat(99) + "ς" + ignored);
    }

    @Test
    public void testCaseIgnorablePunctuation()
    {
        // The punctuation portion of Unicode's Case_Ignorable property.
        for (int codePoint : new int[] {0x0027, 0x002E, 0x003A, 0x00B7, 0x0387, 0x055F, 0x05F4, 0x2018, 0x2019, 0x2024, 0x2027, 0xFE13, 0xFE52, 0xFE55, 0xFF07, 0xFF0E, 0xFF1A}) {
            String punctuation = Character.toString(codePoint);
            assertLower(punctuation + "Σ", punctuation + "σ");
            assertLower("a" + punctuation + "Σ", "a" + punctuation + "ς");
            assertLower("A" + punctuation + "Σ", "a" + punctuation + "ς");
            assertLower("AΣ" + punctuation, "aς" + punctuation);
            assertLower("AΣ" + punctuation + "A", "aσ" + punctuation + "a");
        }
    }

    @Test
    public void testTruncatedUtf8Ranges()
    {
        // The final byte of the combining mark is outside the requested range.
        byte[] trailing = "AΣ\u0301A".getBytes(UTF_8);
        byte[] expectedTrailing = "aς\u0301".getBytes(UTF_8);
        assertThat(toLowerCaseFull(trailing, 0, 4)).isEqualTo(wrappedBuffer(expectedTrailing, 0, 4));
        assertThat(toLowerCaseFull(wrappedBuffer(trailing, 0, 4))).isEqualTo(wrappedBuffer(expectedTrailing, 0, 4));

        // A continuation byte at the start of the range breaks preceding context.
        byte[] leading = "A\u0301Σ".getBytes(UTF_8);
        byte[] expectedLeading = "A\u0301σ".getBytes(UTF_8);
        assertThat(toLowerCaseFull(leading, 2, 3)).isEqualTo(wrappedBuffer(expectedLeading, 2, 3));
        assertThat(toLowerCaseFull(wrappedBuffer(leading, 2, 3))).isEqualTo(wrappedBuffer(expectedLeading, 2, 3));
    }

    @Test
    public void testUnchangedRangesShareStorage()
    {
        for (String text : List.of("", "123", "HELLO", "Ö", "\u0301", "😀")) {
            Slice input = utf8Slice("xx" + text + "xx").slice(2, utf8Slice(text).length());
            Slice result = toUpperCaseFull(input);
            assertThat(result).isEqualTo(input);
            assertThat(result.byteArray()).isSameAs(input.byteArray());
            assertThat(result.byteArrayOffset()).isEqualTo(input.byteArrayOffset());
        }
        for (String text : List.of("", "123", "hello", "ö", "\u0301", "😀")) {
            Slice input = utf8Slice("xx" + text + "xx").slice(2, utf8Slice(text).length());
            Slice result = toLowerCaseFull(input);
            assertThat(result).isEqualTo(input);
            assertThat(result.byteArray()).isSameAs(input.byteArray());
            assertThat(result.byteArrayOffset()).isEqualTo(input.byteArrayOffset());
        }
    }

    @Test
    public void testRangeValidation()
    {
        byte[] bytes = {1, 2, 3};
        for (int[] range : new int[][] {{-1, 0}, {0, -1}, {2, 2}, {4, 0}, {Integer.MAX_VALUE, 1}}) {
            assertThatThrownBy(() -> toUpperCaseFull(bytes, range[0], range[1])).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> toLowerCaseFull(bytes, range[0], range[1])).isInstanceOf(IndexOutOfBoundsException.class);
        }
        assertThat(toUpperCaseFull(bytes, 3, 0).length()).isZero();
        assertThat(toLowerCaseFull(bytes, 3, 0).length()).isZero();
    }

    private static void assertUpper(String input, String expected)
    {
        assertThat(toUpperCaseFull(utf8Slice(input)).toStringUtf8()).isEqualTo(expected);
        byte[] range = ("a" + input + "z").getBytes(UTF_8);
        byte[] original = range.clone();
        assertThat(toUpperCaseFull(range, 1, range.length - 2).toStringUtf8()).isEqualTo(expected);
        assertThat(toUpperCaseFull(wrappedBuffer(range, 1, range.length - 2)).toStringUtf8()).isEqualTo(expected);
        assertThat(range).isEqualTo(original);
    }

    private static void assertLower(String input, String expected)
    {
        assertThat(toLowerCaseFull(utf8Slice(input)).toStringUtf8()).isEqualTo(expected);
        // Outside cased characters must not affect sigma at either range boundary.
        byte[] range = ("A" + input + "Z").getBytes(UTF_8);
        byte[] original = range.clone();
        assertThat(toLowerCaseFull(range, 1, range.length - 2).toStringUtf8()).isEqualTo(expected);
        assertThat(toLowerCaseFull(wrappedBuffer(range, 1, range.length - 2)).toStringUtf8()).isEqualTo(expected);
        assertThat(range).isEqualTo(original);
    }
}
