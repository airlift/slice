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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/exhaustive3_test.cc.
public class TestExhaustiveCharClass
{
    private static final int RUNEMAX = 0x10FFFF;

    // Test simple character classes by themselves.
    @Test
    public void testCharacterClasses()
    {
        runExhaustiveTest(
                List.of("[a]", "[b]", "[ab]", "[^bc]", "[b-d]", "[^b-d]",
                        "[]a]", "[-a]", "[a-]", "[^-a]", "[a-b-c]", "a", "b", "."),
                RegexpGenerator.egrepOps(),
                2, 1, "ab", 5, null);
    }

    @Test
    public void testCharacterClassesInsideContext()
    {
        runExhaustiveTest(
                List.of("[a]", "[b]", "[ab]", "[^bc]", "[b-d]", "[^b-d]",
                        "[]a]", "[-a]", "[a-]", "[^-a]", "[a-b-c]", "a", "b", "."),
                RegexpGenerator.egrepOps(),
                2, 1, "ab", 5, "a%sb");
    }

    // Test interesting UTF-8 characters against character classes.
    @Test
    public void testInterestingUtf8SingleOps()
    {
        List<String> atoms = List.of(
                ".", "^", "$", "\\a", "\\f", "\\n", "\\r", "\\t", "\\v",
                "\\d", "\\D", "\\s", "\\S", "\\w", "\\W", "\\b", "\\B",
                "[[:alnum:]]", "[[:alpha:]]", "[[:blank:]]", "[[:cntrl:]]", "[[:digit:]]",
                "[[:graph:]]", "[[:lower:]]", "[[:print:]]", "[[:punct:]]", "[[:space:]]",
                "[[:upper:]]", "[[:xdigit:]]", "[\\s\\S]", "[\\d\\D]", "[^\\w\\W]", "[^\\d\\D]");

        List<Slice> noOps = List.of();
        List<Slice> interestingChars = generateInterestingUtf8();

        runExhaustiveTestWithAlphabet(
                atoms, noOps, 1, 0, interestingChars, 1, null);
    }

    // Test interesting UTF-8 characters against character classes,
    // but wrap everything inside AB.
    @Test
    public void testInterestingUtf8AB()
    {
        List<String> atoms = List.of(
                ".", "^", "$", "\\a", "\\f", "\\n", "\\r", "\\t", "\\v",
                "\\d", "\\D", "\\s", "\\S", "\\w", "\\W", "\\b", "\\B",
                "[[:alnum:]]", "[[:alpha:]]", "[[:blank:]]", "[[:cntrl:]]", "[[:digit:]]",
                "[[:graph:]]", "[[:lower:]]", "[[:print:]]", "[[:punct:]]", "[[:space:]]",
                "[[:upper:]]", "[[:xdigit:]]", "[\\s\\S]", "[\\d\\D]", "[^\\w\\W]", "[^\\d\\D]");

        List<Slice> noOps = List.of();
        List<Slice> interestingChars = generateInterestingUtf8();

        // Wrap each character as "a" + char + "b".
        Slice aPrefix = Slices.wrappedBuffer("a".getBytes(UTF_8));
        Slice bSuffix = Slices.wrappedBuffer("b".getBytes(UTF_8));
        List<Slice> wrappedAlpha = new ArrayList<>();
        for (Slice ch : interestingChars) {
            wrappedAlpha.add(Slices.wrappedBuffer(ByteArrays.concat(aPrefix, ch, bSuffix)));
        }

        runExhaustiveTestWithAlphabet(
                atoms, noOps, 1, 0, wrappedAlpha, 1, "a%sb");
    }

    // Returns a list of "interesting" UTF-8 characters, matching upstream InterestingUTF8().
    // Unicode is too big to just return all, so we return a set likely to be good test cases.
    private static List<Slice> generateInterestingUtf8()
    {
        List<Slice> result = new ArrayList<>();

        // All the Latin-1 equivalents are interesting (code points 1-255).
        for (int i = 1; i < 256; i++) {
            result.add(Slices.wrappedBuffer(encodeUtf8(i)));
        }

        // After that, the codes near bit boundaries are
        // interesting, because they span byte sequence lengths.
        for (int j = 0; j < 8; j++) {
            result.add(Slices.wrappedBuffer(encodeUtf8(256 + j)));
        }
        for (int i = 512; i < RUNEMAX; i <<= 1) {
            for (int j = -8; j < 8; j++) {
                int codePoint = i + j;
                if (codePoint > 0 && codePoint <= RUNEMAX) {
                    result.add(Slices.wrappedBuffer(encodeUtf8(codePoint)));
                }
            }
        }

        // The codes near Runemax, including Runemax itself, are interesting.
        for (int j = -8; j <= 0; j++) {
            int codePoint = RUNEMAX + j;
            if (codePoint > 0) {
                result.add(Slices.wrappedBuffer(encodeUtf8(codePoint)));
            }
        }

        return result;
    }

    private static byte[] encodeUtf8(int rune)
    {
        if (rune < 0x80) {
            return new byte[] {(byte) rune};
        }
        if (rune < 0x800) {
            return new byte[] {
                    (byte) (0xC0 | (rune >>> 6)),
                    (byte) (0x80 | (rune & 0x3F)),
            };
        }
        if (rune < 0x10000) {
            return new byte[] {
                    (byte) (0xE0 | (rune >>> 12)),
                    (byte) (0x80 | ((rune >>> 6) & 0x3F)),
                    (byte) (0x80 | (rune & 0x3F)),
            };
        }
        return new byte[] {
                (byte) (0xF0 | (rune >>> 18)),
                (byte) (0x80 | ((rune >>> 12) & 0x3F)),
                (byte) (0x80 | ((rune >>> 6) & 0x3F)),
                (byte) (0x80 | (rune & 0x3F)),
        };
    }

    private static void runExhaustiveTest(
            List<String> atomStrings,
            List<Slice> ops,
            int maxAtoms,
            int maxOps,
            String strAlphabet,
            int maxStrLen,
            String topWrapper)
    {
        List<Slice> alphabet = StringGenerator.explodeUtf8(Slices.wrappedBuffer(strAlphabet.getBytes(UTF_8)));
        runExhaustiveTestWithAlphabet(atomStrings, ops, maxAtoms, maxOps, alphabet, maxStrLen, topWrapper);
    }

    private static void runExhaustiveTestWithAlphabet(
            List<String> atomStrings,
            List<Slice> ops,
            int maxAtoms,
            int maxOps,
            List<Slice> alphabet,
            int maxStrLen,
            String topWrapper)
    {
        List<Slice> atoms = new ArrayList<>();
        for (String a : atomStrings) {
            atoms.add(Slices.wrappedBuffer(a.getBytes(UTF_8)));
        }

        CollectingGenerator gen = new CollectingGenerator(atoms, ops, maxAtoms, maxOps);
        gen.generate();

        StringGenerator strings = new StringGenerator(maxStrLen, alphabet);

        Slice wrapperPrefix = null;
        Slice wrapperSuffix = null;
        if (topWrapper != null) {
            int placeholderIndex = topWrapper.indexOf("%s");
            if (placeholderIndex >= 0) {
                wrapperPrefix = Slices.wrappedBuffer(topWrapper.substring(0, placeholderIndex).getBytes(UTF_8));
                wrapperSuffix = Slices.wrappedBuffer(topWrapper.substring(placeholderIndex + 2).getBytes(UTF_8));
            }
        }

        List<String> failures = new ArrayList<>();
        for (Slice regexp : gen.regexps()) {
            Slice testRegexp = regexp;
            if (wrapperPrefix != null) {
                testRegexp = Slices.wrappedBuffer(ByteArrays.concat(wrapperPrefix, regexp, wrapperSuffix));
            }

            Tester tester = new Tester(testRegexp, Tester.Config.fullMatrix());
            if (tester.error()) {
                failures.add("regexp=" + toUtf8(testRegexp) + " error: " + tester.failureMessage());
                continue;
            }

            strings.reset();
            while (strings.hasNext()) {
                Slice text = strings.next();
                if (!tester.testInput(text)) {
                    failures.add("regexp=" + toUtf8(testRegexp) + " " + tester.failureMessage());
                    break;
                }
            }
        }

        assertThat(failures).isEmpty();
    }

    private static String toUtf8(Slice bytes)
    {
        return new String(bytes.byteArray(), bytes.byteArrayOffset(), bytes.length(), UTF_8);
    }

    private static final class CollectingGenerator
            extends RegexpGenerator
    {
        private final List<Slice> regexps = new ArrayList<>();

        CollectingGenerator(List<Slice> atoms, List<Slice> ops, int maxAtoms, int maxOps)
        {
            super(atoms, ops, maxAtoms, maxOps);
        }

        @Override
        protected void handleRegexp(Slice regexp)
        {
            regexps.add(regexp);
        }

        public List<Slice> regexps()
        {
            return regexps;
        }
    }
}
