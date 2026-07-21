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

import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/required_prefix_test.cc.
public class TestUpstreamPrefixAccel
{
    @Test
    public void testFoldCasePrefixAccelCandidateScan()
    {
        // Pattern with case-insensitive prefix
        String pattern = "(?i)hello\\d+";
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), Regexp.LIKE_PERL);

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).isNotNull();
        assertThat(prog.canPrefixAccel()).isTrue();
        assertThat(prog.usesFoldCasePrefixCandidateScan()).isTrue();

        // Test various case combinations
        String[] texts = {
                "hello123",      // lowercase
                "HELLO456",      // uppercase
                "HeLLo789",      // mixed case
                "xHELLO111",     // with prefix
                "xxxhello222",   // with longer prefix
        };
        int[] expectedPositions = {0, 0, 0, 1, 3};

        for (int i = 0; i < texts.length; i++) {
            byte[] data = texts[i].getBytes(StandardCharsets.UTF_8);
            int result = prog.prefixAccel(data, 0, data.length);
            assertThat(result)
                    .as("prefixAccel for '%s'", texts[i])
                    .isEqualTo(expectedPositions[i]);
        }

        // Test that non-matching text returns -1
        byte[] noMatch = "world".getBytes(StandardCharsets.UTF_8);
        assertThat(prog.prefixAccel(noMatch, 0, noMatch.length)).isEqualTo(-1);

        // Test with text shorter than prefix
        byte[] tooShort = "hel".getBytes(StandardCharsets.UTF_8);
        assertThat(prog.prefixAccel(tooShort, 0, tooShort.length)).isEqualTo(-1);
    }

    @Test
    public void testFoldCasePrefixAccelCandidateScanMatchesReference()
    {
        Random random = new Random(1);
        for (String prefix : List.of("ab", "aaaa", "abababa", "Sherlock Holmes", "ABCDEFGHIJKLMNO")) {
            Prog program = compile("x");
            program.configurePrefixAccel(Slices.utf8Slice(prefix), true);
            assertThat(program.canPrefixAccel()).as(prefix).isTrue();
            int acceleratedPrefixLength = Math.min(prefix.length(), 9);

            for (int iteration = 0; iteration < 200; iteration++) {
                byte[] data = new byte[6 + random.nextInt(507)];
                random.nextBytes(data);
                if (data.length >= prefix.length() && random.nextBoolean()) {
                    int insertionPosition = random.nextInt(data.length - prefix.length() + 1);
                    for (int index = 0; index < prefix.length(); index++) {
                        char value = prefix.charAt(index);
                        if (random.nextBoolean() && value >= 'a' && value <= 'z') {
                            value -= 'a' - 'A';
                        }
                        else if (random.nextBoolean() && value >= 'A' && value <= 'Z') {
                            value += 'a' - 'A';
                        }
                        data[insertionPosition + index] = (byte) value;
                    }
                }

                int offset = random.nextInt(4);
                int length = data.length - offset - random.nextInt(4);
                assertThat(program.prefixAccel(data, offset, length))
                        .isEqualTo(referenceFoldCaseSearch(data, offset, length, prefix, acceleratedPrefixLength));
            }
        }
    }

    private static Prog compile(String pattern)
    {
        ParseResult parsed = RegexpParser.parse(Slices.utf8Slice(pattern), Regexp.LIKE_PERL);
        return Compiler.compile(parsed.regexp(), false, 0);
    }

    private static int referenceFoldCaseSearch(byte[] data, int offset, int length, String prefix, int prefixLength)
    {
        int end = offset + length - prefixLength;
        for (int position = offset; position <= end; position++) {
            boolean matches = true;
            for (int index = 0; index < prefixLength; index++) {
                int actual = data[position + index] & 0xFF;
                int expected = prefix.charAt(index);
                if (actual >= 'A' && actual <= 'Z') {
                    actual += 'a' - 'A';
                }
                if (expected >= 'A' && expected <= 'Z') {
                    expected += 'a' - 'A';
                }
                if (actual != expected) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                return position;
            }
        }
        return -1;
    }

    @Test
    public void testPrefixAccelSimpleTests()
    {
        // From upstream re2/testing/required_prefix_test.cc PrefixAccel.SimpleTests.
        List<String> patterns = List.of(
                "aababc\\d+",
                "(?i)AABABC\\d+");

        for (String pattern : patterns) {
            ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), Regexp.LIKE_PERL);

            Prog prog = Compiler.compile(parsed.regexp(), false, 0);
            assertThat(prog).as("compile: %s", pattern).isNotNull();
            assertThat(prog.canPrefixAccel()).as("canPrefixAccel: %s", pattern).isTrue();

            for (int j = 0; j < 100; j++) {
                StringBuilder text = new StringBuilder();
                text.append("a".repeat(j));

                byte[] data = text.toString().getBytes(StandardCharsets.UTF_8);
                assertThat(prog.prefixAccel(data, 0, data.length))
                        .as("prefix accel (no prefix): %s (j=%s)", pattern, j)
                        .isEqualTo(-1);

                text.append("aababc");
                for (int k = 0; k < 100; k++) {
                    text.append("a".repeat(k));
                    data = text.toString().getBytes(StandardCharsets.UTF_8);
                    assertThat(prog.prefixAccel(data, 0, data.length))
                            .as("prefix accel: %s (j=%s k=%s)", pattern, j, k)
                            .isEqualTo(j);
                }
            }
        }
    }
}
