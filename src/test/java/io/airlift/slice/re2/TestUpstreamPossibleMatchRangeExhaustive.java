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

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/possible_match_test.cc.
public class TestUpstreamPossibleMatchRangeExhaustive
{
    private static final int EXPECTED_REGEXP_CALLBACKS = 27_012;
    private static final int EXPECTED_STRINGS_PER_REGEXP = 364;

    @Test
    public void testPossibleMatchRangeExhaustive()
    {
        // From upstream re2/testing/possible_match_test.cc PossibleMatchRange.Exhaustive.
        int maxAtoms = 3;
        int maxOps = 3;
        int maxStrLen = 5;

        List<Slice> atoms = List.of(
                Slices.wrappedBuffer("a".getBytes(StandardCharsets.UTF_8)),
                Slices.wrappedBuffer("b".getBytes(StandardCharsets.UTF_8)),
                Slices.wrappedBuffer("[0-9]".getBytes(StandardCharsets.UTF_8)));

        List<Slice> ops = RegexpGenerator.egrepOps();
        List<Slice> strAlphabet = List.of(
                Slices.wrappedBuffer("a".getBytes(StandardCharsets.UTF_8)),
                Slices.wrappedBuffer("b".getBytes(StandardCharsets.UTF_8)),
                Slices.wrappedBuffer("4".getBytes(StandardCharsets.UTF_8)));

        class Tester
                extends RegexpGenerator
        {
            private final StringGenerator strings = new StringGenerator(maxStrLen, strAlphabet);
            private int regexps;
            private int tests;

            Tester()
            {
                super(atoms, ops, maxAtoms, maxOps);
            }

            @Override
            protected void handleRegexp(Slice regexp)
            {
                regexps++;

                ParseResult parsed = RegexpParser.parse(regexp, Regexp.LIKE_PERL | Regexp.LATIN1);

                Prog prog = Compiler.compile(parsed.regexp(), false, 0);
                assertThat(prog).as("compile: %s", latin1(regexp)).isNotNull();

                Prog.PossibleMatchRangeResult range = prog.possibleMatchRange(10);
                if (range == null) {
                    // There is no good max for \\C* (and regexps containing it).
                    if (containsLiteral(regexp, "\\C*")) {
                        return;
                    }
                    throw new AssertionError("possibleMatchRange failed on: " + latin1(regexp));
                }

                Slice min = range.min();
                Slice max = range.max();

                strings.reset();
                int generatedStrings = 0;
                while (strings.hasNext()) {
                    Slice s = strings.next();
                    generatedStrings++;
                    tests++;

                    if (!Nfa.fullMatch(prog, s)) {
                        continue;
                    }

                    assertThat(compareUnsigned(s, min) >= 0)
                            .as("s >= min (re=%s s=%s min=%s max=%s)",
                                    latin1(regexp), latin1(s), latin1(min), latin1(max))
                            .isTrue();

                    assertThat(compareUnsigned(s, max) <= 0)
                            .as("s <= max (re=%s s=%s min=%s max=%s)",
                                    latin1(regexp), latin1(s), latin1(min), latin1(max))
                            .isTrue();
                }
                assertThat(generatedStrings).isEqualTo(EXPECTED_STRINGS_PER_REGEXP);
            }

            int regexps()
            {
                return regexps;
            }

            int tests()
            {
                return tests;
            }
        }

        Tester tester = new Tester();
        tester.generate();

        assertThat(tester.regexps()).isEqualTo(EXPECTED_REGEXP_CALLBACKS);
        assertThat(tester.tests()).isGreaterThan(0);
    }

    private static boolean containsLiteral(Slice haystack, String needleAscii)
    {
        byte[] n = needleAscii.getBytes(StandardCharsets.US_ASCII);
        byte[] h = haystack.byteArray();
        int ho = haystack.byteArrayOffset();
        int hl = haystack.length();

        outer:
        for (int i = 0; i + n.length <= hl; i++) {
            for (int j = 0; j < n.length; j++) {
                if (h[ho + i + j] != n[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    private static int compareUnsigned(Slice a, Slice b)
    {
        int n = Math.min(a.length(), b.length());
        for (int i = 0; i < n; i++) {
            int x = a.getUnsignedByte(i);
            int y = b.getUnsignedByte(i);
            if (x != y) {
                return Integer.compare(x, y);
            }
        }
        return Integer.compare(a.length(), b.length());
    }

    private static String latin1(Slice s)
    {
        return new String(s.byteArray(), s.byteArrayOffset(), s.length(), StandardCharsets.ISO_8859_1);
    }
}
