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

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBoundedCharacterClassCounter
{
    @Test
    public void testCountsGreedyBoundedRuns()
    {
        assertCount("[a-z]{8,13}", "abcdefgh", 1);
        assertCount("[a-z]{8,13}", "abcdefghijklm", 1);
        assertCount("[a-z]{8,13}", "abcdefghijklmnopqrst", 1);
        assertCount("[a-z]{8,13}", "abcdefghijklmnopqrstu", 2);
        assertCount("[a-z]{8,13}", "abcdefghijklmnopqrstuvwxyz", 2);
        assertCount("(\\p{L}{8,13})", "абвгдежз 12 абвгдежзийклм", 2);

        Re2 re2 = Re2.compile(utf8Slice("[a-z]{2,5}"));
        BoundedCharacterClassCounter counter = re2.createBoundedCharacterClassCounter();
        for (int runLength = 0; runLength <= 30; runLength++) {
            Slice input = utf8Slice("a".repeat(runLength));
            assertThat(counter.count(input))
                    .as("run length %s", runLength)
                    .isEqualTo(countWithGeneralMatcher(re2, input));
        }
    }

    @Test
    public void testMatchesGeneralMatcher()
    {
        for (String pattern : List.of("[a-z]{2,4}", "[0-9]{1,3}", "\\p{L}{2,5}", "[^x]{3,7}")) {
            Re2 re2 = Re2.compile(utf8Slice(pattern));
            BoundedCharacterClassCounter matcher = re2.createBoundedCharacterClassCounter();
            assertThat(matcher).as(pattern).isNotNull();
            for (String input : List.of("", "a", "abc12defgh", "абв гдеёж 123", "xxxxxxxx")) {
                assertThat(matcher.count(utf8Slice(input)))
                        .as("pattern %s input %s", pattern, input)
                        .isEqualTo(countWithGeneralMatcher(re2, utf8Slice(input)));
            }
        }
    }

    @Test
    public void testMalformedUtf8MatchesGeneralMatcher()
    {
        Slice malformed = Slices.wrappedBuffer(new byte[] {(byte) 0xFF});
        for (String pattern : List.of("\\p{L}{1,3}", "[^x]{1,3}")) {
            Re2 re2 = Re2.compile(utf8Slice(pattern));
            BoundedCharacterClassCounter counter = re2.createBoundedCharacterClassCounter();
            assertThat(counter.count(malformed))
                    .as(pattern)
                    .isEqualTo(countWithGeneralMatcher(re2, malformed));
        }

        Re2 replacementCharacter = Re2.compile(utf8Slice("[^x]{1,3}"));
        BoundedCharacterClassCounter counter = replacementCharacter.createBoundedCharacterClassCounter();
        Slice validReplacementCharacter = utf8Slice("�");
        assertThat(counter.count(validReplacementCharacter))
                .isEqualTo(countWithGeneralMatcher(replacementCharacter, validReplacementCharacter));
    }

    @Test
    public void testLatin1MatchesGeneralMatcher()
    {
        Re2 re2 = Re2.compile(utf8Slice("[^x]{1,3}"), Re2.Options.latin1());
        BoundedCharacterClassCounter counter = re2.createBoundedCharacterClassCounter();
        Slice input = Slices.wrappedBuffer(new byte[] {'a', (byte) 0xFF, 'x', (byte) 0x80});

        assertThat(counter.count(input)).isEqualTo(countWithGeneralMatcher(re2, input));
    }

    @Test
    public void testRejectsUnsupportedShapes()
    {
        for (String pattern : List.of("[a-z]*", "[a-z]{2,}", "[a-z]{2,4}?", "^[a-z]{2,4}", "(?:ab){2,4}")) {
            assertThat(Re2.compile(utf8Slice(pattern)).createBoundedCharacterClassCounter())
                    .as(pattern)
                    .isNull();
        }
    }

    private static void assertCount(String pattern, String input, long expected)
    {
        BoundedCharacterClassCounter matcher = Re2.compile(utf8Slice(pattern)).createBoundedCharacterClassCounter();
        assertThat(matcher).isNotNull();
        assertThat(matcher.count(utf8Slice(input))).isEqualTo(expected);
    }

    private static long countWithGeneralMatcher(Re2 re2, Slice input)
    {
        Re2Matcher matcher = re2.groupZeroMatcher(input, null);
        long count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }
}
