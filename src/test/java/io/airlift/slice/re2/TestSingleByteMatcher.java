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
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSingleByteMatcher
{
    @Test
    public void testSupportedMatchersAgreeWithRegularMatching()
    {
        List<PatternCase> patterns = List.of(
                new PatternCase("a", Re2.Options.defaults()),
                new PatternCase("[\\x00-\\x7f]", Re2.Options.defaults()),
                new PatternCase("[,;]", Re2.Options.defaults()),
                new PatternCase("a|b", Re2.Options.defaults()),
                new PatternCase("(?i)a", Re2.Options.defaults()),
                new PatternCase("(a)", Re2.Options.defaults()),
                new PatternCase("a{1}", Re2.Options.defaults()),
                new PatternCase("\\C", Re2.Options.defaults()),
                new PatternCase(".", Re2.Options.latin1()),
                new PatternCase("(?s:.)", Re2.Options.latin1()));

        byte[] bytes = new byte[260];
        bytes[0] = 'x';
        bytes[1] = 'x';
        for (int value = 0; value < 256; value++) {
            bytes[value + 2] = (byte) value;
        }
        bytes[258] = 'x';
        bytes[259] = 'x';
        Slice input = Slices.wrappedBuffer(bytes, 2, 256);

        for (PatternCase patternCase : patterns) {
            Re2 re2 = Re2.compile(Slices.utf8Slice(patternCase.pattern()), patternCase.options());
            assertThat(re2.supportsSingleByteMatcher()).as(patternCase.pattern()).isTrue();
            SingleByteMatcher singleByteMatcher = re2.createSingleByteMatcher();
            assertThat(singleByteMatcher).as(patternCase.pattern()).isNotNull();

            Re2Matcher regularMatcher = re2.matcher(input);
            int nextStart = 0;
            long count = 0;
            while (regularMatcher.find()) {
                assertThat(regularMatcher.end() - regularMatcher.start()).as(patternCase.pattern()).isEqualTo(1);
                assertThat(singleByteMatcher.find(input, nextStart)).as(patternCase.pattern()).isEqualTo(regularMatcher.start());
                nextStart = regularMatcher.end();
                count++;
            }
            assertThat(singleByteMatcher.find(input, nextStart)).as(patternCase.pattern()).isEqualTo(-1);
            assertThat(singleByteMatcher.count(input)).as(patternCase.pattern()).isEqualTo(count);
        }
    }

    @Test
    public void testUnsupportedMatchersAreRejected()
    {
        List<String> patterns = List.of(
                "",
                "a*",
                "a?",
                "a+",
                "ab",
                "a|",
                "^a",
                "a$",
                "\\ba\\b",
                ".",
                "é",
                "[\\x{80}-\\x{ff}]",
                "^ab[,;]");

        for (String pattern : patterns) {
            Re2 re2 = Re2.compile(Slices.utf8Slice(pattern));
            assertThat(re2.supportsSingleByteMatcher()).as(pattern).isFalse();
            assertThat(re2.createSingleByteMatcher()).as(pattern).isNull();
        }
    }

    @Test
    public void testSupportedRepeatedMatchersAgreeWithRegularMatching()
    {
        List<PatternCase> patterns = List.of(
                new PatternCase("a*", Re2.Options.defaults()),
                new PatternCase("a{0,}", Re2.Options.defaults()),
                new PatternCase("[ab]*", Re2.Options.defaults()),
                new PatternCase("(?i:a)*", Re2.Options.defaults()),
                new PatternCase("(a*)", Re2.Options.defaults()),
                new PatternCase("\\C*", Re2.Options.defaults()),
                new PatternCase(".*", Re2.Options.latin1()));
        List<Slice> inputs = List.of(
                Slices.EMPTY_SLICE,
                Slices.utf8Slice("a"),
                Slices.utf8Slice("aaabbaa"),
                Slices.utf8Slice("z世界aaz"),
                Slices.wrappedBuffer(new byte[] {'z', (byte) 0xFF, 'a', 'a', 'z'}),
                Slices.wrappedBuffer(Slices.utf8Slice("xxzaaazyy").getBytes(), 2, 5));

        for (PatternCase patternCase : patterns) {
            Re2 re2 = Re2.compile(Slices.utf8Slice(patternCase.pattern()), patternCase.options());
            SingleByteRepeatMatcher repeatMatcher = re2.createSingleByteRepeatMatcher();
            assertThat(repeatMatcher).as(patternCase.pattern()).isNotNull();

            for (Slice input : inputs) {
                assertThat(repeatMatcher.count(input))
                        .as("pattern %s, input %s", patternCase.pattern(), input)
                        .isEqualTo(countRegularMatches(re2, input));
            }
        }
    }

    @Test
    public void testSupportedRepeatedMatchersFindTheSameBoundaries()
    {
        List<PatternCase> patterns = List.of(
                new PatternCase("a*", Re2.Options.defaults()),
                new PatternCase("a{0,}", Re2.Options.defaults()),
                new PatternCase("[ab]*", Re2.Options.defaults()),
                new PatternCase("(?i:a)*", Re2.Options.defaults()),
                new PatternCase("\\C*", Re2.Options.defaults()),
                new PatternCase(".*", Re2.Options.latin1()));
        List<Slice> inputs = List.of(
                Slices.EMPTY_SLICE,
                Slices.utf8Slice("a"),
                Slices.utf8Slice("aaabbaa"),
                Slices.utf8Slice("z世界aaz"),
                Slices.wrappedBuffer(new byte[] {'z', (byte) 0xFF, 'a', 'a', 'z'}),
                Slices.wrappedBuffer(Slices.utf8Slice("xxzaaazyy").getBytes(), 2, 5));

        for (PatternCase patternCase : patterns) {
            Re2 re2 = Re2.compile(Slices.utf8Slice(patternCase.pattern()), patternCase.options());
            SingleByteRepeatMatcher repeatMatcher = re2.createSingleByteRepeatMatcher();
            assertThat(repeatMatcher).as(patternCase.pattern()).isNotNull();

            for (Slice input : inputs) {
                for (int start = 0; start <= input.length(); start++) {
                    assertSameBoundaries(patternCase.pattern(), re2, repeatMatcher, input, start);
                }
            }
        }
    }

    @Test
    public void testRepeatedMatcherExhaustiveTransitions()
    {
        List<PatternCase> patterns = List.of(
                new PatternCase("a*", Re2.Options.defaults()),
                new PatternCase("[ab]*", Re2.Options.defaults()),
                new PatternCase("(?i:a)*", Re2.Options.defaults()),
                new PatternCase("(a*)", Re2.Options.defaults()),
                new PatternCase("\\C*", Re2.Options.defaults()),
                new PatternCase("a*", Re2.Options.latin1()),
                new PatternCase(".*", Re2.Options.latin1()));
        List<Slice> inputs = new ArrayList<>();
        addGeneratedInputs(
                inputs,
                new byte[0],
                new byte[][] {{'a'}, {'z'}, Slices.utf8Slice("\u4e16").getBytes(), {(byte) 0xFF}},
                5);

        for (PatternCase patternCase : patterns) {
            Re2 re2 = Re2.compile(Slices.utf8Slice(patternCase.pattern()), patternCase.options());
            SingleByteRepeatMatcher repeatMatcher = re2.createSingleByteRepeatMatcher();
            assertThat(repeatMatcher).as(patternCase.pattern()).isNotNull();

            for (Slice input : inputs) {
                assertThat(repeatMatcher.count(input))
                        .as("pattern %s, input %s", patternCase.pattern(), Arrays.toString(input.getBytes()))
                        .isEqualTo(countRegularMatches(re2, input));
            }
        }
    }

    @Test
    public void testUnsupportedRepeatedMatchersAreRejected()
    {
        List<String> patterns = List.of(
                "a*?",
                "a+",
                "a?",
                "a{0,3}",
                "ab*",
                "(?:a|aa)*",
                "^a*",
                "a*$",
                ".*");

        for (String pattern : patterns) {
            assertThat(Re2.compile(Slices.utf8Slice(pattern)).createSingleByteRepeatMatcher())
                    .as(pattern)
                    .isNull();
        }
    }

    private static long countRegularMatches(Re2 re2, Slice input)
    {
        Re2Matcher matcher = re2.matcher(input);
        long count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    private static void assertSameBoundaries(String pattern, Re2 re2, SingleByteRepeatMatcher repeatMatcher, Slice input, int start)
    {
        Re2Matcher regularMatcher = re2.matcher(input);
        SingleByteRepeatMatcher.Cursor repeatCursor = repeatMatcher.matcher(input);

        boolean regularFound = regularMatcher.find(start);
        boolean repeatFound = repeatCursor.find(start);
        while (regularFound) {
            assertThat(repeatFound)
                    .as("pattern %s, input %s, start %s", pattern, input, start)
                    .isTrue();
            assertThat(repeatCursor.start()).isEqualTo(regularMatcher.start());
            assertThat(repeatCursor.end()).isEqualTo(regularMatcher.end());
            regularFound = regularMatcher.find();
            repeatFound = repeatCursor.find();
        }
        assertThat(repeatFound)
                .as("pattern %s, input %s, start %s", pattern, input, start)
                .isFalse();
    }

    private static void addGeneratedInputs(List<Slice> inputs, byte[] prefix, byte[][] tokens, int remainingTokens)
    {
        byte[] backing = new byte[prefix.length + 2];
        System.arraycopy(prefix, 0, backing, 1, prefix.length);
        inputs.add(Slices.wrappedBuffer(backing, 1, prefix.length));

        if (remainingTokens == 0) {
            return;
        }
        for (byte[] token : tokens) {
            byte[] value = Arrays.copyOf(prefix, prefix.length + token.length);
            System.arraycopy(token, 0, value, prefix.length, token.length);
            addGeneratedInputs(inputs, value, tokens, remainingTokens - 1);
        }
    }

    private record PatternCase(String pattern, Re2.Options options) {}
}
