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

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMatchLength
{
    @Test
    public void testConcatenationAndAlternation()
    {
        assertLength("abc", 3, 3);
        assertLength("a|é", 1, -1);
        assertLength("(?:é|💰){2}", 4, -1);
    }

    @Test
    public void testRepetition()
    {
        assertLength("a*", 0, -1);
        assertLength("a?", 0, -1);
        assertLength("a+", 1, -1);
        assertLength("a{3}", 3, 3);
        assertLength("a{2,3}", 2, -1);
        assertLength("(?:){2,3}", 0, 0);
    }

    @Test
    public void testEncodedByteWidths()
    {
        assertLength(".", 1, -1);
        assertLength(".", Regexp.LIKE_PERL | Regexp.LATIN1, 1, 1);
        assertLength("[a💰]", 1, -1);
        assertLength("[éê]", 2, 2);
        assertLength("(?i:K)", 1, -1);
    }

    @Test
    public void testImpossibleLanguage()
    {
        Regexp noMatch = Regexp.noMatch(Regexp.LIKE_PERL);
        assertThat(MatchLength.analyze(noMatch)).isEqualTo(new MatchLength.Analysis(Integer.MAX_VALUE, -1));

        Regexp literal = Regexp.literal(Regexp.LIKE_PERL, 'a');
        Regexp alternate = Regexp.alternate(Regexp.LIKE_PERL, List.of(noMatch, literal));
        assertThat(MatchLength.analyze(alternate)).isEqualTo(new MatchLength.Analysis(1, 1));

        Regexp optionalNoMatch = Regexp.star(Regexp.LIKE_PERL, noMatch);
        assertThat(MatchLength.analyze(optionalNoMatch)).isEqualTo(new MatchLength.Analysis(0, 0));
    }

    @Test
    public void testSaturatedMinimumLength()
    {
        int parseFlags = Regexp.LIKE_PERL;
        Regexp literal = Regexp.literal(parseFlags, 0x1F4B0);
        Regexp saturated = Regexp.repeat(parseFlags, literal, Integer.MAX_VALUE, Integer.MAX_VALUE);

        assertThat(MatchLength.analyze(saturated)).isEqualTo(new MatchLength.Analysis(Integer.MAX_VALUE, -1));
        assertThat(MatchLength.analyze(Regexp.concat(parseFlags, List.of(saturated, literal))))
                .isEqualTo(new MatchLength.Analysis(Integer.MAX_VALUE, -1));
        assertThat(MatchLength.analyze(Regexp.alternate(parseFlags, List.of(saturated, literal))))
                .isEqualTo(new MatchLength.Analysis(4, 4));
        assertThat(MatchLength.analyze(Regexp.quest(parseFlags, saturated)))
                .isEqualTo(new MatchLength.Analysis(0, 0));
        assertThat(MatchLength.analyze(Regexp.repeat(parseFlags, saturated, 2, 2)))
                .isEqualTo(new MatchLength.Analysis(Integer.MAX_VALUE, -1));
    }

    private static void assertLength(String pattern, int minimum, int fixed)
    {
        assertLength(pattern, Regexp.LIKE_PERL, minimum, fixed);
    }

    private static void assertLength(String pattern, int parseFlags, int minimum, int fixed)
    {
        Regexp regexp = RegexpParser.parse(utf8Slice(pattern), parseFlags).regexp();
        assertThat(MatchLength.analyze(regexp)).isEqualTo(new MatchLength.Analysis(minimum, fixed));
    }
}
