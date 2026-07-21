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

public class TestNfaFullMatchSmoke
{
    private record Case(String pattern, String text, int flags, boolean expected) {}

    @Test
    public void testFullMatchSmoke()
    {
        List<Case> cases = List.of(
                new Case("a", "a", Regexp.LIKE_PERL, true),
                new Case("a", "b", Regexp.LIKE_PERL, false),
                new Case("a*", "", Regexp.LIKE_PERL, true),
                new Case("a*", "aaaa", Regexp.LIKE_PERL, true),
                new Case("a*", "aaab", Regexp.LIKE_PERL, false),
                new Case("a*hello", "hello", Regexp.LIKE_PERL, true),
                new Case("a*hello", "aahello", Regexp.LIKE_PERL, true),
                new Case("a*hello", "ahell", Regexp.LIKE_PERL, false),
                new Case("(ab|x)?(c|z)?", "", Regexp.LIKE_PERL | Regexp.LATIN1, true),
                new Case("(ab|x)?(c|z)?", "abc", Regexp.LIKE_PERL | Regexp.LATIN1, true),
                new Case("(ab|x)?(c|z)?", "xz", Regexp.LIKE_PERL | Regexp.LATIN1, true),
                new Case("(ab|x)?(c|z)?", "abz", Regexp.LIKE_PERL | Regexp.LATIN1, true),
                new Case("(ab|x)?(c|z)?", "abcz", Regexp.LIKE_PERL | Regexp.LATIN1, false),
                new Case("\\Aa*hello", "hello", Regexp.LIKE_PERL, true),
                new Case("\\Aa*hello", "xhello", Regexp.LIKE_PERL, false));

        for (Case c : cases) {
            Prog prog = compile(c.pattern, c.flags);
            Slice input = Slices.wrappedBuffer(c.text.getBytes(StandardCharsets.ISO_8859_1));
            assertThat(Nfa.fullMatch(prog, input))
                    .as("pattern=%s text=%s", c.pattern, c.text)
                    .isEqualTo(c.expected);
        }
    }

    private static Prog compile(String pattern, int flags)
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.ISO_8859_1)), flags);
        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).as("compile: %s", pattern).isNotNull();
        return prog;
    }
}
