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

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDfaCountMatches
{
    @Test
    public void testCountsSuccessiveMatchesInOneSearchSession()
    {
        Prog program = compile("[a-z]+");

        assertThat(Dfa.countMatches(program, utf8Slice("one 22 three 444 five"), Prog.MatchKind.FIRST_MATCH))
                .isEqualTo(3);
    }

    @Test
    public void testPreservesContextAtSuccessiveBoundaries()
    {
        Prog program = compile("\\b[a-z]+\\b");

        assertThat(Dfa.countMatches(program, utf8Slice("one_two three four5 five"), Prog.MatchKind.FIRST_MATCH))
                .isEqualTo(2);
    }

    @Test
    public void testHonorsMatchKind()
    {
        Prog program = compile("a|aa");

        assertThat(Dfa.countMatches(program, utf8Slice("aa"), Prog.MatchKind.FIRST_MATCH))
                .isEqualTo(2);
        assertThat(Dfa.countMatches(program, utf8Slice("aa"), Prog.MatchKind.LONGEST_MATCH))
                .isEqualTo(1);
    }

    @Test
    public void testNonzeroSliceOffset()
    {
        Prog program = compile("[a-z]+");
        byte[] bytes = utf8Slice("--one 22 three--").getBytes();

        assertThat(Dfa.countMatches(program, Slices.wrappedBuffer(bytes, 2, bytes.length - 4), Prog.MatchKind.FIRST_MATCH))
                .isEqualTo(2);
    }

    @Test
    public void testRejectsEmptyMatches()
    {
        Prog program = compile("x*");

        assertThat(Dfa.countMatches(program, utf8Slice("abc"), Prog.MatchKind.FIRST_MATCH))
                .isEqualTo(Dfa.COUNT_UNSUPPORTED);
    }

    private static Prog compile(String pattern)
    {
        ParseResult parsed = RegexpParser.parse(utf8Slice(pattern), Regexp.LIKE_PERL);
        return Compiler.compile(parsed.regexp(), false, 0);
    }
}
