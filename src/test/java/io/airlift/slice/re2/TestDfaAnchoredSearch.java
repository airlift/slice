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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDfaAnchoredSearch
{
    @Test
    public void testAnchoredEndBoundaryAgreesWithNfaForSimpleCases()
    {
        assertAnchoredEndBoundary("a", "a");
        assertAnchoredEndBoundary("a*", "");
        assertAnchoredEndBoundary("a*", "aaa");
        assertAnchoredEndBoundary("a*?", "aaa");
        assertAnchoredEndBoundary("a+?", "aaa");
        assertAnchoredEndBoundary("a$", "a");
    }

    private static void assertAnchoredEndBoundary(String pattern, String text)
    {
        int flags = Regexp.LIKE_PERL | Regexp.LATIN1;
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), flags);

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).as("compile: %s", pattern).isNotNull();

        Slice input = Slices.wrappedBuffer(text.getBytes(UTF_8));

        long dfa = Dfa.search(prog, input, true, Prog.MatchKind.FIRST_MATCH, true);

        int[] submatch = new int[2];
        boolean nfaMatched = Nfa.search(prog, input, true, Prog.MatchKind.FIRST_MATCH, submatch);

        assertThat(dfa >= 0).as("dfa matched: %s / %s", pattern, text).isEqualTo(nfaMatched);
        if (dfa >= 0) {
            assertThat((int) dfa).as("dfa end boundary: %s / %s", pattern, text).isEqualTo(submatch[1]);
        }
    }
}
