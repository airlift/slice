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

import static org.assertj.core.api.Assertions.assertThat;

public class TestFusedPrefixEngineIntegration
{
    private static final String PREFIX = "Шерлок Холмс";
    private static final String PATTERN = PREFIX + "([0-9]+)";

    @Test
    public void testDfaResumesAfterRejectedCandidate()
    {
        SearchCase searchCase = createSearchCase();
        Prog program = compile(PATTERN);

        assertThat(program.canPrefixAccel()).isTrue();
        assertThat(program.prefixAccelStrategy(searchCase.text().length()))
                .isEqualTo(Prog.PrefixAccelStrategy.FUSED_VECTOR);
        assertThat(Dfa.search(program, searchCase.text(), false, Prog.MatchKind.FIRST_MATCH, true))
                .isEqualTo(searchCase.matchEnd());
        assertThat(Dfa.search(program, searchCase.text(), false, Prog.MatchKind.LONGEST_MATCH, true))
                .isEqualTo(searchCase.matchEnd());
    }

    @Test
    public void testNfaWithAndWithoutCaptures()
    {
        SearchCase searchCase = createSearchCase();
        Prog program = compile(PATTERN);

        assertThat(Nfa.search(program, searchCase.text(), false, Prog.MatchKind.FIRST_MATCH, null)).isTrue();

        int[] groups = new int[4];
        assertThat(Nfa.search(program, searchCase.text(), false, Prog.MatchKind.FIRST_MATCH, groups)).isTrue();
        assertThat(groups).containsExactly(
                searchCase.matchStart(), searchCase.matchEnd(),
                searchCase.captureStart(), searchCase.matchEnd());
    }

    @Test
    public void testBitStateWithCaptures()
    {
        SearchCase searchCase = createSearchCase();
        Prog program = compile(PATTERN);
        assertThat(program.canBitState()).isTrue();

        int[] groups = new int[4];
        assertThat(BitState.search(program, searchCase.text(), false, Prog.MatchKind.FIRST_MATCH, groups)).isTrue();
        assertThat(groups).containsExactly(
                searchCase.matchStart(), searchCase.matchEnd(),
                searchCase.captureStart(), searchCase.matchEnd());
    }

    @Test
    public void testDfaCountWithRejectedCandidatesAndBackingOffset()
    {
        Prog program = compile(PATTERN);
        String body = "x".repeat(1_100) + PREFIX + "x " + PREFIX + "12 " + PREFIX + "345";
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        byte[] backing = new byte[bodyBytes.length + 11];
        System.arraycopy(bodyBytes, 0, backing, 7, bodyBytes.length);
        Slice text = Slices.wrappedBuffer(backing, 7, bodyBytes.length);

        assertThat(Dfa.countMatches(program, text, Prog.MatchKind.FIRST_MATCH)).isEqualTo(2);
        assertThat(Dfa.countMatches(program, text, Prog.MatchKind.LONGEST_MATCH)).isEqualTo(2);
    }

    private static SearchCase createSearchCase()
    {
        String rejectedCandidate = PREFIX + "x";
        String prefix = "x".repeat(1_100) + rejectedCandidate + " ";
        String match = PREFIX + "123";
        Slice text = Slices.utf8Slice(prefix + match);
        int matchStart = prefix.getBytes(StandardCharsets.UTF_8).length;
        int captureStart = matchStart + PREFIX.getBytes(StandardCharsets.UTF_8).length;
        return new SearchCase(text, matchStart, captureStart, text.length());
    }

    private static Prog compile(String pattern)
    {
        ParseResult parsed = RegexpParser.parse(Slices.utf8Slice(pattern), Regexp.LIKE_PERL);
        return Compiler.compile(parsed.regexp(), false, 0);
    }

    private record SearchCase(Slice text, int matchStart, int captureStart, int matchEnd) {}
}
