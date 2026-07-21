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

public class TestDfaContextFlags
{
    private static final int MULTI_LINE = Regexp.LIKE_PERL & ~Regexp.ONE_LINE;

    @Test
    public void testBeginLineStateUsesContextByteBeforeText()
    {
        // The text slice begins after a newline inside the context.
        byte[] ctx = "x\nabc".getBytes(UTF_8);
        Slice context = Slices.wrappedBuffer(ctx);
        Slice text = Slices.wrappedBuffer(ctx, 2, 3);

        assertEndBoundaryAgrees("^abc", MULTI_LINE, text, context, true);
    }

    @Test
    public void testWordBoundaryStateUsesContextByteBeforeText()
    {
        byte[] ctx1 = " abc".getBytes(UTF_8);
        Slice context1 = Slices.wrappedBuffer(ctx1);
        Slice text1 = Slices.wrappedBuffer(ctx1, 1, 3);
        assertEndBoundaryAgrees("\\babc", Regexp.LIKE_PERL, text1, context1, true);

        // Between two word characters there is no word boundary.
        byte[] ctx2 = "xabc".getBytes(UTF_8);
        Slice context2 = Slices.wrappedBuffer(ctx2);
        Slice text2 = Slices.wrappedBuffer(ctx2, 1, 3);
        assertEndBoundaryAgrees("\\babc", Regexp.LIKE_PERL, text2, context2, true);
    }

    @Test
    public void testUnanchoredEndBoundaryAgreesWithNfaForSimpleCase()
    {
        Slice text = Slices.wrappedBuffer("ba".getBytes(UTF_8));
        assertEndBoundaryAgrees("a", Regexp.LIKE_PERL, text, text, false);
    }

    @Test
    public void testLogicalRegionHasIndependentContextBoundaries()
    {
        ParseResult parsed = RegexpParser.parse(Slices.utf8Slice("\\babc\\b"), Regexp.LIKE_PERL);
        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        Slice context = Slices.utf8Slice("xabcx");

        assertThat(Dfa.search(
                prog,
                context,
                1,
                4,
                1,
                4,
                false,
                Prog.MatchKind.FIRST_MATCH,
                true))
                .isEqualTo(3);
    }

    private static void assertEndBoundaryAgrees(String pattern, int flags, Slice text, Slice context, boolean anchored)
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), flags);

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).as("compile: %s", pattern).isNotNull();

        int start = text.byteArrayOffset() - context.byteArrayOffset();
        int end = start + text.length();
        long dfa = Dfa.search(prog, context, start, end, anchored, Prog.MatchKind.FIRST_MATCH, false);

        int[] submatch = new int[2];
        boolean nfaMatched = Nfa.search(prog, context, start, end, anchored, Prog.MatchKind.FIRST_MATCH, submatch);

        assertThat(dfa >= 0).as("dfa matched: %s", pattern).isEqualTo(nfaMatched);
        if (dfa >= 0) {
            assertThat((int) dfa).as("dfa end: %s", pattern).isEqualTo(submatch[1]);
        }
    }
}
