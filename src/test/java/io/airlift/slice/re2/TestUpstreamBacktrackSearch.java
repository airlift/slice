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

// Adapted from upstream RE2: re2/testing/search_test.cc.
public class TestUpstreamBacktrackSearch
{
    private static Prog compile(String pattern, int flags)
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), flags);

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).as("compile: %s", pattern).isNotNull();
        return prog;
    }

    private static Slice latin1(String s)
    {
        return Slices.wrappedBuffer(s.getBytes(StandardCharsets.ISO_8859_1));
    }

    private static String sliceLatin1(Slice text, int start, int end)
    {
        if (start < 0 || end < 0) {
            return null;
        }
        return new String(text.byteArray(), text.byteArrayOffset() + start, end - start, StandardCharsets.ISO_8859_1);
    }

    @Test
    public void testLeftmostFirstVsLeftmostLongest()
    {
        Prog prog = compile("(fo|foo)", Regexp.LIKE_PERL | Regexp.LATIN1);
        Slice text = latin1("foo");

        int[] first = new int[4];
        assertThat(Backtrack.search(prog, text, true, false, first)).isTrue();
        assertThat(sliceLatin1(text, first[0], first[1])).isEqualTo("fo");
        assertThat(sliceLatin1(text, first[2], first[3])).isEqualTo("fo");

        int[] longest = new int[4];
        assertThat(Backtrack.search(prog, text, true, true, longest)).isTrue();
        assertThat(sliceLatin1(text, longest[0], longest[1])).isEqualTo("foo");
        assertThat(sliceLatin1(text, longest[2], longest[3])).isEqualTo("foo");
    }

    @Test
    public void testWordBoundary()
    {
        Prog prog = compile("\\bfoo\\b", Regexp.LIKE_PERL | Regexp.LATIN1);
        Slice text = latin1("nofoo foo that");

        int[] m = new int[2];
        assertThat(Backtrack.search(prog, text, false, false, m)).isTrue();
        assertThat(sliceLatin1(text, m[0], m[1])).isEqualTo("foo");
    }

    @Test
    public void testAnyByteStarGreedyVsNonGreedy()
    {
        Slice text = latin1("abc");

        Prog greedy = compile("\\C*", Regexp.LIKE_PERL | Regexp.LATIN1);
        int[] g = new int[2];
        assertThat(Backtrack.search(greedy, text, true, false, g)).isTrue();
        assertThat(g[0]).isEqualTo(0);
        assertThat(g[1]).isEqualTo(3);

        Prog nonGreedy = compile("\\C*?", Regexp.LIKE_PERL | Regexp.LATIN1);
        int[] ng = new int[2];
        assertThat(Backtrack.search(nonGreedy, text, true, false, ng)).isTrue();
        assertThat(ng[0]).isEqualTo(0);
        assertThat(ng[1]).isEqualTo(0);
    }
}
