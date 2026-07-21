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

import static io.airlift.slice.re2.Re2.Anchor.ANCHOR_BOTH;
import static io.airlift.slice.re2.Re2.Anchor.UNANCHORED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRe2SliceApi
{
    @Test
    public void testOneShotMatchingAndCaptures()
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice("(?<word>\\w+)-(\\d+)"));
        Slice input = Slices.utf8Slice("prefix abc-42 suffix");

        assertThat(pattern.partialMatch(input)).isTrue();
        assertThat(pattern.fullMatch(input)).isFalse();

        int[] groups = new int[6];
        assertThat(pattern.matchInto(input, UNANCHORED, groups)).isTrue();
        assertThat(groups).containsExactly(7, 13, 7, 10, 11, 13);

        MatchResult result = pattern.matchResult(input, UNANCHORED);
        assertThat(result.groupSlice(0).toStringUtf8()).isEqualTo("abc-42");
        assertThat(result.groupSlice("word").toStringUtf8()).isEqualTo("abc");
    }

    @Test
    public void testLogicalSliceOffsets()
    {
        byte[] bytes = Slices.utf8Slice("xxabc-42yy").getBytes();
        Slice input = Slices.wrappedBuffer(bytes, 2, 6);
        Re2 pattern = Re2.compile(Slices.utf8Slice("(abc)-(\\d+)"));

        int[] groups = new int[6];
        assertThat(pattern.matchInto(input, ANCHOR_BOTH, groups)).isTrue();
        assertThat(groups).containsExactly(0, 6, 0, 3, 4, 6);
        assertThat(pattern.fullMatchResult(input).groupSlice(2).toStringUtf8()).isEqualTo("42");
    }

    @Test
    public void testCompiledPatternOwnsPatternBytes()
    {
        Slice sourcePattern = Slices.utf8Slice("abc");
        Re2 pattern = Re2.compile(sourcePattern);
        sourcePattern.setByte(0, 'x');

        assertThat(pattern.pattern().toStringUtf8()).isEqualTo("abc");
        assertThat(pattern.fullMatch(Slices.utf8Slice("abc"))).isTrue();
        assertThat(pattern.fullMatch(Slices.utf8Slice("xbc"))).isFalse();
    }

    @Test
    public void testQuoteReturnsSlice()
    {
        assertThat(Re2.quote(Slices.utf8Slice("a+b")).toStringUtf8()).isEqualTo("a\\+b");
    }
}
