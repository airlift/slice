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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRe2MatchIntoBuffer
{
    @Test
    public void testPartialMatchWithLimitedGroupsBuffer()
    {
        Re2 re = Re2.compile(utf8("(\\d+):(\\w+)"), Re2.Options.defaults());

        int[] oneCapture = new int[4];
        assertThat(re.matchInto(utf8("answer: 42:life"), Re2.Anchor.UNANCHORED, oneCapture)).isTrue();

        MatchResult partial = re.partialMatchResult(utf8("answer: 42:life"));
        assertThat(partial).isNotNull();
        assertThat(partial.groupUtf8(1)).isEqualTo("42");
        assertThat(partial.groupUtf8(2)).isEqualTo("life");

        assertThat(oneCapture[0]).isEqualTo(partial.start(0));
        assertThat(oneCapture[1]).isEqualTo(partial.end(0));
        assertThat(oneCapture[2]).isEqualTo(partial.start(1));
        assertThat(oneCapture[3]).isEqualTo(partial.end(1));
    }

    @Test
    public void testFullMatchWithNoCaptureBuffer()
    {
        Re2 re = Re2.compile(utf8("h.*o"), Re2.Options.defaults());
        assertThat(re.matchInto(utf8("hello"), Re2.Anchor.ANCHOR_BOTH, null)).isTrue();
        assertThat(re.matchInto(utf8("othello"), Re2.Anchor.ANCHOR_BOTH, null)).isFalse();
    }

    @Test
    public void testOddGroupLengthThrows()
    {
        Re2 re = Re2.compile(utf8("(\\d+)"), Re2.Options.defaults());
        assertThatThrownBy(() -> re.matchInto(utf8("1001"), Re2.Anchor.ANCHOR_BOTH, new int[3]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("groups length must be even");
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
