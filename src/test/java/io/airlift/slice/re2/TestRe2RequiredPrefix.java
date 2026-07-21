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

public class TestRe2RequiredPrefix
{
    @Test
    public void testForwardProgramCompilesSuffix()
    {
        Re2 prefixed = Re2.compile(Slices.utf8Slice("^abcdef(.*)$"));
        Re2 suffix = Re2.compile(Slices.utf8Slice("(.*)$"));

        assertThat(prefixed.programSize()).isEqualTo(suffix.programSize());
    }

    @Test
    public void testCaptureOffsetsIncludeStrippedPrefix()
    {
        Re2 regexp = Re2.compile(Slices.utf8Slice("^abcdef(.*)$"));
        Slice text = Slices.utf8Slice("abcdefxyz");
        int[] groups = new int[4];

        assertThat(regexp.matchInto(text, UNANCHORED, groups)).isTrue();
        assertThat(groups).containsExactly(0, 9, 6, 9);

        assertThat(regexp.matchInto(text, ANCHOR_BOTH, groups)).isTrue();
        assertThat(groups).containsExactly(0, 9, 6, 9);
    }

    @Test
    public void testRequiredPrefixRejectsMiddleWindow()
    {
        Re2 regexp = Re2.compile(Slices.utf8Slice("^abcdef(.*)$"));
        Slice text = Slices.utf8Slice("xabcdefxyz");

        assertThat(regexp.matchInto(text, 1, text.length(), UNANCHORED, new int[4])).isFalse();
    }
}
