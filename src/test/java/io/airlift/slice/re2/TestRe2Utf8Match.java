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
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2Utf8Match
{
    @Test
    public void testUtf8()
    {
        byte[] utf8String = new byte[] {
                (byte) 0xe6, (byte) 0x97, (byte) 0xa5,
                (byte) 0xe6, (byte) 0x9c, (byte) 0xac,
                (byte) 0xe8, (byte) 0xaa, (byte) 0x9e
        };
        byte[] utf8Pattern = new byte[] {
                (byte) '.',
                (byte) 0xe6, (byte) 0x9c, (byte) 0xac,
                (byte) '.'
        };

        Slice text = Slices.wrappedBuffer(utf8String);
        Re2.Options latin1 = latin1Options();

        Re2 reLatinDots = Re2.compile(Slices.wrappedBuffer(".........".getBytes(StandardCharsets.UTF_8)), latin1);
        assertThat(reLatinDots.fullMatch(text)).isTrue();
        Re2 reUtf8Dots = Re2.compile(Slices.wrappedBuffer("...".getBytes(StandardCharsets.UTF_8)), Re2.Options.defaults());
        assertThat(reUtf8Dots.fullMatch(text)).isTrue();

        Re2 reLatinDot = Re2.compile(Slices.wrappedBuffer("(.)".getBytes(StandardCharsets.UTF_8)), latin1);
        MatchResult latinCapture = reLatinDot.partialMatchResult(text);
        assertThat(latinCapture).isNotNull();
        assertThat(toByteArray(latinCapture.groupSlice(1))).isEqualTo(new byte[] {(byte) 0xe6});

        Re2 reUtf8Dot = Re2.compile(Slices.wrappedBuffer("(.)".getBytes(StandardCharsets.UTF_8)), Re2.Options.defaults());
        MatchResult utf8Capture = reUtf8Dot.partialMatchResult(text);
        assertThat(utf8Capture).isNotNull();
        assertThat(toByteArray(utf8Capture.groupSlice(1))).isEqualTo(new byte[] {(byte) 0xe6, (byte) 0x97, (byte) 0xa5});

        Re2 reLatinSelf = Re2.compile(Slices.wrappedBuffer(Arrays.copyOf(utf8String, utf8String.length)), latin1);
        assertThat(reLatinSelf.fullMatch(text)).isTrue();
        Re2 reUtf8Self = Re2.compile(Slices.wrappedBuffer(Arrays.copyOf(utf8String, utf8String.length)), Re2.Options.defaults());
        assertThat(reUtf8Self.fullMatch(text)).isTrue();

        Re2 reLatinPattern = Re2.compile(Slices.wrappedBuffer(utf8Pattern), latin1);
        assertThat(reLatinPattern.fullMatch(text)).isFalse();
        Re2 reUtf8Pattern = Re2.compile(Slices.wrappedBuffer(utf8Pattern), Re2.Options.defaults());
        assertThat(reUtf8Pattern.fullMatch(text)).isTrue();
    }

    @Test
    public void testUngreedyUtf8()
    {
        Slice target = Slices.wrappedBuffer("a aX".getBytes(StandardCharsets.UTF_8));
        Re2.Options latin1 = latin1Options();

        Re2 reLatin = Re2.compile(Slices.wrappedBuffer("\\w+X".getBytes(StandardCharsets.UTF_8)), latin1);
        Re2 reUtf8 = Re2.compile(Slices.wrappedBuffer("\\w+X".getBytes(StandardCharsets.UTF_8)), Re2.Options.defaults());
        assertThat(reLatin.fullMatch(target)).isFalse();
        assertThat(reUtf8.fullMatch(target)).isFalse();

        Re2 reLatinUngreedy = Re2.compile(Slices.wrappedBuffer("(?U)\\w+X".getBytes(StandardCharsets.UTF_8)), latin1);
        Re2 reUtf8Ungreedy = Re2.compile(Slices.wrappedBuffer("(?U)\\w+X".getBytes(StandardCharsets.UTF_8)), Re2.Options.defaults());
        assertThat(reLatinUngreedy.fullMatch(target)).isFalse();
        assertThat(reUtf8Ungreedy.fullMatch(target)).isFalse();
    }

    private static Re2.Options latin1Options()
    {
        return Re2.Options.latin1();
    }

    private static byte[] toByteArray(Slice slice)
    {
        return slice.getBytes();
    }
}
