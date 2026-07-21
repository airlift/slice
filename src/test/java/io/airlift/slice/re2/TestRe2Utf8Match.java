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

    @Test
    public void testLatin1HighByteFusedPrefix()
    {
        byte[] prefix = {(byte) 0xE9, (byte) 0xF1};
        byte[] pattern = concatenate(prefix, "([0-9]+)".getBytes(StandardCharsets.ISO_8859_1));
        Re2 re2 = Re2.compile(Slices.wrappedBuffer(pattern), Re2.Options.latin1());

        byte[] padding = new byte[1_100];
        Arrays.fill(padding, (byte) 'x');
        byte[] rejectedCandidate = concatenate(prefix, new byte[] {'x', ' '});
        byte[] match = concatenate(prefix, new byte[] {'1', '2'});
        Slice text = Slices.wrappedBuffer(concatenate(padding, rejectedCandidate, match));

        MatchResult result = re2.partialMatchResult(text);
        assertThat(result).isNotNull();
        assertThat(result.start(0)).isEqualTo(padding.length + rejectedCandidate.length);
        assertThat(result.end(0)).isEqualTo(text.length());
        assertThat(result.groupSlice(1).getBytes()).containsExactly((byte) '1', (byte) '2');

        assertThat(re2.partialMatchResult(Slices.wrappedBuffer(concatenate(padding, rejectedCandidate)))).isNull();
    }

    @Test
    public void testFusedPrefixWithMalformedUtf8Haystack()
    {
        byte[] prefix = "Шерлок Холмс".getBytes(StandardCharsets.UTF_8);
        Re2 re2 = Re2.compile(Slices.utf8Slice("Шерлок Холмс([0-9]+)"));
        byte[] padding = new byte[1_100];
        Arrays.fill(padding, (byte) 'x');
        byte[] malformed = {(byte) 0xFF, (byte) 0xC2, (byte) 0xE0, (byte) 0x80, (byte) 0xF0, (byte) 0x80, (byte) 0x80};
        byte[] rejectedCandidate = concatenate(prefix, new byte[] {'x'});
        byte[] match = concatenate(prefix, new byte[] {'4', '2'});
        Slice text = Slices.wrappedBuffer(concatenate(padding, malformed, rejectedCandidate, malformed, match));

        MatchResult result = re2.partialMatchResult(text);
        assertThat(result).isNotNull();
        int expectedStart = padding.length + malformed.length + rejectedCandidate.length + malformed.length;
        assertThat(result.start(0)).isEqualTo(expectedStart);
        assertThat(result.end(0)).isEqualTo(text.length());

        assertThat(re2.partialMatchResult(Slices.wrappedBuffer(concatenate(padding, malformed, rejectedCandidate, malformed)))).isNull();
    }

    private static Re2.Options latin1Options()
    {
        return Re2.Options.latin1();
    }

    private static byte[] toByteArray(Slice slice)
    {
        return slice.getBytes();
    }

    private static byte[] concatenate(byte[]... arrays)
    {
        int length = Arrays.stream(arrays)
                .mapToInt(array -> array.length)
                .sum();
        byte[] result = new byte[length];
        int offset = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }
}
