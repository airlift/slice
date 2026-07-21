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

public class TestFullMatchState
{
    @Test
    public void testLatin1GeneratesAltMatch()
    {
        Prog prog = compile("(?s).*", Regexp.LIKE_PERL | Regexp.LATIN1);
        assertThat(prog.dump()).contains("altmatch");
    }

    @Test
    public void testUtf8DoesNotGenerateAltMatch()
    {
        Prog prog = compile("(?s).*", Regexp.LIKE_PERL);
        assertThat(prog.dump()).doesNotContain("altmatch");
    }

    @Test
    public void testMatchesAnyByteStringMetadata()
    {
        assertThat(compile("(?s).*", Regexp.LIKE_PERL).matchesAnyByteString()).isFalse();
        assertThat(compile("(?s).*", Regexp.LIKE_PERL | Regexp.LATIN1).matchesAnyByteString()).isTrue();
        assertThat(compile("\\C*", Regexp.LIKE_PERL).matchesAnyByteString()).isTrue();
        assertThat(compile(".*", Regexp.LIKE_PERL).matchesAnyByteString()).isFalse();
        assertThat(compile("abc", Regexp.LIKE_PERL).matchesAnyByteString()).isFalse();
    }

    @Test
    public void testUtf8DotStarRejectsInvalidUtf8()
    {
        Re2 regexp = Re2TestAccess.compile(Slices.utf8Slice("(?s).*"), Regexp.LIKE_PERL);
        assertThat(regexp.fullMatch(Slices.wrappedBuffer(new byte[] {(byte) 0xFF}))).isFalse();
    }

    @Test
    public void testUtf8DotStarSubmatch()
    {
        Re2 regexp = Re2TestAccess.compile(Slices.utf8Slice("(?s).*"), Regexp.LIKE_PERL);
        byte[] text = "hello world".getBytes(StandardCharsets.UTF_8);
        int[] submatch = new int[2];

        assertThat(regexp.matchInto(Slices.wrappedBuffer(text), Re2.Anchor.ANCHOR_BOTH, submatch)).isTrue();
        assertThat(submatch).containsExactly(0, text.length);
    }

    private static Prog compile(String pattern, int flags)
    {
        ParseResult parsed = RegexpParser.parse(utf8(pattern), flags);
        return Compiler.compile(parsed.regexp(), false, 1024 * 1024);
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
