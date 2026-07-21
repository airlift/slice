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

import static io.airlift.slice.re2.Regexp.LATIN1;
import static io.airlift.slice.re2.Regexp.LIKE_PERL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRe2Api
{
    private static final int DEFAULT_FLAGS = LIKE_PERL;

    @Test
    public void testPartialAndFullMatch()
    {
        Re2 re2 = Re2.compile(Slices.wrappedBuffer("abc".getBytes(StandardCharsets.UTF_8)), DEFAULT_FLAGS);

        assertThat(re2.partialMatch(Slices.wrappedBuffer("xxabczz".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(re2.fullMatch(Slices.wrappedBuffer("xxabczz".getBytes(StandardCharsets.UTF_8)))).isFalse();

        assertThat(re2.fullMatch(Slices.wrappedBuffer("abc".getBytes(StandardCharsets.UTF_8)))).isTrue();
    }

    @Test
    public void testWordBoundaries()
    {
        Re2 re2 = Re2.compile(Slices.wrappedBuffer("\\babc\\b".getBytes(StandardCharsets.UTF_8)), DEFAULT_FLAGS);

        assertThat(re2.partialMatch(Slices.wrappedBuffer(" abc ".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(re2.partialMatch(Slices.wrappedBuffer("zabc ".getBytes(StandardCharsets.UTF_8)))).isFalse();
    }

    @Test
    public void testPerlCharacterClasses()
    {
        Re2 digit = Re2.compile(Slices.wrappedBuffer("\\d+".getBytes(StandardCharsets.UTF_8)), DEFAULT_FLAGS);
        assertThat(digit.partialMatch(Slices.wrappedBuffer("abc123def".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(digit.fullMatch(Slices.wrappedBuffer("123".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(digit.partialMatch(Slices.wrappedBuffer("abcdef".getBytes(StandardCharsets.UTF_8)))).isFalse();

        Re2 digitInClass = Re2.compile(Slices.wrappedBuffer("[\\d]+".getBytes(StandardCharsets.UTF_8)), DEFAULT_FLAGS);
        assertThat(digitInClass.fullMatch(Slices.wrappedBuffer("123".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(digitInClass.partialMatch(Slices.wrappedBuffer("a1b".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(digitInClass.fullMatch(Slices.wrappedBuffer("abc".getBytes(StandardCharsets.UTF_8)))).isFalse();

        Re2 nonDigitInClass = Re2.compile(Slices.wrappedBuffer("[^\\d]+".getBytes(StandardCharsets.UTF_8)), DEFAULT_FLAGS);
        assertThat(nonDigitInClass.fullMatch(Slices.wrappedBuffer("abc".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(nonDigitInClass.fullMatch(Slices.wrappedBuffer("123".getBytes(StandardCharsets.UTF_8)))).isFalse();

        Re2 nonDigit = Re2.compile(Slices.wrappedBuffer("\\D+".getBytes(StandardCharsets.UTF_8)), DEFAULT_FLAGS);
        assertThat(nonDigit.fullMatch(Slices.wrappedBuffer("abc".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(nonDigit.fullMatch(Slices.wrappedBuffer("123".getBytes(StandardCharsets.UTF_8)))).isFalse();

        Re2 space = Re2.compile(Slices.wrappedBuffer("\\s+".getBytes(StandardCharsets.UTF_8)), DEFAULT_FLAGS);
        assertThat(space.fullMatch(Slices.wrappedBuffer(" \t".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(space.partialMatch(Slices.wrappedBuffer("x \t y".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(space.fullMatch(Slices.wrappedBuffer("x".getBytes(StandardCharsets.UTF_8)))).isFalse();
        assertThat(space.fullMatch(Slices.wrappedBuffer("\u000b".getBytes(StandardCharsets.UTF_8)))).isFalse(); // vertical tab is not in \s

        Re2 nonSpace = Re2.compile(Slices.wrappedBuffer("\\S+".getBytes(StandardCharsets.UTF_8)), DEFAULT_FLAGS);
        assertThat(nonSpace.fullMatch(Slices.wrappedBuffer("abc".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(nonSpace.partialMatch(Slices.wrappedBuffer(" \t abc".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(nonSpace.fullMatch(Slices.wrappedBuffer(" \t".getBytes(StandardCharsets.UTF_8)))).isFalse();
        assertThat(nonSpace.fullMatch(Slices.wrappedBuffer("\u000b".getBytes(StandardCharsets.UTF_8)))).isTrue();

        Re2 word = Re2.compile(Slices.wrappedBuffer("\\w+".getBytes(StandardCharsets.UTF_8)), DEFAULT_FLAGS);
        assertThat(word.fullMatch(Slices.wrappedBuffer("Az_09".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(word.partialMatch(Slices.wrappedBuffer("--Az_09--".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(word.fullMatch(Slices.wrappedBuffer("-".getBytes(StandardCharsets.UTF_8)))).isFalse();

        Re2 nonWord = Re2.compile(Slices.wrappedBuffer("\\W+".getBytes(StandardCharsets.UTF_8)), DEFAULT_FLAGS);
        assertThat(nonWord.fullMatch(Slices.wrappedBuffer("-".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(nonWord.partialMatch(Slices.wrappedBuffer("a-b".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(nonWord.fullMatch(Slices.wrappedBuffer("a".getBytes(StandardCharsets.UTF_8)))).isFalse();
    }

    @Test
    public void testDotDoesNotMatchNewlineByDefault()
    {
        Re2 dot = Re2.compile(Slices.wrappedBuffer(".".getBytes(StandardCharsets.UTF_8)), DEFAULT_FLAGS);
        assertThat(dot.fullMatch(Slices.wrappedBuffer("a".getBytes(StandardCharsets.UTF_8)))).isTrue();
        assertThat(dot.fullMatch(Slices.wrappedBuffer("\n".getBytes(StandardCharsets.UTF_8)))).isFalse();

        Re2 dotLatin1 = Re2.compile(Slices.wrappedBuffer(".".getBytes(StandardCharsets.UTF_8)), LATIN1);
        assertThat(dotLatin1.fullMatch(Slices.wrappedBuffer(new byte[] {(byte) 'a'}))).isTrue();
        assertThat(dotLatin1.fullMatch(Slices.wrappedBuffer(new byte[] {(byte) '\n'}))).isFalse();
    }

    @Test
    public void testMatchWithSubmatchesAndAnchors()
    {
        Re2 re2 = Re2.compile(Slices.wrappedBuffer("a(b)c".getBytes(StandardCharsets.UTF_8)), DEFAULT_FLAGS);
        Slice text = Slices.wrappedBuffer("zzabczz".getBytes(StandardCharsets.UTF_8));

        int[] submatch = new int[4];
        assertThat(re2.matchInto(text, Re2.Anchor.UNANCHORED, submatch)).isTrue();
        assertThat(submatch[0]).isEqualTo(2);
        assertThat(submatch[1]).isEqualTo(5);
        assertThat(submatch[2]).isEqualTo(3);
        assertThat(submatch[3]).isEqualTo(4);

        assertThat(re2.matchInto(text, 2, 5, Re2.Anchor.ANCHOR_BOTH, submatch)).isTrue();
        assertThat(submatch[0]).isEqualTo(2);
        assertThat(submatch[1]).isEqualTo(5);
    }
}
