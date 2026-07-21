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

/**
 * Tests for reverse DFA position accuracy.
 * <p>
 * These tests verify that reverse DFA returns correct match start positions,
 * following the pattern established by DfaReverseSearchTest.
 */
public class TestDfaReversePosition
{
    private static class CompilationResult
    {
        final Prog forward;
        final Prog reverse;

        CompilationResult(Prog forward, Prog reverse)
        {
            this.forward = forward;
            this.reverse = reverse;
        }
    }

    private CompilationResult compile(String pattern)
    {
        byte[] patternBytes = pattern.getBytes(StandardCharsets.UTF_8);
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(patternBytes), Regexp.LIKE_PERL | Regexp.LATIN1);

        Prog forward = Compiler.compile(parsed.regexp(), false, 0);
        Prog reverse = Compiler.compile(parsed.regexp(), true, 0);
        assertThat(forward).isNotNull();
        assertThat(reverse).isNotNull();

        return new CompilationResult(forward, reverse);
    }

    private void testPattern(String pattern, String text, int expectedStart)
    {
        CompilationResult progs = compile(pattern);
        byte[] textBytes = text.getBytes(StandardCharsets.UTF_8);
        Slice textSlice = Slices.wrappedBuffer(textBytes);

        // First, run forward DFA to find match end
        long forwardResult = Dfa.search(progs.forward, textSlice, false, Prog.MatchKind.FIRST_MATCH, true);
        assertThat(forwardResult >= 0).isTrue();

        int matchEnd = (int) forwardResult;

        // Then run reverse DFA on prefix to find match start
        Slice prefix = Slices.wrappedBuffer(textBytes, 0, matchEnd);
        long reverseResult = Dfa.search(progs.reverse, textSlice, 0, prefix.length(), true, Prog.MatchKind.LONGEST_MATCH, true);

        assertThat(reverseResult >= 0).isTrue();
        assertThat((int) reverseResult)
                .as("Pattern '%s' on text '%s': expected match start at %d", pattern, text, expectedStart)
                .isEqualTo(expectedStart);
    }

    @Test
    public void testSimpleLiteral()
    {
        testPattern("ab", "zzabyy", 2);
    }

    @Test
    public void testDigit()
    {
        testPattern("\\d", "abc1xyz", 3);
    }

    @Test
    public void testDigitPlus()
    {
        testPattern("\\d+", "abc123xyz", 3);
    }

    @Test
    public void testWordBoundary()
    {
        testPattern("\\w+", "  hello  ", 2);
    }

    @Test
    public void testCharacterClass()
    {
        testPattern("[a-z]+", "123abc456", 3);
    }

    @Test
    public void testDot()
    {
        testPattern(".", "x", 0);
    }

    @Test
    public void testMultipleDigits()
    {
        testPattern("\\d\\d", "aa12bb", 2);
    }
}
