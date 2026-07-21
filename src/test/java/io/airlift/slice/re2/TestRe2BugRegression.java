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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Includes tests ported from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2BugRegression
{
    /**
     * CL8622304 - Bugs introduced by CL 8622304
     * Tests that capturing groups work properly with escaped backslash in char class.
     */
    @Test
    public void testCL8622304()
    {
        // reported by ingow
        Re2 re1 = Re2.compile(utf8("([^\\\\])"), Re2.Options.defaults());
        assertThat(re1.fullMatch(utf8("D"))).isTrue();

        MatchResult dir = re1.fullMatchResult(utf8("D"));
        assertThat(dir).isNotNull();
        assertThat(dir.groupUtf8(1)).isEqualTo("D");

        // reported by jacobsa
        Re2 re2 = Re2.compile(utf8("(\\w+)(?::((?:[^;\\\\]|\\\\.)*))?;?"), Re2.Options.defaults());
        MatchResult keyValue = re2.partialMatchResult(utf8("bar:1,0x2F,030,4,5;baz:true;fooby:false,true"));
        assertThat(keyValue).isNotNull();
        assertThat(keyValue.groupUtf8(1)).isEqualTo("bar");
        assertThat(keyValue.groupUtf8(2)).isEqualTo("1,0x2F,030,4,5");
    }

    /**
     * Bug 1816809 - Issue with anchored matching and complex nested capturing groups.
     */
    @Test
    public void testBug1816809AnchoredMatch()
    {
        Re2 re = Re2.compile(utf8("(((((llx((-3)|(4)))(;(llx((-3)|(4))))*))))"), Re2.Options.defaults());
        MatchResult result = re.matchResult(utf8("llx-3;llx4"), Re2.Anchor.ANCHOR_START);
        assertThat(result).isNotNull();
    }

    /**
     * Bug 3061120 - Case folding issue with Kelvin sign and Latin long s.
     * (?i)\W should not match word characters like 'x', 'k', or 's'.
     */
    @Test
    public void testBug3061120CaseFold()
    {
        Re2 re = Re2.compile(utf8("(?i)\\W"), Re2.Options.defaults());
        assertThat(re.partialMatch(utf8("x"))).isFalse();
        assertThat(re.partialMatch(utf8("k"))).isFalse();  // broke because of kelvin
        assertThat(re.partialMatch(utf8("s"))).isFalse();  // broke because of latin long s
    }

    /**
     * Bug 10131674 - Some octal escapes describe values that do not fit in a byte.
     * The pattern should fail to compile due to invalid escape sequences.
     */
    @Test
    public void testBug10131674()
    {
        // Some of these escapes describe values that do not fit in a byte (e.g., \440, \656)
        assertThatThrownBy(() -> Re2.compile(latin1("\\140\\440\\174\\271\\150\\656\\106\\201\\004\\332"), Re2.Options.latin1()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Bug 18391750 - Stray write past end of match_ in nfa.cc, caught by fuzzing + address sanitizer.
     * This tests specific binary patterns that triggered the bug.
     */
    @Test
    public void testBug18391750()
    {
        byte[] patternBytes = {
                (byte) 0x28, (byte) 0x28, (byte) 0xfc, (byte) 0xfc, (byte) 0x08, (byte) 0x08,
                (byte) 0x26, (byte) 0x26, (byte) 0x28, (byte) 0xc2, (byte) 0x9b, (byte) 0xc5,
                (byte) 0xc5, (byte) 0xd4, (byte) 0x8f, (byte) 0x8f, (byte) 0x69, (byte) 0x69,
                (byte) 0xe7, (byte) 0x29, (byte) 0x7b, (byte) 0x37, (byte) 0x31, (byte) 0x31,
                (byte) 0x7d, (byte) 0xae, (byte) 0x7c, (byte) 0x7c, (byte) 0xf3, (byte) 0x29,
                (byte) 0xae, (byte) 0xae, (byte) 0x2e, (byte) 0x2a, (byte) 0x29
        };
        Re2 re = Re2.compile(
                Slices.wrappedBuffer(patternBytes),
                Re2.Options.latin1()
                        .setLongestMatch(true)
                        .setDotMatchesNewline(true)
                        .setCaseSensitive(false));
        // Just ensure this doesn't crash - the bug was a memory write past end
        re.partialMatch(Slices.wrappedBuffer(patternBytes));
    }

    /**
     * Bug 18458852 - Bug in parser accepting invalid (too large) rune,
     * causing compiler to fail in DCHECK() in UTF-8 character class code.
     * The pattern should fail to compile.
     */
    @Test
    public void testBug18458852()
    {
        byte[] patternBytes = {
                (byte) 0x28, (byte) 0x05, (byte) 0x05, (byte) 0x41, (byte) 0x41, (byte) 0x28,
                (byte) 0x24, (byte) 0x5b, (byte) 0x5e, (byte) 0xf5, (byte) 0x87, (byte) 0x87,
                (byte) 0x90, (byte) 0x29, (byte) 0x5d, (byte) 0x29, (byte) 0x29
        };
        assertThatThrownBy(() -> Re2.compile(Slices.wrappedBuffer(patternBytes), Re2.Options.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Bug 18523943 - Bug in BitState: case kFailInst failed the match entirely.
     * Tests that a pattern with specific options matches correctly.
     */
    @Test
    public void testBug18523943()
    {
        byte[] textBytes = {
                (byte) 0x29, (byte) 0x29, (byte) 0x24
        };
        byte[] patternBytes = {
                (byte) 0x28, (byte) 0x0a, (byte) 0x2a, (byte) 0x2a, (byte) 0x29
        };
        Re2 re = Re2.compile(
                Slices.wrappedBuffer(patternBytes),
                Re2.Options.latin1()
                        .setPosixSyntax(true)
                        .setLongestMatch(true)
                        .setLiteral(false)
                        .setNeverNewline(true));
        MatchResult s1 = re.partialMatchResult(Slices.wrappedBuffer(textBytes));
        assertThat(s1).isNotNull();
    }

    /**
     * Bug 21371806 - Bug in parser accepting Unicode groups in Latin-1 mode,
     * causing compiler to fail in DCHECK() in prog.cc.
     * The pattern should compile successfully in Latin-1 mode.
     */
    @Test
    public void testBug21371806()
    {
        Re2 re = Re2.compile(utf8("g\\p{Zl}]"), Re2.Options.latin1());
    }

    /**
     * Bug 26356109 - Bug in parser caused by factoring of common prefixes in alternations.
     * In the past, "a\C*?c|a\C*?b" was factored to "a\C*?[bc]". Thus, the automaton would
     * consume "ab" and then stop (when unanchored) whereas it should consume all
     * of "abc" as per first-match semantics.
     */
    @Test
    public void testBug26356109()
    {
        Re2 re = Re2.compile(utf8("a\\C*?c|a\\C*?b"), Re2.Options.defaults());

        Slice text = utf8("abc");
        int[] submatch = new int[2];

        assertThat(re.matchInto(text, Re2.Anchor.UNANCHORED, submatch)).isTrue();
        assertThat(submatch[0]).as("UNANCHORED start").isEqualTo(0);
        assertThat(submatch[1]).as("UNANCHORED end").isEqualTo(3);

        assertThat(re.matchInto(text, Re2.Anchor.ANCHOR_BOTH, submatch)).isTrue();
        assertThat(submatch[0]).as("ANCHOR_BOTH start").isEqualTo(0);
        assertThat(submatch[1]).as("ANCHOR_BOTH end").isEqualTo(3);
    }

    /**
     * Issue 104 - RE2::GlobalReplace always advanced by one byte when the empty string was
     * matched, which would clobber any rune that is longer than one byte.
     */
    @Test
    public void testIssue104()
    {
        // Basic ASCII test
        Re2 re1 = Re2.compile(utf8("a*"), Re2.Options.defaults());
        Re2.GlobalReplaceResult result1 = re1.globalReplace(utf8("bc"), utf8("d"));
        assertThat(result1.count()).isEqualTo(3);
        assertThat(toString(result1.result())).isEqualTo("dbdcd");

        // Polish characters (2-byte UTF-8)
        Re2 re2 = Re2.compile(utf8("\u0106*"), Re2.Options.defaults());  // Capital C with acute
        Re2.GlobalReplaceResult result2 = re2.globalReplace(utf8("\u0105\u0107"), utf8("\u0108"));  // a ogonek, c acute -> C circumflex
        assertThat(result2.count()).isEqualTo(3);
        assertThat(toString(result2.result())).isEqualTo("\u0108\u0105\u0108\u0107\u0108");

        // Chinese characters (3-byte UTF-8)
        Re2 re3 = Re2.compile(utf8("\u5927*"), Re2.Options.defaults());  // "big"
        Re2.GlobalReplaceResult result3 = re3.globalReplace(utf8("\u4eba\u7c7b"), utf8("\u5c0f"));  // "people", "class" -> "small"
        assertThat(result3.count()).isEqualTo(3);
        assertThat(toString(result3.result())).isEqualTo("\u5c0f\u4eba\u5c0f\u7c7b\u5c0f");
    }

    /**
     * Issue 310 - (?:|a)* matched more text than (?:|a)+ did.
     * Both should match empty string first due to first-match semantics.
     */
    @Test
    public void testIssue310()
    {
        Slice text = utf8("aaa");
        int[] submatch = new int[2];

        Re2 star = Re2.compile(utf8("(?:|a)*"), Re2.Options.defaults());
        assertThat(star.matchInto(text, Re2.Anchor.UNANCHORED, submatch)).isTrue();
        // Should match empty string due to first-match semantics (empty alternative comes first)
        assertThat(submatch[0]).isEqualTo(0);
        assertThat(submatch[1]).isEqualTo(0);

        Re2 plus = Re2.compile(utf8("(?:|a)+"), Re2.Options.defaults());
        assertThat(plus.matchInto(text, Re2.Anchor.UNANCHORED, submatch)).isTrue();
        // Should also match empty string
        assertThat(submatch[0]).isEqualTo(0);
        assertThat(submatch[1]).isEqualTo(0);
    }

    /**
     * Issue 477 - Regexp::LeadingString didn't output Latin1 into flags.
     * In the given pattern, 0xA5 should be factored out, but shouldn't lose its
     * Latin1-ness in the process. Because that was happening, the prefix for accel
     * was 0xC2 0xA5 instead of 0xA5.
     */
    @Test
    public void testIssue477()
    {
        byte[] bytes = {
                (byte) 0xa5, (byte) 0xd1, (byte) 0xa5, (byte) 0xd1,
                (byte) 0x61, (byte) 0x63, (byte) 0xa5, (byte) 0x64
        };
        Slice text = Slices.wrappedBuffer(bytes);

        // Pattern: \xa5\xd1|\xa5\x64 (matching 0xa5 0xd1 or 0xa5 0x64)
        byte[] patternBytes = {(byte) 0xa5, (byte) 0xd1, (byte) 0x7c, (byte) 0xa5, (byte) 0x64};
        Re2 re = Re2.compile(Slices.wrappedBuffer(patternBytes), Re2.Options.latin1());

        Re2.GlobalReplaceResult result = re.globalReplace(text, Slices.wrappedBuffer(new byte[0]));
        assertThat(result.count()).isEqualTo(3);
        // After removing all matches of \xa5\xd1 and \xa5\x64, only "ac" (0x61 0x63) should remain
        assertThat(toByteArray(result.result())).isEqualTo(new byte[] {(byte) 0x61, (byte) 0x63});
    }

    @Test
    public void testOnePassPriorityMatchPreservesCaptures()
    {
        int parseFlags = (Regexp.LIKE_PERL & ~Regexp.ONE_LINE) | Regexp.NON_GREEDY;
        Re2 regexp = Re2TestAccess.compile(utf8("(?:(b)(?:(b)?))"), parseFlags);

        int[] groups = new int[6];
        assertThat(regexp.matchInto(utf8("bbbbbcac"), 0, 7, Re2.Anchor.ANCHOR_START, groups)).isTrue();
        assertThat(groups).containsExactly(0, 1, 0, 1, -1, -1);
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Slice latin1(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(ISO_8859_1));
    }

    private static String toString(byte[] value)
    {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static String toString(Slice slice)
    {
        return slice.toStringUtf8();
    }

    private static byte[] toByteArray(Slice slice)
    {
        return slice.getBytes();
    }
}
