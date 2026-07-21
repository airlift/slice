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

// Ported from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2UnicodeClasses
{
    @Test
    public void testUnicodeClasses()
    {
        String han = "\u8b5a";
        String yong = "\u6c38";
        String feng = "\u92b0";
        String str = "ABCDEFGHI" + han + yong + feng;

        assertThat(fullMatch("A", "\\p{L}")).isTrue();
        assertThat(fullMatch("A", "\\p{Lu}")).isTrue();
        assertThat(fullMatch("A", "\\p{Ll}")).isFalse();
        assertThat(fullMatch("A", "\\P{L}")).isFalse();
        assertThat(fullMatch("A", "\\P{Lu}")).isFalse();
        assertThat(fullMatch("A", "\\P{Ll}")).isTrue();

        assertThat(fullMatch(han, "\\p{L}")).isTrue();
        assertThat(fullMatch(han, "\\p{Lu}")).isFalse();
        assertThat(fullMatch(han, "\\p{Ll}")).isFalse();
        assertThat(fullMatch(han, "\\P{L}")).isFalse();
        assertThat(fullMatch(han, "\\P{Lu}")).isTrue();
        assertThat(fullMatch(han, "\\P{Ll}")).isTrue();

        assertThat(fullMatch(yong, "\\p{L}")).isTrue();
        assertThat(fullMatch(yong, "\\p{Lu}")).isFalse();
        assertThat(fullMatch(yong, "\\p{Ll}")).isFalse();
        assertThat(fullMatch(yong, "\\P{L}")).isFalse();
        assertThat(fullMatch(yong, "\\P{Lu}")).isTrue();
        assertThat(fullMatch(yong, "\\P{Ll}")).isTrue();

        assertThat(fullMatch(feng, "\\p{L}")).isTrue();
        assertThat(fullMatch(feng, "\\p{Lu}")).isFalse();
        assertThat(fullMatch(feng, "\\p{Ll}")).isFalse();
        assertThat(fullMatch(feng, "\\P{L}")).isFalse();
        assertThat(fullMatch(feng, "\\P{Lu}")).isTrue();
        assertThat(fullMatch(feng, "\\P{Ll}")).isTrue();

        Re2 re = Re2.compile(utf8("(.).*?(.).*?(.)"), Re2.Options.defaults());
        MatchResult result = re.partialMatchResult(utf8(str));
        assertThat(result).isNotNull();
        assertThat(result.groupUtf8(1)).isEqualTo("A");
        assertThat(result.groupUtf8(2)).isEqualTo("B");
        assertThat(result.groupUtf8(3)).isEqualTo("C");

        Re2 reWithClass = Re2.compile(utf8("(.).*?([\\p{L}]).*?(.)"), Re2.Options.defaults());
        MatchResult classResult = reWithClass.partialMatchResult(utf8(str));
        assertThat(classResult).isNotNull();
        assertThat(classResult.groupUtf8(1)).isEqualTo("A");
        assertThat(classResult.groupUtf8(2)).isEqualTo("B");
        assertThat(classResult.groupUtf8(3)).isEqualTo("C");

        assertThat(Re2.compile(utf8("\\P{L}"), Re2.Options.defaults()).partialMatch(utf8(str))).isFalse();

        Re2 uppercase = Re2.compile(utf8("(.).*?([\\p{Lu}]).*?(.)"), Re2.Options.defaults());
        MatchResult uppercaseResult = uppercase.partialMatchResult(utf8(str));
        assertThat(uppercaseResult).isNotNull();
        assertThat(uppercaseResult.groupUtf8(1)).isEqualTo("A");
        assertThat(uppercaseResult.groupUtf8(2)).isEqualTo("B");
        assertThat(uppercaseResult.groupUtf8(3)).isEqualTo("C");

        assertThat(Re2.compile(utf8("[^\\p{Lu}\\p{Lo}]"), Re2.Options.defaults()).partialMatch(utf8(str))).isFalse();

        Re2 letters = Re2.compile(utf8(".*(.).*?([\\p{Lu}\\p{Lo}]).*?(.)"), Re2.Options.defaults());
        MatchResult lettersResult = letters.partialMatchResult(utf8(str));
        assertThat(lettersResult).isNotNull();
        assertThat(lettersResult.groupUtf8(1)).isEqualTo(han);
        assertThat(lettersResult.groupUtf8(2)).isEqualTo(yong);
        assertThat(lettersResult.groupUtf8(3)).isEqualTo(feng);
    }

    private static boolean fullMatch(String text, String pattern)
    {
        return Re2.compile(utf8(pattern), Re2.Options.defaults()).fullMatch(utf8(text));
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
