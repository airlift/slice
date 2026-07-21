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

// Adapted from upstream RE2: re2/testing/re2_test.cc and re2/testing/re2_arg_test.cc.
public class TestRe2FullMatchTypes
{
    @Test
    public void testSignedIntParsing()
    {
        Re2 re = Re2.compile(utf8("(-?\\d+)"), Re2.Options.defaults());
        String zeros = "0".repeat(1_000);

        assertThat(fullMatchResult("100", re).parseInt(1)).isEqualTo(100);
        assertThat(fullMatchResult("-100", re).parseInt(1)).isEqualTo(-100);
        assertThat(fullMatchResult("2147483647", re).parseInt(1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(fullMatchResult("-2147483648", re).parseInt(1)).isEqualTo(Integer.MIN_VALUE);

        assertThatThrownBy(() -> fullMatchResult("2147483648", re).parseInt(1)).isInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> fullMatchResult("-2147483649", re).parseInt(1)).isInstanceOf(NumberFormatException.class);

        assertThat(fullMatchResult(zeros + "2147483647", re).parseInt(1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(fullMatchResult("-" + zeros + "2147483648", re).parseInt(1)).isEqualTo(Integer.MIN_VALUE);
        assertThatThrownBy(() -> fullMatchResult("-" + zeros + "2147483649", re).parseInt(1))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    public void testUnsignedIntParsing()
    {
        Re2 re = Re2.compile(utf8("(\\d+)"), Re2.Options.defaults());
        String zeros = "0".repeat(1_000);

        assertThat(fullMatchResult("100", re).parseUnsignedInt(1)).isEqualTo(100L);
        assertThat(fullMatchResult("4294967295", re).parseUnsignedInt(1)).isEqualTo(0xFFFF_FFFFL);

        assertThatThrownBy(() -> fullMatchResult("4294967296", re).parseUnsignedInt(1)).isInstanceOf(NumberFormatException.class);
        assertThat(fullMatchResult(zeros + "4294967295", re).parseUnsignedInt(1)).isEqualTo(0xFFFF_FFFFL);
    }

    @Test
    public void testSignedLongParsing()
    {
        Re2 re = Re2.compile(utf8("(-?\\d+)"), Re2.Options.defaults());

        assertThat(fullMatchResult("100", re).parseLong(1)).isEqualTo(100L);
        assertThat(fullMatchResult("-100", re).parseLong(1)).isEqualTo(-100L);
        assertThat(fullMatchResult(String.valueOf(Long.MAX_VALUE), re).parseLong(1)).isEqualTo(Long.MAX_VALUE);
        assertThat(fullMatchResult(String.valueOf(Long.MIN_VALUE), re).parseLong(1)).isEqualTo(Long.MIN_VALUE);

        assertThatThrownBy(() -> fullMatchResult("9223372036854775808", re).parseLong(1)).isInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> fullMatchResult("-9223372036854775809", re).parseLong(1)).isInstanceOf(NumberFormatException.class);
    }

    @Test
    public void testUnsignedLongParsing()
    {
        Re2 re = Re2.compile(utf8("(\\d+)"), Re2.Options.defaults());

        String maxUnsigned = Long.toUnsignedString(-1L);
        assertThat(fullMatchResult(maxUnsigned, re).parseUnsignedLong(1)).isEqualTo(-1L);

        assertThatThrownBy(() -> fullMatchResult("18446744073709551616", re).parseUnsignedLong(1)).isInstanceOf(NumberFormatException.class);
    }

    @Test
    public void testRadixParsing()
    {
        Re2 re = Re2.compile(utf8("([0-9a-fA-FxX]+)"), Re2.Options.defaults());

        assertThat(fullMatchResult("0x7fffffff", re).parseInt(1, 0)).isEqualTo(Integer.MAX_VALUE);
        assertThat(fullMatchResult("777", re).parseInt(1, 8)).isEqualTo(511);

        assertThatThrownBy(() -> fullMatchResult("000x7fffffff", re).parseInt(1, 0)).isInstanceOf(NumberFormatException.class);
    }

    @Test
    public void testFullMatchAnchored()
    {
        Re2 re = Re2.compile(utf8("(\\d+)"), Re2.Options.defaults());
        Re2 reX = Re2.compile(utf8("x(\\d+)"), Re2.Options.defaults());
        Re2 reXEnd = Re2.compile(utf8("(\\d+)x"), Re2.Options.defaults());

        assertThat(re.fullMatch(utf8("x1001"))).isFalse();
        assertThat(re.fullMatch(utf8("1001x"))).isFalse();

        assertThat(fullMatchResult("x1001", reX).parseInt(1)).isEqualTo(1001);
        assertThat(fullMatchResult("1001x", reXEnd).parseInt(1)).isEqualTo(1001);
    }

    @Test
    public void testFullMatchBraces()
    {
        Re2 re = Re2.compile(utf8("[0-9a-f+.-]{5,}"), Re2.Options.defaults());

        assertThat(re.fullMatch(utf8("0abcd"))).isTrue();
        assertThat(re.fullMatch(utf8("0abcde"))).isTrue();
        assertThat(re.fullMatch(utf8("0abc"))).isFalse();
    }

    @Test
    public void testFullMatchComplicated()
    {
        Re2 re = Re2.compile(utf8("foo|bar|[A-Z]"), Re2.Options.defaults());

        assertThat(re.fullMatch(utf8("foo"))).isTrue();
        assertThat(re.fullMatch(utf8("bar"))).isTrue();
        assertThat(re.fullMatch(utf8("X"))).isTrue();
        assertThat(re.fullMatch(utf8("XY"))).isFalse();
    }

    private static MatchResult fullMatchResult(String text, Re2 re)
    {
        MatchResult result = re.fullMatchResult(utf8(text));
        assertThat(result).isNotNull();
        return result;
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
