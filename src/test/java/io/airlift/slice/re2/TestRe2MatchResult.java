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

public class TestRe2MatchResult
{
    @Test
    public void testResultPresence()
    {
        Re2 re = Re2.compile(utf8("(foo)"), Re2.Options.defaults());

        assertThat(re.fullMatchResult(utf8("foo"))).isNotNull();
        assertThat(re.fullMatchResult(utf8("bar"))).isNull();
        assertThat(re.partialMatchResult(utf8("xfoo"))).isNotNull();
    }

    @Test
    public void testGroupByIndexAndName()
    {
        Re2 re = Re2.compile(utf8("(?P<word>\\w+)-(?P<num>\\d+)"), Re2.Options.defaults());
        MatchResult result = re.fullMatchResult(utf8("abc-42"));

        assertThat(result).isNotNull();
        assertThat(result.groupCount()).isEqualTo(2);
        assertThat(result.groupUtf8(1)).isEqualTo("abc");
        assertThat(result.groupUtf8(2)).isEqualTo("42");
        assertThat(result.groupUtf8("word")).isEqualTo("abc");
        assertThat(result.groupUtf8("num")).isEqualTo("42");

        assertThatThrownBy(() -> result.groupUtf8("missing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown named group");
    }

    @Test
    public void testNumericParsing()
    {
        Re2 re = Re2.compile(utf8("([0-9a-fA-FxX]+)"), Re2.Options.defaults());

        MatchResult decimal = re.fullMatchResult(utf8("123"));
        assertThat(decimal).isNotNull();
        assertThat(decimal.parseInt(1)).isEqualTo(123);
        assertThat(decimal.parseLong(1)).isEqualTo(123L);
        assertThat(decimal.parseUnsignedInt(1)).isEqualTo(123L);
        assertThat(decimal.parseUnsignedLong(1)).isEqualTo(123L);

        MatchResult hex = re.fullMatchResult(utf8("0xff"));
        assertThat(hex).isNotNull();
        assertThat(hex.parseInt(1, 0)).isEqualTo(255);
        assertThat(hex.parseUnsignedLong(1, 0)).isEqualTo(255L);

        MatchResult overflow = re.fullMatchResult(utf8("999999999999999999999999"));
        assertThat(overflow).isNotNull();
        assertThatThrownBy(() -> overflow.parseLong(1)).isInstanceOf(NumberFormatException.class);
    }

    @Test
    public void testConvenienceOverloads()
    {
        Re2 re = Re2.compile(utf8("(foo)"), Re2.Options.defaults());

        byte[] bytes = "foo".getBytes(StandardCharsets.UTF_8);
        MatchResult byteResult = re.fullMatchResult(Slices.wrappedBuffer(bytes, 0, bytes.length));
        assertThat(byteResult).isNotNull();
        assertThat(byteResult.groupUtf8(1)).isEqualTo("foo");

        MatchResult sliceResult = re.fullMatchResult(Slices.utf8Slice("foo"));
        assertThat(sliceResult).isNotNull();
        assertThat(sliceResult.groupUtf8(1)).isEqualTo("foo");
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
