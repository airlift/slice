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

// Adapted from upstream RE2: re2/testing/re2_test.cc and re2/testing/re2_arg_test.cc.
public class TestRe2NumericParsing
{
    @Test
    public void testHexParsing()
    {
        Re2 hex = Re2.compile(utf8("([0-9a-fA-F]+)[uUlL]*"), Re2.Options.defaults());
        Re2 cRadix = Re2.compile(utf8("([0-9a-fA-FxX]+)[uUlL]*"), Re2.Options.defaults());

        assertThat(fullMatchResult("dead", hex).parseInt(1, 16)).isEqualTo((int) Long.parseLong("dead", 16));
        assertThat(fullMatchResult("0xdead", cRadix).parseInt(1, 0)).isEqualTo((int) Long.parseLong("dead", 16));

        assertThat(fullMatchResult("deadU", hex).parseUnsignedInt(1, 16)).isEqualTo(Long.parseUnsignedLong("dead", 16));
        assertThat(fullMatchResult("0xdeadU", cRadix).parseUnsignedInt(1, 0)).isEqualTo(Long.parseUnsignedLong("dead", 16));

        assertThat(fullMatchResult("7eadbeefL", hex).parseLong(1, 16)).isEqualTo(Long.parseLong("7eadbeef", 16));
        assertThat(fullMatchResult("0x7eadbeefL", cRadix).parseLong(1, 0)).isEqualTo(Long.parseLong("7eadbeef", 16));
        assertThat(fullMatchResult("12345678deadbeefLL", hex).parseLong(1, 16)).isEqualTo(Long.parseLong("12345678deadbeef", 16));
        assertThat(fullMatchResult("0x12345678deadbeefLL", cRadix).parseLong(1, 0)).isEqualTo(Long.parseLong("12345678deadbeef", 16));

        assertThat(fullMatchResult("cafebabedeadbeefULL", hex).parseUnsignedLong(1, 16)).isEqualTo(Long.parseUnsignedLong("cafebabedeadbeef", 16));
        assertThat(fullMatchResult("0xcafebabedeadbeefULL", cRadix).parseUnsignedLong(1, 0)).isEqualTo(Long.parseUnsignedLong("cafebabedeadbeef", 16));
    }

    @Test
    public void testOctalParsing()
    {
        Re2 octal = Re2.compile(utf8("([0-7]+)[uUlL]*"), Re2.Options.defaults());
        Re2 cRadix = Re2.compile(utf8("([0-9a-fA-FxX]+)[uUlL]*"), Re2.Options.defaults());

        assertThat(fullMatchResult("77777", octal).parseInt(1, 8)).isEqualTo((int) Long.parseLong("77777", 8));
        assertThat(fullMatchResult("077777", cRadix).parseInt(1, 0)).isEqualTo((int) Long.parseLong("77777", 8));

        assertThat(fullMatchResult("37777777777U", octal).parseUnsignedInt(1, 8)).isEqualTo(Long.parseUnsignedLong("37777777777", 8));
        assertThat(fullMatchResult("037777777777U", cRadix).parseUnsignedInt(1, 0)).isEqualTo(Long.parseUnsignedLong("37777777777", 8));

        assertThat(fullMatchResult("777777777777777777777LL", octal).parseLong(1, 8)).isEqualTo(Long.MAX_VALUE);
        assertThat(fullMatchResult("0777777777777777777777LL", cRadix).parseLong(1, 0)).isEqualTo(Long.MAX_VALUE);

        assertThat(fullMatchResult("1777777777777777777777ULL", octal).parseUnsignedLong(1, 8)).isEqualTo(-1L);
        assertThat(fullMatchResult("01777777777777777777777ULL", cRadix).parseUnsignedLong(1, 0)).isEqualTo(-1L);
    }

    @Test
    public void testDecimalParsing()
    {
        Re2 decimal = Re2.compile(utf8("(-?[0-9]+)[uUlL]*"), Re2.Options.defaults());
        Re2 cRadix = Re2.compile(utf8("(-?[0-9a-fA-FxX]+)[uUlL]*"), Re2.Options.defaults());

        assertThat(fullMatchResult("-1", decimal).parseInt(1)).isEqualTo(-1);
        assertThat(fullMatchResult("-1", cRadix).parseInt(1, 0)).isEqualTo(-1);

        assertThat(fullMatchResult("12345U", decimal).parseUnsignedInt(1)).isEqualTo(12345L);
        assertThat(fullMatchResult("12345U", cRadix).parseUnsignedInt(1, 0)).isEqualTo(12345L);

        assertThat(fullMatchResult("-10000000L", decimal).parseLong(1)).isEqualTo(-10_000_000L);
        assertThat(fullMatchResult("-10000000L", cRadix).parseLong(1, 0)).isEqualTo(-10_000_000L);
        assertThat(fullMatchResult("-100000000000000LL", decimal).parseLong(1)).isEqualTo(-100_000_000_000_000L);
        assertThat(fullMatchResult("-100000000000000LL", cRadix).parseLong(1, 0)).isEqualTo(-100_000_000_000_000L);

        assertThat(fullMatchResult("1234567890987654321ULL", decimal).parseUnsignedLong(1)).isEqualTo(1234567890987654321L);
        assertThat(fullMatchResult("1234567890987654321ULL", cRadix).parseUnsignedLong(1, 0)).isEqualTo(1234567890987654321L);
    }

    @Test
    public void testParsingStopsAtLogicalSliceBoundary()
    {
        Slice input = Slices.wrappedBuffer(new byte[] {'1', 'x'}).slice(0, 1);
        MatchResult result = Re2.compile(utf8("(.*)")).fullMatchResult(input);

        assertThat(result).isNotNull();
        assertThat(result.parseInt(1)).isEqualTo(1);
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
