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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Ported from upstream RE2: re2/testing/parse_test.cc.
public class TestUpstreamBadRegexps
{
    private sealed interface Pattern permits Utf8Pattern, RawBytesPattern
    {
        Slice bytes();

        String debugString();
    }

    private record Utf8Pattern(String pattern) implements Pattern
    {
        @Override
        public Slice bytes()
        {
            return Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String debugString()
        {
            return pattern;
        }
    }

    private record RawBytesPattern(byte[] rawBytes, String debugString) implements Pattern
    {
        @Override
        public Slice bytes()
        {
            return Slices.wrappedBuffer(rawBytes);
        }
    }

    @Test
    public void testUpstreamBadtestsParseAsInvalid()
    {
        // Sourced from upstream re2/testing/parse_test.cc badtests[].
        List<Pattern> bad = List.of(
                new Utf8Pattern("("),
                new Utf8Pattern(")"),
                new Utf8Pattern("(a"),
                new Utf8Pattern("(a|b|"),
                new Utf8Pattern("(a|b"),
                new Utf8Pattern("[a-z"),
                new Utf8Pattern("([a-z)"),
                new Utf8Pattern("x{1001}"),
                rawByte((byte) 0xFF),
                rawBytes(new byte[] {'[', (byte) 0xFF, ']'}, "[\\xff]"),
                rawBytes(new byte[] {'[', '\\', (byte) 0xFF, ']'}, "[\\\\\\xff]"),
                rawBytes(new byte[] {'\\', (byte) 0xFF}, "\\\\xff"),
                new Utf8Pattern("(?P<name>a"),
                new Utf8Pattern("(?P<name>"),
                new Utf8Pattern("(?P<name"),
                new Utf8Pattern("(?P<x y>a)"),
                new Utf8Pattern("(?P<>a)"),
                new Utf8Pattern("(?<name>a"),
                new Utf8Pattern("(?<name>"),
                new Utf8Pattern("(?<name"),
                new Utf8Pattern("(?<x y>a)"),
                new Utf8Pattern("(?<>a)"),
                new Utf8Pattern("[a-Z]"),
                new Utf8Pattern("(?i)[a-Z]"),
                new Utf8Pattern("a{100000}"),
                new Utf8Pattern("a{100000,}"),
                new Utf8Pattern("((((((((((x{2}){2}){2}){2}){2}){2}){2}){2}){2}){2})"),
                new Utf8Pattern("(((x{7}){11}){13})"),
                new Utf8Pattern("\\Q\\E*"));

        for (Pattern p : bad) {
            assertInvalid(p, Regexp.PERL_EXTENSIONS);
            assertInvalid(p, 0);
        }
    }

    private static RawBytesPattern rawByte(byte b)
    {
        return new RawBytesPattern(new byte[] {b}, "\\xff");
    }

    private static RawBytesPattern rawBytes(byte[] b, String debug)
    {
        return new RawBytesPattern(b, debug);
    }

    private static void assertInvalid(Pattern pattern, int flags)
    {
        assertThatThrownBy(() -> RegexpParser.parse(pattern.bytes(), flags))
                .as("expected invalid pattern %s with flags 0x%x", pattern.debugString(), flags)
                .isInstanceOf(RegexpParseException.class);
    }
}
