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

// Ported from upstream RE2: re2/testing/parse_test.cc.
public class TestUpstreamSyntaxModes
{
    @Test
    public void testOnlyPerlFromUpstreamParseTest()
    {
        // Sourced from upstream re2/testing/parse_test.cc only_perl[].
        String[] onlyPerl = {
                "[a-b-c]",
                "\\Qabc\\E",
                "\\Q*+?{[\\E",
                "\\Q\\\\E",
                "\\Q\\\\\\E",
                "\\Q\\\\\\\\E",
                "\\Q\\\\\\\\\\E",
                "(?:a)",
                "(?P<name>a)",
                "(?<name>a)",
        };

        for (String pattern : onlyPerl) {
            assertThatThrownBy(() -> parse(pattern, 0)).as("NoParseFlags should reject: %s", pattern).isInstanceOf(RegexpParseException.class);
            assertThat(parse(pattern, Regexp.PERL_EXTENSIONS)).as("Perl extensions should accept: %s", pattern).isNotNull();
        }
    }

    @Test
    public void testOnlyPosixFromUpstreamParseTest()
    {
        // Sourced from upstream re2/testing/parse_test.cc only_posix[].
        String[] onlyPosix = {
                "a++",
                "a**",
                "a?*",
                "a+*",
                "a{1}*",
        };

        for (String pattern : onlyPosix) {
            assertThatThrownBy(() -> parse(pattern, Regexp.PERL_EXTENSIONS)).as("Perl extensions should reject: %s", pattern).isInstanceOf(RegexpParseException.class);
            assertThat(parse(pattern, 0)).as("NoParseFlags should accept: %s", pattern).isNotNull();
        }
    }

    private static ParseResult parse(String pattern, int flags)
    {
        Slice pat = Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8));
        return RegexpParser.parse(pat, flags);
    }
}
