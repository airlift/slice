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

import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Adapted from upstream RE2: re2/parse.cc and re2/testing/search_test.cc.
public class TestUpstreamParseEscape
{
    @Test
    public void testOctalEscapes()
    {
        parse("\\141", Regexp.LIKE_PERL); // 'a'
        parse("\\060", Regexp.LIKE_PERL); // '0'
        parse("\\0600", Regexp.LIKE_PERL); // '0' + '0'
        parse("\\608", Regexp.LIKE_PERL); // '0' + '8'
        parse("\\01", Regexp.LIKE_PERL); // byte 1
        parse("\\018", Regexp.LIKE_PERL); // byte 1 + '8'

        // Single non-zero octal digit would be a backreference (unsupported), so it's a bad escape.
        assertBadEscape("\\1", Regexp.LIKE_PERL);
        assertBadEscape("\\1a", Regexp.LIKE_PERL);

        // \400 is 256 in octal. This is allowed in UTF-8 mode but rejected in Latin-1 mode.
        parse("\\400", Regexp.LIKE_PERL);
        assertBadEscape("\\400", Regexp.LIKE_PERL | Regexp.LATIN1);
    }

    @Test
    public void testHexEscapes()
    {
        parse("\\x61", Regexp.LIKE_PERL); // 'a'
        parse("\\x{61}", Regexp.LIKE_PERL); // 'a'
        parse("\\x{00000061}", Regexp.LIKE_PERL); // 'a'

        // Reject values above the rune maximum for the configured encoding.
        assertBadEscape("\\x{110000}", Regexp.LIKE_PERL);
        assertBadEscape("\\x{100}", Regexp.LIKE_PERL | Regexp.LATIN1);
    }

    private static void assertBadEscape(String pattern, int flags)
    {
        assertThatThrownBy(() -> parse(pattern, flags))
                .isInstanceOfSatisfying(RegexpParseException.class, parseException ->
                        assertThat(parseException.statusCode()).isEqualTo(RegexpStatusCode.BAD_ESCAPE));
    }

    private static ParseResult parse(String pattern, int flags)
    {
        return RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), flags);
    }
}
