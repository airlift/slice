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
public class TestRe2CaptureParsing
{
    @Test
    public void testParseIntCaptures()
    {
        MatchResult positive = Re2.compile(utf8("(\\d+)"), Re2.Options.defaults()).fullMatchResult(utf8("1001"));
        assertThat(positive).isNotNull();
        assertThat(positive.parseInt(1)).isEqualTo(1001);

        MatchResult negative = Re2.compile(utf8("(-?\\d+)"), Re2.Options.defaults()).fullMatchResult(utf8("-123"));
        assertThat(negative).isNotNull();
        assertThat(negative.parseInt(1)).isEqualTo(-123);
    }

    @Test
    public void testParseIntFailures()
    {
        MatchResult emptyCapture = Re2.compile(utf8("()\\d+"), Re2.Options.defaults()).fullMatchResult(utf8("10"));
        assertThat(emptyCapture).isNotNull();
        assertThatThrownBy(() -> emptyCapture.parseInt(1)).isInstanceOf(NumberFormatException.class);

        MatchResult overflow = Re2.compile(utf8("(\\d+)"), Re2.Options.defaults()).fullMatchResult(utf8("1234567890123456789012345678901234567890"));
        assertThat(overflow).isNotNull();
        assertThatThrownBy(() -> overflow.parseInt(1)).isInstanceOf(NumberFormatException.class);
    }

    @Test
    public void testCaptureContent()
    {
        MatchResult text = Re2.compile(utf8("h(.*)o"), Re2.Options.defaults()).fullMatchResult(utf8("hello"));
        assertThat(text).isNotNull();
        assertThat(text.groupUtf8(1)).isEqualTo("ell");

        MatchResult pair = Re2.compile(utf8("(\\w+):(\\d+)"), Re2.Options.defaults()).fullMatchResult(utf8("ruby:1234"));
        assertThat(pair).isNotNull();
        assertThat(pair.groupUtf8(1)).isEqualTo("ruby");
        assertThat(pair.parseInt(2)).isEqualTo(1234);
    }

    @Test
    public void testUnmatchedCaptureIsNull()
    {
        Re2 re = Re2.compile(utf8("(foo)|hello"), Re2.Options.defaults());
        MatchResult result = re.fullMatchResult(utf8("hello"));
        assertThat(result).isNotNull();
        assertThat(result.groupSlice(1)).isNull();
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
