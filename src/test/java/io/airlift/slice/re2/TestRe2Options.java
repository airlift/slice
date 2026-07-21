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

// Includes tests ported from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2Options
{
    @Test
    public void testDefaultMemoryBudget()
    {
        assertThat(Re2.Options.defaults().maxMemory()).isEqualTo(96L << 20);
    }

    @Test
    public void testNeverNewline()
    {
        String[] regexps = {
                "(.*)",
                "(?s)(abc.*def)",
                "(abc(.|\\n)*def)",
                "(abc[^x]*def)",
                "(abc[^x]*def)"
        };
        String[] texts = {
                "abc\ndef\nghi\n",
                "abc\ndef\n",
                "abc\ndef\n",
                "abc\ndef\n",
                "abczzzdef\ndef\n"
        };
        String[] matches = {
                "abc",
                null,
                null,
                null,
                "abczzzdef"
        };

        Re2.Options options = Re2.Options.defaults()
                .setNeverNewline(true);

        for (int i = 0; i < regexps.length; i++) {
            Re2 re = Re2.compile(utf8(regexps[i]), options);
            if (matches[i] == null) {
                assertThat(re.partialMatch(utf8(texts[i]))).isFalse();
            }
            else {
                MatchResult result = re.partialMatchResult(utf8(texts[i]));
                assertThat(result).isNotNull();
                assertThat(result.groupUtf8(1)).isEqualTo(matches[i]);
            }
        }
    }

    @Test
    public void testDotNl()
    {
        Re2.Options options = Re2.Options.defaults()
                .setDotMatchesNewline(true);
        assertThat(Re2.compile(utf8("."), options).partialMatch(utf8("\n"))).isTrue();
        assertThat(Re2.compile(utf8("(?-s)."), options).partialMatch(utf8("\n"))).isFalse();

        options.setNeverNewline(true);
        assertThat(Re2.compile(utf8("."), options).partialMatch(utf8("\n"))).isFalse();
    }

    @Test
    public void testNeverCapture()
    {
        Re2 re = Re2.compile(utf8("(r)(e)"), Re2.Options.defaults().setNeverCapture(true));
        assertThat(re.capturingGroupCount()).isEqualTo(0);
    }

    @Test
    public void testBitstateCaptureBug()
    {
        Re2 re = Re2.compile(utf8("(_________$)"), Re2.Options.defaults().setMaxMemory(20000));
        Slice text = utf8("xxxxxxxxxxxxxxxxxxxxxxxxxx_________x");
        assertThat(re.matchInto(text, Re2.Anchor.UNANCHORED, null)).isFalse();
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
