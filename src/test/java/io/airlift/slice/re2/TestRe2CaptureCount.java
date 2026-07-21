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

// Adapted from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2CaptureCount
{
    @Test
    public void testFullMatchCaptureCount()
    {
        assertThat(Re2.compile(utf8(""), Re2.Options.defaults()).fullMatch(utf8(""))).isTrue();

        assertDigitsMatch("1", 1);
        assertDigitsMatch("12", 2);
        assertDigitsMatch("123", 3);
        assertDigitsMatch("1234", 4);
        assertDigitsMatch("12345", 5);
        assertDigitsMatch("123456", 6);
        assertDigitsMatch("1234567", 7);
        assertDigitsMatch("1234567890123456", 16);
    }

    private static void assertDigitsMatch(String text, int count)
    {
        StringBuilder pattern = new StringBuilder();
        for (int i = 0; i < count; i++) {
            pattern.append("(\\d)");
        }

        Re2 re = Re2.compile(utf8(pattern.toString()), Re2.Options.defaults());
        int[] groups = new int[2 * (count + 1)];

        assertThat(re.matchInto(utf8(text), Re2.Anchor.ANCHOR_BOTH, groups)).isTrue();
        for (int i = 0; i < count; i++) {
            int expected = text.charAt(i) - '0';
            int captureStart = groups[(i + 1) * 2];
            int captureEnd = groups[(i + 1) * 2 + 1];
            assertThat(captureEnd - captureStart).isEqualTo(1);
            assertThat(text.charAt(captureStart) - '0').isEqualTo(expected);
        }
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
