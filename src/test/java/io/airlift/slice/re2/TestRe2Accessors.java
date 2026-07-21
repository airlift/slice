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

// Includes tests ported from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2Accessors
{
    @Test
    public void testPatternAccessor()
    {
        String pattern = "http://([^/]+)/.*";
        Re2 re = Re2.compile(utf8(pattern), Re2.Options.defaults());
        assertThat(re.pattern().toStringUtf8()).isEqualTo(pattern);
    }

    @Test
    public void testInvalidPatternThrows()
    {
        Re2 ok = Re2.compile(utf8("foo"), Re2.Options.defaults());
        assertThat(ok).isNotNull();

        assertThatThrownBy(() -> Re2.compile(utf8("("), Re2.Options.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
