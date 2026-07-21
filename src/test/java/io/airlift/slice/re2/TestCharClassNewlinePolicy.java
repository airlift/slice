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

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCharClassNewlinePolicy
{
    @Test
    public void testNegatedCharClassExcludesNewlineWhenClassNlIsOff()
    {
        int flags = Regexp.PERL_EXTENSIONS | Regexp.LATIN1;
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer("[^ab]".getBytes(StandardCharsets.UTF_8)), flags);

        assertThat(RegexpDump.dump(parsed.regexp())).isEqualTo("cc{0-0x9 0xb-0x60 0x63-0xff}");
    }

    @Test
    public void testNegatedCharClassIncludesNewlineWhenClassNlIsOn()
    {
        int flags = Regexp.PERL_EXTENSIONS | Regexp.LATIN1 | Regexp.CLASS_NEWLINE;
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer("[^ab]".getBytes(StandardCharsets.UTF_8)), flags);

        assertThat(RegexpDump.dump(parsed.regexp())).isEqualTo("cc{0-0x60 0x63-0xff}");
    }
}
