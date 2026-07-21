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

public class TestUpstreamIsOnePass
{
    private static Prog compile(String pattern, int flags)
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), flags);

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).as("compile: %s", pattern).isNotNull();
        return prog;
    }

    private static void assertIsOnePass(String pattern, boolean expected)
    {
        Prog prog = compile(pattern, Regexp.LIKE_PERL | Regexp.LATIN1);
        assertThat(prog.isOnePass()).as("isOnePass: %s", pattern).isEqualTo(expected);
    }

    @Test
    public void testOnePassExamplesFromUpstreamCommentary()
    {
        // Examples discussed in upstream re2/onepass.cc.
        assertIsOnePass("^x*yx*$", true);
        assertIsOnePass("^([^ ]*) (.*)$", true);
        assertIsOnePass("^(.*) (.*)$", false);
        assertIsOnePass("^x(y|z)$", true);
        assertIsOnePass("^(xy|xz)$", true);
    }
}
