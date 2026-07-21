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

public class TestUpstreamCompileAnchorFlags
{
    private static Regexp parse(String pattern, int flags)
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), flags);
        return parsed.regexp();
    }

    @Test
    public void testBeginEndTextAnchorsAreRecordedAndRemoved()
    {
        // In LikePerl mode, ^ and $ are BEGIN_TEXT/END_TEXT (one-line mode).
        Regexp re = parse("^a$", Regexp.LIKE_PERL | Regexp.LATIN1);

        Prog prog = Compiler.compile(re, false, 0);
        assertThat(prog).isNotNull();
        assertThat(prog.anchorStart()).isTrue();
        assertThat(prog.anchorEnd()).isTrue();
        assertThat(prog.startUnanchored()).isEqualTo(prog.start());
    }

    @Test
    public void testReverseProgramSwapsAnchorFlags()
    {
        Regexp re = parse("^a", Regexp.LIKE_PERL | Regexp.LATIN1);

        Prog forward = Compiler.compile(re, false, 0);
        assertThat(forward).isNotNull();
        assertThat(forward.reversed()).isFalse();
        assertThat(forward.anchorStart()).isTrue();
        assertThat(forward.anchorEnd()).isFalse();

        Prog reverse = Compiler.compile(re, true, 0);
        assertThat(reverse).isNotNull();
        assertThat(reverse.reversed()).isTrue();
        assertThat(reverse.anchorStart()).isFalse();
        assertThat(reverse.anchorEnd()).isTrue();
    }
}
