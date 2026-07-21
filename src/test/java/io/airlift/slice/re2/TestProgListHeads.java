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

public class TestProgListHeads
{
    @Test
    public void testListHeadsArePopulatedForSmallPrograms()
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer("a".getBytes(StandardCharsets.UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1);

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).isNotNull();
        assertThat(prog.didFlatten()).isTrue();
        assertThat(prog.canBitState()).isTrue();
        assertThat(prog.bitStateTextMaxSize()).isGreaterThan(0);

        short[] heads = prog.listHeads();
        assertThat(heads).isNotNull();
        assertThat(heads).hasSize(prog.size());

        // The FAIL instruction is always list 0.
        assertThat((int) heads[0]).isEqualTo(0);

        int su = prog.startUnanchored();
        int s = prog.start();
        assertThat(su).isNotZero();
        assertThat(s).isNotZero();

        int suHead = heads[su];
        int sHead = heads[s];
        assertThat(suHead).isGreaterThanOrEqualTo(0);
        assertThat(sHead).isGreaterThanOrEqualTo(0);
        assertThat(suHead).isNotEqualTo(sHead);
        assertThat(prog.listCount()).isGreaterThan(sHead);
        assertThat(prog.listCount()).isGreaterThan(suHead);
    }
}
