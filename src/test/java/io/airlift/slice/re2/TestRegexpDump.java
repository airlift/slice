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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRegexpDump
{
    @Test
    public void testDumpMatchesUpstreamStyle()
    {
        Regexp re = Regexp.concat(0, List.of(
                Regexp.literal(0, 'a'),
                Regexp.star(0, Regexp.literal(0, 'b'))));

        String dump = RegexpDump.dump(re);
        assertThat(dump).isEqualTo("cat{lit{a}star{lit{b}}}");
    }
}
