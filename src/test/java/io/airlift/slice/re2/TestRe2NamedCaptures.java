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
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2NamedCaptures
{
    @Test
    public void testNamedGroups()
    {
        Re2 re = Re2.compile(utf8("(hello world)"), Re2.Options.defaults());
        assertThat(re.capturingGroupCount()).isEqualTo(1);
        assertThat(re.namedCapturingGroups()).isEmpty();

        Re2 named = Re2.compile(utf8("(?P<A>expr(?P<B>expr)(?P<C>expr))((expr)(?P<D>expr))"), Re2.Options.defaults());
        assertThat(named.capturingGroupCount()).isEqualTo(6);
        Map<String, Integer> groups = named.namedCapturingGroups();
        assertThat(groups).hasSize(4);
        assertThat(groups.get("A")).isEqualTo(1);
        assertThat(groups.get("B")).isEqualTo(2);
        assertThat(groups.get("C")).isEqualTo(3);
        assertThat(groups.get("D")).isEqualTo(6);
    }

    @Test
    public void testCapturedGroupTest()
    {
        Re2 re = Re2.compile(utf8("directions from (?P<S>.*) to (?P<D>.*)"), Re2.Options.defaults());
        int capturingGroupCount = re.capturingGroupCount();
        assertThat(capturingGroupCount).isEqualTo(2);

        MatchResult match = re.fullMatchResult(utf8("directions from mountain view to san jose"));
        assertThat(match).isNotNull();

        Map<String, Integer> namedGroups = re.namedCapturingGroups();
        assertThat(namedGroups.containsKey("S")).isTrue();
        assertThat(namedGroups.containsKey("D")).isTrue();

        int sourceIndex = namedGroups.get("S");
        int destinationIndex = namedGroups.get("D");
        assertThat(sourceIndex).isEqualTo(1);
        assertThat(destinationIndex).isEqualTo(2);

        assertThat(match.groupUtf8(sourceIndex)).isEqualTo("mountain view");
        assertThat(match.groupUtf8(destinationIndex)).isEqualTo("san jose");
        assertThat(match.groupUtf8("S")).isEqualTo("mountain view");
        assertThat(match.groupUtf8("D")).isEqualTo("san jose");
    }

    @Test
    public void testCapturingGroupNames()
    {
        Re2 re = Re2.compile(utf8("((abc)(?P<G2>)|((e+)(?P<G2>.*)(?P<G1>u+)))"), Re2.Options.defaults());

        Map<Integer, String> have = re.capturingGroupNames();
        Map<Integer, String> want = new HashMap<>();
        want.put(3, "G2");
        want.put(6, "G2");
        want.put(7, "G1");

        assertThat(have).isEqualTo(want);
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
