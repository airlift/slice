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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Ported from upstream RE2: re2/testing/set_test.cc.
public class TestRe2Set
{
    private static Slice toSlice(String value)
    {
        return Slices.utf8Slice(value);
    }

    @Test
    public void testUnanchored()
    {
        Re2Set s = new Re2Set(Re2.Options.defaults(), Re2.Anchor.UNANCHORED);

        assertThat(s.size()).isEqualTo(0);
        assertThat(s.add(Slices.utf8Slice("foo"))).isEqualTo(0);
        assertThat(s.size()).isEqualTo(1);
        assertThatThrownBy(() -> s.add(Slices.utf8Slice("(")))
                .isInstanceOf(RegexpParseException.class);
        assertThat(s.size()).isEqualTo(1);
        assertThat(s.add(Slices.utf8Slice("bar"))).isEqualTo(1);
        assertThat(s.size()).isEqualTo(2);
        s.compile();
        assertThat(s.size()).isEqualTo(2);

        assertThat(s.match(toSlice("foobar"))).isTrue();
        assertThat(s.match(toSlice("fooba"))).isTrue();
        assertThat(s.match(toSlice("oobar"))).isTrue();

        List<Integer> v = new ArrayList<>();
        assertThat(s.match(toSlice("foobar"), v)).isTrue();
        assertThat(v).hasSize(2);
        assertThat(v).containsExactlyInAnyOrder(0, 1);

        assertThat(s.match(toSlice("fooba"), v)).isTrue();
        assertThat(v).hasSize(1);
        assertThat(v).containsExactly(0);

        assertThat(s.match(toSlice("oobar"), v)).isTrue();
        assertThat(v).hasSize(1);
        assertThat(v).containsExactly(1);
    }

    @Test
    public void testUnanchoredFactored()
    {
        Re2Set s = new Re2Set(Re2.Options.defaults(), Re2.Anchor.UNANCHORED);

        assertThat(s.add(Slices.utf8Slice("foo"))).isEqualTo(0);
        assertThatThrownBy(() -> s.add(Slices.utf8Slice("(")))
                .isInstanceOf(RegexpParseException.class);
        assertThat(s.add(Slices.utf8Slice("foobar"))).isEqualTo(1);
        s.compile();

        assertThat(s.match(toSlice("foobar"))).isTrue();
        assertThat(s.match(toSlice("obarfoobaroo"))).isTrue();
        assertThat(s.match(toSlice("fooba"))).isTrue();
        assertThat(s.match(toSlice("oobar"))).isFalse();

        List<Integer> v = new ArrayList<>();
        assertThat(s.match(toSlice("foobar"), v)).isTrue();
        assertThat(v).hasSize(2);
        assertThat(v).containsExactlyInAnyOrder(0, 1);

        assertThat(s.match(toSlice("obarfoobaroo"), v)).isTrue();
        assertThat(v).hasSize(2);
        assertThat(v).containsExactlyInAnyOrder(0, 1);

        assertThat(s.match(toSlice("fooba"), v)).isTrue();
        assertThat(v).hasSize(1);
        assertThat(v).containsExactly(0);

        assertThat(s.match(toSlice("oobar"), v)).isFalse();
        assertThat(v).isEmpty();
    }

    @Test
    public void testUnanchoredDollar()
    {
        Re2Set s = new Re2Set(Re2.Options.defaults(), Re2.Anchor.UNANCHORED);

        assertThat(s.add(Slices.utf8Slice("foo$"))).isEqualTo(0);
        s.compile();

        assertThat(s.match(toSlice("foo"))).isTrue();
        assertThat(s.match(toSlice("foobar"))).isFalse();

        List<Integer> v = new ArrayList<>();
        assertThat(s.match(toSlice("foo"), v)).isTrue();
        assertThat(v).hasSize(1);
        assertThat(v).containsExactly(0);

        assertThat(s.match(toSlice("foobar"), v)).isFalse();
        assertThat(v).isEmpty();
    }

    @Test
    public void testUnanchoredWordBoundary()
    {
        Re2Set s = new Re2Set(Re2.Options.defaults(), Re2.Anchor.UNANCHORED);

        assertThat(s.add(Slices.utf8Slice("foo\\b"))).isEqualTo(0);
        s.compile();

        assertThat(s.match(toSlice("foo"))).isTrue();
        assertThat(s.match(toSlice("foobar"))).isFalse();
        assertThat(s.match(toSlice("foo bar"))).isTrue();

        List<Integer> v = new ArrayList<>();
        assertThat(s.match(toSlice("foo"), v)).isTrue();
        assertThat(v).hasSize(1);
        assertThat(v).containsExactly(0);

        assertThat(s.match(toSlice("foobar"), v)).isFalse();
        assertThat(v).isEmpty();

        assertThat(s.match(toSlice("foo bar"), v)).isTrue();
        assertThat(v).hasSize(1);
        assertThat(v).containsExactly(0);
    }

    @Test
    public void testAnchored()
    {
        Re2Set s = new Re2Set(Re2.Options.defaults(), Re2.Anchor.ANCHOR_BOTH);

        assertThat(s.add(Slices.utf8Slice("foo"))).isEqualTo(0);
        assertThatThrownBy(() -> s.add(Slices.utf8Slice("(")))
                .isInstanceOf(RegexpParseException.class);
        assertThat(s.add(Slices.utf8Slice("bar"))).isEqualTo(1);
        s.compile();

        assertThat(s.match(toSlice("foobar"))).isFalse();
        assertThat(s.match(toSlice("fooba"))).isFalse();
        assertThat(s.match(toSlice("oobar"))).isFalse();
        assertThat(s.match(toSlice("foo"))).isTrue();
        assertThat(s.match(toSlice("bar"))).isTrue();

        List<Integer> v = new ArrayList<>();
        assertThat(s.match(toSlice("foobar"), v)).isFalse();
        assertThat(v).isEmpty();

        assertThat(s.match(toSlice("fooba"), v)).isFalse();
        assertThat(v).isEmpty();

        assertThat(s.match(toSlice("oobar"), v)).isFalse();
        assertThat(v).isEmpty();

        assertThat(s.match(toSlice("foo"), v)).isTrue();
        assertThat(v).hasSize(1);
        assertThat(v).containsExactly(0);

        assertThat(s.match(toSlice("bar"), v)).isTrue();
        assertThat(v).hasSize(1);
        assertThat(v).containsExactly(1);
    }

    @Test
    public void testEmptyUnanchored()
    {
        Re2Set s = new Re2Set(Re2.Options.defaults(), Re2.Anchor.UNANCHORED);

        s.compile();

        assertThat(s.match(toSlice(""))).isFalse();
        assertThat(s.match(toSlice("foobar"))).isFalse();

        List<Integer> v = new ArrayList<>();
        assertThat(s.match(toSlice(""), v)).isFalse();
        assertThat(v).isEmpty();

        assertThat(s.match(toSlice("foobar"), v)).isFalse();
        assertThat(v).isEmpty();
    }

    @Test
    public void testEmptyAnchored()
    {
        Re2Set s = new Re2Set(Re2.Options.defaults(), Re2.Anchor.ANCHOR_BOTH);

        s.compile();

        assertThat(s.match(toSlice(""))).isFalse();
        assertThat(s.match(toSlice("foobar"))).isFalse();

        List<Integer> v = new ArrayList<>();
        assertThat(s.match(toSlice(""), v)).isFalse();
        assertThat(v).isEmpty();

        assertThat(s.match(toSlice("foobar"), v)).isFalse();
        assertThat(v).isEmpty();
    }

    @Test
    public void testPrefix()
    {
        Re2Set s = new Re2Set(Re2.Options.defaults(), Re2.Anchor.ANCHOR_BOTH);

        assertThat(s.add(Slices.utf8Slice("/prefix/\\d*"))).isEqualTo(0);
        s.compile();

        assertThat(s.match(toSlice("/prefix"))).isFalse();
        assertThat(s.match(toSlice("/prefix/"))).isTrue();
        assertThat(s.match(toSlice("/prefix/42"))).isTrue();

        List<Integer> v = new ArrayList<>();
        assertThat(s.match(toSlice("/prefix"), v)).isFalse();
        assertThat(v).isEmpty();

        assertThat(s.match(toSlice("/prefix/"), v)).isTrue();
        assertThat(v).hasSize(1);
        assertThat(v).containsExactly(0);

        assertThat(s.match(toSlice("/prefix/42"), v)).isTrue();
        assertThat(v).hasSize(1);
        assertThat(v).containsExactly(0);
    }

    @Test
    public void testConstructorDoesNotMutateOptions()
    {
        Re2.Options options = Re2.Options.defaults();
        assertThat(options.neverCapture()).isFalse();

        Re2Set set = new Re2Set(options, Re2.Anchor.UNANCHORED);
        assertThat(set.add(Slices.utf8Slice("(a)"))).isEqualTo(0);
        assertThat(options.neverCapture()).isFalse();

        set.compile();
        List<Integer> matches = new ArrayList<>();
        assertThat(set.match(toSlice("a"), matches)).isTrue();
        assertThat(matches).containsExactly(0);
    }

    @Test
    public void testAddAfterCompileThrows()
    {
        Re2Set set = new Re2Set(Re2.Options.defaults(), Re2.Anchor.UNANCHORED);
        assertThat(set.add(Slices.utf8Slice("foo"))).isEqualTo(0);
        set.compile();
        assertThatThrownBy(() -> set.add(Slices.utf8Slice("bar")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("called after compiling");
    }

    @Test
    public void testCompileFailureThrows()
    {
        Re2Set set = new Re2Set(Re2.Options.defaults().setMaxMemory(1), Re2.Anchor.UNANCHORED);
        set.add(Slices.utf8Slice("foo"));

        assertThatThrownBy(set::compile)
                .isInstanceOf(RegexpCompileOutOfMemoryException.class);
    }

    // Note: MoveSemantics test from upstream is not applicable to Java
    // since Java does not have move semantics. Re2Set is a regular
    // mutable object that can be garbage collected.
}
