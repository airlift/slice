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
public class TestRe2Match
{
    @Test
    public void testMatchNumberPeculiarity()
    {
        Re2 re2 = Re2.compile(utf8("(foo)|(bar)|(baz)"), Re2.Options.defaults());

        MatchResult foo = re2.partialMatchResult(utf8("foo"));
        assertThat(foo).isNotNull();
        assertThat(foo.groupUtf8(1)).isEqualTo("foo");
        assertThat(foo.groupUtf8(2)).isNull();
        assertThat(foo.groupUtf8(3)).isNull();

        MatchResult bar = re2.partialMatchResult(utf8("bar"));
        assertThat(bar).isNotNull();
        assertThat(bar.groupUtf8(1)).isNull();
        assertThat(bar.groupUtf8(2)).isEqualTo("bar");
        assertThat(bar.groupUtf8(3)).isNull();

        MatchResult baz = re2.partialMatchResult(utf8("baz"));
        assertThat(baz).isNotNull();
        assertThat(baz.groupUtf8(1)).isNull();
        assertThat(baz.groupUtf8(2)).isNull();
        assertThat(baz.groupUtf8(3)).isEqualTo("baz");

        assertThat(re2.partialMatch(utf8("f"))).isFalse();

        MatchResult alternation = Re2.compile(utf8("(foo)|hello"), Re2.Options.defaults()).fullMatchResult(utf8("hello"));
        assertThat(alternation).isNotNull();
        assertThat(alternation.groupUtf8(1)).isNull();
    }

    @Test
    public void testMatch()
    {
        Re2 re2 = Re2.compile(utf8("((\\w+):([0-9]+))"), Re2.Options.defaults());
        int[] group = new int[8];

        Slice text = utf8("zyzzyva");
        assertThat(re2.matchInto(text, Re2.Anchor.UNANCHORED, group)).isFalse();

        text = utf8("a chrisr:9000 here");
        assertThat(re2.matchInto(text, Re2.Anchor.UNANCHORED, group)).isTrue();
        assertThat(slice(text, group[0], group[1])).isEqualTo("chrisr:9000");
        assertThat(slice(text, group[2], group[3])).isEqualTo("chrisr:9000");
        assertThat(slice(text, group[4], group[5])).isEqualTo("chrisr");
        assertThat(slice(text, group[6], group[7])).isEqualTo("9000");

        MatchResult result = re2.partialMatchResult(utf8("a chrisr:9000 here"));
        assertThat(result).isNotNull();
        assertThat(result.groupUtf8(1)).isEqualTo("chrisr:9000");
        assertThat(result.groupUtf8(2)).isEqualTo("chrisr");
        assertThat(result.parseInt(3)).isEqualTo(9000);
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String slice(Slice text, int start, int end)
    {
        return new String(text.byteArray(), text.byteArrayOffset() + start, end - start, StandardCharsets.UTF_8);
    }

    @Test
    public void testFoldCasePrefixAcceleration()
    {
        Re2 re2 = Re2.compile(utf8("(?i)HELLO.*"), Re2.Options.defaults());

        assertThat(re2.partialMatch(utf8("HELLO world"))).isTrue();
        assertThat(re2.partialMatch(utf8("hello world"))).isTrue();
        assertThat(re2.partialMatch(utf8("HeLLo world"))).isTrue();

        assertThat(re2.partialMatch(utf8("HELL"))).isFalse();
        assertThat(re2.partialMatch(utf8("xhello"))).isTrue();
    }

    @Test
    public void testInlineFlagScope()
    {
        Re2 scoped = Re2.compile(utf8("(?i:a)b"), Re2.Options.defaults());
        assertThat(scoped.fullMatch(utf8("Ab"))).isTrue();
        assertThat(scoped.fullMatch(utf8("AB"))).isFalse();

        Re2 nestedDirective = Re2.compile(utf8("((?i)a)b"), Re2.Options.defaults());
        assertThat(nestedDirective.fullMatch(utf8("Ab"))).isTrue();
        assertThat(nestedDirective.fullMatch(utf8("AB"))).isFalse();

        Re2 unscoped = Re2.compile(utf8("(?i)ab"), Re2.Options.defaults());
        assertThat(unscoped.fullMatch(utf8("AB"))).isTrue();
    }

    @Test
    public void testPrefixOptimizationFastRejection()
    {
        Re2 re2 = Re2.compile(utf8("^foobar.*"), Re2.Options.defaults());

        assertThat(re2.partialMatch(utf8("foobar123"))).isTrue();
        assertThat(re2.partialMatch(utf8("xfoobar"))).isFalse();
        assertThat(re2.partialMatch(utf8("foo"))).isFalse();
    }

    @Test
    public void testAnchorModeUpgrade()
    {
        Re2 re2 = Re2.compile(utf8("^foo$"), Re2.Options.defaults());

        assertThat(re2.fullMatch(utf8("foo"))).isTrue();

        assertThat(re2.partialMatch(utf8("afoo"))).isFalse();
        assertThat(re2.partialMatch(utf8("foob"))).isFalse();

        Re2 startAnchored = Re2.compile(utf8("^foo"), Re2.Options.defaults());
        assertThat(startAnchored.partialMatch(utf8("foobar"))).isTrue();
        assertThat(startAnchored.partialMatch(utf8("xfoo"))).isFalse();
    }
}
