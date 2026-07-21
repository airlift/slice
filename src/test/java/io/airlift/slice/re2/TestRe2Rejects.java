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

// Ported from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2Rejects
{
    @Test
    public void testRejects()
    {
        assertRejected("a\\1");
        assertRejected("a[x");
        assertRejected("a[z-a]");
        assertRejected("a[[:foobar:]]");
        assertRejected("a(b");
        assertRejected("a\\");
    }

    @Test
    public void testNoCrash()
    {
        assertThatThrownBy(() -> Re2.compile(utf8("a\\"), defaultOptions()))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> Re2.compile(utf8("(((.{100}){100}){100}){100}"), defaultOptions()))
                .isInstanceOf(IllegalArgumentException.class);

        Re2 ok = Re2.compile(utf8(".{512}x"), defaultOptions());
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 515; i++) {
            builder.append('c');
        }
        builder.append('x');
        assertThat(ok.partialMatch(utf8(builder.toString()))).isTrue();
    }

    @Test
    public void testBigCountedRepetition()
    {
        Re2 re = Re2.compile(utf8(".{512}x"), Re2.Options.defaults().setMaxMemory(256L << 20));

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 515; i++) {
            builder.append('c');
        }
        builder.append('x');
        assertThat(re.partialMatch(utf8(builder.toString()))).isTrue();
    }

    @Test
    public void testDeepRecursion()
    {
        StringBuilder comment = new StringBuilder("x*");
        for (int i = 0; i < 131072; i++) {
            comment.append('a');
        }
        comment.append("*x");

        Re2 re = Re2.compile(utf8("((?:\\s|xx.*\n|x[*](?:\n|.)*?[*]x)*)"), defaultOptions());
        assertThat(re.fullMatch(utf8(comment.toString()))).isTrue();
    }

    private static void assertRejected(String pattern)
    {
        assertThatThrownBy(() -> Re2.compile(utf8(pattern), defaultOptions()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Re2.Options defaultOptions()
    {
        return Re2.Options.defaults();
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
