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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Ported from upstream RE2: re2/testing/string_generator_test.cc.
public class TestStringGenerator
{
    @Test
    public void testEnumeratedCount()
    {
        List<Slice> alphabet = StringGenerator.explodeUtf8(Slices.wrappedBuffer("ab".getBytes(UTF_8)));
        StringGenerator gen = new StringGenerator(2, alphabet);

        List<Slice> out = new ArrayList<>();
        while (gen.hasNext()) {
            out.add(gen.next());
        }

        // 1 + 2 + 4 = 7
        assertThat(out).hasSize(7);
        assertThat(out.getFirst().length()).isEqualTo(0);
    }

    @Test
    public void testEnumeratedUniqueness()
    {
        List<Slice> alphabet = StringGenerator.explodeUtf8(Slices.wrappedBuffer("ab".getBytes(UTF_8)));
        StringGenerator gen = new StringGenerator(3, alphabet);

        Set<String> seen = new HashSet<>();
        while (gen.hasNext()) {
            Slice s = gen.next();
            // Tests are allowed to use Strings; core engine is byte-based.
            seen.add(new String(s.byteArray(), s.byteArrayOffset(), s.length(), UTF_8));
        }

        assertThat(seen).hasSize(1 + 2 + 4 + 8);
    }

    @Test
    public void testGenerateNull()
    {
        List<Slice> alphabet = List.of(Slices.wrappedBuffer(new byte[] {'a'}));
        StringGenerator gen = new StringGenerator(1, alphabet);
        gen.generateNull();

        assertThat(gen.next()).isNull();
        assertThat(gen.next().length()).isEqualTo(0);
    }

    @Test
    public void testRandomCount()
    {
        List<Slice> alphabet = StringGenerator.explodeUtf8(Slices.wrappedBuffer("abc".getBytes(UTF_8)));
        StringGenerator gen = new StringGenerator(5, alphabet);
        gen.random(123, 10);

        int count = 0;
        while (gen.hasNext()) {
            assertThat(gen.next()).isNotNull();
            count++;
        }
        assertThat(count).isEqualTo(10);
    }

    @Test
    public void testNextThrowsWhenExhausted()
    {
        List<Slice> alphabet = List.of(Slices.wrappedBuffer(new byte[] {'a'}));
        StringGenerator gen = new StringGenerator(0, alphabet);

        assertThat(gen.next().length()).isEqualTo(0);
        assertThat(gen.hasNext()).isFalse();

        assertThatThrownBy(gen::next)
                .isInstanceOf(IllegalStateException.class);
    }
}
