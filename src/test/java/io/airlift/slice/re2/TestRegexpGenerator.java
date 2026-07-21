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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRegexpGenerator
{
    @Test
    public void testGenerateAtomsOnly()
    {
        CollectingGenerator gen = new CollectingGenerator(
                List.of(Slices.wrappedBuffer("a".getBytes(UTF_8))),
                List.of(),
                1,
                0);
        gen.generate();

        assertThat(gen.regexpsAsStrings())
                .containsExactlyInAnyOrder(
                        "a",
                        "^(?:a)$",
                        "^(?:a)",
                        "(?:a)$");
    }

    @Test
    public void testGenerateRandomCallsHandleRegexp()
    {
        CollectingGenerator gen = new CollectingGenerator(
                List.of(Slices.wrappedBuffer("a".getBytes(UTF_8))),
                List.of(),
                1,
                0);
        gen.generateRandom(123, 5);

        // Each postfix run yields 4 regexps in runPostfix().
        assertThat(gen.regexps()).hasSize(20);
    }

    private static final class CollectingGenerator
            extends RegexpGenerator
    {
        private final List<Slice> regexps = new ArrayList<>();

        CollectingGenerator(List<Slice> atoms, List<Slice> ops, int maxAtoms, int maxOps)
        {
            super(atoms, ops, maxAtoms, maxOps);
        }

        @Override
        protected void handleRegexp(Slice regexp)
        {
            regexps.add(regexp);
        }

        List<Slice> regexps()
        {
            return regexps;
        }

        List<String> regexpsAsStrings()
        {
            List<String> out = new ArrayList<>();
            for (Slice regexp : regexps) {
                out.add(new String(regexp.byteArray(), regexp.byteArrayOffset(), regexp.length(), UTF_8));
            }
            return out;
        }
    }
}
