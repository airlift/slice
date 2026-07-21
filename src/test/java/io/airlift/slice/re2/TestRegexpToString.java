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

public class TestRegexpToString
{
    @Test
    public void testLiteralEscapesMeta()
    {
        Regexp re = Regexp.literal(0, '.');
        assertThat(RegexpToString.toString(re)).isEqualTo("\\.");
    }

    @Test
    public void testConcatAndAlternate()
    {
        Regexp concat = Regexp.concat(0, List.of(Regexp.literal(0, 'a'), Regexp.literal(0, 'b')));
        assertThat(RegexpToString.toString(concat)).isEqualTo("ab");

        Regexp alt = Regexp.alternate(0, List.of(Regexp.literal(0, 'a'), Regexp.literal(0, 'b')));
        assertThat(RegexpToString.toString(alt)).isEqualTo("a|b");
    }

    @Test
    public void testCharClassAndRepeat()
    {
        CharClassBuilder b = new CharClassBuilder();
        b.addRange('a', 'z');
        Regexp cc = Regexp.charClass(0, b.toCharClass());
        assertThat(RegexpToString.toString(cc)).isEqualTo("[a-z]");

        Regexp rep = Regexp.repeat(0, Regexp.literal(0, 'a'), 2, 3);
        assertThat(RegexpToString.toString(rep)).isEqualTo("a{2,3}");
    }
}
