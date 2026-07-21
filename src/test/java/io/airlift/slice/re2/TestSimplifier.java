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

public class TestSimplifier
{
    @Test
    public void testFlattenConcatAndRemoveEmpty()
    {
        Regexp re = Regexp.concat(0, List.of(
                Regexp.emptyMatch(0),
                Regexp.concat(0, List.of(Regexp.literal(0, 'a'), Regexp.literal(0, 'b'))),
                Regexp.emptyMatch(0)));

        Regexp simplified = Simplifier.simplify(re);
        assertThat(simplified.op()).isEqualTo(RegexpOp.LITERAL_STRING);
        assertThat(simplified.runes()).containsExactly('a', 'b');
    }

    @Test
    public void testConcatWithNoMatchIsNoMatch()
    {
        Regexp re = Regexp.concat(0, List.of(Regexp.literal(0, 'a'), Regexp.noMatch(0)));
        assertThat(Simplifier.simplify(re).op()).isEqualTo(RegexpOp.NO_MATCH);
    }

    @Test
    public void testAlternateDropsNoMatchAndFlattens()
    {
        Regexp re = Regexp.alternate(0, List.of(
                Regexp.noMatch(0),
                Regexp.alternate(0, List.of(Regexp.literal(0, 'a'), Regexp.literal(0, 'b')))));

        Regexp simplified = Simplifier.simplify(re);
        assertThat(simplified.op()).isEqualTo(RegexpOp.CHAR_CLASS);
        assertThat(simplified.charClass().contains('a')).isTrue();
        assertThat(simplified.charClass().contains('b')).isTrue();
    }

    @Test
    public void testStarNoMatchIsEmpty()
    {
        Regexp re = Regexp.star(0, Regexp.noMatch(0));
        assertThat(Simplifier.simplify(re).op()).isEqualTo(RegexpOp.EMPTY_MATCH);
    }

    @Test
    public void testSimpleFieldIsSet()
    {
        // Leaf nodes have simple=true (indicating they are already in simplified form)
        Regexp literal = Regexp.literal(0, 'a');
        assertThat(literal.simple()).isTrue();

        Regexp emptyMatch = Regexp.emptyMatch(0);
        assertThat(emptyMatch.simple()).isTrue();

        Regexp noMatch = Regexp.noMatch(0);
        assertThat(noMatch.simple()).isTrue();

        Regexp anyChar = Regexp.anyChar(0);
        assertThat(anyChar.simple()).isTrue();

        Regexp beginText = Regexp.beginText(0);
        assertThat(beginText.simple()).isTrue();

        // Simplification should produce equivalent results
        assertThat(Simplifier.simplify(literal).op()).isEqualTo(RegexpOp.LITERAL);
        assertThat(Simplifier.simplify(emptyMatch).op()).isEqualTo(RegexpOp.EMPTY_MATCH);
        assertThat(Simplifier.simplify(noMatch).op()).isEqualTo(RegexpOp.NO_MATCH);
    }
}
