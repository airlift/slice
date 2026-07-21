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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Includes tests ported from upstream RE2: re2/testing/regexp_test.cc.
public class TestRegexpAst
{
    @Test
    public void testLiteralAndLiteralString()
    {
        Regexp lit = Regexp.literal(0, 'x');
        assertThat(lit.op()).isEqualTo(RegexpOp.LITERAL);
        assertThat(lit.rune()).isEqualTo('x');

        Regexp str = Regexp.literalString(0, new int[] {'a', 'b'});
        assertThat(str.op()).isEqualTo(RegexpOp.LITERAL_STRING);
        assertThat(str.runes()).containsExactly('a', 'b');

        assertThatThrownBy(str::rune).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testConcatRequiresAtLeastTwoChildren()
    {
        assertThatThrownBy(() -> Regexp.concat(0, List.of(Regexp.emptyMatch(0))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testEqualsAndHashCode()
    {
        Regexp left = Regexp.concat(0, List.of(
                Regexp.literal(0, 'a'),
                Regexp.capture(0, Regexp.literal(0, 'b'), 1, null)));
        Regexp right = Regexp.concat(0, List.of(
                Regexp.literal(0, 'a'),
                Regexp.capture(0, Regexp.literal(0, 'b'), 1, null)));
        Regexp different = Regexp.concat(0, List.of(
                Regexp.literal(0, 'a'),
                Regexp.capture(0, Regexp.literal(0, 'c'), 1, null)));

        assertThat(left).isEqualTo(right);
        assertThat(left.hashCode()).isEqualTo(right.hashCode());
        assertThat(left).isNotEqualTo(different);
    }

    @Test
    public void testEqualsUsesOnlyOperationRelevantFlags()
    {
        assertEqualWithEqualHash(Regexp.noMatch(0), Regexp.noMatch(Regexp.PERL_EXTENSIONS));
        assertEqualWithEqualHash(Regexp.emptyMatch(0), Regexp.emptyMatch(Regexp.PERL_EXTENSIONS));
        assertEqualWithEqualHash(Regexp.anyChar(0), Regexp.anyChar(Regexp.PERL_EXTENSIONS));
        assertEqualWithEqualHash(Regexp.anyByte(0), Regexp.anyByte(Regexp.PERL_EXTENSIONS));
        assertEqualWithEqualHash(Regexp.beginLine(0), Regexp.beginLine(Regexp.PERL_EXTENSIONS));
        assertEqualWithEqualHash(Regexp.endLine(0), Regexp.endLine(Regexp.PERL_EXTENSIONS));
        assertEqualWithEqualHash(Regexp.wordBoundary(0), Regexp.wordBoundary(Regexp.PERL_EXTENSIONS));
        assertEqualWithEqualHash(Regexp.noWordBoundary(0), Regexp.noWordBoundary(Regexp.PERL_EXTENSIONS));
        assertEqualWithEqualHash(Regexp.beginText(0), Regexp.beginText(Regexp.PERL_EXTENSIONS));
        assertEqualWithEqualHash(Regexp.endText(0), Regexp.endText(Regexp.FOLD_CASE));
        assertThat(Regexp.endText(0)).isNotEqualTo(Regexp.endText(Regexp.WAS_DOLLAR));

        assertEqualWithEqualHash(Regexp.literal(0, 'a'), Regexp.literal(Regexp.PERL_EXTENSIONS, 'a'));
        assertThat(Regexp.literal(0, 'a')).isNotEqualTo(Regexp.literal(Regexp.FOLD_CASE, 'a'));
        assertEqualWithEqualHash(
                Regexp.literalString(0, new int[] {'a', 'b'}),
                Regexp.literalString(Regexp.PERL_EXTENSIONS, new int[] {'a', 'b'}));
        assertThat(Regexp.literalString(0, new int[] {'a', 'b'}))
                .isNotEqualTo(Regexp.literalString(Regexp.FOLD_CASE, new int[] {'a', 'b'}));

        Regexp child = Regexp.literal(0, 'a');
        assertEqualWithEqualHash(Regexp.star(0, child), Regexp.star(Regexp.PERL_EXTENSIONS, child));
        assertThat(Regexp.star(0, child)).isNotEqualTo(Regexp.star(Regexp.NON_GREEDY, child));
        assertEqualWithEqualHash(Regexp.plus(0, child), Regexp.plus(Regexp.PERL_EXTENSIONS, child));
        assertThat(Regexp.plus(0, child)).isNotEqualTo(Regexp.plus(Regexp.NON_GREEDY, child));
        assertEqualWithEqualHash(Regexp.quest(0, child), Regexp.quest(Regexp.PERL_EXTENSIONS, child));
        assertThat(Regexp.quest(0, child)).isNotEqualTo(Regexp.quest(Regexp.NON_GREEDY, child));
        assertEqualWithEqualHash(Regexp.repeat(0, child, 2, 3), Regexp.repeat(Regexp.PERL_EXTENSIONS, child, 2, 3));
        assertThat(Regexp.repeat(0, child, 2, 3)).isNotEqualTo(Regexp.repeat(Regexp.NON_GREEDY, child, 2, 3));

        assertEqualWithEqualHash(
                Regexp.concat(0, List.of(child, child)),
                Regexp.concat(Regexp.PERL_EXTENSIONS, List.of(child, child)));
        assertEqualWithEqualHash(
                Regexp.alternate(0, List.of(child, Regexp.emptyMatch(0))),
                Regexp.alternate(Regexp.PERL_EXTENSIONS, List.of(child, Regexp.emptyMatch(0))));
        assertEqualWithEqualHash(
                Regexp.capture(0, child, 1, Slices.utf8Slice("name")),
                Regexp.capture(Regexp.PERL_EXTENSIONS, child, 1, Slices.utf8Slice("name")));

        CharClass characterClass = new CharClass(false, 1, new RuneRange[] {new RuneRange('a', 'a')});
        assertEqualWithEqualHash(
                Regexp.charClass(0, characterClass),
                Regexp.charClass(Regexp.PERL_EXTENSIONS, characterClass));
        assertEqualWithEqualHash(Regexp.haveMatch(0, 1), Regexp.haveMatch(Regexp.PERL_EXTENSIONS, 1));
    }

    @Test
    public void testLargeConcat()
    {
        Regexp literal = Regexp.literal(0, 'x');
        Regexp concat = Regexp.concat(0, Collections.nCopies(90_000, literal));

        assertThat(RegexpToString.toString(concat)).isEqualTo("x".repeat(90_000));
    }

    @Test
    public void testNamedCaptureMaps()
    {
        Regexp regexp = RegexpParser.parse(
                Slices.utf8Slice("(?P<g1>a+)|(e)(?P<g2>w*)+(?P<g1>b+)"),
                Regexp.PERL_EXTENSIONS)
                .regexp();

        assertThat(regexp.namedCaptures()).isEqualTo(Map.of("g1", 1, "g2", 3));
        assertThat(regexp.captureNames()).isEqualTo(Map.of(1, "g1", 3, "g2", 4, "g1"));
    }

    private static void assertEqualWithEqualHash(Regexp left, Regexp right)
    {
        assertThat(left).isEqualTo(right);
        assertThat(left.hashCode()).isEqualTo(right.hashCode());
    }
}
