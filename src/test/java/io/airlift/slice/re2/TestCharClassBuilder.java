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

import static io.airlift.slice.re2.CharClass.RUNEMAX;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCharClassBuilder
{
    @Test
    public void testAddRangeMergesAndCounts()
    {
        CharClassBuilder b = new CharClassBuilder();
        b.addRange(10, 12);
        b.addRange(15, 17);
        b.addRange(13, 14);

        assertThat(b.rangeCount()).isEqualTo(1);
        assertThat(b.runeCount()).isEqualTo(8);
        assertThat(b.range(0)).isEqualTo(new RuneRange(10, 17));
        assertThat(b.contains(9)).isFalse();
        assertThat(b.contains(10)).isTrue();
        assertThat(b.contains(17)).isTrue();
        assertThat(b.contains(18)).isFalse();
    }

    @Test
    public void testNegate()
    {
        CharClassBuilder b = new CharClassBuilder();
        b.addRange(10, 20);
        b.negate();

        assertThat(b.contains(0)).isTrue();
        assertThat(b.contains(9)).isTrue();
        assertThat(b.contains(10)).isFalse();
        assertThat(b.contains(20)).isFalse();
        assertThat(b.contains(21)).isTrue();
        assertThat(b.contains(RUNEMAX)).isTrue();
    }

    @Test
    public void testFoldsAscii()
    {
        CharClassBuilder b = new CharClassBuilder();
        b.addRange('A', 'A');
        assertThat(b.foldsAscii()).isFalse();
        b.addRange('a', 'a');
        assertThat(b.foldsAscii()).isTrue();
    }
}
