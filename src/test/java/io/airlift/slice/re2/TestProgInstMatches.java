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

import static org.assertj.core.api.Assertions.assertThat;

public class TestProgInstMatches
{
    @Test
    public void testByteRangeDoesNotMatchNonByteSentinels()
    {
        Prog.Inst br = Prog.Inst.createByteRange(0x00, 0x09, false, 0);

        assertThat(br.matches(-1)).isFalse();
        assertThat(br.matches(256)).isFalse();
        assertThat(br.matches(0)).isTrue();
        assertThat(br.matches(9)).isTrue();
        assertThat(br.matches(10)).isFalse();
    }
}
