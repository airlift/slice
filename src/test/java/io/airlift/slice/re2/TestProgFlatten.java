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

public class TestProgFlatten
{
    @Test
    public void testFlattenAltOfTwoByteRanges()
    {
        Prog prog = new Prog();

        prog.add(Prog.Inst.createAlt(2, 3));                    // 1
        prog.add(Prog.Inst.createByteRange(0x61, 0x61, false, 4)); // 2
        prog.add(Prog.Inst.createByteRange(0x63, 0x63, false, 4)); // 3
        prog.add(Prog.Inst.createMatch(0));                     // 4

        prog.setStartUnanchored(1);
        prog.setStart(1);

        prog.flatten();

        assertThat(prog.dump()).isEqualTo("""
                1+ byte [61-61] 0 -> 3
                2. byte [63-63] 0 -> 3
                3. match! 0
                """);
    }

    @Test
    public void testInstCountTracking()
    {
        // Build a program manually to test instruction counting.
        Prog prog = new Prog();

        prog.add(Prog.Inst.createAlt(2, 3));                        // 1 - ALT
        prog.add(Prog.Inst.createByteRange(0x61, 0x61, false, 4));  // 2 - BYTE_RANGE
        prog.add(Prog.Inst.createByteRange(0x62, 0x62, false, 5));  // 3 - BYTE_RANGE
        prog.add(Prog.Inst.createCapture(0, 6));                    // 4 - CAPTURE
        prog.add(Prog.Inst.createEmptyWidth(EmptyOp.EMPTY_BEGIN_TEXT, 6)); // 5 - EMPTY_WIDTH
        prog.add(Prog.Inst.createMatch(0));                         // 6 - MATCH

        prog.setStartUnanchored(1);
        prog.setStart(1);

        // Before flatten, inst counts should be zero
        assertThat(prog.getInstCount(InstOp.BYTE_RANGE)).isEqualTo(0);
        assertThat(prog.getInstCount(InstOp.MATCH)).isEqualTo(0);

        prog.flatten();

        // After flatten, verify counts match the flattened program
        // The ALT is converted to list form (no longer exists as ALT)
        // FAIL at position 0 always exists
        int[] counts = prog.getInstCount();
        int total = 0;
        for (int count : counts) {
            total += count;
        }
        assertThat(total).isEqualTo(prog.size());

        // Verify we have at least one BYTE_RANGE and one MATCH
        assertThat(prog.getInstCount(InstOp.BYTE_RANGE)).isGreaterThanOrEqualTo(2);
        assertThat(prog.getInstCount(InstOp.MATCH)).isGreaterThanOrEqualTo(1);
        assertThat(prog.getInstCount(InstOp.FAIL)).isGreaterThanOrEqualTo(1);
    }
}
