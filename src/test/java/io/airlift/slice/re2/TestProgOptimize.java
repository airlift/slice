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

public class TestProgOptimize
{
    @Test
    public void testEliminateNopsInOuts()
    {
        Prog prog = new Prog();

        prog.add(Prog.Inst.createAlt(2, 3));     // 1
        prog.add(Prog.Inst.createNop(4));        // 2
        prog.add(Prog.Inst.createNop(5));        // 3
        prog.add(Prog.Inst.createMatch(0));      // 4
        prog.add(Prog.Inst.createMatch(1));      // 5
        prog.setStart(1);

        prog.optimize();

        Prog.Inst alt = prog.inst(1);
        assertThat(alt.opcode()).isEqualTo(InstOp.ALT);
        assertThat(alt.out()).isEqualTo(4);
        assertThat(alt.out1()).isEqualTo(5);
    }

    @Test
    public void testAltMatchInsertedGreedyForm()
    {
        Prog prog = new Prog();

        prog.add(Prog.Inst.createAlt(2, 4));                 // 1
        prog.add(Prog.Inst.createByteRange(0x00, 0xff, false, 1)); // 2
        prog.add(Prog.Inst.createFail());                    // 3 (unused)
        prog.add(Prog.Inst.createMatch(0));                  // 4
        prog.setStart(1);

        prog.optimize();

        assertThat(prog.inst(1).opcode()).isEqualTo(InstOp.ALT_MATCH);
    }

    @Test
    public void testAltMatchInsertedNonGreedyForm()
    {
        Prog prog = new Prog();

        prog.add(Prog.Inst.createAlt(4, 2));                 // 1
        prog.add(Prog.Inst.createByteRange(0x00, 0xff, false, 1)); // 2
        prog.add(Prog.Inst.createFail());                    // 3 (unused)
        prog.add(Prog.Inst.createMatch(0));                  // 4
        prog.setStart(1);

        prog.optimize();

        assertThat(prog.inst(1).opcode()).isEqualTo(InstOp.ALT_MATCH);
    }
}
