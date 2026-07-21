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

public class TestProgDump
{
    @Test
    public void testDumpReachableWorkQueue()
    {
        Prog prog = new Prog();

        // 0 is the sentinel "null" inst (fail), mirroring upstream.
        prog.add(Prog.Inst.createAlt(2, 3));              // 1
        prog.add(Prog.Inst.createMatch(0));               // 2
        prog.add(Prog.Inst.createMatch(1));               // 3
        prog.setStart(1);

        assertThat(prog.dump()).isEqualTo("""
                1. alt -> 2 | 3
                2. match! 0
                3. match! 1
                """);
    }

    @Test
    public void testDumpFlattened()
    {
        Prog prog = new Prog();
        prog.add(Prog.Inst.createNop(2));     // 1
        Prog.Inst match = Prog.Inst.createMatch(0);
        match.setLast();
        prog.add(match);                      // 2

        prog.setStart(1);
        prog.setDidFlatten(true);

        assertThat(prog.dump()).isEqualTo("""
                1+ nop -> 2
                2. match! 0
                """);
    }

    @Test
    public void testDumpByteMap()
    {
        Prog prog = new Prog();
        for (int c = 0; c < 256; c++) {
            prog.setBytemap(c, 0);
        }
        for (int c = 0x20; c <= 0x2F; c++) {
            prog.setBytemap(c, 7);
        }

        assertThat(prog.dumpByteMap())
                .contains("[00-1f] -> 0\n")
                .contains("[20-2f] -> 7\n")
                .contains("[30-ff] -> 0\n");
    }
}
