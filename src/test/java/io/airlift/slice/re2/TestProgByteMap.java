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

// Ported from upstream RE2: re2/testing/compile_test.cc.
public class TestProgByteMap
{
    @Test
    public void testLatin1DotRanges()
    {
        Prog prog = new Prog();

        Prog.Inst r0 = Prog.Inst.createByteRange(0x00, 0x09, false, 3);
        Prog.Inst r1 = Prog.Inst.createByteRange(0x0b, 0xff, false, 3);
        r1.setLast();
        prog.add(r0); // 1
        prog.add(r1); // 2
        prog.add(Prog.Inst.createMatch(0)); // 3

        prog.computeByteMap();

        assertThat(prog.dumpByteMap()).isEqualTo(
                "[00-09] -> 0\n" +
                        "[0a-0a] -> 1\n" +
                        "[0b-ff] -> 0\n");
    }

    @Test
    public void testAbsentRangesMapToSameByteClass()
    {
        Prog prog = new Prog();

        Prog.Inst digits = Prog.Inst.createByteRange(0x30, 0x39, false, 4);
        Prog.Inst upper = Prog.Inst.createByteRange(0x41, 0x46, false, 4);
        Prog.Inst lower = Prog.Inst.createByteRange(0x61, 0x66, false, 4);
        lower.setLast();
        prog.add(digits); // 1
        prog.add(upper);  // 2
        prog.add(lower);  // 3
        prog.add(Prog.Inst.createMatch(0)); // 4

        prog.computeByteMap();

        assertThat(prog.dumpByteMap()).isEqualTo(
                "[00-2f] -> 0\n" +
                        "[30-39] -> 1\n" +
                        "[3a-40] -> 0\n" +
                        "[41-46] -> 1\n" +
                        "[47-60] -> 0\n" +
                        "[61-66] -> 1\n" +
                        "[67-ff] -> 0\n");
    }

    @Test
    public void testWordBoundaryByteClasses()
    {
        Prog prog = new Prog();
        prog.add(Prog.Inst.createEmptyWidth(EmptyOp.EMPTY_WORD_BOUNDARY, 0));

        prog.computeByteMap();

        assertThat(prog.dumpByteMap()).isEqualTo(
                "[00-2f] -> 0\n" +
                        "[30-39] -> 1\n" +
                        "[3a-40] -> 0\n" +
                        "[41-5a] -> 1\n" +
                        "[5b-5e] -> 0\n" +
                        "[5f-5f] -> 1\n" +
                        "[60-60] -> 0\n" +
                        "[61-7a] -> 1\n" +
                        "[7b-ff] -> 0\n");
    }

    @Test
    public void testAsciiCaseFoldingOptimizationRegression()
    {
        Prog prog = new Prog();

        Prog.Inst r0 = Prog.Inst.createByteRange(0x00, 0x5e, false, 3);
        Prog.Inst r1 = Prog.Inst.createByteRange(0x60, 0xff, false, 3);
        r1.setLast();
        prog.add(r0);
        prog.add(r1);
        prog.add(Prog.Inst.createMatch(0));

        prog.computeByteMap();

        assertThat(prog.dumpByteMap()).isEqualTo(
                "[00-5e] -> 0\n" +
                        "[5f-5f] -> 1\n" +
                        "[60-ff] -> 0\n");
    }

    @Test
    public void testUtf8DotRanges()
    {
        Prog prog = new Prog();

        // Batch 1: ASCII except newline.
        Prog.Inst ascii0 = Prog.Inst.createByteRange(0x00, 0x09, false, 99);
        Prog.Inst ascii1 = Prog.Inst.createByteRange(0x0b, 0x7f, false, 99);
        ascii1.setLast();
        prog.add(ascii0);
        prog.add(ascii1);

        // Batch 2+: The key UTF-8 lead/continuation byte ranges.
        Prog.Inst cont = Prog.Inst.createByteRange(0x80, 0xbf, false, 98);
        cont.setLast();
        prog.add(cont);

        Prog.Inst lead2 = Prog.Inst.createByteRange(0xc2, 0xdf, false, 97);
        lead2.setLast();
        prog.add(lead2);

        Prog.Inst lead3 = Prog.Inst.createByteRange(0xe0, 0xef, false, 96);
        lead3.setLast();
        prog.add(lead3);

        Prog.Inst lead4 = Prog.Inst.createByteRange(0xf0, 0xf4, false, 95);
        lead4.setLast();
        prog.add(lead4);

        prog.computeByteMap();

        assertThat(prog.dumpByteMap()).isEqualTo(
                "[00-09] -> 0\n" +
                        "[0a-0a] -> 1\n" +
                        "[0b-7f] -> 0\n" +
                        "[80-bf] -> 2\n" +
                        "[c0-c1] -> 1\n" +
                        "[c2-df] -> 3\n" +
                        "[e0-ef] -> 4\n" +
                        "[f0-f4] -> 5\n" +
                        "[f5-ff] -> 1\n");
    }
}
