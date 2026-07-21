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

import java.lang.reflect.Field;
import java.util.List;

import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestNfaCaptureInstructionWords
{
    private static final long OPCODE_MASK = 0b111;
    private static final int LAST_SHIFT = 3;
    private static final int OUT_SHIFT = 4;
    private static final long OUT_MASK = (1L << 28) - 1;
    private static final int PAYLOAD_SHIFT = 32;
    private static final int ALT_MATCH_GREEDY_SHIFT = 31;

    @Test
    public void testPackedInstructionsPreserveLogicalFields()
    {
        List<Prog> programs = List.of(
                allInstructionKinds(),
                compileProg("((a|aa)+)(a?)"),
                compileProg("(?i:a+)(b*)"),
                compileProg("^([0-9]+)-(.*)$"),
                compileProg("\\b(foo|bar)\\b"),
                compileProg("\\C*"));

        for (Prog program : programs) {
            long[] instructionWords = program.getOrCreateCaptureInstructionWords();
            assertThat(program.getOrCreateCaptureInstructionWords()).isSameAs(instructionWords);
            assertThat(instructionWords).hasSize(program.size());

            for (int instructionId = 0; instructionId < program.size(); instructionId++) {
                assertInstruction(program, instructionId, instructionWords[instructionId]);
            }
        }
    }

    private static Prog allInstructionKinds()
    {
        Prog program = new Prog();
        program.add(Prog.Inst.createByteRange('a', 'z', true, 4));
        program.add(Prog.Inst.createCapture(3, 1));
        program.add(Prog.Inst.createEmptyWidth(EmptyOp.EMPTY_BEGIN_TEXT, 1));
        program.add(Prog.Inst.createMatch(7));
        program.add(Prog.Inst.createNop(1));
        program.add(Prog.Inst.createAlt(1, 2));
        program.add(Prog.Inst.createAltMatch(1, 2));
        program.inst(1).setLast();
        program.setDidFlatten(true);
        return program;
    }

    @Test
    public void testPackedInstructionsRequireFlattenedProgram()
    {
        Prog program = new Prog();

        assertThatThrownBy(program::getOrCreateCaptureInstructionWords)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("flattened");
    }

    @Test
    public void testPackedInstructionsRejectOutputOverflow()
    {
        Prog program = new Prog();
        program.add(Prog.Inst.createNop(1 << 28));
        program.setDidFlatten(true);

        assertThatThrownBy(program::getOrCreateCaptureInstructionWords)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("output exceeds packed instruction limit");
    }

    @Test
    public void testCaptureNfaUsesPackedInstructions()
            throws ReflectiveOperationException
    {
        Class<?> implementation = Class.forName("io.airlift.slice.re2.Nfa$NfaImpl");
        Field instructionWords = implementation.getDeclaredField("instructionWords");
        assertThat(instructionWords.getType()).isEqualTo(long[].class);
    }

    private static void assertInstruction(Prog program, int instructionId, long word)
    {
        Prog.Inst instruction = program.inst(instructionId);
        assertThat((int) (word & OPCODE_MASK)).isEqualTo(instruction.opcode().ordinal());
        assertThat(((word >>> LAST_SHIFT) & 1) != 0).isEqualTo(instruction.last());
        assertThat((int) ((word >>> OUT_SHIFT) & OUT_MASK)).isEqualTo(instruction.out());

        int payload = (int) (word >>> PAYLOAD_SHIFT);
        switch (instruction.opcode()) {
            case ALT -> assertThat((int) (payload & OUT_MASK)).isEqualTo(instruction.out1());
            case ALT_MATCH -> {
                assertThat((int) (payload & OUT_MASK)).isEqualTo(instruction.out1());
                assertThat(((payload >>> ALT_MATCH_GREEDY_SHIFT) & 1) != 0).isEqualTo(instruction.greedy(program));
            }
            case BYTE_RANGE -> {
                assertThat(payload & 0xFF).isEqualTo(instruction.lo());
                assertThat((payload >>> 8) & 0xFF).isEqualTo(instruction.hi());
                assertThat((payload >>> 16) & 0x7FFF).isEqualTo(instruction.hint());
                assertThat(payload < 0).isEqualTo(instruction.foldCase());
            }
            case CAPTURE -> assertThat(payload).isEqualTo(instruction.cap());
            case EMPTY_WIDTH -> assertThat(payload).isEqualTo(instruction.empty());
            case MATCH -> assertThat(payload).isEqualTo(instruction.matchId());
            case NOP, FAIL -> assertThat(payload).isZero();
        }
    }
}
