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
import jdk.incubator.vector.ByteVector;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFusedPrefixAcceleration
{
    private static final byte[] PREFIX = "Шерлок Холмс".getBytes(StandardCharsets.UTF_8);

    @Test
    public void testExactStrategyCutoff()
    {
        Prog program = configuredProgram(PREFIX, false);

        assertThat(program.prefixAccelStrategy(1_023)).isEqualTo(Prog.PrefixAccelStrategy.FUSED_SWAR);
        assertThat(program.prefixAccelStrategy(1_024)).isEqualTo(Prog.PrefixAccelStrategy.FUSED_VECTOR);
        assertThat(program.prefixAccelStrategy(1_025)).isEqualTo(Prog.PrefixAccelStrategy.FUSED_VECTOR);

        for (int length : new int[] {1_023, 1_024, 1_025}) {
            assertMatchAt(program, PREFIX, length, 7, 0);
            assertMatchAt(program, PREFIX, length, 7, length - PREFIX.length);
            assertNoMatch(program, length, 7);
        }
    }

    @Test
    public void testVectorLaneAndTailBoundaries()
    {
        int vectorWidth = ByteVector.SPECIES_PREFERRED.length();
        int length = 1_024 + (2 * vectorWidth) + PREFIX.length;
        for (int position : new int[] {
                vectorWidth - 1,
                vectorWidth,
                vectorWidth + 1,
                (2 * vectorWidth) - 1,
                2 * vectorWidth,
                length - PREFIX.length}) {
            assertMatchAt(configuredProgram(PREFIX, false), PREFIX, length, vectorWidth - 1, position);
        }
    }

    @Test
    public void testDenseFalseCandidatesBeforeFinalMatch()
    {
        Prog program = configuredProgram(PREFIX, false);
        int length = 2_048;
        byte[] data = new byte[length];
        Arrays.fill(data, (byte) 'x');
        int backOffset = PREFIX.length - 1;
        for (int position = 0; position <= length - PREFIX.length; position += 3) {
            data[position] = PREFIX[0];
            data[position + backOffset] = PREFIX[backOffset];
        }
        int matchPosition = length - PREFIX.length;
        System.arraycopy(PREFIX, 0, data, matchPosition, PREFIX.length);

        assertThat(program.prefixAccel(data, 0, data.length)).isEqualTo(matchPosition);
    }

    @Test
    public void testRepeatedEndpointAndLongPrefixBoundaries()
    {
        byte[] repeatedEndpointPrefix = {(byte) 0xC3, 1, 2, (byte) 0xC3};
        assertMatchAt(configuredProgram(repeatedEndpointPrefix, false), repeatedEndpointPrefix, 1_025, 1, 1_021);

        byte[] longPrefix = new byte[1_030];
        Arrays.fill(longPrefix, (byte) 0xA5);
        Prog program = configuredProgram(longPrefix, false);
        assertThat(program.prefixAccel(new byte[1_029], 0, 1_029)).isEqualTo(-1);
        assertThat(program.prefixAccel(longPrefix.clone(), 0, longPrefix.length)).isZero();
    }

    @Test
    public void testIneligiblePrefixesStayOnExistingRoutes()
    {
        assertThat(configuredProgram(PREFIX, true).prefixAccelStrategy(4_096))
                .isEqualTo(Prog.PrefixAccelStrategy.REPEATED_BYTE);
        assertThat(configuredProgram(new byte[] {(byte) 0xD0}, false).prefixAccelStrategy(4_096))
                .isEqualTo(Prog.PrefixAccelStrategy.REPEATED_BYTE);
        assertThat(configuredProgram("Sherlock Holmes".getBytes(StandardCharsets.UTF_8), false).prefixAccelStrategy(4_096))
                .isEqualTo(Prog.PrefixAccelStrategy.REPEATED_BYTE);
    }

    private static void assertMatchAt(Prog program, byte[] prefix, int length, int backingOffset, int matchPosition)
    {
        byte[] data = new byte[backingOffset + length + 5];
        Arrays.fill(data, (byte) 'x');
        System.arraycopy(prefix, 0, data, backingOffset + matchPosition, prefix.length);
        assertThat(program.prefixAccel(data, backingOffset, length)).isEqualTo(backingOffset + matchPosition);
    }

    private static void assertNoMatch(Prog program, int length, int backingOffset)
    {
        byte[] data = new byte[backingOffset + length + 5];
        Arrays.fill(data, (byte) 'x');
        assertThat(program.prefixAccel(data, backingOffset, length)).isEqualTo(-1);
    }

    private static Prog configuredProgram(byte[] prefix, boolean foldCase)
    {
        Prog program = new Prog();
        program.configurePrefixAccel(Slices.wrappedBuffer(prefix), foldCase);
        return program;
    }
}
