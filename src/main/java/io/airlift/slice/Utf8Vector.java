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
package io.airlift.slice;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;

import java.util.Arrays;

import static jdk.incubator.vector.VectorOperators.IS_NEGATIVE;
import static jdk.incubator.vector.VectorOperators.LSHR;
import static jdk.incubator.vector.VectorOperators.NE;
import static jdk.incubator.vector.VectorOperators.SUSUB;
import static jdk.incubator.vector.VectorOperators.XOR;

/**
 * UTF-8 validation using the Vector API, following the lookup algorithm of
 * John Keiser and Daniel Lemire, "Validating UTF-8 In Less Than One Instruction Per Byte".
 * Kept in a separate class so that the rest of {@link SliceUtf8} works without the
 * {@code jdk.incubator.vector} module.
 */
final class Utf8Vector
{
    // Table lookups index 16 entries, so the species must have at least 16 lanes
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED.length() >= 16 ? ByteVector.SPECIES_PREFERRED : ByteVector.SPECIES_128;
    private static final int LENGTH = SPECIES.length();
    private static final int UNROLLED_LENGTH = 4 * LENGTH;

    // Error classes. Each error is flagged when it appears in all three tables for a pair of
    // adjacent bytes, indexed by the high nibble of the first byte, the low nibble of the first
    // byte and the high nibble of the second byte.
    private static final byte TOO_SHORT = 1 << 0; // leading byte followed by a non continuation byte
    private static final byte TOO_LONG = 1 << 1; // ASCII byte followed by a continuation byte
    private static final byte OVERLONG_3 = 1 << 2; // E0 followed by 80..9F
    private static final byte SURROGATE = 1 << 4; // ED followed by A0..BF
    private static final byte OVERLONG_2 = 1 << 5; // C0 or C1
    private static final byte TWO_CONTS = (byte) (1 << 7); // two continuation bytes in a row, verified by the length check
    private static final byte TOO_LARGE = 1 << 3; // F4 followed by 90..BF, or F5..FF
    private static final byte TOO_LARGE_1000 = 1 << 6; // F8..FF followed by 80..8F, which TOO_LARGE cannot express
    private static final byte OVERLONG_4 = 1 << 6; // F0 followed by 80..8F
    private static final byte CARRY = TOO_SHORT | TOO_LONG | TWO_CONTS;

    private static final ByteVector BYTE_1_HIGH = table(
            TOO_LONG, TOO_LONG, TOO_LONG, TOO_LONG, TOO_LONG, TOO_LONG, TOO_LONG, TOO_LONG, // 0..7: ASCII
            TWO_CONTS, TWO_CONTS, TWO_CONTS, TWO_CONTS, // 8..B: continuation
            TOO_SHORT | OVERLONG_2, // C
            TOO_SHORT, // D
            TOO_SHORT | OVERLONG_3 | SURROGATE, // E
            TOO_SHORT | TOO_LARGE | TOO_LARGE_1000 | OVERLONG_4); // F

    private static final ByteVector BYTE_1_LOW = table(
            CARRY | OVERLONG_3 | OVERLONG_2 | OVERLONG_4, // 0
            CARRY | OVERLONG_2, // 1
            CARRY, CARRY, // 2..3
            CARRY | TOO_LARGE, // 4
            CARRY | TOO_LARGE | TOO_LARGE_1000, // 5
            CARRY | TOO_LARGE | TOO_LARGE_1000, // 6
            CARRY | TOO_LARGE | TOO_LARGE_1000, // 7
            CARRY | TOO_LARGE | TOO_LARGE_1000, // 8
            CARRY | TOO_LARGE | TOO_LARGE_1000, // 9
            CARRY | TOO_LARGE | TOO_LARGE_1000, // A
            CARRY | TOO_LARGE | TOO_LARGE_1000, // B
            CARRY | TOO_LARGE | TOO_LARGE_1000, // C
            CARRY | TOO_LARGE | TOO_LARGE_1000 | SURROGATE, // D
            CARRY | TOO_LARGE | TOO_LARGE_1000, // E
            CARRY | TOO_LARGE | TOO_LARGE_1000); // F

    private static final ByteVector BYTE_2_HIGH = table(
            TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT, // 0..7: ASCII
            TOO_LONG | OVERLONG_2 | TWO_CONTS | OVERLONG_3 | TOO_LARGE_1000 | OVERLONG_4, // 8
            TOO_LONG | OVERLONG_2 | TWO_CONTS | OVERLONG_3 | TOO_LARGE, // 9
            TOO_LONG | OVERLONG_2 | TWO_CONTS | SURROGATE | TOO_LARGE, // A
            TOO_LONG | OVERLONG_2 | TWO_CONTS | SURROGATE | TOO_LARGE, // B
            TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT); // C..F: leading

    // Subtracting these saturated leaves a non zero lane where a sequence started in the last three
    // bytes of a block cannot complete within it
    private static final ByteVector INCOMPLETE_LIMITS = incompleteLimits();

    // Indexed by the number of trailing bytes: move them to the start of the block and zero the rest
    private static final VectorShuffle<Byte>[] TAIL_SHUFFLES = tailShuffles();
    private static final VectorMask<Byte>[] TAIL_MASKS = tailMasks();

    private Utf8Vector() {}

    static boolean isUtf8(byte[] bytes, int offset, int length)
    {
        if (length < LENGTH) {
            return SliceUtf8.isUtf8Raw(bytes, offset, length);
        }

        int limit = offset + length;
        int loopBound = offset + SPECIES.loopBound(length);
        ByteVector previous = ByteVector.zero(SPECIES);
        ByteVector previousIncomplete = ByteVector.zero(SPECIES);
        ByteVector error = ByteVector.zero(SPECIES);

        // The block check is large once the Vector API is inlined, so it is called from a single
        // place: the loop also handles the trailing bytes as a zero padded block.
        int position = offset;
        while (true) {
            ByteVector block;
            if (position < loopBound) {
                block = ByteVector.fromArray(SPECIES, bytes, position);
            }
            else {
                int remaining = limit - position;
                if (remaining <= 0) {
                    // A sequence left open by the last block cannot complete
                    error = error.or(previousIncomplete);
                    break;
                }
                // Re-read the last block, then move out the bytes that were already validated and
                // zeroes in. Padding bytes are ASCII, so a truncated sequence is reported as too short.
                block = ByteVector.fromArray(SPECIES, bytes, limit - LENGTH)
                        .rearrange(TAIL_SHUFFLES[remaining], TAIL_MASKS[remaining]);
            }

            if (block.test(IS_NEGATIVE).anyTrue()) {
                error = error.or(check(block, previous));
                previousIncomplete = block.lanewise(SUSUB, INCOMPLETE_LIMITS);
                previous = block;
                position += LENGTH;
                continue;
            }

            // An ASCII block cannot complete a sequence left open by the previous block
            error = error.or(previousIncomplete);
            previousIncomplete = ByteVector.zero(SPECIES);
            previous = block;
            position += LENGTH;

            // Most text is ASCII, so skip runs of ASCII several blocks at a time
            while (position <= loopBound - UNROLLED_LENGTH) {
                ByteVector block0 = ByteVector.fromArray(SPECIES, bytes, position);
                ByteVector block1 = ByteVector.fromArray(SPECIES, bytes, position + LENGTH);
                ByteVector block2 = ByteVector.fromArray(SPECIES, bytes, position + 2 * LENGTH);
                ByteVector block3 = ByteVector.fromArray(SPECIES, bytes, position + 3 * LENGTH);
                if (block0.or(block1).or(block2).or(block3).test(IS_NEGATIVE).anyTrue()) {
                    break;
                }
                previous = block3;
                position += UNROLLED_LENGTH;
            }
        }
        return !error.compare(NE, (byte) 0).anyTrue();
    }

    /**
     * Returns non zero lanes where the block, read after the previous block, contains invalid UTF-8.
     */
    private static ByteVector check(ByteVector block, ByteVector previous)
    {
        ByteVector previous1 = previous.slice(LENGTH - 1, block);
        ByteVector previous2 = previous.slice(LENGTH - 2, block);
        ByteVector previous3 = previous.slice(LENGTH - 3, block);

        // Classify pairs of adjacent bytes. This finds every error except for the third and
        // fourth bytes of a sequence, which only look like two continuation bytes in a row.
        ByteVector byte1High = previous1.lanewise(LSHR, 4).selectFrom(BYTE_1_HIGH);
        ByteVector byte1Low = previous1.and((byte) 0x0F).selectFrom(BYTE_1_LOW);
        ByteVector byte2High = block.lanewise(LSHR, 4).selectFrom(BYTE_2_HIGH);
        ByteVector specialCases = byte1High.and(byte1Low).and(byte2High);

        // A byte must be the third byte of a sequence exactly when a 111x_xxxx byte is two positions
        // back, and the fourth byte exactly when a 1111_xxxx byte is three positions back. The
        // saturating subtractions leave 1..0x20 in those lanes, which adding 0x7F turns into the
        // TWO_CONTS bit. Mismatches in either direction survive the exclusive or.
        ByteVector thirdOrFourthByte = previous2.lanewise(SUSUB, (byte) 0xDF).or(previous3.lanewise(SUSUB, (byte) 0xEF));
        ByteVector expectedTwoConts = thirdOrFourthByte.add((byte) 0x7F).and(TWO_CONTS);
        return expectedTwoConts.lanewise(XOR, specialCases);
    }

    private static ByteVector table(int... entries)
    {
        byte[] values = new byte[LENGTH];
        for (int i = 0; i < LENGTH; i++) {
            values[i] = (byte) entries[i % 16];
        }
        return ByteVector.fromArray(SPECIES, values, 0);
    }

    @SuppressWarnings("unchecked")
    private static VectorShuffle<Byte>[] tailShuffles()
    {
        VectorShuffle<Byte>[] shuffles = new VectorShuffle[LENGTH];
        for (int remaining = 1; remaining < LENGTH; remaining++) {
            shuffles[remaining] = VectorShuffle.iota(SPECIES, LENGTH - remaining, 1, true);
        }
        return shuffles;
    }

    @SuppressWarnings("unchecked")
    private static VectorMask<Byte>[] tailMasks()
    {
        VectorMask<Byte>[] masks = new VectorMask[LENGTH];
        for (int remaining = 1; remaining < LENGTH; remaining++) {
            masks[remaining] = SPECIES.indexInRange(0, remaining);
        }
        return masks;
    }

    private static ByteVector incompleteLimits()
    {
        byte[] values = new byte[LENGTH];
        Arrays.fill(values, (byte) 0xFF);
        values[LENGTH - 3] = (byte) (0xF0 - 1);
        values[LENGTH - 2] = (byte) (0xE0 - 1);
        values[LENGTH - 1] = (byte) (0xC0 - 1);
        return ByteVector.fromArray(SPECIES, values, 0);
    }
}
