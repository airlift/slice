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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

final class VectorPrefixScanner
{
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;

    private VectorPrefixScanner() {}

    // PERFORMANCE-SENSITIVE HOT LOOP: changes here require target-host assembly
    // and benchmark evidence. Keep Vector API linkage isolated in this class.
    static int find(byte[] data, int offset, int length, byte[] prefix, int primaryOffset, int secondaryOffset)
    {
        int prefixSize = prefix.length;
        int candidateCount = length - prefixSize + 1;
        if (candidateCount <= 0) {
            return -1;
        }

        int vectorEnd = offset + SPECIES.loopBound(candidateCount);
        ByteVector primaryVector = ByteVector.broadcast(SPECIES, prefix[primaryOffset]);
        ByteVector secondaryVector = ByteVector.broadcast(SPECIES, prefix[secondaryOffset]);
        int position = offset;

        for (; position < vectorEnd; position += SPECIES.length()) {
            VectorMask<Byte> candidates = ByteVector.fromArray(SPECIES, data, position + primaryOffset)
                    .compare(VectorOperators.EQ, primaryVector)
                    .and(ByteVector.fromArray(SPECIES, data, position + secondaryOffset)
                            .compare(VectorOperators.EQ, secondaryVector));
            long candidateBits = candidates.toLong();
            while (candidateBits != 0) {
                int candidate = position + Long.numberOfTrailingZeros(candidateBits);
                // Keep sparse verification in this method. When composed with the DFA,
                // C2 otherwise treats this call site as cold and leaves it out of line.
                int prefixIndex = 0;
                while (prefixIndex < prefix.length && data[candidate + prefixIndex] == prefix[prefixIndex]) {
                    prefixIndex++;
                }
                if (prefixIndex == prefix.length) {
                    return candidate;
                }
                candidateBits &= candidateBits - 1;
            }
        }
        return findScalar(data, position, offset + candidateCount - position, prefix, primaryOffset, secondaryOffset);
    }

    private static int findScalar(byte[] data, int offset, int candidateCount, byte[] prefix, int primaryOffset, int secondaryOffset)
    {
        int end = offset + candidateCount;
        for (int position = offset; position < end; position++) {
            if (data[position + primaryOffset] == prefix[primaryOffset] &&
                    data[position + secondaryOffset] == prefix[secondaryOffset] &&
                    prefixMatchesAt(data, position, prefix)) {
                return position;
            }
        }
        return -1;
    }

    private static boolean prefixMatchesAt(byte[] data, int position, byte[] prefix)
    {
        for (int index = 0; index < prefix.length; index++) {
            if (data[position + index] != prefix[index]) {
                return false;
            }
        }
        return true;
    }
}
