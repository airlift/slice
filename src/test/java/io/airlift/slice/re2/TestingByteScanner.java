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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

final class TestingByteScanner
{
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    private static final long LOW_BITS = 0x0101010101010101L;
    private static final long HIGH_BITS = 0x8080808080808080L;

    private TestingByteScanner() {}

    static int indexOfScalar(byte[] data, int offset, int length, byte first, byte second, byte third, int candidateCount)
    {
        return switch (candidateCount) {
            case 1 -> indexOfOneScalar(data, offset, length, first);
            case 2 -> indexOfTwoScalar(data, offset, length, first, second);
            case 3 -> indexOfThreeScalar(data, offset, length, first, second, third);
            default -> throw new IllegalArgumentException("candidateCount must be between one and three");
        };
    }

    static int indexOfSwar(byte[] data, int offset, int length, byte first, byte second, byte third, int candidateCount)
    {
        return switch (candidateCount) {
            case 1 -> indexOfOneSwar(data, offset, length, first);
            case 2 -> indexOfTwoSwar(data, offset, length, first, second);
            case 3 -> indexOfThreeSwar(data, offset, length, first, second, third);
            default -> throw new IllegalArgumentException("candidateCount must be between one and three");
        };
    }

    static int indexOfVector(byte[] data, int offset, int length, byte first, byte second, byte third, int candidateCount, VectorSpecies<Byte> species)
    {
        return switch (candidateCount) {
            case 1 -> indexOfOneVector(data, offset, length, first, species);
            case 2 -> indexOfTwoVector(data, offset, length, first, second, species);
            case 3 -> indexOfThreeVector(data, offset, length, first, second, third, species);
            default -> throw new IllegalArgumentException("candidateCount must be between one and three");
        };
    }

    static int indexOfHybrid(
            byte[] data,
            int offset,
            int length,
            byte first,
            byte second,
            byte third,
            int candidateCount,
            int swarProbeLength,
            VectorSpecies<Byte> species)
    {
        int probeLength = Math.min(length, swarProbeLength);
        int candidate = indexOfSwar(data, offset, probeLength, first, second, third, candidateCount);
        if (candidate >= 0) {
            return candidate;
        }
        return indexOfVector(
                data,
                offset + probeLength,
                length - probeLength,
                first,
                second,
                third,
                candidateCount,
                species);
    }

    static int indexOfLiteralSwar(byte[] data, int offset, int length, byte[] literal, int firstOffset, int secondOffset)
    {
        int candidateCount = length - literal.length + 1;
        if (candidateCount <= 0) {
            return -1;
        }

        long firstBroadcast = broadcast(literal[firstOffset]);
        long secondBroadcast = broadcast(literal[secondOffset]);
        int wordEnd = offset + (candidateCount / Long.BYTES) * Long.BYTES;
        int position = offset;
        for (; position < wordEnd; position += Long.BYTES) {
            long firstBytes = (long) LONG_HANDLE.get(data, position + firstOffset);
            long secondBytes = (long) LONG_HANDLE.get(data, position + secondOffset);
            long candidates = matchingBytes(firstBytes, firstBroadcast) & matchingBytes(secondBytes, secondBroadcast);
            while (candidates != 0) {
                int candidate = position + (Long.numberOfTrailingZeros(candidates) >>> 3);
                if (literalMatches(data, candidate, literal)) {
                    return candidate;
                }
                candidates &= candidates - 1;
            }
        }
        return indexOfLiteralScalar(data, position, offset + candidateCount - position, literal, firstOffset, secondOffset);
    }

    static int indexOfLiteralRepeatedSwar(byte[] data, int offset, int length, byte[] literal)
    {
        int lastStart = offset + length - literal.length;
        int position = offset;
        while (position <= lastStart) {
            int candidate = indexOfSwar(data, position, lastStart + 1 - position, literal[0], (byte) 0, (byte) 0, 1);
            if (candidate < 0) {
                return -1;
            }
            if (data[candidate + literal.length - 1] == literal[literal.length - 1] && literalMatches(data, candidate, literal)) {
                return candidate;
            }
            position = candidate + 1;
        }
        return -1;
    }

    static int indexOfLiteralVector(byte[] data, int offset, int length, byte[] literal, int firstOffset, int secondOffset, VectorSpecies<Byte> species)
    {
        int candidateCount = length - literal.length + 1;
        if (candidateCount <= 0) {
            return -1;
        }

        ByteVector firstVector = ByteVector.broadcast(species, literal[firstOffset]);
        ByteVector secondVector = ByteVector.broadcast(species, literal[secondOffset]);
        int vectorEnd = offset + species.loopBound(candidateCount);
        int position = offset;
        for (; position < vectorEnd; position += species.length()) {
            VectorMask<Byte> candidates = ByteVector.fromArray(species, data, position + firstOffset)
                    .compare(VectorOperators.EQ, firstVector)
                    .and(ByteVector.fromArray(species, data, position + secondOffset)
                            .compare(VectorOperators.EQ, secondVector));
            long candidateBits = candidates.toLong();
            while (candidateBits != 0) {
                int candidate = position + Long.numberOfTrailingZeros(candidateBits);
                if (literalMatches(data, candidate, literal)) {
                    return candidate;
                }
                candidateBits &= candidateBits - 1;
            }
        }
        return indexOfLiteralScalar(data, position, offset + candidateCount - position, literal, firstOffset, secondOffset);
    }

    static int indexOfLiteralScalar(byte[] data, int offset, int candidateCount, byte[] literal, int firstOffset, int secondOffset)
    {
        int end = offset + candidateCount;
        for (int position = offset; position < end; position++) {
            if (data[position + firstOffset] == literal[firstOffset] &&
                    data[position + secondOffset] == literal[secondOffset] &&
                    literalMatches(data, position, literal)) {
                return position;
            }
        }
        return -1;
    }

    private static boolean literalMatches(byte[] data, int position, byte[] literal)
    {
        for (int index = 0; index < literal.length; index++) {
            if (data[position + index] != literal[index]) {
                return false;
            }
        }
        return true;
    }

    private static int indexOfOneScalar(byte[] data, int offset, int length, byte first)
    {
        int end = offset + length;
        for (int position = offset; position < end; position++) {
            if (data[position] == first) {
                return position;
            }
        }
        return -1;
    }

    private static int indexOfTwoScalar(byte[] data, int offset, int length, byte first, byte second)
    {
        int end = offset + length;
        for (int position = offset; position < end; position++) {
            byte value = data[position];
            if (value == first || value == second) {
                return position;
            }
        }
        return -1;
    }

    private static int indexOfThreeScalar(byte[] data, int offset, int length, byte first, byte second, byte third)
    {
        int end = offset + length;
        for (int position = offset; position < end; position++) {
            byte value = data[position];
            if (value == first || value == second || value == third) {
                return position;
            }
        }
        return -1;
    }

    private static int indexOfOneSwar(byte[] data, int offset, int length, byte first)
    {
        long firstBroadcast = broadcast(first);
        int wordEnd = offset + ((length / Long.BYTES) * Long.BYTES);
        int position = offset;
        for (; position < wordEnd; position += Long.BYTES) {
            long matches = matchingBytes((long) LONG_HANDLE.get(data, position), firstBroadcast);
            if (matches != 0) {
                return position + (Long.numberOfTrailingZeros(matches) >>> 3);
            }
        }
        return indexOfOneScalar(data, position, offset + length - position, first);
    }

    private static int indexOfTwoSwar(byte[] data, int offset, int length, byte first, byte second)
    {
        long firstBroadcast = broadcast(first);
        long secondBroadcast = broadcast(second);
        int wordEnd = offset + ((length / Long.BYTES) * Long.BYTES);
        int position = offset;
        for (; position < wordEnd; position += Long.BYTES) {
            long word = (long) LONG_HANDLE.get(data, position);
            long matches = matchingBytes(word, firstBroadcast) | matchingBytes(word, secondBroadcast);
            if (matches != 0) {
                return position + (Long.numberOfTrailingZeros(matches) >>> 3);
            }
        }
        return indexOfTwoScalar(data, position, offset + length - position, first, second);
    }

    private static int indexOfThreeSwar(byte[] data, int offset, int length, byte first, byte second, byte third)
    {
        long firstBroadcast = broadcast(first);
        long secondBroadcast = broadcast(second);
        long thirdBroadcast = broadcast(third);
        int wordEnd = offset + ((length / Long.BYTES) * Long.BYTES);
        int position = offset;
        for (; position < wordEnd; position += Long.BYTES) {
            long word = (long) LONG_HANDLE.get(data, position);
            long matches = matchingBytes(word, firstBroadcast) |
                    matchingBytes(word, secondBroadcast) |
                    matchingBytes(word, thirdBroadcast);
            if (matches != 0) {
                return position + (Long.numberOfTrailingZeros(matches) >>> 3);
            }
        }
        return indexOfThreeScalar(data, position, offset + length - position, first, second, third);
    }

    private static int indexOfOneVector(byte[] data, int offset, int length, byte first, VectorSpecies<Byte> species)
    {
        ByteVector firstVector = ByteVector.broadcast(species, first);
        int vectorEnd = offset + species.loopBound(length);
        int position = offset;
        for (; position < vectorEnd; position += species.length()) {
            VectorMask<Byte> matches = ByteVector.fromArray(species, data, position)
                    .compare(VectorOperators.EQ, firstVector);
            if (matches.anyTrue()) {
                return position + matches.firstTrue();
            }
        }
        return indexOfOneScalar(data, position, offset + length - position, first);
    }

    private static int indexOfTwoVector(byte[] data, int offset, int length, byte first, byte second, VectorSpecies<Byte> species)
    {
        ByteVector firstVector = ByteVector.broadcast(species, first);
        ByteVector secondVector = ByteVector.broadcast(species, second);
        int vectorEnd = offset + species.loopBound(length);
        int position = offset;
        for (; position < vectorEnd; position += species.length()) {
            ByteVector values = ByteVector.fromArray(species, data, position);
            VectorMask<Byte> matches = values.compare(VectorOperators.EQ, firstVector)
                    .or(values.compare(VectorOperators.EQ, secondVector));
            if (matches.anyTrue()) {
                return position + matches.firstTrue();
            }
        }
        return indexOfTwoScalar(data, position, offset + length - position, first, second);
    }

    private static int indexOfThreeVector(byte[] data, int offset, int length, byte first, byte second, byte third, VectorSpecies<Byte> species)
    {
        ByteVector firstVector = ByteVector.broadcast(species, first);
        ByteVector secondVector = ByteVector.broadcast(species, second);
        ByteVector thirdVector = ByteVector.broadcast(species, third);
        int vectorEnd = offset + species.loopBound(length);
        int position = offset;
        for (; position < vectorEnd; position += species.length()) {
            ByteVector values = ByteVector.fromArray(species, data, position);
            VectorMask<Byte> matches = values.compare(VectorOperators.EQ, firstVector)
                    .or(values.compare(VectorOperators.EQ, secondVector))
                    .or(values.compare(VectorOperators.EQ, thirdVector));
            if (matches.anyTrue()) {
                return position + matches.firstTrue();
            }
        }
        return indexOfThreeScalar(data, position, offset + length - position, first, second, third);
    }

    private static long broadcast(byte value)
    {
        return (value & 0xFFL) * LOW_BITS;
    }

    private static long matchingBytes(long word, long broadcast)
    {
        long difference = word ^ broadcast;
        return (difference - LOW_BITS) & ~difference & HIGH_BITS;
    }
}
