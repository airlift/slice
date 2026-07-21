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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

import static jdk.incubator.vector.ByteVector.SPECIES_128;
import static jdk.incubator.vector.ByteVector.SPECIES_256;
import static jdk.incubator.vector.ByteVector.SPECIES_512;
import static jdk.incubator.vector.ByteVector.SPECIES_PREFERRED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBenchmarkByteScanner
{
    @Test
    public void testBenchmarkMethods()
    {
        BenchmarkByteScanner benchmark = new BenchmarkByteScanner();
        for (BenchmarkByteScanner.InputShape inputShape : BenchmarkByteScanner.InputShape.values()) {
            for (int candidateCount = 1; candidateCount <= 3; candidateCount++) {
                BenchmarkByteScanner.BenchmarkData data = new BenchmarkByteScanner.BenchmarkData();
                data.inputShape = inputShape;
                data.candidateCount = candidateCount;
                data.sourceLength = 128;
                data.arrayOffset = 7;
                data.setup();

                assertThat(benchmark.scalar(data)).isEqualTo(data.expectedIndex());
                assertThat(benchmark.swar(data)).isEqualTo(data.expectedIndex());
                assertThat(benchmark.vector128(data)).isEqualTo(data.expectedIndex());
                assertThat(benchmark.hybrid8Vector128(data)).isEqualTo(data.expectedIndex());
                assertThat(benchmark.hybrid16Vector128(data)).isEqualTo(data.expectedIndex());
                assertThat(benchmark.vector256(data)).isEqualTo(data.expectedIndex());
                assertThat(benchmark.vector512(data)).isEqualTo(data.expectedIndex());
                assertThat(benchmark.vectorPreferred(data)).isEqualTo(data.expectedIndex());
            }
        }
    }

    @Test
    public void testScannerAgreement()
    {
        Random random = new Random(0);
        byte[] data = new byte[320];
        for (int iteration = 0; iteration < 1_000; iteration++) {
            random.nextBytes(data);
            int offset = random.nextInt(16);
            int length = random.nextInt(257);
            byte first = (byte) random.nextInt(256);
            byte second = (byte) random.nextInt(256);
            byte third = (byte) random.nextInt(256);

            for (int candidateCount = 1; candidateCount <= 3; candidateCount++) {
                int expected = TestingByteScanner.indexOfScalar(data, offset, length, first, second, third, candidateCount);
                assertThat(TestingByteScanner.indexOfSwar(data, offset, length, first, second, third, candidateCount)).isEqualTo(expected);
                assertThat(TestingByteScanner.indexOfVector(data, offset, length, first, second, third, candidateCount, SPECIES_128)).isEqualTo(expected);
                assertThat(TestingByteScanner.indexOfHybrid(data, offset, length, first, second, third, candidateCount, 8, SPECIES_128)).isEqualTo(expected);
                assertThat(TestingByteScanner.indexOfHybrid(data, offset, length, first, second, third, candidateCount, 16, SPECIES_128)).isEqualTo(expected);
                assertThat(TestingByteScanner.indexOfVector(data, offset, length, first, second, third, candidateCount, SPECIES_256)).isEqualTo(expected);
                assertThat(TestingByteScanner.indexOfVector(data, offset, length, first, second, third, candidateCount, SPECIES_512)).isEqualTo(expected);
                assertThat(TestingByteScanner.indexOfVector(data, offset, length, first, second, third, candidateCount, SPECIES_PREFERRED)).isEqualTo(expected);
            }
        }
    }

    @Test
    public void testLiteralScannerAgreement()
    {
        Random random = new Random(2);
        for (String value : List.of("Sherlock Holmes", "Шерлок Холмс", "夏洛克·福尔摩斯")) {
            byte[] literal = value.getBytes(StandardCharsets.UTF_8);
            for (int iteration = 0; iteration < 1_000; iteration++) {
                int offset = random.nextInt(8);
                int length = random.nextInt(513);
                byte[] source = new byte[offset + length + literal.length + 64];
                random.nextBytes(source);
                if (length >= literal.length && random.nextBoolean()) {
                    int insertionPosition = offset + random.nextInt(length - literal.length + 1);
                    System.arraycopy(literal, 0, source, insertionPosition, literal.length);
                }

                int firstOffset = random.nextInt(literal.length - 1);
                int secondOffset = firstOffset + 1 + random.nextInt(literal.length - firstOffset - 1);
                int expected = TestingByteScanner.indexOfLiteralScalar(source, offset, length - literal.length + 1, literal, firstOffset, secondOffset);
                assertThat(TestingByteScanner.indexOfLiteralRepeatedSwar(source, offset, length, literal)).isEqualTo(expected);
                assertThat(TestingByteScanner.indexOfLiteralSwar(source, offset, length, literal, firstOffset, secondOffset)).isEqualTo(expected);
                assertThat(TestingByteScanner.indexOfLiteralVector(source, offset, length, literal, firstOffset, secondOffset, SPECIES_128)).isEqualTo(expected);
                assertThat(TestingByteScanner.indexOfLiteralVector(source, offset, length, literal, firstOffset, secondOffset, SPECIES_256)).isEqualTo(expected);
                assertThat(TestingByteScanner.indexOfLiteralVector(source, offset, length, literal, firstOffset, secondOffset, SPECIES_512)).isEqualTo(expected);
            }
        }
    }
}
