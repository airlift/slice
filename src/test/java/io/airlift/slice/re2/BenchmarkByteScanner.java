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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.re2.Re2BenchmarkRunner.buildOptions;
import static jdk.incubator.vector.ByteVector.SPECIES_128;
import static jdk.incubator.vector.ByteVector.SPECIES_256;
import static jdk.incubator.vector.ByteVector.SPECIES_512;
import static jdk.incubator.vector.ByteVector.SPECIES_PREFERRED;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkByteScanner
{
    private static final byte FIRST_CANDIDATE = (byte) 0xA5;
    private static final byte SECOND_CANDIDATE = 0;
    private static final byte THIRD_CANDIDATE = (byte) 0xFF;

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1", "2", "3"})
        int candidateCount;

        @Param({"16", "32", "64", "128", "256", "1024", "32768"})
        int sourceLength;

        @Param
        InputShape inputShape;

        @Param({"0", "1", "7"})
        int arrayOffset;

        private byte[] source;
        private int expectedIndex;

        @Setup
        public void setup()
        {
            source = new byte[arrayOffset + sourceLength + 64];
            Arrays.fill(source, (byte) 0x5A);
            expectedIndex = switch (inputShape) {
                case ABSENT -> -1;
                case EARLY -> placeCandidate(1);
                case LATE -> placeCandidate(sourceLength - 1);
                case SPARSE -> {
                    int firstPosition = Math.max(1, sourceLength / 4);
                    placeCandidate(firstPosition);
                    placeCandidate(sourceLength / 2);
                    placeCandidate(Math.max(firstPosition, sourceLength * 3 / 4));
                    yield arrayOffset + firstPosition;
                }
                case DENSE -> {
                    for (int position = 1; position < sourceLength; position += 4) {
                        placeCandidate(position);
                    }
                    yield sourceLength > 1 ? arrayOffset + 1 : -1;
                }
            };
        }

        int expectedIndex()
        {
            return expectedIndex;
        }

        private int placeCandidate(int relativePosition)
        {
            if (relativePosition < 0 || relativePosition >= sourceLength) {
                return -1;
            }
            source[arrayOffset + relativePosition] = switch (relativePosition % candidateCount) {
                case 0 -> FIRST_CANDIDATE;
                case 1 -> SECOND_CANDIDATE;
                default -> THIRD_CANDIDATE;
            };
            return arrayOffset + relativePosition;
        }
    }

    public enum InputShape
    {
        ABSENT,
        EARLY,
        LATE,
        SPARSE,
        DENSE,
    }

    @Benchmark
    public int scalar(BenchmarkData data)
    {
        return TestingByteScanner.indexOfScalar(
                data.source,
                data.arrayOffset,
                data.sourceLength,
                FIRST_CANDIDATE,
                SECOND_CANDIDATE,
                THIRD_CANDIDATE,
                data.candidateCount);
    }

    @Benchmark
    public int swar(BenchmarkData data)
    {
        return TestingByteScanner.indexOfSwar(
                data.source,
                data.arrayOffset,
                data.sourceLength,
                FIRST_CANDIDATE,
                SECOND_CANDIDATE,
                THIRD_CANDIDATE,
                data.candidateCount);
    }

    @Benchmark
    public int vector128(BenchmarkData data)
    {
        return TestingByteScanner.indexOfVector(
                data.source,
                data.arrayOffset,
                data.sourceLength,
                FIRST_CANDIDATE,
                SECOND_CANDIDATE,
                THIRD_CANDIDATE,
                data.candidateCount,
                SPECIES_128);
    }

    @Benchmark
    public int hybrid8Vector128(BenchmarkData data)
    {
        return TestingByteScanner.indexOfHybrid(
                data.source,
                data.arrayOffset,
                data.sourceLength,
                FIRST_CANDIDATE,
                SECOND_CANDIDATE,
                THIRD_CANDIDATE,
                data.candidateCount,
                8,
                SPECIES_128);
    }

    @Benchmark
    public int hybrid16Vector128(BenchmarkData data)
    {
        return TestingByteScanner.indexOfHybrid(
                data.source,
                data.arrayOffset,
                data.sourceLength,
                FIRST_CANDIDATE,
                SECOND_CANDIDATE,
                THIRD_CANDIDATE,
                data.candidateCount,
                16,
                SPECIES_128);
    }

    @Benchmark
    public int vector256(BenchmarkData data)
    {
        return TestingByteScanner.indexOfVector(
                data.source,
                data.arrayOffset,
                data.sourceLength,
                FIRST_CANDIDATE,
                SECOND_CANDIDATE,
                THIRD_CANDIDATE,
                data.candidateCount,
                SPECIES_256);
    }

    @Benchmark
    public int vector512(BenchmarkData data)
    {
        return TestingByteScanner.indexOfVector(
                data.source,
                data.arrayOffset,
                data.sourceLength,
                FIRST_CANDIDATE,
                SECOND_CANDIDATE,
                THIRD_CANDIDATE,
                data.candidateCount,
                SPECIES_512);
    }

    @Benchmark
    public int vectorPreferred(BenchmarkData data)
    {
        return TestingByteScanner.indexOfVector(
                data.source,
                data.arrayOffset,
                data.sourceLength,
                FIRST_CANDIDATE,
                SECOND_CANDIDATE,
                THIRD_CANDIDATE,
                data.candidateCount,
                SPECIES_PREFERRED);
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = buildOptions(BenchmarkByteScanner.class, args);
        new Runner(options).run();
    }
}
