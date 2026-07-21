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

import java.nio.charset.StandardCharsets;
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
public class BenchmarkLiteralScanner
{
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"ENGLISH", "RUSSIAN", "CHINESE"})
        Language language;

        @Param({"64", "256", "1024", "32768"})
        int sourceLength;

        @Param
        InputShape inputShape;

        @Param
        OffsetSelection offsetSelection;

        private byte[] source;
        private byte[] literal;
        private int firstOffset;
        private int secondOffset;
        private int expectedIndex;

        @Setup
        public void setup()
        {
            literal = language.literal().getBytes(StandardCharsets.UTF_8);
            firstOffset = offsetSelection.firstOffset(language);
            secondOffset = offsetSelection.secondOffset(language);
            source = new byte[sourceLength + literal.length + 64];
            Arrays.fill(source, language.backgroundByte());

            expectedIndex = switch (inputShape) {
                case ABSENT -> -1;
                case EARLY -> insertLiteral(1);
                case LATE -> insertLiteral(sourceLength - literal.length);
                case DENSE_FIRST_BYTE_FALSE -> {
                    for (int position = 0; position < sourceLength - literal.length; position += 2) {
                        source[position + firstOffset] = literal[firstOffset];
                    }
                    yield -1;
                }
                case DENSE_TWO_OFFSET_FALSE -> {
                    for (int position = 0; position < sourceLength - literal.length; position += literal.length) {
                        source[position + firstOffset] = literal[firstOffset];
                        source[position + secondOffset] = literal[secondOffset];
                    }
                    yield -1;
                }
            };
        }

        int expectedIndex()
        {
            return expectedIndex;
        }

        private int insertLiteral(int position)
        {
            if (position < 0 || position + literal.length > sourceLength) {
                return -1;
            }
            System.arraycopy(literal, 0, source, position, literal.length);
            return position;
        }
    }

    public enum Language
    {
        ENGLISH("Sherlock Holmes", (byte) 'e'),
        RUSSIAN("Шерлок Холмс", (byte) 0xD0),
        CHINESE("夏洛克·福尔摩斯", (byte) 0xE6);

        private final String literal;
        private final byte backgroundByte;

        Language(String literal, byte backgroundByte)
        {
            this.literal = literal;
            this.backgroundByte = backgroundByte;
        }

        String literal()
        {
            return literal;
        }

        byte backgroundByte()
        {
            return backgroundByte;
        }
    }

    public enum InputShape
    {
        ABSENT,
        EARLY,
        LATE,
        DENSE_FIRST_BYTE_FALSE,
        DENSE_TWO_OFFSET_FALSE,
    }

    public enum OffsetSelection
    {
        FRONT_BACK,
        TWO_RAREST;

        int firstOffset(Language language)
        {
            return switch (this) {
                case FRONT_BACK -> 0;
                case TWO_RAREST -> switch (language) {
                    case ENGLISH -> 9;
                    case RUSSIAN -> 1;
                    case CHINESE -> 9;
                };
            };
        }

        int secondOffset(Language language)
        {
            return switch (this) {
                case FRONT_BACK -> language.literal().getBytes(StandardCharsets.UTF_8).length - 1;
                case TWO_RAREST -> switch (language) {
                    case ENGLISH -> 0;
                    case RUSSIAN -> 14;
                    case CHINESE -> 19;
                };
            };
        }
    }

    @Benchmark
    public int scalar(BenchmarkData data)
    {
        return TestingByteScanner.indexOfLiteralScalar(data.source, 0, data.sourceLength - data.literal.length + 1, data.literal, data.firstOffset, data.secondOffset);
    }

    @Benchmark
    public int repeatedSwar(BenchmarkData data)
    {
        return TestingByteScanner.indexOfLiteralRepeatedSwar(data.source, 0, data.sourceLength, data.literal);
    }

    @Benchmark
    public int swar(BenchmarkData data)
    {
        return TestingByteScanner.indexOfLiteralSwar(data.source, 0, data.sourceLength, data.literal, data.firstOffset, data.secondOffset);
    }

    @Benchmark
    public int vector128(BenchmarkData data)
    {
        return TestingByteScanner.indexOfLiteralVector(data.source, 0, data.sourceLength, data.literal, data.firstOffset, data.secondOffset, SPECIES_128);
    }

    @Benchmark
    public int vector256(BenchmarkData data)
    {
        return TestingByteScanner.indexOfLiteralVector(data.source, 0, data.sourceLength, data.literal, data.firstOffset, data.secondOffset, SPECIES_256);
    }

    @Benchmark
    public int vector512(BenchmarkData data)
    {
        return TestingByteScanner.indexOfLiteralVector(data.source, 0, data.sourceLength, data.literal, data.firstOffset, data.secondOffset, SPECIES_512);
    }

    @Benchmark
    public int vectorPreferred(BenchmarkData data)
    {
        return TestingByteScanner.indexOfLiteralVector(data.source, 0, data.sourceLength, data.literal, data.firstOffset, data.secondOffset, SPECIES_PREFERRED);
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = buildOptions(BenchmarkLiteralScanner.class, args);
        new Runner(options).run();
    }
}
