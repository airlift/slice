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
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static java.lang.Long.numberOfTrailingZeros;

/**
 * Benchmarks the shipped scalar byte-scanning routines ({@link Slice#indexOfByte(byte)} and
 * {@link Slice#indexOfAnyByte}) against a Vector API (SIMD) kernel with a scalar SWAR tail.
 *
 * <p>The scalar variants are what the library ships (the SIMD path is not merged into {@code Slice}
 * because it would require every consumer to pass {@code --add-modules=jdk.incubator.vector}); the
 * vector variants document the SIMD ceiling that becomes available if/when the Vector API is
 * finalized or a consumer opts in.
 *
 * <p>Worst-case setup: the needle only appears at the very last position, forcing a full scan.
 *
 * <p>Requires {@code --add-modules=jdk.incubator.vector} in the forked JVM (set via {@code @Fork}).
 */
@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1, jvmArgsAppend = "--add-modules=jdk.incubator.vector")
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkOptimizations
{
    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;

    @Benchmark
    public int indexOfByteScalar(BenchmarkData data)
    {
        return data.slice.indexOfByte(data.needle);
    }

    @Benchmark
    public int indexOfByteVector(BenchmarkData data)
    {
        return indexOfByteVector(data.bytes, 0, data.bytes.length, data.needle);
    }

    @Benchmark
    public int indexOfAnyByteScalar(BenchmarkData data)
    {
        return data.slice.indexOfAnyByte(data.needle, data.needle2, 0);
    }

    @Benchmark
    public int indexOfAnyByteVector(BenchmarkData data)
    {
        return indexOfAnyByteVector(data.bytes, 0, data.bytes.length, data.needle, data.needle2);
    }

    static int indexOfByteVector(byte[] base, int offset, int end, byte value)
    {
        int bound = end - BYTE_SPECIES.length();
        for (; offset <= bound; offset += BYTE_SPECIES.length()) {
            ByteVector chunk = ByteVector.fromArray(BYTE_SPECIES, base, offset);
            VectorMask<Byte> matches = chunk.compare(VectorOperators.EQ, value);
            if (matches.anyTrue()) {
                return offset + matches.firstTrue();
            }
        }

        // SWAR tail
        long pattern = (value & 0xFFL) * 0x01010101_01010101L;
        for (; offset <= end - 8; offset += 8) {
            long matches = match((long) LONG_HANDLE.get(base, offset), pattern);
            if (matches != 0) {
                return offset + (numberOfTrailingZeros(matches) >>> 3);
            }
        }
        for (; offset < end; offset++) {
            if (base[offset] == value) {
                return offset;
            }
        }
        return -1;
    }

    static int indexOfAnyByteVector(byte[] base, int offset, int end, byte first, byte second)
    {
        int bound = end - BYTE_SPECIES.length();
        for (; offset <= bound; offset += BYTE_SPECIES.length()) {
            ByteVector chunk = ByteVector.fromArray(BYTE_SPECIES, base, offset);
            VectorMask<Byte> matches = chunk.compare(VectorOperators.EQ, first)
                    .or(chunk.compare(VectorOperators.EQ, second));
            if (matches.anyTrue()) {
                return offset + matches.firstTrue();
            }
        }

        long firstPattern = (first & 0xFFL) * 0x01010101_01010101L;
        long secondPattern = (second & 0xFFL) * 0x01010101_01010101L;
        for (; offset <= end - 8; offset += 8) {
            long value = (long) LONG_HANDLE.get(base, offset);
            long matches = match(value, firstPattern) | match(value, secondPattern);
            if (matches != 0) {
                return offset + (numberOfTrailingZeros(matches) >>> 3);
            }
        }
        for (; offset < end; offset++) {
            byte current = base[offset];
            if (current == first || current == second) {
                return offset;
            }
        }
        return -1;
    }

    private static final java.lang.invoke.VarHandle LONG_HANDLE =
            java.lang.invoke.MethodHandles.byteArrayViewVarHandle(long[].class, java.nio.ByteOrder.LITTLE_ENDIAN);

    private static long match(long value, long pattern)
    {
        long xor = value ^ pattern;
        return (xor - 0x01010101_01010101L) & ~xor & 0x80808080_80808080L;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"7", "16", "32", "64", "127", "1024", "32779"})
        private int size;

        private byte[] bytes;
        private Slice slice;
        private final byte needle = (byte) 0xAB;
        private final byte needle2 = (byte) 0xCD;

        @Setup(Level.Iteration)
        public void setup()
        {
            bytes = new byte[size];
            // fill with a value that is neither needle; needle appears only at the last position
            Arrays.fill(bytes, (byte) 0x01);
            bytes[size - 1] = needle;
            slice = Slices.wrappedBuffer(bytes);
        }
    }

    static void main()
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkOptimizations.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
