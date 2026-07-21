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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.nio.charset.StandardCharsets;

/**
 * Shared utilities for RE2 JMH benchmarks.
 *
 * <p>Command-line args: [filter] [forks] [warmupIters] [measIters] [iterTimeMs]
 *
 * <p>Examples:
 * <pre>
 *   # Calibrate a single method (1 fork, 20 warmup, 10 measurement, 200ms iterations):
 *   java ... BenchmarkRe2Search searchEasy0Re2 1 20 10 200
 *
 *   # Smoke test (1 fork, annotation defaults for iterations):
 *   java ... BenchmarkRe2Search BenchmarkRe2Search 1
 *
 *   # Full run (uses annotation defaults):
 *   java ... BenchmarkRe2Search
 * </pre>
 */
public final class Re2BenchmarkRunner
{
    private static final long CACHED_PROGRAM_MEMORY = 1L << 31;

    private Re2BenchmarkRunner() {}

    /**
     * Build JMH Options from a benchmark class and optional command-line args.
     *
     * @param benchmarkClass the JMH benchmark class
     * @param args [filter] [forks] [warmupIters] [measIters] [iterTimeMs]
     */
    public static Options buildOptions(Class<?> benchmarkClass, String[] args)
    {
        String filter = (args.length >= 1) ? args[0] : benchmarkClass.getSimpleName();

        ChainedOptionsBuilder builder = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + filter + ".*");

        if (args.length >= 2) {
            builder.forks(Integer.parseInt(args[1]));
        }
        if (args.length >= 3) {
            builder.warmupIterations(Integer.parseInt(args[2]));
        }
        if (args.length >= 4) {
            builder.measurementIterations(Integer.parseInt(args[3]));
        }
        if (args.length >= 5) {
            long ms = Long.parseLong(args[4]);
            builder.warmupTime(TimeValue.milliseconds(ms));
            builder.measurementTime(TimeValue.milliseconds(ms));
        }

        return builder.build();
    }

    /** Generates the byte-identical corpus used by the patched native benchmark. */
    public static byte[] randomText(int nbytes)
    {
        long randomState = 1;
        byte[] text = new byte[nbytes];
        for (int i = 0; i < nbytes; i++) {
            randomState ^= randomState << 13;
            randomState ^= randomState >>> 7;
            randomState ^= randomState << 17;
            int randomByte = (int) randomState & 0x7F;
            if (randomByte < 0x20) {
                randomByte = 0x20;
            }
            text[i] = (byte) randomByte;
        }
        return text;
    }

    public static Prog compileProg(String pattern)
    {
        Slice patBytes = Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8));
        ParseResult parsed = RegexpParser.parse(patBytes, Regexp.LIKE_PERL);
        return Compiler.compile(parsed.regexp(), false, CACHED_PROGRAM_MEMORY);
    }

    public static Re2 compileRe2(String pattern)
    {
        return Re2.compile(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)));
    }

    public static Re2 compileRe2Latin1(String pattern)
    {
        return Re2.compile(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), Re2.Options.latin1());
    }
}
