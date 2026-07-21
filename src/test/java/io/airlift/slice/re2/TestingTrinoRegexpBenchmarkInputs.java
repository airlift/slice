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

import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.re2.Re2BenchmarkRunner.randomText;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class TestingTrinoRegexpBenchmarkInputs
{
    private static final byte[] LITERAL = "literal".getBytes(UTF_8);
    private static final byte[] CAPTURE = "abc-123".getBytes(UTF_8);
    private static final byte[] MONEY_BAG = "\uD83D\uDCB0".getBytes(UTF_8);

    private static final List<String> WORKLOADS = List.of(
            "literalSparse",
            "captureSparse",
            "delimiterDense",
            "emptyMatches",
            "unicodeSparse");
    private static final List<Integer> SOURCE_LENGTHS = List.of(1_024, 32_768);

    private TestingTrinoRegexpBenchmarkInputs() {}

    public static List<String> workloads()
    {
        return WORKLOADS;
    }

    public static List<Integer> sourceLengths()
    {
        return SOURCE_LENGTHS;
    }

    public static Input create(String workload, int sourceLength)
    {
        byte[] sourceBytes;
        String pattern;
        switch (workload) {
            case "literalSparse" -> {
                pattern = "literal";
                sourceBytes = randomText(sourceLength);
                inject(sourceBytes, LITERAL, sourceLength / 4, sourceLength / 2, sourceLength * 3 / 4);
            }
            case "captureSparse" -> {
                pattern = "([a-z]+)-([0-9]+)";
                sourceBytes = new byte[sourceLength];
                Arrays.fill(sourceBytes, (byte) '.');
                inject(sourceBytes, CAPTURE, sourceLength / 4, sourceLength / 2, sourceLength * 3 / 4);
            }
            case "delimiterDense" -> {
                pattern = "[,;]";
                sourceBytes = new byte[sourceLength];
                Arrays.fill(sourceBytes, (byte) 'a');
                for (int position = 5; position < sourceBytes.length; position += 6) {
                    sourceBytes[position] = ((position / 6) & 1) == 0 ? (byte) ',' : (byte) ';';
                }
            }
            case "emptyMatches" -> {
                pattern = "x*";
                sourceBytes = new byte[sourceLength];
                Arrays.fill(sourceBytes, (byte) 'a');
            }
            case "unicodeSparse" -> {
                pattern = "(\uD83D\uDCB0)";
                sourceBytes = new byte[sourceLength];
                Arrays.fill(sourceBytes, (byte) 'a');
                for (int position = 32; position + MONEY_BAG.length <= sourceBytes.length; position += 64) {
                    System.arraycopy(MONEY_BAG, 0, sourceBytes, position, MONEY_BAG.length);
                }
            }
            default -> throw new IllegalArgumentException("unknown workload: " + workload);
        }
        return new Input(Slices.utf8Slice(pattern), Slices.wrappedBuffer(sourceBytes));
    }

    private static void inject(byte[] target, byte[] value, int... positions)
    {
        for (int position : positions) {
            System.arraycopy(value, 0, target, position, value.length);
        }
    }

    public record Input(Slice pattern, Slice source) {}
}
