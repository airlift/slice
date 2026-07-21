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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestRebarRunner
{
    @Test
    void testCompile()
            throws Exception
    {
        assertCounts(run("compile", bytes("a+"), true, bytes("baaa ca")), 2, 2);
    }

    @Test
    void testCount()
            throws Exception
    {
        assertCounts(run("count", bytes("a+"), true, bytes("baaa ca")), 2, 2);
    }

    @Test
    void testCountDoesNotComputeMatchStarts()
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice("a+b"));

        assertThat(RebarRunner.countMatches(pattern, Slices.utf8Slice("aaab ab"))).isEqualTo(2);
        assertThat(pattern.isReverseProgramComputed()).isFalse();
    }

    @Test
    void testCountUsesSpecializedCounter()
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice("\\p{L}{8,13}"));

        assertThat(RebarRunner.countMatches(pattern, Slices.utf8Slice("abcdefgh abcdefghijk 123"))).isEqualTo(2);
        assertThat(pattern.isBoundedCharacterClassCounterComputed()).isTrue();
        assertThat(pattern.isReverseProgramComputed()).isFalse();
    }

    @Test
    void testCountSpans()
            throws Exception
    {
        assertCounts(run("count-spans", bytes("a+"), true, bytes("baaa ca")), 4, 4);
    }

    @Test
    void testCountCaptures()
            throws Exception
    {
        assertCounts(run("count-captures", bytes("(a)(b)?"), true, bytes("a ab")), 5, 5);
    }

    @Test
    void testGrep()
            throws Exception
    {
        assertCounts(run("grep", bytes("^a"), true, bytes("a\nba\r\na2\n")), 2, 2);
    }

    @Test
    void testGrepCaptures()
            throws Exception
    {
        assertCounts(run("grep-captures", bytes("(a)"), true, bytes("a\nba\n")), 4, 4);
    }

    @Test
    void testLatin1Haystack()
            throws Exception
    {
        assertCounts(run("count", bytes("\\xFF"), false, new byte[] {(byte) 0xFF}), 1, 1);
    }

    @Test
    void testMatchModelsRequestOnlyRequiredCaptures()
    {
        Re2 pattern = Re2.compile(Slices.utf8Slice("(a)(b)?"));
        Slice haystack = Slices.utf8Slice("a ab");

        assertThat(RebarRunner.MatchMeasurement.COUNT.createMatcher(pattern, haystack).groupCount()).isZero();
        assertThat(RebarRunner.MatchMeasurement.SPAN_LENGTH.createMatcher(pattern, haystack).groupCount()).isZero();
        assertThat(RebarRunner.MatchMeasurement.CAPTURES.createMatcher(pattern, haystack).groupCount()).isEqualTo(2);
    }

    @Test
    void testManifest()
            throws Exception
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        RebarRunner.writeManifest(
                new ByteArrayInputStream(input("count", bytes("a+"), true, bytes("baaa ca"))),
                output);

        List<String> fields = parseCsv(output.toString(StandardCharsets.UTF_8).strip());
        assertThat(fields)
                .hasSize(28)
                .containsExactly(
                        "test",
                        "count",
                        "false",
                        "true",
                        "2",
                        "85e200cddd8f0561cb88c3dcdf9baf7fe1b363679c1749ed58b8aad551e9a233",
                        "7",
                        "42be0b25b4655c360bb29b18997d5186e238ca5aa8be80035c8d17bf48f1b93f",
                        "2",
                        "match-count",
                        Boolean.toString(Dfa.nativeAccessEnabled()),
                        "false",
                        "false",
                        "true",
                        "true",
                        "FIRST_MATCH",
                        fields.get(16),
                        fields.get(17),
                        fields.get(18),
                        fields.get(19),
                        "0",
                        "false",
                        "",
                        "0",
                        "0",
                        "0",
                        "0",
                        "0");
        assertThat(Long.parseLong(fields.get(16))).isNotNegative();
        assertThat(Integer.parseInt(fields.get(17))).isNotNegative();
        assertThat(Long.parseLong(fields.get(18))).isNotNegative();
        assertThat(Integer.parseInt(fields.get(19))).isNotNegative();
    }

    private static List<String> run(String model, byte[] pattern, boolean unicode, byte[] haystack)
            throws Exception
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        RebarRunner.run(new ByteArrayInputStream(input(model, pattern, unicode, haystack)), output);
        return output.toString(StandardCharsets.US_ASCII).lines().toList();
    }

    private static byte[] input(String model, byte[] pattern, boolean unicode, byte[] haystack)
    {
        ByteArrayOutputStream input = new ByteArrayOutputStream();
        write(input, "name", bytes("test"));
        write(input, "model", bytes(model));
        write(input, "case-insensitive", bytes("false"));
        write(input, "unicode", bytes(Boolean.toString(unicode)));
        write(input, "max-iters", bytes("2"));
        write(input, "max-warmup-iters", bytes("1"));
        write(input, "max-time", bytes("1000000000"));
        write(input, "max-warmup-time", bytes("1000000000"));
        write(input, "pattern", pattern);
        write(input, "haystack", haystack);
        return input.toByteArray();
    }

    private static List<String> parseCsv(String line)
    {
        List<String> fields = new java.util.ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        for (int index = 0; index < line.length(); index++) {
            char character = line.charAt(index);
            if (character == '"') {
                if (quoted && index + 1 < line.length() && line.charAt(index + 1) == '"') {
                    field.append('"');
                    index++;
                }
                else {
                    quoted = !quoted;
                }
            }
            else if (character == ',' && !quoted) {
                fields.add(field.toString());
                field.setLength(0);
            }
            else {
                field.append(character);
            }
        }
        fields.add(field.toString());
        return fields;
    }

    private static void assertCounts(List<String> samples, long... expectedCounts)
    {
        assertThat(samples)
                .hasSize(expectedCounts.length);
        for (int index = 0; index < expectedCounts.length; index++) {
            String[] fields = samples.get(index).split(",");
            assertThat(Long.parseLong(fields[0])).isPositive();
            assertThat(Long.parseLong(fields[1])).isEqualTo(expectedCounts[index]);
        }
    }

    private static void write(ByteArrayOutputStream output, String key, byte[] value)
    {
        output.writeBytes(bytes(key + ":" + value.length + ":"));
        output.writeBytes(value);
        output.write('\n');
    }

    private static byte[] bytes(String value)
    {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
