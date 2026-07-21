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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRebarDfaCorpusAnalyzer
{
    @Test
    public void testReadKlv()
            throws IOException
    {
        byte[] haystack = {'a', 0, (byte) 0xFF, 'z'};
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeKlv(output, "name", "example");
        writeKlv(output, "model", "count-spans");
        writeKlv(output, "case-insensitive", "true");
        writeKlv(output, "unicode", "false");
        writeKlv(output, "pattern", "[a-z]+");
        writeKlv(output, "haystack", haystack);

        RebarDfaCorpusAnalyzer.RebarBenchmark benchmark = RebarDfaCorpusAnalyzer.readKlv(new ByteArrayInputStream(output.toByteArray()));
        assertThat(benchmark.name()).isEqualTo("example");
        assertThat(benchmark.model()).isEqualTo("count-spans");
        assertThat(benchmark.caseInsensitive()).isTrue();
        assertThat(benchmark.unicode()).isFalse();
        assertThat(benchmark.patterns()).containsExactly("[a-z]+");
        assertThat(benchmark.haystack()).containsExactly(haystack);
    }

    @Test
    public void testRouteClassification()
    {
        assertThat(analyze("paired", "[ -~]*ABC$", "x".repeat(300) + "ABC").route()).isEqualTo("PAIRED_DFA_FULL");
        assertThat(analyze(
                "partial",
                "[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$",
                "x".repeat(32 * 1024) + "ABCDEFGHIJKLMNOPQRSTUVWXYZ").route())
                .isEqualTo("PAIRED_DFA_PARTIAL");
        assertThat(analyze("prefix", "abc", "x".repeat(300)).route()).isEqualTo("PREFIX_ACCELERATION");
        assertThat(analyze("required", "^abc", "abc").route()).isEqualTo("REQUIRED_PREFIX");
        assertThat(analyze("fixed", "[a-z]{8}-[0-9]{4}", "x".repeat(4 * 1024)).route())
                .isEqualTo("FIXED_DISTANCE_BYTE_ACCELERATION");
        assertThat(analyze("short", "[ -~]*ABC$", "short").route()).isEqualTo("COMPACT_SHORT_INPUT");
    }

    private static RebarDfaCorpusAnalyzer.AnalysisResult analyze(String name, String pattern, String haystack)
    {
        return RebarDfaCorpusAnalyzer.analyze(new RebarDfaCorpusAnalyzer.RebarBenchmark(
                name,
                "count-spans",
                List.of(pattern),
                false,
                false,
                haystack.getBytes(UTF_8)));
    }

    private static void writeKlv(ByteArrayOutputStream output, String key, String value)
            throws IOException
    {
        writeKlv(output, key, value.getBytes(UTF_8));
    }

    private static void writeKlv(ByteArrayOutputStream output, String key, byte[] value)
            throws IOException
    {
        output.write(key.getBytes(StandardCharsets.US_ASCII));
        output.write(':');
        output.write(Integer.toString(value.length).getBytes(StandardCharsets.US_ASCII));
        output.write(':');
        output.write(value);
        output.write('\n');
    }
}
