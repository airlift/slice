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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestCountPipelineRunner
{
    @Test
    void testReportsCountAndDfaState()
    {
        RebarRunner.Benchmark benchmark = RebarRunner.Benchmark.read(benchmark(
                "count",
                "[A-Za-z]{10}\\s+[\\s\\S]{0,100}Result[\\s\\S]{0,100}\\s+[A-Za-z]{10}",
                "abcdefghij x Result y klmnopqrst -- abcdefghij z Result q klmnopqrst"));

        List<CountPipelineRunner.Sample> samples = CountPipelineRunner.run(benchmark, Re2.Options.DEFAULT_MAX_MEMORY);

        assertThat(samples).hasSize(2);
        assertThat(samples).allSatisfy(sample -> {
            assertThat(sample.durationNanos()).isPositive();
            assertThat(sample.maximumMemoryBytes()).isEqualTo(Re2.Options.DEFAULT_MAX_MEMORY);
            assertThat(sample.count()).isEqualTo(1);
            assertThat(sample.cacheResets()).isNotNegative();
            assertThat(sample.stateCount()).isPositive();
            assertThat(sample.cacheEntries()).isPositive();
            assertThat(sample.stateBudgetBytes()).isPositive();
            assertThat(sample.availableStateBytes()).isBetween(0L, sample.stateBudgetBytes());
        });
    }

    @Test
    void testRejectsUnsupportedModel()
    {
        RebarRunner.Benchmark benchmark = RebarRunner.Benchmark.read(benchmark(
                "grep",
                "[A-Za-z]{3}",
                "one\ntwo\n"));

        assertThatThrownBy(() -> CountPipelineRunner.run(benchmark, Re2.Options.DEFAULT_MAX_MEMORY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("count pipeline requires the count model: grep");
    }

    @Test
    void testRejectsDfaFallback()
    {
        RebarRunner.Benchmark benchmark = RebarRunner.Benchmark.read(benchmark(
                "count",
                "a*",
                "aaaa"));

        assertThatThrownBy(() -> CountPipelineRunner.run(benchmark, Re2.Options.DEFAULT_MAX_MEMORY))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("count pipeline fell back from the DFA");
    }

    private static byte[] benchmark(String model, String pattern, String haystack)
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        write(output, "name", "test/count-pipeline");
        write(output, "model", model);
        write(output, "case-insensitive", "false");
        write(output, "unicode", "false");
        write(output, "max-iters", "2");
        write(output, "max-warmup-iters", "1");
        write(output, "max-time", "1000000000");
        write(output, "max-warmup-time", "1000000000");
        write(output, "pattern", pattern);
        write(output, "haystack", haystack);
        return output.toByteArray();
    }

    private static void write(ByteArrayOutputStream output, String key, String value)
    {
        byte[] bytes = utf8Slice(value).getBytes();
        output.writeBytes((key + ":" + bytes.length + ":").getBytes(StandardCharsets.US_ASCII));
        output.writeBytes(bytes);
        output.write('\n');
    }
}
