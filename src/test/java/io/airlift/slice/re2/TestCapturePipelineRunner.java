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
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TestCapturePipelineRunner
{
    @Test
    void testCountCapturesReplay()
            throws Exception
    {
        byte[] input = input("count-captures", "([a-z]+)([0-9]+)?", "ab12 cd");

        Map<String, String> manifest = manifest(input);
        assertThat(manifest)
                .containsEntry("attempts", "3")
                .containsEntry("matches", "2")
                .containsEntry("matcher_resets", "1")
                .containsEntry("capture_engine", "ONE_PASS")
                .containsEntry("public_result", "5");

        assertStageResult(input, "public", 5);
        assertStageResult(input, "forward", 5);
        assertStageResult(input, "reverse", 5);
        assertStageResult(input, "capture", 5);
        assertStageResult(input, "composed", 5);
        assertStageResult(input, "setup", 5);
        assertStageResult(input, "result", 5);
        assertStageResult(input, "control", 5);
        assertStageResult(input, "direct-capture", 5);
        assertStageResult(input, "bit-state-capture", 5);
        assertStageResult(input, "bit-state-capture-reused", 5);
        assertStageResult(input, "direct-bit-state-capture", 5);
    }

    @Test
    void testGrepCapturesReplayPreservesLineWindows()
            throws Exception
    {
        byte[] input = input("grep-captures", "^(a+)", "aa\nxx\r\na\n");

        Map<String, String> manifest = manifest(input);
        assertThat(manifest)
                .containsEntry("attempts", "5")
                .containsEntry("matches", "2")
                .containsEntry("matcher_resets", "3")
                .containsEntry("public_result", "4");

        assertStageResult(input, "composed", 4);
    }

    @Test
    void testGroupZeroSpansDoNotRunCaptureEngine()
            throws Exception
    {
        byte[] input = input("count-spans", "a+|b+", "baa");

        Map<String, String> manifest = manifest(input);
        assertThat(manifest)
                .containsEntry("attempts", "3")
                .containsEntry("matches", "2")
                .containsEntry("capture_calls", "0")
                .containsEntry("capture_bytes", "0")
                .containsEntry("candidate_start_enabled", "false")
                .containsEntry("candidate_start_routes", "1")
                .containsEntry("candidate_start_fallbacks", "1")
                .containsEntry("public_result", "3");

        assertStageResult(input, "public", 3);
        assertStageResult(input, "composed", 3);
    }

    @Test
    void testDirectBitStateRouteReplay()
            throws Exception
    {
        byte[] input = input("count-captures", "[0-9]+.(.*)", "650-253-" + "0".repeat(100_000));

        Map<String, String> manifest = manifest(input);
        assertThat(manifest)
                .containsEntry("attempts", "2")
                .containsEntry("matches", "1")
                .containsEntry("forward_calls", "1")
                .containsEntry("reverse_calls", "0")
                .containsEntry("capture_calls", "1")
                .containsEntry("capture_engine", "BIT_STATE")
                .containsEntry("public_result", "2");

        assertStageResult(input, "public", 2);
        assertStageResult(input, "capture", 2);
        assertStageResult(input, "composed", 2);
    }

    @Test
    void testExtendedOnePassCaptureRoute()
            throws Exception
    {
        byte[] input = input("grep-captures", "^(a)(b)(c)(d)(e)$", "abcde\n");

        Map<String, String> manifest = manifest(input);
        assertThat(manifest)
                .containsEntry("program_one_pass", "true")
                .containsEntry("one_pass_capture_eligible", "true")
                .containsEntry("bit_state_eligible", "true")
                .containsEntry("anchored_dfa_skipped", "true")
                .containsEntry("capture_engine", "ONE_PASS");
    }

    private static void assertStageResult(byte[] input, String stage, long expected)
            throws Exception
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        CapturePipelineRunner.run(new ByteArrayInputStream(input), output, CapturePipelineRunner.Stage.fromCli(stage));
        for (String sample : output.toString(StandardCharsets.US_ASCII).lines().toList()) {
            String[] fields = sample.split(",");
            assertThat(Long.parseLong(fields[0])).isPositive();
            assertThat(Long.parseLong(fields[1])).isEqualTo(expected);
        }
    }

    private static Map<String, String> manifest(byte[] input)
            throws Exception
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        CapturePipelineRunner.writeManifest(new ByteArrayInputStream(input), output);
        return output.toString(StandardCharsets.US_ASCII).strip().lines()
                .map(line -> line.split("=", 2))
                .collect(java.util.stream.Collectors.toMap(fields -> fields[0], fields -> fields[1]));
    }

    private static byte[] input(String model, String pattern, String haystack)
    {
        ByteArrayOutputStream input = new ByteArrayOutputStream();
        write(input, "name", "test");
        write(input, "model", model);
        write(input, "case-insensitive", "false");
        write(input, "unicode", "true");
        write(input, "max-iters", "2");
        write(input, "max-warmup-iters", "1");
        write(input, "max-time", "1000000000");
        write(input, "max-warmup-time", "1000000000");
        write(input, "pattern", pattern);
        write(input, "haystack", haystack);
        return input.toByteArray();
    }

    private static void write(ByteArrayOutputStream output, String key, String value)
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeBytes((key + ":" + bytes.length + ":").getBytes(StandardCharsets.US_ASCII));
        output.writeBytes(bytes);
        output.write('\n');
    }
}
