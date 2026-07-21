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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOptionalVectorPrefixScanner
{
    @Test
    public void testWithAndWithoutVectorModule()
            throws Exception
    {
        ProbeResult disabled = runProbe(false);
        assertThat(disabled.exitCode()).as(disabled.output()).isZero();
        assertThat(disabled.output()).contains("OK swar");
        assertThat(disabled.classLoadLog())
                .doesNotContain("io.airlift.slice.re2.VectorPrefixScanner source:")
                .doesNotContain("jdk.incubator.vector.");

        ProbeResult enabled = runProbe(true);
        assertThat(enabled.exitCode()).as(enabled.output()).isZero();
        assertThat(enabled.output()).contains("OK vector");
        assertThat(enabled.classLoadLog())
                .contains("io.airlift.slice.re2.VectorPrefixScanner source:")
                .contains("jdk.incubator.vector.ByteVector");
    }

    private static ProbeResult runProbe(boolean enableVectorModule)
            throws IOException, InterruptedException
    {
        Path classLoadLog = Files.createTempFile("vector-prefix-class-load-", ".log");
        try {
            List<String> command = new ArrayList<>();
            command.add(System.getProperty("java.home") + "/bin/java");
            command.add("-Xlog:class+load=info:file=" + classLoadLog);
            if (enableVectorModule) {
                command.add("--add-modules");
                command.add("jdk.incubator.vector");
            }
            command.add("-cp");
            command.add(System.getProperty("java.class.path"));
            command.add(VectorPrefixScannerProbe.class.getName());
            command.add(Boolean.toString(enableVectorModule));

            ProcessBuilder processBuilder = new ProcessBuilder(command)
                    .redirectErrorStream(true);
            processBuilder.environment().remove("JDK_JAVA_OPTIONS");
            Process process = processBuilder.start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            int exitCode = process.waitFor();
            return new ProbeResult(exitCode, output, Files.readString(classLoadLog));
        }
        finally {
            Files.deleteIfExists(classLoadLog);
        }
    }

    private record ProbeResult(int exitCode, String output, String classLoadLog) {}
}
