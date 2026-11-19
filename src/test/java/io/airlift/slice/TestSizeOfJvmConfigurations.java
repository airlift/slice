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

import com.sun.management.HotSpotDiagnosticMXBean;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static io.airlift.slice.ArraySizeProbe.COMPRESSED_CLASS_POINTERS;
import static io.airlift.slice.ArraySizeProbe.FLAG_PREFIX;
import static java.util.jar.Attributes.Name.MANIFEST_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * SizeOf reads the JVM layout options once at class initialization, so every layout needs its own JVM.
 * Each configuration runs {@link ArraySizeProbe} in a child JVM and compares SizeOf with
 * {@link java.lang.instrument.Instrumentation#getObjectSize(Object)}.
 */
public class TestSizeOfJvmConfigurations
{
    private static final long TIMEOUT_SECONDS = 60;
    // JDK 27 removed the option: the JVM only warns about it and class pointers are always compressed
    private static final boolean COMPRESSED_CLASS_POINTERS_OPTION_PRESENT = hasVmOption(COMPRESSED_CLASS_POINTERS);

    @TempDir
    static Path tempDir;

    private static Path agentJar;

    @BeforeAll
    public static void createAgentJar()
            throws IOException
    {
        // The agent class is loaded from the child's class path, so the jar only needs the manifest
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Premain-Class", ArraySizeProbe.class.getName());

        agentJar = tempDir.resolve("size-agent.jar");
        try (OutputStream output = Files.newOutputStream(agentJar);
                JarOutputStream ignored = new JarOutputStream(output, manifest)) {
            // the manifest is written by the constructor
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configurations")
    public void testArraySizesAgainstJvm(JvmConfiguration configuration)
            throws Exception
    {
        ProcessResult result = runProbe(configuration);

        // The JVM may reject or override options, in which case a green run would not test the requested layout
        assumeFalse(
                result.exitCode() != 0 && result.output().contains("Unrecognized VM option"),
                () -> "JVM does not support " + configuration.arguments());

        assertThat(result.exitCode())
                .as("exit code of probe JVM with %s%n%s", configuration.arguments(), result.output())
                .isZero();
        assertThat(result.effectiveFlags())
                .as("effective flags of probe JVM with %s%n%s", configuration.arguments(), result.output())
                .isEqualTo(configuration.expectedFlags());
    }

    private static List<JvmConfiguration> configurations()
    {
        List<JvmConfiguration> configurations = new ArrayList<>();
        for (int alignment : List.of(8, 16, 32)) {
            for (boolean compressedOops : List.of(true, false)) {
                // Compact object headers require compressed class pointers
                configurations.add(new JvmConfiguration(true, compressedOops, true, alignment));
                configurations.add(new JvmConfiguration(false, compressedOops, true, alignment));
                if (COMPRESSED_CLASS_POINTERS_OPTION_PRESENT) {
                    configurations.add(new JvmConfiguration(false, compressedOops, false, alignment));
                }
            }
        }
        return configurations;
    }

    private static boolean hasVmOption(String option)
    {
        try {
            ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class).getVMOption(option);
            return true;
        }
        catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static ProcessResult runProbe(JvmConfiguration configuration)
            throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        // The child inherits JAVA_TOOL_OPTIONS, where a large -Xmx would turn off the requested compressed oops
        // and a large -Xms would exceed the maximum heap, so override both
        command.add("-Xms16m");
        command.add("-Xmx128m");
        command.addAll(configuration.arguments());
        command.add("-javaagent:" + agentJar);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(ArraySizeProbe.class.getName());

        Path outputFile = Files.createTempFile(tempDir, "probe", ".log");
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(outputFile.toFile())
                .start();
        if (!process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("Probe JVM timed out with %s%n%s".formatted(configuration.arguments(), Files.readString(outputFile)));
        }
        return new ProcessResult(process.exitValue(), Files.readString(outputFile));
    }

    private record JvmConfiguration(boolean compactHeaders, boolean compressedOops, boolean compressedClassPointers, int alignment)
    {
        List<String> arguments()
        {
            List<String> arguments = new ArrayList<>();
            arguments.add(toggle("UseCompactObjectHeaders", compactHeaders));
            arguments.add(toggle("UseCompressedOops", compressedOops));
            if (COMPRESSED_CLASS_POINTERS_OPTION_PRESENT) {
                arguments.add(toggle(COMPRESSED_CLASS_POINTERS, compressedClassPointers));
            }
            arguments.add("-XX:ObjectAlignmentInBytes=" + alignment);
            return arguments;
        }

        Map<String, String> expectedFlags()
        {
            return Map.of(
                    "UseCompactObjectHeaders", String.valueOf(compactHeaders),
                    "UseCompressedOops", String.valueOf(compressedOops),
                    COMPRESSED_CLASS_POINTERS, String.valueOf(compressedClassPointers),
                    "ObjectAlignmentInBytes", String.valueOf(alignment));
        }

        @Override
        public String toString()
        {
            return "compactHeaders=%s, compressedOops=%s, compressedClassPointers=%s, alignment=%s"
                    .formatted(compactHeaders, compressedOops, compressedClassPointers, alignment);
        }

        private static String toggle(String option, boolean enabled)
        {
            return "-XX:" + (enabled ? "+" : "-") + option;
        }
    }

    private record ProcessResult(int exitCode, String output)
    {
        Map<String, String> effectiveFlags()
        {
            Map<String, String> flags = new LinkedHashMap<>();
            output.lines()
                    .filter(line -> line.startsWith(FLAG_PREFIX))
                    .map(line -> line.substring(FLAG_PREFIX.length()).split("=", 2))
                    .forEach(parts -> flags.put(parts[0], parts[1]));
            return flags;
        }
    }
}
