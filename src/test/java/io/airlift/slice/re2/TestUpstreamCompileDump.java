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

import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Ported from upstream RE2: re2/testing/compile_test.cc.
public class TestUpstreamCompileDump
{
    @Test
    public void testSubsetOfUpstreamCompileTest()
    {
        // Sourced from upstream re2/testing/compile_test.cc (subset).
        var cases = GoldenJsonl.readObjects("io/airlift/slice/re2/prog/upstream_compile_dump.jsonl");
        assertThat(readPatterns("io/airlift/slice/re2/prog/upstream_compile_dump_patterns.txt"))
                .as("patterns list")
                .isEqualTo(cases.stream().map(c -> c.getString("pattern")).toList());

        int flags = Regexp.PERL_EXTENSIONS | Regexp.LATIN1;

        for (GoldenJsonl.JsonObject obj : cases) {
            assertThat(obj.getBoolean("ok")).as("ok: %s", obj).isTrue();
            String pattern = obj.getString("pattern");
            String expectedDump = obj.getString("dump");
            String flagsValue = obj.getString("flags");
            assertThat(Integer.decode(flagsValue)).as("flags: %s", pattern).isEqualTo(flags);
            ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), flags);

            Prog prog = Compiler.compile(parsed.regexp());
            assertThat(prog).as("compile: %s", pattern).isNotNull();

            assertThat(prog.dump()).as("dump: %s", pattern).isEqualTo(expectedDump);

            // Upstream compile_test.cc also asserts that a tiny memory budget fails.
            assertThatThrownBy(() -> Compiler.compile(parsed.regexp(), false, 1))
                    .as("compile with maxMemory=1: %s", pattern)
                    .isInstanceOf(RegexpCompileOutOfMemoryException.class);
        }
    }

    private static List<String> readPatterns(String resourcePath)
    {
        InputStream stream = TestUpstreamCompileDump.class.getClassLoader().getResourceAsStream(resourcePath);
        if (stream == null) {
            throw new IllegalArgumentException("resource not found: " + resourcePath);
        }

        List<String> patterns = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty() && line.charAt(0) == '#') {
                    continue;
                }
                patterns.add(line);
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("failed to read resource: " + resourcePath, e);
        }
        return patterns;
    }
}
