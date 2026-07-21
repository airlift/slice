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

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/compile_test.cc.
public class TestUpstreamCompileByteMap
{
    @Test
    public void testUpstreamCompileByteMaps()
    {
        // From upstream re2/testing/compile_test.cc (Latin1Ranges, UTF8Ranges, OtherByteMapTests).
        var cases = GoldenJsonl.readObjects("io/airlift/slice/re2/prog/upstream_compile_bytemap.jsonl");
        for (GoldenJsonl.JsonObject obj : cases) {
            assertThat(obj.getBoolean("ok")).as("ok: %s", obj).isTrue();
            String pattern = obj.getString("pattern");
            String expected = obj.getString("bytemap");
            int flags = Integer.decode(obj.getString("flags"));

            ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), flags);

            Prog prog = Compiler.compile(parsed.regexp(), false, 0);
            assertThat(prog).as("compile: %s", pattern).isNotNull();

            Prog reverseProg = Compiler.compile(parsed.regexp(), true, 0);
            assertThat(reverseProg).as("reverse compile: %s", pattern).isNotNull();
            assertThat(reverseProg.dumpByteMap()).as("reverse bytemap: %s", pattern).isEqualTo(prog.dumpByteMap());

            assertThat(prog.dumpByteMap()).as("bytemap: %s", pattern).isEqualTo(expected);
        }
    }
}
