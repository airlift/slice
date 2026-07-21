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

public class TestProgCompileEquivalence
{
    private static final String[] PATTERNS = {
            "(.*)-(\\d+)-of-(\\d+)",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ$",
            "[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$",
            "([ -~])*(A)(B)(C)(D)(E)(F)(G)(H)(I)(J)(K)(L)(M)(N)(O)(P)(Q)(R)(S)(T)(U)(V)(W)(X)(Y)(Z)$",
            "[0-9]+.(.*)",
            "(?i)ABCDEFGHIJKLMNOPQRSTUVWXYZ$",
    };

    @Test
    public void testCompileAndBytemapStageProduceEquivalentPrograms()
    {
        for (String pattern : PATTERNS) {
            ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), Regexp.LIKE_PERL);

            assertEquivalent(pattern, false, parsed.regexp());
            assertEquivalent(pattern, true, parsed.regexp());
        }
    }

    private static void assertEquivalent(String pattern, boolean reversed, Regexp re)
    {
        Prog baseline = Compiler.compile(re, reversed, 0);
        Prog staged = Compiler.compileForBenchmark(re, reversed, 0, Compiler.CompileStage.BYTEMAP);

        assertThat(baseline).as("baseline compile: %s reversed=%s", pattern, reversed).isNotNull();
        assertThat(staged).as("staged compile: %s reversed=%s", pattern, reversed).isNotNull();

        assertThat(staged.dump()).as("dump: %s reversed=%s", pattern, reversed).isEqualTo(baseline.dump());
        assertThat(staged.dumpUnanchored()).as("dumpUnanchored: %s reversed=%s", pattern, reversed).isEqualTo(baseline.dumpUnanchored());
        assertThat(staged.dumpByteMap()).as("dumpByteMap: %s reversed=%s", pattern, reversed).isEqualTo(baseline.dumpByteMap());

        assertThat(staged.start()).as("start: %s reversed=%s", pattern, reversed).isEqualTo(baseline.start());
        assertThat(staged.startUnanchored()).as("startUnanchored: %s reversed=%s", pattern, reversed).isEqualTo(baseline.startUnanchored());
        assertThat(staged.anchorStart()).as("anchorStart: %s reversed=%s", pattern, reversed).isEqualTo(baseline.anchorStart());
        assertThat(staged.anchorEnd()).as("anchorEnd: %s reversed=%s", pattern, reversed).isEqualTo(baseline.anchorEnd());

        for (InstOp op : InstOp.values()) {
            assertThat(staged.getInstCount(op))
                    .as("instCount[%s]: %s reversed=%s", op, pattern, reversed)
                    .isEqualTo(baseline.getInstCount(op));
        }
    }
}
