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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBenchmarkRe2CompileFocused
{
    @ParameterizedTest
    @ValueSource(strings = {
            "(.*)-(\\d+)-of-(\\d+)",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ$",
            "[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$",
            "([ -~])*(A)(B)(C)(D)(E)(F)(G)(H)(I)(J)(K)(L)(M)(N)(O)(P)(Q)(R)(S)(T)(U)(V)(W)(X)(Y)(Z)$",
            "[0-9]+.(.*)",
            "(?i)ABCDEFGHIJKLMNOPQRSTUVWXYZ$",
            "([a-z]+)-([0-9]+)",
    })
    public void testCompileAndFullMatch(String pattern)
    {
        BenchmarkRe2CompileFocused.CompilePatternState state = new BenchmarkRe2CompileFocused.CompilePatternState();
        state.pattern = pattern;
        state.setup();

        BenchmarkRe2CompileFocused benchmark = new BenchmarkRe2CompileFocused();
        assertThat(benchmark.re2CompileAndFullMatch(state)).isTrue();
        assertThat(benchmark.compileToProgAndConstructFirstMatchDfa(state)).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            ";",
            "[,;]",
            "([a-z]+)-([0-9]+)",
            "x$",
            "\\bX",
    })
    public void testTrinoCompile(String pattern)
    {
        BenchmarkRe2CompileFocused.TrinoCompilePatternState state = new BenchmarkRe2CompileFocused.TrinoCompilePatternState();
        state.pattern = pattern;
        state.setup();

        BenchmarkRe2CompileFocused benchmark = new BenchmarkRe2CompileFocused();
        TrinoRegexp regexp = (TrinoRegexp) benchmark.trinoRegexpCompileTotal(state);
        assertThat(regexp).isNotNull();
    }
}
