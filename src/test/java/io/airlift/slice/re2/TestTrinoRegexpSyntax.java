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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoRegexpSyntax
{
    @ParameterizedTest(name = "{0}")
    @MethodSource("syntaxCases")
    public void testSyntaxContract(SyntaxCase syntaxCase)
    {
        switch (syntaxCase.expected()) {
            case ACCEPT -> assertThatCode(() -> TrinoRegexp.compile(Slices.utf8Slice(syntaxCase.pattern())))
                    .doesNotThrowAnyException();
            case REJECT -> assertThatThrownBy(() -> TrinoRegexp.compile(Slices.utf8Slice(syntaxCase.pattern())))
                    .isInstanceOf(RegexpParseException.class);
            case COLLISION -> {
                assertThatCode(() -> Re2.compile(Slices.utf8Slice(syntaxCase.pattern())))
                        .doesNotThrowAnyException();
                assertThatThrownBy(() -> TrinoRegexp.compile(Slices.utf8Slice(syntaxCase.pattern())))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("conflicts with RE2 syntax");
            }
        }
    }

    private static Stream<SyntaxCase> syntaxCases()
            throws IOException
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                TestTrinoRegexpSyntax.class.getResourceAsStream("/io/airlift/slice/re2/trino-syntax-cases.tsv"),
                StandardCharsets.UTF_8));
        return reader.lines()
                .filter(line -> !line.isBlank() && !line.startsWith("#"))
                .map(line -> line.split("\\t", 3))
                .map(parts -> new SyntaxCase(parts[1], Expected.valueOf(parts[0]), parts[2]));
    }

    private enum Expected
    {
        ACCEPT,
        REJECT,
        COLLISION,
    }

    private record SyntaxCase(String pattern, Expected expected, String reason)
    {
        @Override
        public String toString()
        {
            return expected + ": " + pattern + " (" + reason + ")";
        }
    }
}
