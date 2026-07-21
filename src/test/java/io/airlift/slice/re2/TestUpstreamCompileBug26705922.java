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
public class TestUpstreamCompileBug26705922
{
    private static Prog compile(String pattern, int flags, boolean reversed)
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), flags);

        Prog prog = Compiler.compile(parsed.regexp(), reversed, 0);
        assertThat(prog).as("compile: %s (reversed=%s)", pattern, reversed).isNotNull();
        return prog;
    }

    @Test
    public void testBug26705922()
    {
        // From upstream re2/testing/compile_test.cc TestCompile.Bug26705922.
        String pattern;

        pattern = "[\\x{10000}\\x{10010}]";
        assertThat(compile(pattern, Regexp.LIKE_PERL, false).dump()).isEqualTo(
                "3. byte [f0-f0] 0 -> 4\n" +
                        "4. byte [90-90] 0 -> 5\n" +
                        "5. byte [80-80] 0 -> 6\n" +
                        "6+ byte [80-80] 0 -> 8\n" +
                        "7. byte [90-90] 0 -> 8\n" +
                        "8. match! 0\n");
        assertThat(compile(pattern, Regexp.LIKE_PERL, true).dump()).isEqualTo(
                "3+ byte [80-80] 0 -> 5\n" +
                        "4. byte [90-90] 0 -> 5\n" +
                        "5. byte [80-80] 0 -> 6\n" +
                        "6. byte [90-90] 0 -> 7\n" +
                        "7. byte [f0-f0] 0 -> 8\n" +
                        "8. match! 0\n");

        pattern = "[\\x{8000}-\\x{10FFF}]";
        assertThat(compile(pattern, Regexp.LIKE_PERL, false).dump()).isEqualTo(
                "3+ byte [e8-ef] 0 -> 5\n" +
                        "4. byte [f0-f0] 0 -> 8\n" +
                        "5. byte [80-bf] 0 -> 6\n" +
                        "6. byte [80-bf] 0 -> 7\n" +
                        "7. match! 0\n" +
                        "8. byte [90-90] 0 -> 5\n");
        assertThat(compile(pattern, Regexp.LIKE_PERL, true).dump()).isEqualTo(
                "3. byte [80-bf] 0 -> 4\n" +
                        "4. byte [80-bf] 0 -> 5\n" +
                        "5+ byte [e8-ef] 0 -> 7\n" +
                        "6. byte [90-90] 0 -> 8\n" +
                        "7. match! 0\n" +
                        "8. byte [f0-f0] 0 -> 7\n");

        pattern = "[\\x{80}-\\x{10FFFF}]";
        assertThat(compile(pattern, Regexp.LIKE_PERL, false).dump()).isEqualTo(
                "3+ byte [c2-df] 0 -> 6\n" +
                        "4+ byte [e0-ef] 0 -> 8\n" +
                        "5. byte [f0-f4] 0 -> 9\n" +
                        "6. byte [80-bf] 0 -> 7\n" +
                        "7. match! 0\n" +
                        "8. byte [80-bf] 0 -> 6\n" +
                        "9. byte [80-bf] 0 -> 8\n");
        assertThat(compile(pattern, Regexp.LIKE_PERL, true).dump()).isEqualTo(
                "3. byte [80-bf] 0 -> 4\n" +
                        "4+ byte [c2-df] 0 -> 6\n" +
                        "5. byte [80-bf] 0 -> 7\n" +
                        "6. match! 0\n" +
                        "7+ byte [e0-ef] 0 -> 6\n" +
                        "8. byte [80-bf] 0 -> 9\n" +
                        "9. byte [f0-f4] 0 -> 6\n");
    }
}
