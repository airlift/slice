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
public class TestUpstreamCompileBug35237384
{
    private static Prog compile(String pattern, int flags)
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), flags);

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).as("compile: %s", pattern).isNotNull();
        return prog;
    }

    @Test
    public void testBug35237384()
    {
        // From upstream re2/testing/compile_test.cc TestCompile.Bug35237384.
        int flags = Regexp.LATIN1 | Regexp.NEVER_CAPTURE;

        assertThat(compile("a**{3,}", flags).dump()).isEqualTo(
                "3+ byte [61-61] 1 -> 3\n" +
                        "4. nop -> 5\n" +
                        "5+ byte [61-61] 1 -> 5\n" +
                        "6. nop -> 7\n" +
                        "7+ byte [61-61] 1 -> 7\n" +
                        "8. match! 0\n");

        assertThat(compile("(a*|b*)*{3,}", flags).dump()).isEqualTo(
                "3+ nop -> 28\n" +
                        "4. nop -> 30\n" +
                        "5+ byte [61-61] 1 -> 5\n" +
                        "6. nop -> 32\n" +
                        "7+ byte [61-61] 1 -> 7\n" +
                        "8. nop -> 26\n" +
                        "9+ byte [61-61] 1 -> 9\n" +
                        "10. nop -> 20\n" +
                        "11+ byte [62-62] 1 -> 11\n" +
                        "12. nop -> 20\n" +
                        "13+ byte [62-62] 1 -> 13\n" +
                        "14. nop -> 26\n" +
                        "15+ byte [62-62] 1 -> 15\n" +
                        "16. nop -> 32\n" +
                        "17+ nop -> 9\n" +
                        "18. nop -> 11\n" +
                        "19. match! 0\n" +
                        "20+ nop -> 17\n" +
                        "21. nop -> 19\n" +
                        "22+ nop -> 7\n" +
                        "23. nop -> 13\n" +
                        "24+ nop -> 17\n" +
                        "25. nop -> 19\n" +
                        "26+ nop -> 22\n" +
                        "27. nop -> 24\n" +
                        "28+ nop -> 5\n" +
                        "29. nop -> 15\n" +
                        "30+ nop -> 22\n" +
                        "31. nop -> 24\n" +
                        "32+ nop -> 28\n" +
                        "33. nop -> 30\n");

        assertThat(compile("((|S.+)+|(|S.+)+|){2}", flags).dump()).isEqualTo(
                "3+ nop -> 36\n" +
                        "4+ nop -> 31\n" +
                        "5. nop -> 33\n" +
                        "6+ byte [00-09] 0 -> 8\n" +
                        "7. byte [0b-ff] 0 -> 8\n" +
                        "8+ nop -> 6\n" +
                        "9+ nop -> 29\n" +
                        "10. nop -> 28\n" +
                        "11+ byte [00-09] 0 -> 13\n" +
                        "12. byte [0b-ff] 0 -> 13\n" +
                        "13+ nop -> 11\n" +
                        "14+ nop -> 26\n" +
                        "15. nop -> 28\n" +
                        "16+ byte [00-09] 0 -> 18\n" +
                        "17. byte [0b-ff] 0 -> 18\n" +
                        "18+ nop -> 16\n" +
                        "19+ nop -> 36\n" +
                        "20. nop -> 33\n" +
                        "21+ byte [00-09] 0 -> 23\n" +
                        "22. byte [0b-ff] 0 -> 23\n" +
                        "23+ nop -> 21\n" +
                        "24+ nop -> 31\n" +
                        "25. nop -> 33\n" +
                        "26+ nop -> 28\n" +
                        "27. byte [53-53] 0 -> 11\n" +
                        "28. match! 0\n" +
                        "29+ nop -> 28\n" +
                        "30. byte [53-53] 0 -> 6\n" +
                        "31+ nop -> 33\n" +
                        "32. byte [53-53] 0 -> 21\n" +
                        "33+ nop -> 29\n" +
                        "34+ nop -> 26\n" +
                        "35. nop -> 28\n" +
                        "36+ nop -> 33\n" +
                        "37. byte [53-53] 0 -> 16\n");
    }
}
