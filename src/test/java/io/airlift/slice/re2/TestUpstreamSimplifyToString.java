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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/simplify_test.cc.
public class TestUpstreamSimplifyToString
{
    private record Case(String pattern, String expected) {}

    @Test
    public void testUpstreamSimplifyTest()
    {
        // Sourced from upstream re2/testing/simplify_test.cc.
        List<Case> cases = List.of(
            new Case("a", "a"),
            new Case("ab", "ab"),
            new Case("a|b", "[a-b]"),
            new Case("ab|cd", "ab|cd"),
            new Case("(ab)*", "(ab)*"),
            new Case("(ab)+", "(ab)+"),
            new Case("(ab)?", "(ab)?"),
            new Case(".", "."),
            new Case("^", "^"),
            new Case("$", "$"),
            new Case("[ac]", "[ac]"),
            new Case("[^ac]", "[^ac]"),
            new Case("[[:alnum:]]", "[0-9A-Za-z]"),
            new Case("[[:alpha:]]", "[A-Za-z]"),
            new Case("[[:blank:]]", "[\\t ]"),
            new Case("[[:cntrl:]]", "[\\x00-\\x1f\\x7f]"),
            new Case("[[:digit:]]", "[0-9]"),
            new Case("[[:graph:]]", "[!-~]"),
            new Case("[[:lower:]]", "[a-z]"),
            new Case("[[:print:]]", "[ -~]"),
            new Case("[[:punct:]]", "[!-/:-@\\[-`{-~]"),
            new Case("[[:space:]]", "[\\t-\\r ]"),
            new Case("[[:upper:]]", "[A-Z]"),
            new Case("[[:xdigit:]]", "[0-9A-Fa-f]"),
            new Case("\\d", "[0-9]"),
            new Case("\\s", "[\\t-\\n\\f-\\r ]"),
            new Case("\\w", "[0-9A-Z_a-z]"),
            new Case("\\D", "[^0-9]"),
            new Case("\\S", "[^\\t-\\n\\f-\\r ]"),
            new Case("\\W", "[^0-9A-Z_a-z]"),
            new Case("[\\d]", "[0-9]"),
            new Case("[\\s]", "[\\t-\\n\\f-\\r ]"),
            new Case("[\\w]", "[0-9A-Z_a-z]"),
            new Case("[\\D]", "[^0-9]"),
            new Case("[\\S]", "[^\\t-\\n\\f-\\r ]"),
            new Case("[\\W]", "[^0-9A-Z_a-z]"),
            new Case("a{1}", "a"),
            new Case("a{2}", "aa"),
            new Case("a{5}", "aaaaa"),
            new Case("a{0,1}", "a?"),
            new Case("(a){0,2}", "(?:(a)(a)?)?"),
            new Case("(a){0,4}", "(?:(a)(?:(a)(?:(a)(a)?)?)?)?"),
            new Case("(a){2,6}", "(a)(a)(?:(a)(?:(a)(?:(a)(a)?)?)?)?"),
            new Case("a{0,2}", "(?:aa?)?"),
            new Case("a{0,4}", "(?:a(?:a(?:aa?)?)?)?"),
            new Case("a{2,6}", "aa(?:a(?:a(?:aa?)?)?)?"),
            new Case("a{0,}", "a*"),
            new Case("a{1,}", "a+"),
            new Case("a{2,}", "aa+"),
            new Case("a{5,}", "aaaaa+"),
            new Case("(?:a{1,}){1,}", "a+"),
            new Case("(a{1,}b{1,})", "(a+b+)"),
            new Case("a{1,}|b{1,}", "a+|b+"),
            new Case("(?:a{1,})*", "(?:a+)*"),
            new Case("(?:a{1,})+", "a+"),
            new Case("(?:a{1,})?", "(?:a+)?"),
            new Case("a{0}", ""),
            new Case("[ab]", "[a-b]"),
            new Case("[a-za-za-z]", "[a-z]"),
            new Case("[A-Za-zA-Za-z]", "[A-Za-z]"),
            new Case("[ABCDEFGH]", "[A-H]"),
            new Case("[AB-CD-EF-GH]", "[A-H]"),
            new Case("[W-ZP-XE-R]", "[E-Z]"),
            new Case("[a-ee-gg-m]", "[a-m]"),
            new Case("[a-ea-ha-m]", "[a-m]"),
            new Case("[a-ma-ha-e]", "[a-m]"),
            new Case("[a-zA-Z0-9 -~]", "[ -~]"),
            new Case("[^[:cntrl:][:^cntrl:]]", "[^\\x00-\\x{10ffff}]"),
            new Case("[[:cntrl:][:^cntrl:]]", "."),
            new Case("(?i)A", "[Aa]"),
            new Case("(?i)a", "[Aa]"),
            new Case("(?i)K", "[Kk\\x{212a}]"),
            new Case("(?i)k", "[Kk\\x{212a}]"),
            new Case("(?i)\\x{212a}", "[Kk\\x{212a}]"),
            // JVM case equivalence also includes dotted and dotless I.
            new Case("(?i)[a-z]", "[A-Za-z\\x{130}-\\x{131}\\x{17f}\\x{212a}]"),
            new Case("(?i)[\\x00-\\x{FFFD}]", "[\\x00-\\x{fffd}]"),
            new Case("(?i)[\\x00-\\x{10ffff}]", "."),
            new Case("(a|b|)", "([a-b]|(?:))"),
            new Case("(|)", "((?:)|(?:))"),
            new Case("a()", "a()"),
            new Case("(()|())", "(()|())"),
            new Case("(a|)", "(a|(?:))"),
            new Case("ab()cd()", "ab()cd()"),
            new Case("()", "()"),
            new Case("()*", "()*"),
            new Case("()+", "()+"),
            new Case("()?", "()?"),
            new Case("(){0}", ""),
            new Case("(){1}", "()"),
            new Case("(){1,}", "()+"),
            new Case("(){0,2}", "(?:()()?)?"),
            new Case("(?:^){0,}", "^*"),
            new Case("(?:$){28,}", "$+"),
            new Case("(?-m:^){0,30}", "(?-m:^)?"),
            new Case("(?-m:$){28,30}", "(?-m:$)"),
            new Case("\\b(?:\\b\\B){999}\\B", "\\b\\b\\B\\B"),
            new Case("\\b(?:\\b|\\B){999}\\B", "\\b(?:\\b|\\B)\\B"),
            new Case("(?:^){0,}?", "^*?"),
            new Case("(?:$){28,}?", "$+?"),
            new Case("(?-m:^){0,30}?", "(?-m:^)??"),
            new Case("(?-m:$){28,30}?", "(?-m:$)"),
            new Case("\\b(?:\\b\\B){999}?\\B", "\\b\\b\\B\\B"),
            new Case("\\b(?:\\b|\\B){999}?\\B", "\\b(?:\\b|\\B)\\B"),
            new Case("a*a*", "a*"),
            new Case("a*a+", "a+"),
            new Case("a*a?", "a*"),
            new Case("a*a{2}", "aa+"),
            new Case("a*a{2,}", "aa+"),
            new Case("a*a{2,3}", "aa+"),
            new Case("a+a*", "a+"),
            new Case("a+a+", "aa+"),
            new Case("a+a?", "a+"),
            new Case("a+a{2}", "aaa+"),
            new Case("a+a{2,}", "aaa+"),
            new Case("a+a{2,3}", "aaa+"),
            new Case("a?a*", "a*"),
            new Case("a?a+", "a+"),
            new Case("a?a?", "(?:aa?)?"),
            new Case("a?a{2}", "aaa?"),
            new Case("a?a{2,}", "aa+"),
            new Case("a?a{2,3}", "aa(?:aa?)?"),
            new Case("a{2}a*", "aa+"),
            new Case("a{2}a+", "aaa+"),
            new Case("a{2}a?", "aaa?"),
            new Case("a{2}a{2}", "aaaa"),
            new Case("a{2}a{2,}", "aaaa+"),
            new Case("a{2}a{2,3}", "aaaaa?"),
            new Case("a{2,}a*", "aa+"),
            new Case("a{2,}a+", "aaa+"),
            new Case("a{2,}a?", "aa+"),
            new Case("a{2,}a{2}", "aaaa+"),
            new Case("a{2,}a{2,}", "aaaa+"),
            new Case("a{2,}a{2,3}", "aaaa+"),
            new Case("a{2,3}a*", "aa+"),
            new Case("a{2,3}a+", "aaa+"),
            new Case("a{2,3}a?", "aa(?:aa?)?"),
            new Case("a{2,3}a{2}", "aaaaa?"),
            new Case("a{2,3}a{2,}", "aaaa+"),
            new Case("a{2,3}a{2,3}", "aaaa(?:aa?)?"),
            new Case("\\d*\\d*", "[0-9]*"),
            new Case(".*.*", ".*"),
            new Case("\\C*\\C*", "\\C*"),
            new Case("(?i)A*a*", "[Aa]*"),
            new Case("(?i)a+A+", "[Aa][Aa]+"),
            new Case("(?i)A*(?-i)a*", "[Aa]*a*"),
            new Case("(?i)a+(?-i)A+", "[Aa]+A+"),
            new Case("a*?a*?", "a*?"),
            new Case("a+?a+?", "aa+?"),
            new Case("a*?a*", "a*?a*"),
            new Case("a+a+?", "a+a+?"),
            new Case("a*a", "a+"),
            new Case("\\d*\\d", "[0-9]+"),
            new Case(".*.", ".+"),
            new Case("\\C*\\C", "\\C+"),
            new Case("(?i)A*a", "[Aa]+"),
            new Case("(?i)a+A", "[Aa][Aa]+"),
            new Case("(?i)A*(?-i)a", "[Aa]*a"),
            new Case("(?i)a+(?-i)A", "[Aa]+A"),
            new Case("a*aa", "aa+"),
            new Case("a*aab", "aa+b"),
            new Case("(?i)a*aa", "[Aa][Aa]+"),
            new Case("(?i)a*aab", "[Aa][Aa]+[Bb]"),
            new Case("(?i)a*(?-i)aa", "[Aa]*aa"),
            new Case("(?i)a*(?-i)aab", "[Aa]*aab"),
            new Case("a*b*", "a*b*"),
            new Case("\\d*\\D*", "[0-9]*[^0-9]*"),
            new Case("a+b", "a+b"),
            new Case("\\d+\\D", "[0-9]+[^0-9]"),
            new Case("a?bb", "a?bb"),
            new Case("(a*)a*", "(a*)a*"),
            new Case("a+(a)", "a+(a)"),
            new Case("(a?)(aa)", "(a?)(aa)"),
            new Case("aa*aa+aa?aa{2}aaa{2,}aaa{2,3}a", "aaaaaaaaaaaaaaaa+"),
            new Case("(?:a*aab){2}", "aa+baa+b"),
            new Case("(a*aab)", "(aa+b)"),
            new Case("(?:(?:a){0,}){0,}", "a*"),
            new Case("(?:(?:a){1,}){1,}", "a+"),
            new Case("(?:(?:a){0,1}){0,1}", "a?"),
            new Case("(?:(?:a){0,}){1,}", "a*"),
            new Case("(?:(?:a){0,}){0,1}", "a*"),
            new Case("(?:(?:a){1,}){0,}", "a*"),
            new Case("(?:(?:a){1,}){0,1}", "a*"),
            new Case("(?:(?:a){0,1}){0,}", "a*"),
            new Case("(?:(?:a){0,1}){1,}", "a*"));

        int flags = Regexp.MATCH_NEWLINE | (Regexp.LIKE_PERL & ~Regexp.ONE_LINE);

        for (Case c : cases) {
            ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(c.pattern().getBytes(StandardCharsets.UTF_8)), flags);

            Regexp simplified = Simplifier.simplify(parsed.regexp());
            assertThat(RegexpToString.toString(simplified))
                    .as("simplified: %s", c.pattern())
                    .isEqualTo(c.expected());
        }
    }
}
