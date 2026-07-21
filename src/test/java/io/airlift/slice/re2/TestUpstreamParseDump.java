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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/parse_test.cc.
public class TestUpstreamParseDump
{
    private record Case(String pattern, String expectedDump) {}

    private record CaseFlags(String pattern, String expectedDump, int flags) {}

    private record ByteCase(byte[] pattern, String expectedDump, int flags) {}

    private static final int UPSTREAM_TEST_FLAGS = Regexp.MATCH_NEWLINE | Regexp.PERL_EXTENSIONS | Regexp.PERL_CLASSES | Regexp.UNICODE_GROUPS;

    @Test
    public void testUpstreamParseTestCases()
    {
        // Sourced from upstream re2/testing/parse_test.cc (default flag cases).
        List<Case> cases = List.of(
            new Case("a", "lit{a}"),
            new Case("a.", "cat{lit{a}dot{}}"),
            new Case("a.b", "cat{lit{a}dot{}lit{b}}"),
            new Case("ab", "str{ab}"),
            new Case("a.b.c", "cat{lit{a}dot{}lit{b}dot{}lit{c}}"),
            new Case("abc", "str{abc}"),
            new Case("a|^", "alt{lit{a}bol{}}"),
            new Case("a|b", "cc{0x61-0x62}"),
            new Case("(a)", "cap{lit{a}}"),
            new Case("(a)|b", "alt{cap{lit{a}}lit{b}}"),
            new Case("a*", "star{lit{a}}"),
            new Case("a+", "plus{lit{a}}"),
            new Case("a?", "que{lit{a}}"),
            new Case("a{2}", "rep{2,2 lit{a}}"),
            new Case("a{2,3}", "rep{2,3 lit{a}}"),
            new Case("a{2,}", "rep{2,-1 lit{a}}"),
            new Case("a*?", "nstar{lit{a}}"),
            new Case("a+?", "nplus{lit{a}}"),
            new Case("a??", "nque{lit{a}}"),
            new Case("a{2}?", "nrep{2,2 lit{a}}"),
            new Case("a{2,3}?", "nrep{2,3 lit{a}}"),
            new Case("a{2,}?", "nrep{2,-1 lit{a}}"),
            new Case("", "emp{}"),
            new Case("|", "alt{emp{}emp{}}"),
            new Case("|x|", "alt{emp{}lit{x}emp{}}"),
            new Case(".", "dot{}"),
            new Case("^", "bol{}"),
            new Case("$", "eol{}"),
            new Case("\\|", "lit{|}"),
            new Case("\\(", "lit{(}"),
            new Case("\\)", "lit{)}"),
            new Case("\\*", "lit{*}"),
            new Case("\\+", "lit{+}"),
            new Case("\\?", "lit{?}"),
            new Case("{", "lit{{}"),
            new Case("}", "lit{}}"),
            new Case("\\.", "lit{.}"),
            new Case("\\^", "lit{^}"),
            new Case("\\$", "lit{$}"),
            new Case("\\\\", "lit{\\}"),
            new Case("[ace]", "cc{0x61 0x63 0x65}"),
            new Case("[abc]", "cc{0x61-0x63}"),
            new Case("[a-z]", "cc{0x61-0x7a}"),
            new Case("[a]", "lit{a}"),
            new Case("\\-", "lit{-}"),
            new Case("-", "lit{-}"),
            new Case("\\_", "lit{_}"),
            new Case("[[:lower:]]", "cc{0x61-0x7a}"),
            new Case("[a-z]", "cc{0x61-0x7a}"),
            new Case("[^[:lower:]]", "cc{0-0x60 0x7b-0x10ffff}"),
            new Case("[[:^lower:]]", "cc{0-0x60 0x7b-0x10ffff}"),
            // JVM case equivalence adds dotted and dotless I to folded ASCII letter classes.
            new Case("(?i)[[:lower:]]", "cc{0x41-0x5a 0x61-0x7a 0x130-0x131 0x17f 0x212a}"),
            new Case("(?i)[a-z]", "cc{0x41-0x5a 0x61-0x7a 0x130-0x131 0x17f 0x212a}"),
            new Case("(?i)[^[:lower:]]", "cc{0-0x40 0x5b-0x60 0x7b-0x12f 0x132-0x17e 0x180-0x2129 0x212b-0x10ffff}"),
            new Case("(?i)[[:^lower:]]", "cc{0-0x40 0x5b-0x60 0x7b-0x12f 0x132-0x17e 0x180-0x2129 0x212b-0x10ffff}"),
            new Case("\\d", "cc{0x30-0x39}"),
            new Case("\\D", "cc{0-0x2f 0x3a-0x10ffff}"),
            new Case("\\s", "cc{0x9-0xa 0xc-0xd 0x20}"),
            new Case("\\S", "cc{0-0x8 0xb 0xe-0x1f 0x21-0x10ffff}"),
            new Case("\\w", "cc{0x30-0x39 0x41-0x5a 0x5f 0x61-0x7a}"),
            new Case("\\W", "cc{0-0x2f 0x3a-0x40 0x5b-0x5e 0x60 0x7b-0x10ffff}"),
            new Case("(?i)\\w", "cc{0x30-0x39 0x41-0x5a 0x5f 0x61-0x7a 0x130-0x131 0x17f 0x212a}"),
            new Case("(?i)\\W", "cc{0-0x2f 0x3a-0x40 0x5b-0x5e 0x60 0x7b-0x12f 0x132-0x17e 0x180-0x2129 0x212b-0x10ffff}"),
            new Case("[^\\\\]", "cc{0-0x5b 0x5d-0x10ffff}"),
            new Case("\\C", "byte{}"),
            new Case("\\p{Braille}", "cc{0x2800-0x28ff}"),
            new Case("\\P{Braille}", "cc{0-0x27ff 0x2900-0x10ffff}"),
            new Case("\\p{^Braille}", "cc{0-0x27ff 0x2900-0x10ffff}"),
            new Case("\\P{^Braille}", "cc{0x2800-0x28ff}"),
            new Case("a{,2}", "str{a{,2}}"),
            new Case("\\.\\^\\$\\\\", "str{.^$\\}"),
            new Case("[a-zABC]", "cc{0x41-0x43 0x61-0x7a}"),
            new Case("[^a]", "cc{0-0x60 0x62-0x10ffff}"),
            new Case("[α-ε☺]", "cc{0x3b1-0x3b5 0x263a}"),
            new Case("a*{", "cat{star{lit{a}}lit{{}}"),
            new Case("(?:ab)*", "star{str{ab}}"),
            new Case("(ab)*", "star{cap{str{ab}}}"),
            new Case("ab|cd", "alt{str{ab}str{cd}}"),
            new Case("a(b|c)d", "cat{lit{a}cap{cc{0x62-0x63}}lit{d}}"),
            new Case("(?:(?:a)*)*", "star{lit{a}}"),
            new Case("(?:(?:a)+)+", "plus{lit{a}}"),
            new Case("(?:(?:a)?)?", "que{lit{a}}"),
            new Case("(?:(?:a)*)+", "star{lit{a}}"),
            new Case("(?:(?:a)*)?", "star{lit{a}}"),
            new Case("(?:(?:a)+)*", "star{lit{a}}"),
            new Case("(?:(?:a)+)?", "star{lit{a}}"),
            new Case("(?:(?:a)?)*", "star{lit{a}}"),
            new Case("(?:(?:a)?)+", "star{lit{a}}"),
            new Case("(?:a)", "lit{a}"),
            new Case("(?:ab)(?:cd)", "str{abcd}"),
            new Case("(?:a|b)|(?:c|d)", "cc{0x61-0x64}"),
            new Case("a|c", "cc{0x61 0x63}"),
            new Case("a|[cd]", "cc{0x61 0x63-0x64}"),
            new Case("a|.", "dot{}"),
            new Case("[ab]|c", "cc{0x61-0x63}"),
            new Case("[ab]|[cd]", "cc{0x61-0x64}"),
            new Case("[ab]|.", "dot{}"),
            new Case(".|c", "dot{}"),
            new Case(".|[cd]", "dot{}"),
            new Case(".|.", "dot{}"),
            new Case("\\Q+|*?{[\\E", "str{+|*?{[}"),
            new Case("\\Q+\\E+", "plus{lit{+}}"),
            new Case("\\Q\\\\E", "lit{\\}"),
            new Case("\\Q\\\\\\E", "str{\\\\}"),
            new Case("\\Qa\\E*", "star{lit{a}}"),
            new Case("\\Qab\\E*", "cat{lit{a}star{lit{b}}}"),
            new Case("\\Qabc\\E*", "cat{str{ab}star{lit{c}}}"),
            new Case("(?m)^", "bol{}"),
            new Case("(?m)$", "eol{}"),
            new Case("(?-m)^", "bot{}"),
            new Case("(?-m)$", "eot{}"),
            new Case("(?m)\\A", "bot{}"),
            new Case("(?m)\\z", "eot{\\z}"),
            new Case("(?-m)\\A", "bot{}"),
            new Case("(?-m)\\z", "eot{\\z}"),
            new Case("(?P<name>a)", "cap{name:lit{a}}"),
            new Case("(?P<中文>a)", "cap{中文:lit{a}}"),
            new Case("(?<name>a)", "cap{name:lit{a}}"),
            new Case("(?<中文>a)", "cap{中文:lit{a}}"),
            new Case("[Aa]", "litfold{a}"),
            new Case("abcde", "str{abcde}"),
            new Case("[Aa][Bb]cd", "cat{strfold{ab}str{cd}}"),
            new Case("[\\s\\S]", "cc{0-0x10ffff}"));

        assertCases(cases, UPSTREAM_TEST_FLAGS, true);
    }

    @Test
    public void testUpstreamParseFoldCaseCases()
    {
        // From upstream re2/testing/parse_test.cc foldCase_tests.
        List<Case> cases = List.of(
                new Case("AbCdE", "strfold{abcde}"),
                new Case("[Aa]", "litfold{a}"),
                new Case("a", "litfold{a}"),
                new Case("A[F-g]", "cat{litfold{a}cc{0x41-0x7a 0x130-0x131 0x17f 0x212a}}"),
                new Case("[[:upper:]]", "cc{0x41-0x5a 0x61-0x7a 0x130-0x131 0x17f 0x212a}"),
                new Case("[[:lower:]]", "cc{0x41-0x5a 0x61-0x7a 0x130-0x131 0x17f 0x212a}"));

        assertCases(cases, Regexp.FOLD_CASE, false);
    }

    @Test
    public void testUpstreamParseMatchNlCases()
    {
        // From upstream re2/testing/parse_test.cc matchnl_tests.
        List<Case> cases = List.of(
                new Case(".", "dot{}"),
                new Case("\n", "lit{\n}"),
                new Case("[^a]", "cc{0-0x60 0x62-0x10ffff}"),
                new Case("[a\\n]", "cc{0xa 0x61}"));

        assertCases(cases, Regexp.MATCH_NEWLINE, false);
    }

    @Test
    public void testUpstreamParseNoMatchNlCases()
    {
        // From upstream re2/testing/parse_test.cc nomatchnl_tests.
        List<Case> cases = List.of(
                new Case(".", "cc{0-0x9 0xb-0x10ffff}"),
                new Case("\n", "lit{\n}"),
                new Case("[^a]", "cc{0-0x9 0xb-0x60 0x62-0x10ffff}"),
                new Case("[a\\n]", "cc{0xa 0x61}"));

        assertCases(cases, 0, false);
    }

    @Test
    public void testUpstreamParseLiteralCases()
    {
        // From upstream re2/testing/parse_test.cc literal_tests.
        assertThat(parseAndDump("(|)^$.[*+?]{5,10},\\", Regexp.LITERAL))
                .isEqualTo("str{(|)^$.[*+?]{5,10},\\}");
    }

    @Test
    public void testUpstreamParsePrefixCases()
    {
        // From upstream re2/testing/parse_test.cc prefix_tests.
        List<Case> cases = List.of(
                new Case("abc|abd", "cat{str{ab}cc{0x63-0x64}}"),
                new Case("a(?:b)c|abd", "cat{str{ab}cc{0x63-0x64}}"),
                new Case("abc|abd|aef|bcx|bcy",
                        "alt{cat{lit{a}alt{cat{lit{b}cc{0x63-0x64}}str{ef}}}cat{str{bc}cc{0x78-0x79}}}"),
                new Case("abc|x|abd", "alt{str{abc}lit{x}str{abd}}"),
                new Case("(?i)abc|ABD", "cat{strfold{ab}cc{0x43-0x44 0x63-0x64}}"),
                new Case("[ab]c|[ab]d", "cat{cc{0x61-0x62}cc{0x63-0x64}}"),
                new Case(".c|.d", "cat{cc{0-0x9 0xb-0x10ffff}cc{0x63-0x64}}"),
                new Case("\\Cc|\\Cd", "cat{byte{}cc{0x63-0x64}}"),
                new Case("x{2}|x{2}[0-9]", "cat{rep{2,2 lit{x}}alt{emp{}cc{0x30-0x39}}}"),
                new Case("x{2}y|x{2}[0-9]y", "cat{rep{2,2 lit{x}}alt{lit{y}cat{cc{0x30-0x39}lit{y}}}}"),
                new Case("n|r|rs", "alt{lit{n}cat{lit{r}alt{emp{}lit{s}}}}"),
                new Case("n|rs|r", "alt{lit{n}cat{lit{r}alt{lit{s}emp{}}}}"),
                new Case("r|rs|n", "alt{cat{lit{r}alt{emp{}lit{s}}}lit{n}}"),
                new Case("rs|r|n", "alt{cat{lit{r}alt{lit{s}emp{}}}lit{n}}"),
                new Case("a\\C*?c|a\\C*?b", "cat{lit{a}alt{cat{nstar{byte{}}lit{c}}cat{nstar{byte{}}lit{b}}}}"),
                new Case("^/a/bc|^/a/de", "cat{bol{}cat{str{/a/}alt{str{bc}str{de}}}}"),
                new Case("a|aa|aaa|aaaa|aaaaa|aaaaaa|aaaaaaa|aaaaaaaa|aaaaaaaaa|aaaaaaaaaa",
                        "cat{lit{a}alt{emp{}cat{lit{a}alt{emp{}cat{lit{a}alt{emp{}cat{lit{a}alt{emp{}cat{lit{a}"
                                + "alt{emp{}cat{lit{a}alt{emp{}cat{lit{a}alt{emp{}cat{lit{a}alt{emp{}cat{lit{a}alt{emp{}"
                                + "lit{a}}}}}}}}}}}}}}}}}}}"),
                new Case("a|aardvark|aardvarks|abaci|aback|abacus|abacuses|abaft|abalone|abalones",
                        "cat{lit{a}alt{emp{}cat{str{ardvark}alt{emp{}lit{s}}}cat{str{ba}alt{cat{lit{c}alt{cc{0x69 0x6b}"
                                + "cat{str{us}alt{emp{}str{es}}}}}str{ft}cat{str{lone}alt{emp{}lit{s}}}}}}}"),
                new Case("0A|0[aA]", "cat{lit{0}cc{0x41 0x61}}"),
                new Case("0a|0[aA]", "cat{lit{0}cc{0x41 0x61}}"),
                new Case("0[aA]|0A", "cat{lit{0}cc{0x41 0x61}}"),
                new Case("0[aA]|0a", "cat{lit{0}cc{0x41 0x61}}"));

        assertCases(cases, Regexp.PERL_EXTENSIONS, false);
    }

    @Test
    public void testUpstreamParseNestedCases()
    {
        // From upstream re2/testing/parse_test.cc nested_tests.
        List<Case> cases = List.of(
                new Case("((((((((((x{2}){2}){2}){2}){2}){2}){2}){2}){2}))",
                        "cap{cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 lit{x}}}}}}}}}}}}}}}}}}}}"),
                new Case("((((((((((x{1}){2}){2}){2}){2}){2}){2}){2}){2}){2})",
                        "cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{1,1 lit{x}}}}}}}}}}}}}}}}}}}}}"),
                new Case("((((((((((x{0}){2}){2}){2}){2}){2}){2}){2}){2}){2})",
                        "cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 cap{rep{0,0 lit{x}}}}}}}}}}}}}}}}}}}}}"),
                new Case("((((((x{2}){2}){2}){5}){5}){5})",
                        "cap{rep{5,5 cap{rep{5,5 cap{rep{5,5 cap{rep{2,2 cap{rep{2,2 cap{rep{2,2 lit{x}}}}}}}}}}}}}"));

        assertCases(cases, Regexp.PERL_EXTENSIONS, false);
    }

    @Test
    public void testUpstreamParseNeverNlCases()
    {
        // From upstream re2/testing/parse_test.cc (NeverNL regression cases).
        int testZeroFlags = Regexp.WAS_DOLLAR;
        List<CaseFlags> cases = List.of(
                new CaseFlags("[^ ]", "cc{0-0x9 0xb-0x1f 0x21-0x10ffff}", testZeroFlags),
                new CaseFlags("[^ ]", "cc{0-0x9 0xb-0x1f 0x21-0x10ffff}", Regexp.FOLD_CASE),
                new CaseFlags("[^ ]", "cc{0-0x9 0xb-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE),
                new CaseFlags("[^ ]", "cc{0-0x9 0xb-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE | Regexp.FOLD_CASE),
                new CaseFlags("[^ \f]", "cc{0-0x9 0xb 0xd-0x1f 0x21-0x10ffff}", testZeroFlags),
                new CaseFlags("[^ \f]", "cc{0-0x9 0xb 0xd-0x1f 0x21-0x10ffff}", Regexp.FOLD_CASE),
                new CaseFlags("[^ \f]", "cc{0-0x9 0xb 0xd-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE),
                new CaseFlags("[^ \f]", "cc{0-0x9 0xb 0xd-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE | Regexp.FOLD_CASE),
                new CaseFlags("[^ \r]", "cc{0-0x9 0xb-0xc 0xe-0x1f 0x21-0x10ffff}", testZeroFlags),
                new CaseFlags("[^ \r]", "cc{0-0x9 0xb-0xc 0xe-0x1f 0x21-0x10ffff}", Regexp.FOLD_CASE),
                new CaseFlags("[^ \r]", "cc{0-0x9 0xb-0xc 0xe-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE),
                new CaseFlags("[^ \r]", "cc{0-0x9 0xb-0xc 0xe-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE | Regexp.FOLD_CASE),
                new CaseFlags("[^ \u000b]", "cc{0-0x9 0xc-0x1f 0x21-0x10ffff}", testZeroFlags),
                new CaseFlags("[^ \u000b]", "cc{0-0x9 0xc-0x1f 0x21-0x10ffff}", Regexp.FOLD_CASE),
                new CaseFlags("[^ \u000b]", "cc{0-0x9 0xc-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE),
                new CaseFlags("[^ \u000b]", "cc{0-0x9 0xc-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE | Regexp.FOLD_CASE),
                new CaseFlags("[^ \t]", "cc{0-0x8 0xb-0x1f 0x21-0x10ffff}", testZeroFlags),
                new CaseFlags("[^ \t]", "cc{0-0x8 0xb-0x1f 0x21-0x10ffff}", Regexp.FOLD_CASE),
                new CaseFlags("[^ \t]", "cc{0-0x8 0xb-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE),
                new CaseFlags("[^ \t]", "cc{0-0x8 0xb-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE | Regexp.FOLD_CASE),
                new CaseFlags("[^ \r\f\u000b]", "cc{0-0x9 0xe-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE),
                new CaseFlags("[^ \r\f\u000b]", "cc{0-0x9 0xe-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE | Regexp.FOLD_CASE),
                new CaseFlags("[^ \r\f\t\u000b]", "cc{0-0x8 0xe-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE),
                new CaseFlags("[^ \r\f\t\u000b]", "cc{0-0x8 0xe-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE | Regexp.FOLD_CASE),
                new CaseFlags("[^ \r\n\f\t\u000b]", "cc{0-0x8 0xe-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE),
                new CaseFlags("[^ \r\n\f\t\u000b]", "cc{0-0x8 0xe-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE | Regexp.FOLD_CASE),
                new CaseFlags("[^ \r\n\f\t]", "cc{0-0x8 0xb 0xe-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE),
                new CaseFlags("[^ \r\n\f\t]", "cc{0-0x8 0xb 0xe-0x1f 0x21-0x10ffff}", Regexp.NEVER_NEWLINE | Regexp.FOLD_CASE),
                new CaseFlags("[^\\t-\\n\\f-\\r ]", "cc{0-0x8 0xb 0xe-0x1f 0x21-0x10ffff}", Regexp.PERL_CLASSES),
                new CaseFlags("[^\\t-\\n\\f-\\r ]", "cc{0-0x8 0xb 0xe-0x1f 0x21-0x10ffff}", Regexp.PERL_CLASSES | Regexp.FOLD_CASE),
                new CaseFlags("[^\\t-\\n\\f-\\r ]", "cc{0-0x8 0xb 0xe-0x1f 0x21-0x10ffff}", Regexp.PERL_CLASSES | Regexp.NEVER_NEWLINE),
                new CaseFlags("[^\\t-\\n\\f-\\r ]", "cc{0-0x8 0xb 0xe-0x1f 0x21-0x10ffff}", Regexp.PERL_CLASSES | Regexp.NEVER_NEWLINE | Regexp.FOLD_CASE),
                new CaseFlags("\\S", "cc{0-0x8 0xb 0xe-0x1f 0x21-0x10ffff}", Regexp.PERL_CLASSES),
                new CaseFlags("\\S", "cc{0-0x8 0xb 0xe-0x1f 0x21-0x10ffff}", Regexp.PERL_CLASSES | Regexp.FOLD_CASE),
                new CaseFlags("\\S", "cc{0-0x8 0xb 0xe-0x1f 0x21-0x10ffff}", Regexp.PERL_CLASSES | Regexp.NEVER_NEWLINE),
                new CaseFlags("\\S", "cc{0-0x8 0xb 0xe-0x1f 0x21-0x10ffff}", Regexp.PERL_CLASSES | Regexp.NEVER_NEWLINE | Regexp.FOLD_CASE));

        assertFlagCases(cases);
    }

    @Test
    public void testUpstreamParseLatin1Cases()
    {
        // From upstream re2/testing/parse_test.cc Latin1 cases.
        List<ByteCase> cases = List.of(
                new ByteCase(new byte[] {(byte) 0xa5, 0x64, (byte) 0xd1}, "str{\u00a5d\u00d1}", Regexp.LATIN1),
                new ByteCase(new byte[] {(byte) 0xa5, (byte) 0xd1, 0x64}, "str{\u00a5\u00d1d}", Regexp.LATIN1),
                new ByteCase(new byte[] {(byte) 0xa5, 0x64, '[', (byte) 0xd1, (byte) 0xd2, ']'}, "cat{str{\u00a5d}cc{0xd1-0xd2}}", Regexp.LATIN1),
                new ByteCase(new byte[] {(byte) 0xa5, '[', (byte) 0xd1, (byte) 0xd2, ']', 0x64}, "cat{lit{\u00a5}cc{0xd1-0xd2}lit{d}}", Regexp.LATIN1),
                new ByteCase(new byte[] {(byte) 0xa5, 0x64, '|', (byte) 0xa5, (byte) 0xd1}, "cat{lit{\u00a5}cc{0x64 0xd1}}", Regexp.LATIN1),
                new ByteCase(new byte[] {(byte) 0xa5, (byte) 0xd1, '|', (byte) 0xa5, 0x64}, "cat{lit{\u00a5}cc{0x64 0xd1}}", Regexp.LATIN1),
                new ByteCase(new byte[] {(byte) 0xa5, 0x64, '|', (byte) 0xa5, '[', (byte) 0xd1, (byte) 0xd2, ']'}, "cat{lit{\u00a5}cc{0x64 0xd1-0xd2}}", Regexp.LATIN1),
                new ByteCase(new byte[] {(byte) 0xa5, '[', (byte) 0xd1, (byte) 0xd2, ']', '|', (byte) 0xa5, 0x64}, "cat{lit{\u00a5}cc{0x64 0xd1-0xd2}}", Regexp.LATIN1),
                new ByteCase(new byte[] {(byte) 0xa5, 0x64, (byte) 0xd1}, "strfold{\u00a5d\u00d1}", Regexp.LATIN1 | Regexp.FOLD_CASE),
                new ByteCase(new byte[] {(byte) 0xa5, (byte) 0xd1, 0x64}, "strfold{\u00a5\u00d1d}", Regexp.LATIN1 | Regexp.FOLD_CASE),
                new ByteCase(new byte[] {(byte) 0xa5, 0x64, '[', (byte) 0xd1, (byte) 0xd2, ']'}, "cat{strfold{\u00a5d}cc{0xd1-0xd2}}", Regexp.LATIN1 | Regexp.FOLD_CASE),
                new ByteCase(new byte[] {(byte) 0xa5, '[', (byte) 0xd1, (byte) 0xd2, ']', 0x64}, "cat{lit{\u00a5}cc{0xd1-0xd2}litfold{d}}", Regexp.LATIN1 | Regexp.FOLD_CASE),
                new ByteCase(new byte[] {(byte) 0xa5, 0x64, '|', (byte) 0xa5, (byte) 0xd1}, "cat{lit{\u00a5}cc{0x44 0x64 0xd1}}", Regexp.LATIN1 | Regexp.FOLD_CASE),
                new ByteCase(new byte[] {(byte) 0xa5, (byte) 0xd1, '|', (byte) 0xa5, 0x64}, "cat{lit{\u00a5}cc{0x44 0x64 0xd1}}", Regexp.LATIN1 | Regexp.FOLD_CASE),
                new ByteCase(new byte[] {(byte) 0xa5, 0x64, '|', (byte) 0xa5, '[', (byte) 0xd1, (byte) 0xd2, ']'}, "cat{lit{\u00a5}cc{0x44 0x64 0xd1-0xd2}}", Regexp.LATIN1 | Regexp.FOLD_CASE),
                new ByteCase(new byte[] {(byte) 0xa5, '[', (byte) 0xd1, (byte) 0xd2, ']', '|', (byte) 0xa5, 0x64}, "cat{lit{\u00a5}cc{0x44 0x64 0xd1-0xd2}}", Regexp.LATIN1 | Regexp.FOLD_CASE));

        assertByteCases(cases);
    }

    private static void assertCases(List<Case> cases, int flags, boolean testToString)
    {
        List<Regexp> regexps = new ArrayList<>(cases.size());
        for (Case testCase : cases) {
            Regexp regexp = parse(testCase.pattern(), flags);
            assertThat(RegexpDump.dump(regexp)).as("pattern %s", testCase.pattern()).isEqualTo(testCase.expectedDump());
            regexps.add(regexp);
            if (testToString) {
                assertToStringFixedPoint(regexp, flags);
            }
        }
        assertStructuralEquality(cases.stream().map(Case::expectedDump).toList(), regexps);
    }

    private static void assertFlagCases(List<CaseFlags> cases)
    {
        List<Regexp> regexps = new ArrayList<>(cases.size());
        for (CaseFlags testCase : cases) {
            int flags = testCase.flags() & ~Regexp.WAS_DOLLAR;
            Regexp regexp = parse(testCase.pattern(), flags);
            assertThat(RegexpDump.dump(regexp)).as("pattern %s", testCase.pattern()).isEqualTo(testCase.expectedDump());
            regexps.add(regexp);
            assertToStringFixedPoint(regexp, flags);
        }
        assertStructuralEquality(cases.stream().map(CaseFlags::expectedDump).toList(), regexps);
    }

    private static void assertByteCases(List<ByteCase> cases)
    {
        List<Regexp> regexps = new ArrayList<>(cases.size());
        for (ByteCase testCase : cases) {
            Regexp regexp = parse(Slices.wrappedBuffer(testCase.pattern()), testCase.flags());
            assertThat(RegexpDump.dump(regexp)).isEqualTo(testCase.expectedDump());
            regexps.add(regexp);
            assertToStringFixedPoint(regexp, testCase.flags());
        }
        assertStructuralEquality(cases.stream().map(ByteCase::expectedDump).toList(), regexps);
    }

    private static void assertStructuralEquality(List<String> expectedDumps, List<Regexp> regexps)
    {
        for (int leftIndex = 0; leftIndex < regexps.size(); leftIndex++) {
            for (int rightIndex = 0; rightIndex < regexps.size(); rightIndex++) {
                boolean expectedEqual = expectedDumps.get(leftIndex).equals(expectedDumps.get(rightIndex));
                assertThat(regexps.get(leftIndex).equals(regexps.get(rightIndex)))
                        .as("case %s compared with case %s", leftIndex, rightIndex)
                        .isEqualTo(expectedEqual);
                if (expectedEqual) {
                    assertThat(regexps.get(leftIndex).hashCode()).isEqualTo(regexps.get(rightIndex).hashCode());
                }
            }
        }
    }

    private static void assertToStringFixedPoint(Regexp regexp, int flags)
    {
        String formatted = RegexpToString.toString(regexp);
        Regexp reparsed = parse(formatted, flags);
        assertThat(reparsed).isEqualTo(regexp);
        assertThat(RegexpToString.toString(reparsed)).isEqualTo(formatted);
    }

    private static String parseAndDump(String pattern)
    {
        return parseAndDump(pattern, UPSTREAM_TEST_FLAGS);
    }

    private static String parseAndDump(String pattern, int flags)
    {
        return RegexpDump.dump(parse(pattern, flags));
    }

    private static String parseAndDump(Slice pat, int flags)
    {
        return RegexpDump.dump(parse(pat, flags));
    }

    private static Regexp parse(String pattern, int flags)
    {
        Charset charset = (flags & Regexp.LATIN1) == 0 ? StandardCharsets.UTF_8 : StandardCharsets.ISO_8859_1;
        return parse(Slices.wrappedBuffer(pattern.getBytes(charset)), flags);
    }

    private static Regexp parse(Slice pattern, int flags)
    {
        ParseResult result = RegexpParser.parse(pattern, flags);
        return result.regexp();
    }
}
