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

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TestNfaNoSubmatchParity
{
    private static final int FLAGS = Regexp.LIKE_PERL | Regexp.LATIN1;

    @Test
    public void testNoSubmatchParityAcrossRepresentativePatterns()
    {
        String[] patterns = {
                "(fo|foo)",
                "([ab]+)(cd)?e",
                "\\b(foo|bar)\\b",
                "[XYZ]ABCDEFGHIJKLMNOPQRSTUVWXYZ$",
                "^([A-Z]+)$",
        };

        Slice[] texts = {
                latin1(""),
                latin1("foo"),
                latin1("x foobar baz y"),
                randomPrintableText(256, 11),
                randomPrintableText(4096, 12),
        };

        for (String pattern : patterns) {
            Prog prog = compile(pattern);
            for (Slice text : texts) {
                assertParity(prog, text, text, false, false, pattern);
                assertParity(prog, text, text, false, true, pattern);
                assertParity(prog, text, text, true, false, pattern);
                assertParity(prog, text, text, true, true, pattern);
            }
        }
    }

    @Test
    public void testNoSubmatchParityWithAnchorsAndContextSlices()
    {
        Prog beginAndEnd = compile("^a$");

        Slice singleA = latin1("a");
        assertParity(beginAndEnd, singleA, singleA, false, false, "^a$");
        assertParity(beginAndEnd, singleA, singleA, true, false, "^a$");

        Slice context = latin1("xa");
        Slice suffix = Slices.wrappedBuffer(context.byteArray(), context.byteArrayOffset() + 1, 1);
        assertParity(beginAndEnd, suffix, context, false, false, "^a$");
        assertParity(beginAndEnd, suffix, context, true, false, "^a$");

        Prog endOnly = compile("a$");
        Slice ba = latin1("ba");
        Slice ab = latin1("ab");
        assertParity(endOnly, ba, ba, false, false, "a$");
        assertParity(endOnly, ab, ab, false, false, "a$");

        Slice endContext = latin1("baZ");
        Slice endSlice = Slices.wrappedBuffer(endContext.byteArray(), endContext.byteArrayOffset(), 2);
        assertParity(endOnly, endSlice, endContext, false, false, "a$");
    }

    @Test
    public void testNoSubmatchParityOnHardAndParensPatterns()
    {
        Prog hard = compile("[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$");
        Prog parens = compile(
                "([ -~])*(A)(B)(C)(D)(E)(F)(G)(H)(I)(J)(K)(L)(M)(N)(O)(P)(Q)(R)(S)(T)(U)(V)(W)(X)(Y)(Z)$");

        Slice text8 = randomPrintableText(8, 1);
        Slice text64 = randomPrintableText(64, 1);
        Slice text256k = randomPrintableText(262_144, 1);

        assertParity(hard, text8, text8, false, false, "hard");
        assertParity(hard, text64, text64, false, false, "hard");
        assertParity(hard, text256k, text256k, false, false, "hard");

        assertParity(parens, text8, text8, false, false, "parens");
        assertParity(parens, text64, text64, false, false, "parens");
        assertParity(parens, text256k, text256k, false, false, "parens");
    }

    private static Prog compile(String pattern)
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), FLAGS);

        Prog prog = Compiler.compile(parsed.regexp(), false, 0);
        assertThat(prog).as("compile: %s", pattern).isNotNull();
        return prog;
    }

    private static Slice latin1(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.ISO_8859_1));
    }

    private static Slice randomPrintableText(int size, int seed)
    {
        Random random = new Random(seed);
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            int c = random.nextInt(128);
            if (c < 0x20) {
                c = 0x20;
            }
            bytes[i] = (byte) c;
        }
        return Slices.wrappedBuffer(bytes);
    }

    private static void assertParity(Prog prog, Slice text, Slice context, boolean anchored, boolean longest, String pattern)
    {
        int start = text.byteArrayOffset() - context.byteArrayOffset();
        int end = start + text.length();
        boolean noSubmatch = Nfa.search(prog, context, start, end, anchored, (longest ? Prog.MatchKind.LONGEST_MATCH : Prog.MatchKind.FIRST_MATCH), null);
        boolean withSubmatch = Nfa.search(prog, context, start, end, anchored, (longest ? Prog.MatchKind.LONGEST_MATCH : Prog.MatchKind.FIRST_MATCH), new int[8]);

        assertThat(noSubmatch)
                .as(
                        "parity pattern=%s textLen=%s contextLen=%s anchored=%s longest=%s",
                        pattern,
                        text.length(),
                        context.length(),
                        anchored,
                        longest)
                .isEqualTo(withSubmatch);
    }
}
