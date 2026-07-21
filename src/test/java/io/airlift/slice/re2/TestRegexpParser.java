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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TestRegexpParser
{
    @Test
    public void testDeeplyNestedGroupsAreStackSafe()
    {
        String pattern = "(?:".repeat(10_000) + "a" + ")".repeat(10_000);
        assertThatCode(() -> RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), Regexp.PERL_EXTENSIONS))
                .doesNotThrowAnyException();
    }

    @Test
    public void testMaximumCapturesAreStackSafe()
            throws InterruptedException
    {
        int captureCount = 10_000;
        String pattern = "(".repeat(captureCount) + "a" + ")".repeat(captureCount) + "{2}";
        AtomicReference<ParseResult> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread parserThread = new Thread(null, () -> {
            try {
                result.set(RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), 0));
            }
            catch (Throwable throwable) {
                failure.set(throwable);
            }
        }, "regexp-parser-small-stack", 256 * 1024);

        parserThread.start();
        parserThread.join();

        assertThat(failure.get()).isNull();
        assertThat(result.get().numCaptures()).isEqualTo(captureCount);
    }

    @Test
    public void testParseLiteralString()
    {
        ParseResult result = RegexpParser.parse(Slices.wrappedBuffer("abc".getBytes(StandardCharsets.UTF_8)), 0);

        Regexp re = result.regexp();
        assertThat(re.op()).isEqualTo(RegexpOp.LITERAL_STRING);
        assertThat(re.runes()).containsExactly('a', 'b', 'c');
    }

    @Test
    public void testParseAlternation()
    {
        ParseResult result = RegexpParser.parse(Slices.wrappedBuffer("a|b".getBytes(StandardCharsets.UTF_8)), 0);

        Regexp re = result.regexp();
        // Parse() returns a simplified canonical form (like upstream RE2).
        assertThat(re.op()).isEqualTo(RegexpOp.CHAR_CLASS);
        assertThat(re.charClass().contains('a')).isTrue();
        assertThat(re.charClass().contains('b')).isTrue();
    }

    @Test
    public void testParseNonCapturingGroup()
    {
        ParseResult result = RegexpParser.parse(Slices.wrappedBuffer("(?:a)".getBytes(StandardCharsets.UTF_8)), Regexp.PERL_EXTENSIONS);
        assertThat(result.numCaptures()).isEqualTo(0);
        assertThat(result.regexp().op()).isEqualTo(RegexpOp.LITERAL);
        assertThat(result.regexp().rune()).isEqualTo('a');
    }

    @Test
    public void testParseCapturingGroup()
    {
        ParseResult result = RegexpParser.parse(Slices.wrappedBuffer("(a)".getBytes(StandardCharsets.UTF_8)), 0);
        assertThat(result.numCaptures()).isEqualTo(1);
        assertThat(result.regexp().op()).isEqualTo(RegexpOp.CAPTURE);
        assertThat(result.regexp().cap()).isEqualTo(1);
        assertThat(result.regexp().sub(0).rune()).isEqualTo('a');
    }

    @Test
    public void testParseCharClassRange()
    {
        ParseResult result = RegexpParser.parse(Slices.wrappedBuffer("[a-z]".getBytes(StandardCharsets.UTF_8)), 0);

        Regexp re = result.regexp();
        assertThat(re.op()).isEqualTo(RegexpOp.CHAR_CLASS);
        assertThat(re.charClass().contains('a')).isTrue();
        assertThat(re.charClass().contains('z')).isTrue();
        assertThat(re.charClass().contains('A')).isFalse();
    }

    @Test
    public void testParseAnyByteEscape()
    {
        ParseResult result = RegexpParser.parse(Slices.wrappedBuffer("\\C".getBytes(StandardCharsets.UTF_8)), Regexp.PERL_EXTENSIONS);
        assertThat(result.regexp().op()).isEqualTo(RegexpOp.ANY_BYTE);
    }

    @Test
    public void testLatin1TreatsBytesAsRunes()
    {
        // UTF-8 encoding of U+00E2 is C3 A2.
        byte[] pattern = new byte[] {(byte) 0xC3, (byte) 0xA2};

        ParseResult utf8 = RegexpParser.parse(Slices.wrappedBuffer(pattern), 0);
        assertThat(utf8.regexp().op()).isEqualTo(RegexpOp.LITERAL);
        assertThat(utf8.regexp().rune()).isEqualTo(0x00E2);

        ParseResult latin1 = RegexpParser.parse(Slices.wrappedBuffer(pattern), Regexp.LATIN1);
        assertThat(latin1.regexp().op()).isEqualTo(RegexpOp.LITERAL_STRING);
        assertThat(latin1.regexp().runes()).containsExactly(0xC3, 0xA2);
    }

    @Test
    public void testLeadingZerosInRepeatTreatedAsLiteral()
    {
        // C++ RE2 treats {01,02} as literal characters, not a repeat operator
        // Pattern "a{01,02}" should match the literal string "a{01,02}"
        ParseResult result = RegexpParser.parse(Slices.wrappedBuffer("a{01,02}".getBytes(StandardCharsets.UTF_8)), 0);

        Regexp re = result.regexp();
        // Should parse as a literal string, not a repeat
        assertThat(re.op()).isEqualTo(RegexpOp.LITERAL_STRING);
        // "a{01,02}" as literal characters
        assertThat(re.runes()).containsExactly('a', '{', '0', '1', ',', '0', '2', '}');

        // Leading zero in second number should also be treated as literal
        ParseResult result2 = RegexpParser.parse(Slices.wrappedBuffer("a{1,02}".getBytes(StandardCharsets.UTF_8)), 0);
        Regexp re2 = result2.regexp();
        assertThat(re2.op()).isEqualTo(RegexpOp.LITERAL_STRING);
        assertThat(re2.runes()).containsExactly('a', '{', '1', ',', '0', '2', '}');

        // Single leading zero alone (e.g., {0,2}) should be valid - 0 is not a leading zero
        ParseResult result3 = RegexpParser.parse(Slices.wrappedBuffer("a{0,2}".getBytes(StandardCharsets.UTF_8)), 0);
        Regexp re3 = result3.regexp();
        // {0,2} is a valid repeat - should not be treated as literal
        assertThat(re3.op()).isNotEqualTo(RegexpOp.LITERAL_STRING);
    }

    @Test
    public void testSquashDuplicateRepeatOperators()
    {
        // ** squashes to * (same operator)
        ParseResult starStar = RegexpParser.parse(Slices.wrappedBuffer("a**".getBytes(StandardCharsets.UTF_8)), 0);
        assertThat(starStar.regexp().op()).isEqualTo(RegexpOp.STAR);
        assertThat(starStar.regexp().sub(0).op()).isEqualTo(RegexpOp.LITERAL);

        // *+ squashes to * (mixed operators become STAR)
        ParseResult starPlus = RegexpParser.parse(Slices.wrappedBuffer("a*+".getBytes(StandardCharsets.UTF_8)), 0);
        assertThat(starPlus.regexp().op()).isEqualTo(RegexpOp.STAR);
        assertThat(starPlus.regexp().sub(0).op()).isEqualTo(RegexpOp.LITERAL);

        // ?* squashes to * (mixed operators become STAR)
        ParseResult questStar = RegexpParser.parse(Slices.wrappedBuffer("a?*".getBytes(StandardCharsets.UTF_8)), 0);
        assertThat(questStar.regexp().op()).isEqualTo(RegexpOp.STAR);
        assertThat(questStar.regexp().sub(0).op()).isEqualTo(RegexpOp.LITERAL);

        // ++ squashes to + (same operator)
        ParseResult plusPlus = RegexpParser.parse(Slices.wrappedBuffer("a++".getBytes(StandardCharsets.UTF_8)), 0);
        assertThat(plusPlus.regexp().op()).isEqualTo(RegexpOp.PLUS);
        assertThat(plusPlus.regexp().sub(0).op()).isEqualTo(RegexpOp.LITERAL);
    }

    @Test
    public void testAlternationPrefixFactoring()
    {
        // hello|help should factor to hel(lo|p)
        // After parsing, we should see a CONCAT with "hel" prefix and alternation suffix
        ParseResult result = RegexpParser.parse(Slices.wrappedBuffer("hello|help".getBytes(StandardCharsets.UTF_8)), 0);

        Regexp re = result.regexp();
        // Should be a CONCAT of "hel" and alternation of "lo|p"
        assertThat(re.op()).isEqualTo(RegexpOp.CONCAT);
        assertThat(re.subs().size()).isEqualTo(2);

        // First part should be "hel"
        Regexp prefix = re.sub(0);
        assertThat(prefix.op()).isEqualTo(RegexpOp.LITERAL_STRING);
        assertThat(prefix.runes()).containsExactly('h', 'e', 'l');

        // Second part should be an alternation of "lo" and "p"
        Regexp alt = re.sub(1);
        assertThat(alt.op()).isEqualTo(RegexpOp.ALTERNATE);
    }

    @Test
    public void testPosixAsciiCharClass()
    {
        // Test [[:ascii:]] matches bytes 0-127 (ASCII range)
        ParseResult result = RegexpParser.parse(Slices.wrappedBuffer("[[:ascii:]]".getBytes(StandardCharsets.UTF_8)), 0);

        Regexp re = result.regexp();
        assertThat(re.op()).isEqualTo(RegexpOp.CHAR_CLASS);

        // Verify ASCII bytes are included
        assertThat(re.charClass().contains(0)).isTrue();      // NUL
        assertThat(re.charClass().contains(0x20)).isTrue();   // space
        assertThat(re.charClass().contains('A')).isTrue();    // 0x41
        assertThat(re.charClass().contains('z')).isTrue();    // 0x7A
        assertThat(re.charClass().contains(0x7F)).isTrue();   // DEL

        // Verify non-ASCII bytes are excluded
        assertThat(re.charClass().contains(0x80)).isFalse();
        assertThat(re.charClass().contains(0xFF)).isFalse();
    }
}
