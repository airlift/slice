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
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Ported from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2ErrorCode
{
    private record ErrorCase(byte[] pattern, RegexpStatusCode code, byte[] arg) {}

    @Test
    public void testErrorCodeAndArg()
    {
        List<ErrorCase> cases = new ArrayList<>();
        cases.add(new ErrorCase(utf8Bytes("ab\\\u03b1cd"), RegexpStatusCode.BAD_ESCAPE, utf8Bytes("\\\u03b1")));
        cases.add(new ErrorCase(utf8Bytes("ef\\x\u263a01"), RegexpStatusCode.BAD_ESCAPE, utf8Bytes("\\x\u263a0")));
        cases.add(new ErrorCase(utf8Bytes("gh\\x1\u263a01"), RegexpStatusCode.BAD_ESCAPE, utf8Bytes("\\x1\u263a")));
        cases.add(new ErrorCase(utf8Bytes("ij\\x1"), RegexpStatusCode.BAD_ESCAPE, utf8Bytes("\\x1")));
        cases.add(new ErrorCase(utf8Bytes("kl\\x"), RegexpStatusCode.BAD_ESCAPE, utf8Bytes("\\x")));
        cases.add(new ErrorCase(utf8Bytes("uv\\x{0000\u263a}"), RegexpStatusCode.BAD_ESCAPE, utf8Bytes("\\x{0000\u263a")));
        cases.add(new ErrorCase(utf8Bytes("wx\\p{ABC"), RegexpStatusCode.BAD_CHAR_RANGE, utf8Bytes("\\p{ABC")));
        cases.add(new ErrorCase(utf8Bytes("yz(?smiUX:abc)"), RegexpStatusCode.BAD_PERL_OP, utf8Bytes("(?smiUX")));
        cases.add(new ErrorCase(utf8Bytes("aa(?sm\u263ai"), RegexpStatusCode.BAD_PERL_OP, utf8Bytes("(?sm\u263a")));
        cases.add(new ErrorCase(utf8Bytes("bb[abc"), RegexpStatusCode.MISSING_BRACKET, utf8Bytes("[abc")));
        cases.add(new ErrorCase(utf8Bytes("abc(def"), RegexpStatusCode.MISSING_PAREN, utf8Bytes("abc(def")));
        cases.add(new ErrorCase(utf8Bytes("abc)def"), RegexpStatusCode.UNEXPECTED_PAREN, utf8Bytes("abc)def")));

        cases.add(new ErrorCase(new byte[] {'m', 'n', '\\', 'x', '1', (byte) 0xFF}, RegexpStatusCode.BAD_UTF8, new byte[0]));
        cases.add(new ErrorCase(new byte[] {'o', 'p', (byte) 0xFF, 'q', 'r'}, RegexpStatusCode.BAD_UTF8, new byte[0]));
        cases.add(new ErrorCase(new byte[] {'s', 't', '\\', 'x', '{', '0', '0', '0', '0', '0', (byte) 0xFF}, RegexpStatusCode.BAD_UTF8, new byte[0]));
        cases.add(new ErrorCase(new byte[] {'z', 'z', '\\', 'p', '{', (byte) 0xFF, '}'}, RegexpStatusCode.BAD_UTF8, new byte[0]));
        cases.add(new ErrorCase(new byte[] {'z', 'z', '\\', 'x', '{', '0', '0', (byte) 0xFF, '}'}, RegexpStatusCode.BAD_UTF8, new byte[0]));
        cases.add(new ErrorCase(new byte[] {'z', 'z', '(', '?', 'P', '<', 'n', 'a', 'm', 'e', (byte) 0xFF, '>', 'a', 'b', 'c', ')'}, RegexpStatusCode.BAD_UTF8, new byte[0]));

        Re2.Options options = Re2.Options.defaults();

        for (ErrorCase test : cases) {
            assertThatThrownBy(() -> Re2.compile(Slices.wrappedBuffer(test.pattern()), options))
                    .as("expected parse failure for: %s", new String(test.pattern(), StandardCharsets.ISO_8859_1))
                    .isInstanceOfSatisfying(RegexpParseException.class, parseException -> {
                        assertThat(parseException.statusCode()).isEqualTo(test.code());
                        assertErrorArg(parseException.errorArgument(), test.arg());
                    });
        }
    }

    private static void assertErrorArg(Slice actual, byte[] expected)
    {
        if (expected.length == 0) {
            assertThat(actual == null || actual.length() == 0).isTrue();
            return;
        }
        assertThat(actual).isNotNull();
        byte[] bytes = new byte[actual.length()];
        System.arraycopy(actual.byteArray(), actual.byteArrayOffset(), bytes, 0, actual.length());
        assertThat(bytes).isEqualTo(expected);
    }

    private static byte[] utf8Bytes(String value)
    {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
