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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Ported from upstream RE2: re2/testing/parse_test.cc.
public class TestUpstreamParseErrorArguments
{
    @Test
    public void testNamedCaptureErrorArgsFromUpstreamParseTest()
    {
        assertError("test(?P<name", RegexpStatusCode.BAD_NAMED_CAPTURE, "(?P<name");
        assertError("test(?P<space bar>z)", RegexpStatusCode.BAD_NAMED_CAPTURE, "(?P<space bar>");

        assertError("test(?<name", RegexpStatusCode.BAD_NAMED_CAPTURE, "(?<name");
        assertError("test(?<space bar>z)", RegexpStatusCode.BAD_NAMED_CAPTURE, "(?<space bar>");
    }

    @Test
    public void testLookAroundErrorArgsFromUpstreamParseTest()
    {
        assertError("(?=foo).*", RegexpStatusCode.BAD_PERL_OP, "(?=");
        assertError("(?!foo).*", RegexpStatusCode.BAD_PERL_OP, "(?!");
        assertError("(?<=foo).*", RegexpStatusCode.BAD_PERL_OP, "(?<=");
        assertError("(?<!foo).*", RegexpStatusCode.BAD_PERL_OP, "(?<!");
    }

    private static void assertError(String pattern, RegexpStatusCode expectedCode, String expectedErrorArg)
    {
        Slice pat = Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> RegexpParser.parse(pat, Regexp.LIKE_PERL))
                .isInstanceOfSatisfying(RegexpParseException.class, parseException -> {
                    assertThat(parseException.statusCode()).isEqualTo(expectedCode);

                    Slice arg = parseException.errorArgument();
                    assertThat(arg).isNotNull();
                    String argString = new String(arg.byteArray(), arg.byteArrayOffset(), arg.length(), StandardCharsets.UTF_8);
                    assertThat(argString).isEqualTo(expectedErrorArg);
                });
    }
}
