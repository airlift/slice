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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2QuoteMeta
{
    @Test
    public void testQuoteMetaSimple()
    {
        List<String> cases = List.of(
                "foo",
                "foo.bar",
                "foo\\.bar",
                "[1-9]",
                "1.5-2.0?",
                "\\d",
                "Who doesn't like ice cream?",
                "((a|b)c?d*e+[f-h]i)",
                "((?!)xxx).*yyy",
                "([");

        for (String c : cases) {
            testQuoteMeta(utf8(c), Re2.Options.defaults());
        }
    }

    @Test
    public void testQuoteMetaSimpleNegative()
    {
        negativeQuoteMeta(utf8("foo"), utf8("bar"), Re2.Options.defaults());
        negativeQuoteMeta(utf8("..."), utf8("bar"), Re2.Options.defaults());
        negativeQuoteMeta(utf8("\\."), utf8("."), Re2.Options.defaults());
        negativeQuoteMeta(utf8("\\."), utf8(".."), Re2.Options.defaults());
        negativeQuoteMeta(utf8("(a)"), utf8("a"), Re2.Options.defaults());
        negativeQuoteMeta(utf8("(a|b)"), utf8("a"), Re2.Options.defaults());
        negativeQuoteMeta(utf8("(a|b)"), utf8("(a)"), Re2.Options.defaults());
        negativeQuoteMeta(utf8("(a|b)"), utf8("a|b"), Re2.Options.defaults());
        negativeQuoteMeta(utf8("[0-9]"), utf8("0"), Re2.Options.defaults());
        negativeQuoteMeta(utf8("[0-9]"), utf8("0-9"), Re2.Options.defaults());
        negativeQuoteMeta(utf8("[0-9]"), utf8("[9]"), Re2.Options.defaults());
        negativeQuoteMeta(utf8("((?!)xxx)"), utf8("xxx"), Re2.Options.defaults());
    }

    @Test
    public void testQuoteMetaLatin1()
    {
        Re2.Options latin1 = Re2.Options.latin1();
        testQuoteMeta(bytes(new byte[] {'3', (byte) 0xb2, ' ', '=', ' ', '9'}), latin1);
    }

    @Test
    public void testQuoteMetaUtf8()
    {
        Re2.Options utf8 = Re2.Options.defaults();
        testQuoteMeta(utf8("Plácido Domingo"), utf8);
        testQuoteMeta(utf8("xyz"), utf8);
        testQuoteMeta(bytes(new byte[] {(byte) 0xc2, (byte) 0xb0}), utf8);
        testQuoteMeta(bytes(new byte[] {'2', '7', (byte) 0xc2, (byte) 0xb0, ' ', 'd', 'e', 'g', 'r', 'e', 'e', 's'}), utf8);
        testQuoteMeta(bytes(new byte[] {(byte) 0xe2, (byte) 0x80, (byte) 0xb3}), utf8);
        testQuoteMeta(bytes(new byte[] {(byte) 0xf0, (byte) 0x9d, (byte) 0x85, (byte) 0x9f}), utf8);
        testQuoteMeta(bytes(new byte[] {'2', '7', (byte) 0xc2, (byte) 0xb0}), utf8);
        negativeQuoteMeta(bytes(new byte[] {'2', '7', (byte) 0xc2, (byte) 0xb0}),
                bytes(new byte[] {'2', '7', '\\', (byte) 0xc2, '\\', (byte) 0xb0}),
                utf8);
    }

    @Test
    public void testQuoteMetaHasNull()
    {
        Re2.Options utf8 = Re2.Options.defaults();
        testQuoteMeta(bytes(new byte[] {0}), utf8);
        negativeQuoteMeta(bytes(new byte[] {0}), bytes(new byte[0]), utf8);

        testQuoteMeta(bytes(new byte[] {0, '1'}), utf8);
        negativeQuoteMeta(bytes(new byte[] {0, '1'}), bytes(new byte[] {1}), utf8);
    }

    private static void testQuoteMeta(Slice unquoted, Re2.Options options)
    {
        Slice quoted = Re2.quote(unquoted);
        Re2 re2 = Re2.compile(quoted, options);
        assertThat(re2.fullMatch(unquoted)).as("unquoted %s", toString(unquoted)).isTrue();
    }

    private static void negativeQuoteMeta(Slice unquoted, Slice shouldNotMatch, Re2.Options options)
    {
        Slice quoted = Re2.quote(unquoted);
        Re2 re2 = Re2.compile(quoted, options);
        assertThat(re2.fullMatch(shouldNotMatch)).as("unquoted %s", toString(unquoted)).isFalse();
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Slice bytes(byte[] value)
    {
        return Slices.wrappedBuffer(value);
    }

    private static String toString(Slice slice)
    {
        return new String(slice.byteArray(), slice.byteArrayOffset(), slice.length(), StandardCharsets.UTF_8);
    }
}
