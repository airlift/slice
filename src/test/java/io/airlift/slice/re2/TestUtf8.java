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

import static org.assertj.core.api.Assertions.assertThat;

public class TestUtf8
{
    @Test
    public void testDecodeAscii()
    {
        byte[] bytes = {0x41};
        long decoded = Utf8.decode(bytes, 0, bytes.length);

        assertThat(Utf8.decodedCodePoint(decoded)).isEqualTo(0x41);
        assertThat(Utf8.decodedWidth(decoded)).isEqualTo(1);
    }

    @Test
    public void testDecodeMultiByte()
    {
        // U+00A2: C2 A2
        byte[] cents = {(byte) 0xC2, (byte) 0xA2};
        long decoded = Utf8.decode(cents, 0, cents.length);
        assertThat(Utf8.decodedCodePoint(decoded)).isEqualTo(0xA2);
        assertThat(Utf8.decodedWidth(decoded)).isEqualTo(2);

        // U+20AC: E2 82 AC
        byte[] euro = {(byte) 0xE2, (byte) 0x82, (byte) 0xAC};
        decoded = Utf8.decode(euro, 0, euro.length);
        assertThat(Utf8.decodedCodePoint(decoded)).isEqualTo(0x20AC);
        assertThat(Utf8.decodedWidth(decoded)).isEqualTo(3);

        // U+1F4A9: F0 9F 92 A9
        byte[] poo = {(byte) 0xF0, (byte) 0x9F, (byte) 0x92, (byte) 0xA9};
        decoded = Utf8.decode(poo, 0, poo.length);
        assertThat(Utf8.decodedCodePoint(decoded)).isEqualTo(0x1F4A9);
        assertThat(Utf8.decodedWidth(decoded)).isEqualTo(4);
    }

    @Test
    public void testCursor()
    {
        byte[] bytes = {0x41, (byte) 0xC2, (byte) 0xA2, 0x42};
        Utf8Cursor cursor = new Utf8Cursor(Slices.wrappedBuffer(bytes));

        assertThat(cursor.readCodePoint()).isEqualTo(0x41);
        assertThat(cursor.readCodePoint()).isEqualTo(0xA2);
        assertThat(cursor.readCodePoint()).isEqualTo(0x42);
        assertThat(cursor.readCodePoint()).isEqualTo(-1);
    }
}
