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

import static org.assertj.core.api.Assertions.assertThat;

public class TestRe2BooleanMatchOptimizations
{
    @Test
    public void testExactLiteralEligibility()
    {
        assertThat(compile("(💰)").exactLiteralForDiagnostics()).isEqualTo(utf8("💰"));
        assertThat(compile("(ab)(💰)").exactLiteralForDiagnostics()).isEqualTo(utf8("ab💰"));
        assertThat(compile("").exactLiteralForDiagnostics()).isEqualTo(Slices.EMPTY_SLICE);

        assertThat(compile("(?i)abc").exactLiteralForDiagnostics()).isNull();
        assertThat(compile("abc?").exactLiteralForDiagnostics()).isNull();
        assertThat(compile("^abc").exactLiteralForDiagnostics()).isNull();
        assertThat(compile("abc$").exactLiteralForDiagnostics()).isNull();
        assertThat(compile("a|b").exactLiteralForDiagnostics()).isNull();
    }

    @Test
    public void testExactLiteralPartialMatch()
    {
        Re2 pattern = compile("(ab)(💰)");
        assertThat(pattern.usesExactLiteralPartialMatchForDiagnostics()).isTrue();
        assertThat(pattern.partialMatch(utf8("xxab💰yy"))).isTrue();
        assertThat(pattern.partialMatch(utf8("xxabyy"))).isFalse();
        assertThat(compile("abc").partialMatch(utf8("xxaxcyy"))).isFalse();

        Slice backing = utf8("ignored-ab💰-ignored");
        Slice region = backing.slice(8, 6);
        assertThat(pattern.partialMatch(region)).isTrue();

        int[] groups = new int[6];
        assertThat(pattern.matchInto(region, Re2.Anchor.UNANCHORED, groups)).isTrue();
        assertThat(groups).containsExactly(0, 6, 0, 2, 2, 6);
    }

    @Test
    public void testLatin1ExactLiteralPartialMatch()
    {
        Re2 pattern = Re2.compile(Slices.wrappedBuffer(new byte[] {'(', (byte) 0xE9, ')'}), Re2.Options.latin1());
        assertThat(pattern.exactLiteralForDiagnostics()).isEqualTo(Slices.wrappedBuffer(new byte[] {(byte) 0xE9}));
        assertThat(pattern.partialMatch(Slices.wrappedBuffer(new byte[] {0, (byte) 0xE9, 1}))).isTrue();
        assertThat(pattern.partialMatch(Slices.wrappedBuffer(new byte[] {0, (byte) 0xE8, 1}))).isFalse();
    }

    @Test
    public void testEmptyAtStartPartialMatch()
    {
        Re2 pattern = compile("x*");
        assertThat(pattern.canReturnEmptyAtStartForBooleanMatch(utf8("aaa"), 0)).isTrue();
        assertThat(pattern.partialMatch(utf8("aaa"))).isTrue();

        assertThat(pattern.canReturnEmptyAtStartForBooleanMatch(utf8("xxx"), 0)).isFalse();
        assertThat(pattern.partialMatch(utf8("xxx"))).isTrue();

        Re2 captured = compile("(x*)");
        assertThat(captured.canReturnEmptyAtStartForBooleanMatch(utf8("aaa"), 0)).isTrue();
        assertThat(captured.partialMatch(utf8("aaa"))).isTrue();

        assertThat(compile("x*$").canReturnEmptyAtStartForBooleanMatch(utf8("aaa"), 0)).isFalse();
        assertThat(compile("\\b|x").canReturnEmptyAtStartForBooleanMatch(utf8("aaa"), 0)).isFalse();
    }

    private static Re2 compile(String pattern)
    {
        return Re2.compile(utf8(pattern));
    }

    private static Slice utf8(String value)
    {
        return Slices.utf8Slice(value);
    }
}
