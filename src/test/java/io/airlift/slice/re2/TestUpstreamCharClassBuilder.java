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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/charclass_test.cc.
public class TestUpstreamCharClassBuilder
{
    private static final class Range
    {
        final int lo;
        final int hi;

        private Range(int lo, int hi)
        {
            this.lo = lo;
            this.hi = hi;
        }

        static Range of(int lo, int hi)
        {
            return new Range(lo, hi);
        }
    }

    private static final class Case
    {
        final Range[] add;
        final int removeAbove;
        final Range[] want;

        private Case(Range[] add, int removeAbove, Range[] want)
        {
            this.add = add;
            this.removeAbove = removeAbove;
            this.want = want;
        }
    }

    @Test
    public void testAddsRemoveAboveNegateAndCopy()
    {
        // From upstream re2/testing/charclass_test.cc.
        // The C++ version tests both CharClass and CharClassBuilder implementations.
        Case[] cases = new Case[] {
                new Case(new Range[] {Range.of(10, 20)}, -1, new Range[] {Range.of(10, 20)}),
                new Case(new Range[] {Range.of(10, 20), Range.of(20, 30)}, -1, new Range[] {Range.of(10, 30)}),
                new Case(new Range[] {Range.of(10, 20), Range.of(30, 40), Range.of(20, 30)}, -1, new Range[] {Range.of(10, 40)}),
                new Case(new Range[] {Range.of(0, 50), Range.of(20, 30)}, -1, new Range[] {Range.of(0, 50)}),

                new Case(new Range[] {Range.of(10, 11), Range.of(13, 14), Range.of(16, 17), Range.of(19, 20), Range.of(22, 23)}, -1,
                        new Range[] {Range.of(10, 11), Range.of(13, 14), Range.of(16, 17), Range.of(19, 20), Range.of(22, 23)}),

                // Out-of-order additions should sort and coalesce.
                new Case(new Range[] {Range.of(13, 14), Range.of(10, 11), Range.of(22, 23), Range.of(19, 20), Range.of(16, 17)}, -1,
                        new Range[] {Range.of(10, 11), Range.of(13, 14), Range.of(16, 17), Range.of(19, 20), Range.of(22, 23)}),

                // Wide range that swallows multiple existing ranges.
                new Case(new Range[] {Range.of(13, 14), Range.of(10, 11), Range.of(22, 23), Range.of(19, 20), Range.of(16, 17), Range.of(5, 25)}, -1,
                        new Range[] {Range.of(5, 25)}),

                // Bridge range that merges adjacent ranges.
                new Case(new Range[] {Range.of(13, 14), Range.of(10, 11), Range.of(22, 23), Range.of(19, 20), Range.of(16, 17), Range.of(12, 21)}, -1,
                        new Range[] {Range.of(10, 23)}),

                // Boundary cases during negation.
                new Case(new Range[] {Range.of(0, CharClass.RUNEMAX)}, -1, new Range[] {Range.of(0, CharClass.RUNEMAX)}),
                new Case(new Range[] {Range.of(0, 50)}, -1, new Range[] {Range.of(0, 50)}),
                new Case(new Range[] {Range.of(50, CharClass.RUNEMAX)}, -1, new Range[] {Range.of(50, CharClass.RUNEMAX)}),

                // RemoveAbove tests.
                new Case(new Range[] {Range.of(50, CharClass.RUNEMAX)}, 255, new Range[] {Range.of(50, 255)}),
                new Case(new Range[] {Range.of(50, CharClass.RUNEMAX)}, 65535, new Range[] {Range.of(50, 65535)}),
                new Case(new Range[] {Range.of(50, CharClass.RUNEMAX)}, CharClass.RUNEMAX, new Range[] {Range.of(50, CharClass.RUNEMAX)}),
                new Case(new Range[] {Range.of(50, 60), Range.of(250, 260), Range.of(350, 360)}, 255, new Range[] {Range.of(50, 60), Range.of(250, 255)}),
                new Case(new Range[] {Range.of(50, 60)}, 255, new Range[] {Range.of(50, 60)}),
                new Case(new Range[] {Range.of(350, 360)}, 255, new Range[] {}),
                new Case(new Range[] {}, 255, new Range[] {}),
        };

        for (Case t : cases) {
            CharClassBuilder ccb = new CharClassBuilder();
            for (Range r : t.add) {
                ccb.addRange(r.lo, r.hi);
            }
            if (t.removeAbove >= 0) {
                ccb.removeAbove(t.removeAbove);
            }

            assertCorrect(ccb, t);
            assertCorrectCharClass(ccb.toCharClass(), t);

            // Copy should preserve structure.
            CharClassBuilder ccbCopy = ccb.copy();
            assertCorrect(ccbCopy, t);
            assertCorrectCharClass(ccbCopy.toCharClass(), t);

            // Negation sanity: contains() should invert, and rune count should match.
            CharClassBuilder neg = ccb.copy();
            neg.negate();
            CharClass ncc = neg.toCharClass();

            for (int j = 0; j < 101; j++) {
                int probe = (j == 100) ? CharClass.RUNEMAX : j;
                boolean wantContains = shouldContain(t, probe);
                assertThat(ncc.contains(probe)).as("negate contains(%s)", probe).isNotEqualTo(wantContains);
            }

            assertThat(ncc.runeCount()).isEqualTo((CharClass.RUNEMAX + 1) - ccb.runeCount());
        }
    }

    private static void assertCorrect(CharClassBuilder ccb, Case t)
    {
        assertThat(ccb.rangeCount()).as("rangeCount").isEqualTo(t.want.length);
        assertThat(ccb.runeCount()).as("runeCount").isEqualTo(expectedRuneCount(t));

        for (int i = 0; i < t.want.length; i++) {
            RuneRange got = ccb.range(i);
            assertThat(got.low()).as("range[%s].low", i).isEqualTo(t.want[i].lo);
            assertThat(got.high()).as("range[%s].high", i).isEqualTo(t.want[i].hi);
        }

        for (int j = 0; j < 101; j++) {
            int probe = (j == 100) ? CharClass.RUNEMAX : j;
            assertThat(ccb.contains(probe)).as("contains(%s)", probe).isEqualTo(shouldContain(t, probe));
        }
    }

    private static void assertCorrectCharClass(CharClass cc, Case t)
    {
        assertThat(cc.rangeCount()).as("rangeCount").isEqualTo(t.want.length);
        assertThat(cc.runeCount()).as("runeCount").isEqualTo(expectedRuneCount(t));
        for (int i = 0; i < t.want.length; i++) {
            RuneRange got = cc.range(i);
            assertThat(got.low()).as("range[%s].low", i).isEqualTo(t.want[i].lo);
            assertThat(got.high()).as("range[%s].high", i).isEqualTo(t.want[i].hi);
        }
        for (int j = 0; j < 101; j++) {
            int probe = (j == 100) ? CharClass.RUNEMAX : j;
            assertThat(cc.contains(probe)).as("contains(%s)", probe).isEqualTo(shouldContain(t, probe));
        }
    }

    private static boolean shouldContain(Case t, int x)
    {
        for (Range r : t.want) {
            if (r.lo <= x && x <= r.hi) {
                return true;
            }
        }
        return false;
    }

    private static int expectedRuneCount(Case t)
    {
        long size = 0;
        for (Range r : t.want) {
            size += (long) r.hi - r.lo + 1;
        }
        return (int) size;
    }
}
