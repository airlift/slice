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

// Includes tests ported from upstream RE2: re2/testing/re2_test.cc.
public class TestRe2Program
{
    @Test
    public void testProgramSize()
    {
        Re2 reSimple = Re2.compile(utf8("simple regexp"), Re2.Options.defaults());
        Re2 reMedium = Re2.compile(utf8("medium.*regexp"), Re2.Options.defaults());
        Re2 reComplex = Re2.compile(utf8("complex.{1,128}regexp"), Re2.Options.defaults());

        assertThat(reSimple.programSize()).isGreaterThan(0);
        assertThat(reMedium.programSize()).isGreaterThan(reSimple.programSize());
        assertThat(reComplex.programSize()).isGreaterThan(reMedium.programSize());

        assertThat(reSimple.reverseProgramSize()).isGreaterThan(0);
        assertThat(reMedium.reverseProgramSize()).isGreaterThan(reSimple.reverseProgramSize());
        assertThat(reComplex.reverseProgramSize()).isGreaterThan(reMedium.reverseProgramSize());
    }

    @Test
    public void testProgramFanout()
    {
        Re2 re1 = Re2.compile(utf8("(?:(?:(?:(?:(?:.)?){1})*)+)"), Re2.Options.defaults());
        Re2 re10 = Re2.compile(utf8("(?:(?:(?:(?:(?:.)?){10})*)+)"), Re2.Options.defaults());
        Re2 re100 = Re2.compile(utf8("(?:(?:(?:(?:(?:.)?){100})*)+)"), Re2.Options.defaults());
        Re2 re1000 = Re2.compile(utf8("(?:(?:(?:(?:(?:.)?){1000})*)+)"), Re2.Options.defaults());

        Re2.FanoutResult fanout;

        fanout = re1.programFanout();
        assertThat(fanout.maxBucket()).isEqualTo(3);
        assertThat(fanout.histogram()[3]).isEqualTo(2);

        fanout = re10.programFanout();
        assertThat(fanout.maxBucket()).isEqualTo(6);
        assertThat(fanout.histogram()[6]).isEqualTo(11);

        fanout = re100.programFanout();
        assertThat(fanout.maxBucket()).isEqualTo(9);
        assertThat(fanout.histogram()[9]).isEqualTo(101);

        fanout = re1000.programFanout();
        assertThat(fanout.maxBucket()).isEqualTo(13);
        assertThat(fanout.histogram()[13]).isEqualTo(1001);

        fanout = re1.reverseProgramFanout();
        assertThat(fanout.maxBucket()).isEqualTo(2);
        assertThat(fanout.histogram()[2]).isEqualTo(2);

        fanout = re10.reverseProgramFanout();
        assertThat(fanout.maxBucket()).isEqualTo(5);
        assertThat(fanout.histogram()[5]).isEqualTo(11);

        fanout = re100.reverseProgramFanout();
        assertThat(fanout.maxBucket()).isEqualTo(9);
        assertThat(fanout.histogram()[9]).isEqualTo(101);

        fanout = re1000.reverseProgramFanout();
        assertThat(fanout.maxBucket()).isEqualTo(12);
        assertThat(fanout.histogram()[12]).isEqualTo(1001);
    }

    @Test
    public void testProgramNullability()
    {
        assertThat(Re2.compile(utf8("")).canMatchEmpty()).isTrue();
        assertThat(Re2.compile(utf8("a*")).canMatchEmpty()).isTrue();
        assertThat(Re2.compile(utf8("a?")).canMatchEmpty()).isTrue();
        assertThat(Re2.compile(utf8("^$")).canMatchEmpty()).isTrue();
        assertThat(Re2.compile(utf8("a|")).canMatchEmpty()).isTrue();

        assertThat(Re2.compile(utf8("a")).canMatchEmpty()).isFalse();
        assertThat(Re2.compile(utf8("a+")).canMatchEmpty()).isFalse();
        assertThat(Re2.compile(utf8("a|b")).canMatchEmpty()).isFalse();
        assertThat(Re2.compile(utf8("[a-z]")).canMatchEmpty()).isFalse();
    }

    private static Slice utf8(String value)
    {
        return Slices.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }
}
