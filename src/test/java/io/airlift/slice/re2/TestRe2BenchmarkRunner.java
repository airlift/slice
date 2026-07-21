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

import java.util.Arrays;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRe2BenchmarkRunner
{
    @Test
    public void testRandomTextCorpus()
    {
        byte[] expected = HexFormat.of().parseHex("41412925652071207f2e64206623647b25312f5c703922597a2069376a202020");
        assertThat(Re2BenchmarkRunner.randomText(expected.length)).containsExactly(expected);
        assertThat(Re2BenchmarkRunner.randomText(8)).containsExactly(Arrays.copyOf(expected, 8));
    }

    @Test
    public void testBigFixedCachedProgramBudget()
    {
        int textSize = 262_144;
        String pattern = "^" + "x".repeat(textSize / 2) + ".*$";

        assertThat(Re2BenchmarkRunner.compileProg(pattern).size()).isGreaterThan(100_000);
    }
}
