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

public class TestBenchmarkDfaFixedDistanceByte
{
    @Test
    public void testBenchmarkMethods()
    {
        BenchmarkDfaFixedDistanceByte benchmark = new BenchmarkDfaFixedDistanceByte();
        for (BenchmarkDfaFixedDistanceByte.InputShape inputShape : BenchmarkDfaFixedDistanceByte.InputShape.values()) {
            BenchmarkDfaFixedDistanceByte.BenchmarkData data = new BenchmarkDfaFixedDistanceByte.BenchmarkData();
            data.inputShape = inputShape;
            data.sourceLength = 1024;
            data.setup();

            boolean expectedMatch = inputShape == BenchmarkDfaFixedDistanceByte.InputShape.SPARSE_MATCH;
            assertThat(benchmark.partialMatch(data)).isEqualTo(expectedMatch);
            assertThat(benchmark.matchBoundary(data)).isEqualTo(expectedMatch);
        }
    }
}
