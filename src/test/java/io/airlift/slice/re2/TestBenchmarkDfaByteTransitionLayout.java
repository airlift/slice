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

public class TestBenchmarkDfaByteTransitionLayout
{
    @Test
    public void testLayoutsProduceSameFinalState()
    {
        for (int stateCount : new int[] {32, 128}) {
            for (int classCount : new int[] {8, 29, 64}) {
                BenchmarkDfaByteTransitionLayout benchmark = new BenchmarkDfaByteTransitionLayout();
                benchmark.stateCount = stateCount;
                benchmark.classCount = classCount;
                benchmark.textLength = 16 * 1024;
                benchmark.setup();

                assertThat(benchmark.hasZeroAbnormalSentinels()).isTrue();
                assertThat(benchmark.hasDirectByteAbnormalSentinels()).isTrue();
                assertThat(benchmark.rowMajorStateIds()).isEqualTo(benchmark.expectedStateId());
                assertThat(benchmark.classMajorStateIds()).isEqualTo(benchmark.expectedStateId());
                assertThat(benchmark.directByteStateIds()).isEqualTo(benchmark.expectedStateId());
                assertThat(benchmark.compactDirectByteStateIds()).isEqualTo(benchmark.expectedStateId());
            }
        }
    }

    @Test
    public void testZeroSentinelStopsTraversal()
    {
        BenchmarkDfaByteTransitionLayout benchmark = new BenchmarkDfaByteTransitionLayout();
        benchmark.stateCount = 32;
        benchmark.classCount = 29;
        benchmark.textLength = 1;
        benchmark.setup();
        benchmark.makeInitialTransitionAbnormal();

        assertThat(benchmark.rowMajorStateIds()).isEqualTo(benchmark.initialStateId());
        assertThat(benchmark.classMajorStateIds()).isEqualTo(benchmark.initialStateId());
        assertThat(benchmark.directByteStateIds()).isEqualTo(benchmark.initialStateId());
        assertThat(benchmark.compactDirectByteStateIds()).isEqualTo(benchmark.initialStateId());
    }
}
