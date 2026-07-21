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

public class TestBenchmarkDfaCharacterOffsetLayout
{
    @Test
    public void testCharacterOffsetsProduceExpectedFinalState()
    {
        for (int stateCount : new int[] {32, 128, 512}) {
            BenchmarkDfaCharacterOffsetLayout benchmark = benchmark(stateCount, 29, 16 * 1024);
            assertThat(benchmark.preShiftedCharacterRowOffsets()).isEqualTo(benchmark.expectedRowOffset());
        }
    }

    @Test
    public void testCharacterSentinelStopsTraversal()
    {
        BenchmarkDfaCharacterOffsetLayout benchmark = benchmark(32, 29, 1);
        benchmark.makeInitialTransitionAbnormal();
        assertThat(benchmark.preShiftedCharacterRowOffsets()).isEqualTo(benchmark.initialRowOffset());
    }

    private static BenchmarkDfaCharacterOffsetLayout benchmark(int stateCount, int classCount, int textLength)
    {
        BenchmarkDfaCharacterOffsetLayout benchmark = new BenchmarkDfaCharacterOffsetLayout();
        benchmark.stateCount = stateCount;
        benchmark.classCount = classCount;
        benchmark.textLength = textLength;
        benchmark.setup();
        return benchmark;
    }
}
