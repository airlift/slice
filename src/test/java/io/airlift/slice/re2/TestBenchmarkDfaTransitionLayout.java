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

public class TestBenchmarkDfaTransitionLayout
{
    @Test
    public void testLayoutsProduceSameFinalState()
    {
        for (int classCount : new int[] {16, 29, 64}) {
            BenchmarkDfaTransitionLayout benchmark = new BenchmarkDfaTransitionLayout();
            benchmark.classCount = classCount;
            benchmark.setup();
            assertThat(benchmark.objectReferences()).isSameAs(benchmark.expectedObjectState());
            assertThat(benchmark.directObjectReferences()).isSameAs(benchmark.expectedDirectObjectState());
            assertThat(benchmark.pairedObjectReferences()).isSameAs(benchmark.expectedPairedObjectState());
            assertThat(benchmark.flatIntegers()).isEqualTo(benchmark.expectedElementOffset());
            assertThat(benchmark.pairedFlatIntegers()).isEqualTo(benchmark.expectedPairedElementOffset());
            assertThat(benchmark.directBytes()).isEqualTo(benchmark.expectedState());
            assertThat(benchmark.directCharacters()).isEqualTo(benchmark.expectedDirectOffset());
            assertThat(benchmark.directIntegers()).isEqualTo(benchmark.expectedDirectOffset());
            assertThat(benchmark.byteArrayOffsets()).isEqualTo(benchmark.expectedByteOffset());
            assertThat(benchmark.sliceOffsets()).isEqualTo(benchmark.expectedByteOffset());
        }
    }
}
