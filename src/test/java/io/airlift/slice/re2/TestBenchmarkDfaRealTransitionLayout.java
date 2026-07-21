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

public class TestBenchmarkDfaRealTransitionLayout
{
    @Test
    public void testRealLayoutsProduceSameFinalState()
    {
        for (String pattern : new String[] {"HARD", "PARENS", "SELF_LOOP", "LATE_TRANSITION", "ALTERNATING", "MULTIBYTE"}) {
            BenchmarkDfaRealTransitionLayout benchmark = new BenchmarkDfaRealTransitionLayout();
            benchmark.pattern = pattern;
            benchmark.setup();
            assertThat(benchmark.compactObjectReferences()).isSameAs(benchmark.expectedState());
            assertThat(benchmark.selfLoopObjectReferences()).isSameAs(benchmark.expectedState());
            assertThat(benchmark.selfLoopFirstObjectReferences()).isSameAs(benchmark.expectedState());
            assertThat(benchmark.sampledSelfLoopObjectReferences()).isSameAs(benchmark.expectedState());
            assertThat(benchmark.pairedObjectReferences()).isSameAs(benchmark.expectedPairState());
            assertThat(benchmark.stateAtExactStrideRowIndex(benchmark.exactStrideHeapRowIndexes()))
                    .isSameAs(benchmark.expectedState());
            assertThat(benchmark.stateAtPreShiftedRowIndex(benchmark.preShiftedPowerOfTwoHeapRowIndexes()))
                    .isSameAs(benchmark.expectedState());
            assertThat(benchmark.stateCount()).isPositive();
            assertThat(benchmark.classCount()).isPositive();
            assertThat(benchmark.pairedTransitionBytes()).isGreaterThan(benchmark.compactTransitionBytes());
            assertThat(benchmark.abnormalExactStrideTransition()).isZero();
            assertThat(benchmark.abnormalPreShiftedTransition()).isZero();
            assertThat(benchmark.selfLoopTransitionCount() + benchmark.stateChangeTransitionCount())
                    .isEqualTo(benchmark.textLength());
            if (pattern.equals("ALTERNATING") || pattern.equals("MULTIBYTE")) {
                assertThat(benchmark.selfLoopTransitionCount()).isZero();
            }
            else {
                assertThat(benchmark.selfLoopTransitionCount()).isPositive();
            }
        }
    }
}
