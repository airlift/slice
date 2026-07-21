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

public class TestBenchmarkDfaCompactTransitionLayout
{
    @Test
    public void testLayoutsProduceSameFinalState()
    {
        for (int stateCount : new int[] {32, 96, 128, 192}) {
            for (int classCount : new int[] {8, 29, 64}) {
                BenchmarkDfaCompactTransitionLayout benchmark = new BenchmarkDfaCompactTransitionLayout();
                benchmark.stateCount = stateCount;
                benchmark.classCount = classCount;
                benchmark.textLength = 16 * 1024;
                benchmark.setup();
                try {
                    assertThat(benchmark.hasZeroAbnormalSentinels()).isTrue();
                    assertThat(benchmark.objectReferences()).isSameAs(benchmark.expectedObjectTransitionRow());
                    assertThat(benchmark.selfLoopAwareObjectReferences()).isSameAs(benchmark.expectedObjectTransitionRow());
                    assertThat(benchmark.selfLoopFirstObjectReferences()).isSameAs(benchmark.expectedObjectTransitionRow());
                    assertThat(benchmark.exactStrideHeapIntegers()).isEqualTo(benchmark.expectedExactStrideRowIndex());
                    assertThat(benchmark.powerOfTwoStateIds()).isEqualTo(benchmark.expectedStateId());
                    assertThat(benchmark.preShiftedHeapRowIndexes()).isEqualTo(benchmark.expectedPreShiftedRowIndex());
                    assertThat(benchmark.rowMajorCharacterStateIds()).isEqualTo(benchmark.expectedStateId());
                    assertThat(benchmark.classMajorCharacterStateIds()).isEqualTo(benchmark.expectedStateId());
                    if (Dfa.nativeAccessEnabled()) {
                        assertThat(benchmark.everythingSegmentPointers()).isEqualTo(benchmark.expectedForeignRowAddress());
                        assertThat(benchmark.everythingSegmentUnalignedPointers()).isEqualTo(benchmark.expectedForeignRowAddress());
                        assertThat(benchmark.everythingSegmentPreScaledPointers()).isEqualTo(benchmark.expectedForeignRowAddress());
                        assertThat(benchmark.everythingSegmentIndexedPointers()).isEqualTo(benchmark.expectedForeignRowAddress());
                        assertThat(benchmark.rowSegmentPointers()).isEqualTo(benchmark.expectedForeignRowAddress());
                    }
                }
                finally {
                    benchmark.tearDown();
                }
            }
        }
    }

    @Test
    public void testZeroSentinelStopsTraversal()
    {
        BenchmarkDfaCompactTransitionLayout benchmark = new BenchmarkDfaCompactTransitionLayout();
        benchmark.stateCount = 32;
        benchmark.classCount = 29;
        benchmark.textLength = 1;
        benchmark.setup();
        try {
            benchmark.makeInitialTransitionAbnormal();

            assertThat(benchmark.objectReferences()).isSameAs(benchmark.initialObjectTransitionRow());
            assertThat(benchmark.selfLoopAwareObjectReferences()).isSameAs(benchmark.initialObjectTransitionRow());
            assertThat(benchmark.selfLoopFirstObjectReferences()).isSameAs(benchmark.initialObjectTransitionRow());
            assertThat(benchmark.exactStrideHeapIntegers()).isEqualTo(benchmark.initialExactStrideRowIndex());
            assertThat(benchmark.powerOfTwoStateIds()).isEqualTo(benchmark.initialStateId());
            assertThat(benchmark.preShiftedHeapRowIndexes()).isEqualTo(benchmark.initialPreShiftedRowIndex());
            assertThat(benchmark.rowMajorCharacterStateIds()).isEqualTo(benchmark.initialStateId());
            assertThat(benchmark.classMajorCharacterStateIds()).isEqualTo(benchmark.initialStateId());
            if (Dfa.nativeAccessEnabled()) {
                assertThat(benchmark.everythingSegmentPointers()).isEqualTo(benchmark.initialForeignRowAddress());
                assertThat(benchmark.everythingSegmentUnalignedPointers()).isEqualTo(benchmark.initialForeignRowAddress());
                assertThat(benchmark.everythingSegmentPreScaledPointers()).isEqualTo(benchmark.initialForeignRowAddress());
                assertThat(benchmark.everythingSegmentIndexedPointers()).isEqualTo(benchmark.initialForeignRowAddress());
                assertThat(benchmark.rowSegmentPointers()).isEqualTo(benchmark.initialForeignRowAddress());
            }
        }
        finally {
            benchmark.tearDown();
        }
    }
}
