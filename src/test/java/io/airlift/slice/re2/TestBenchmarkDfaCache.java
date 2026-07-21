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

public class TestBenchmarkDfaCache
{
    @Test
    public void testSharedWarmParameters()
    {
        BenchmarkDfaCache benchmark = new BenchmarkDfaCache();
        for (int textSize : new int[] {8, 262_144}) {
            BenchmarkDfaCache.SharedWarmState state = new BenchmarkDfaCache.SharedWarmState();
            state.textSize = textSize;
            state.setup();
            assertThat(benchmark.searchSharedWarm(state)).isEqualTo(Dfa.SEARCH_NO_MATCH);
        }
    }

    @Test
    public void testColdStartParameters()
    {
        BenchmarkDfaCache benchmark = new BenchmarkDfaCache();
        for (int textSize : new int[] {8, 262_144}) {
            BenchmarkDfaCache.ColdStartState state = new BenchmarkDfaCache.ColdStartState();
            state.textSize = textSize;
            state.setup();
            state.resetCache();
            assertThat(benchmark.searchColdStart(state)).isEqualTo(Dfa.SEARCH_NO_MATCH);
        }
    }

    @Test
    public void testSharedColdParameters()
            throws InterruptedException
    {
        BenchmarkDfaCache benchmark = new BenchmarkDfaCache();
        for (int textSize : new int[] {8, 262_144}) {
            for (int workerCount : new int[] {1, 2, 4, 8, 16}) {
                BenchmarkDfaCache.SharedColdState state = new BenchmarkDfaCache.SharedColdState();
                state.textSize = textSize;
                state.workerCount = workerCount;
                state.setup();
                try {
                    state.resetCache();
                    assertThat(benchmark.searchSharedColdWave(state)).isEqualTo(-workerCount);
                }
                finally {
                    state.tearDown();
                }
            }
        }
    }

    @Test
    public void testLateTransitionParameters()
    {
        BenchmarkDfaCache benchmark = new BenchmarkDfaCache();
        for (int textSize : new int[] {8, 4_096, 262_144}) {
            BenchmarkDfaCache.LateTransitionState state = new BenchmarkDfaCache.LateTransitionState();
            state.textSize = textSize;
            state.setup();
            state.prepareKnownPath();
            assertThat(benchmark.searchNewTransitionAfterLongScan(state)).isEqualTo(Dfa.SEARCH_NO_MATCH);
        }
    }
}
