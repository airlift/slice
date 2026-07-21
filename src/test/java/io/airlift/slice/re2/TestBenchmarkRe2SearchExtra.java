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

public class TestBenchmarkRe2SearchExtra
{
    @Test
    public void testBigFixedBenchmark()
    {
        BenchmarkRe2SearchExtra.BigFixedState state = new BenchmarkRe2SearchExtra.BigFixedState();
        state.textSize = 262_144;
        state.setup();

        BenchmarkRe2SearchExtra benchmark = new BenchmarkRe2SearchExtra();
        // The end anchor promotes this search to longest-match semantics.
        Dfa.DfaInstance dfa = state.progBigFixed.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        assertThat(dfa).isNotNull();
        assertThat(dfa.ok()).isTrue();
        assertThat((Long) benchmark.searchBigFixedDfa(state))
                .describedAs("DFA cache resets: %s", dfa.resetCount())
                .isNotNegative();
        assertThat(dfa.resetCount()).isZero();
        assertThat(benchmark.searchBigFixedRe2(state)).isTrue();
    }
}
