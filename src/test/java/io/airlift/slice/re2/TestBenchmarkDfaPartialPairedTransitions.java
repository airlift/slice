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

public class TestBenchmarkDfaPartialPairedTransitions
{
    @Test
    public void testParameters()
    {
        for (String workload : new String[] {"MATCH", "NO_MATCH"}) {
            BenchmarkDfaPartialPairedTransitions benchmark = new BenchmarkDfaPartialPairedTransitions();
            benchmark.workload = workload;
            benchmark.textLength = 32 * 1024;
            benchmark.expectPartialTable = true;
            benchmark.setup();
            assertThat(benchmark.search()).isEqualTo(benchmark.expectedResult());
        }
    }
}
