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

public class TestBenchmarkDfaCountMatches
{
    @Test
    public void testMethodsAgree()
    {
        BenchmarkDfaCountMatches benchmark = new BenchmarkDfaCountMatches();
        for (String workload : new String[] {"denseWords", "boundedContext", "captureShape"}) {
            BenchmarkDfaCountMatches.BenchmarkData data = new BenchmarkDfaCountMatches.BenchmarkData();
            data.workload = workload;
            data.sourceLength = 32 * 1024;
            data.setup();

            long batched = benchmark.batched(data);
            assertThat(batched).as(workload).isPositive();
            assertThat(batched).as(workload).isEqualTo(benchmark.repeated(data));
        }
    }
}
