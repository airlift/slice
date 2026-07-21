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

public class TestBenchmarkRe2PublicApi
{
    @Test
    public void testParameters()
    {
        BenchmarkRe2PublicApi benchmark = new BenchmarkRe2PublicApi();
        for (String workload : new String[] {"tinyMatch", "tinyNoMatch", "largeMatch", "largeNoMatch"}) {
            BenchmarkRe2PublicApi.BenchmarkData data = new BenchmarkRe2PublicApi.BenchmarkData();
            data.workload = workload;
            data.setup();

            boolean expectedMatch = !workload.endsWith("NoMatch");
            assertThat(benchmark.partialMatch(data)).isEqualTo(expectedMatch);
            assertThat(benchmark.matchIntoReusedBuffer(data)).isEqualTo(expectedMatch);
            assertThat(benchmark.partialMatchResult(data) != null).isEqualTo(expectedMatch);
            assertThat(benchmark.createMatcher(data)).isNotNull();
            assertThat(benchmark.findWithNewMatcher(data)).isEqualTo(expectedMatch);
            assertThat(benchmark.findWithReusedMatcher(data)).isEqualTo(expectedMatch);

            long newMatcherChecksum = benchmark.findAllWithNewMatcher(data);
            long reusedMatcherChecksum = benchmark.findAllWithReusedMatcher(data);
            assertThat(newMatcherChecksum).isEqualTo(reusedMatcherChecksum);
            if (expectedMatch) {
                assertThat(newMatcherChecksum).isPositive();
            }
            else {
                assertThat(newMatcherChecksum).isZero();
            }
        }
    }
}
