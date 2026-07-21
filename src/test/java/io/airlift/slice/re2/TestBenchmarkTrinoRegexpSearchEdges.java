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

public class TestBenchmarkTrinoRegexpSearchEdges
{
    @Test
    public void testBenchmarkResults()
    {
        BenchmarkTrinoRegexpSearchEdges benchmark = new BenchmarkTrinoRegexpSearchEdges();
        for (String workload : new String[] {
                "singleByteNoMatch",
                "singleByteLateMatch",
                "captureNoMatch",
                "captureLateMatch",
                "unsupportedEndNoMatch",
                "unsupportedEndLateMatch",
                "unsupportedBoundaryNoMatch",
                "unsupportedBoundaryLateMatch"}) {
            for (int sourceLength : new int[] {1024, 32768}) {
                BenchmarkTrinoRegexpSearchEdges.BenchmarkData data = new BenchmarkTrinoRegexpSearchEdges.BenchmarkData();
                data.workload = workload;
                data.sourceLength = sourceLength;
                data.setup();

                assertThat(benchmark.compile(data)).isNotNull();
                assertThat(benchmark.contains(data)).isEqualTo(data.expectedExtract() != null);
                assertThat(benchmark.extract(data)).isEqualTo(data.expectedExtract());
                assertThat(benchmark.replace(data)).isEqualTo(data.expectedReplacement());
            }
        }
    }
}
