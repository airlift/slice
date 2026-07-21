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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBenchmarkSafeReTrinoRegexp
{
    @Test
    public void testParameters()
    {
        Slice replacement = Slices.utf8Slice("_");
        Function<List<Slice>, Slice> lambdaReplacement = groups -> groups.isEmpty() ? replacement : groups.getFirst();
        for (String workload : TestingTrinoRegexpBenchmarkInputs.workloads()) {
            for (int sourceLength : TestingTrinoRegexpBenchmarkInputs.sourceLengths()) {
                TestingTrinoRegexpBenchmarkInputs.Input input = TestingTrinoRegexpBenchmarkInputs.create(workload, sourceLength);
                TrinoRegexp expected = TrinoRegexp.compile(input.pattern());
                TestingSafeReTrinoRegexp actual = TestingSafeReTrinoRegexp.compile(input.pattern());

                assertThat(actual.contains(input.source())).isEqualTo(expected.contains(input.source()));
                assertThat(actual.count(input.source())).isEqualTo(expected.count(input.source()));
                assertThat(actual.position(input.source(), 3)).isEqualTo(expected.position(input.source(), 1, 3));
                assertThat(actual.extract(input.source())).isEqualTo(expected.extract(input.source()));
                assertThat(actual.extractAll(input.source())).isEqualTo(expected.extractAll(input.source()));
                assertThat(actual.split(input.source())).isEqualTo(expected.split(input.source()));
                assertThat(actual.replace(input.source(), replacement)).isEqualTo(expected.replace(input.source(), replacement));
                assertThat(actual.replace(input.source(), lambdaReplacement)).isEqualTo(expected.replace(input.source(), lambdaReplacement));
            }
        }
    }
}
