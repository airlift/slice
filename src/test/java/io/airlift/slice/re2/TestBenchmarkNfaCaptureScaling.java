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

import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBenchmarkNfaCaptureScaling
{
    @Test
    public void testBenchmarkPaths()
    {
        BenchmarkNfaCaptureScaling benchmark = new BenchmarkNfaCaptureScaling();
        for (int capturingGroupCount : new int[] {1, 4, 16, 64}) {
            BenchmarkNfaCaptureScaling.BenchmarkData data = new BenchmarkNfaCaptureScaling.BenchmarkData();
            data.capturingGroupCount = capturingGroupCount;
            data.setup();

            assertThat(benchmark.cachedStackSize(data)).isEqualTo(benchmark.instructionScanStackSize(data));
            assertThat(benchmark.captureSearch(data)).isTrue();
            assertThat(data.groups[0]).isEqualTo(0);
            assertThat(data.groups[1]).isEqualTo(6);
            assertThat(data.groups[2]).isEqualTo(0);
            assertThat(data.groups[3]).isEqualTo(6);

            data.input = Slices.utf8Slice("token0x");
            assertThat(benchmark.captureSearch(data)).isFalse();
        }
    }
}
