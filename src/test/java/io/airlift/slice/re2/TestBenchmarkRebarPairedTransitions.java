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

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestBenchmarkRebarPairedTransitions
{
    @Test
    public void testParameters()
            throws Exception
    {
        assumeTrue(Files.isDirectory(Path.of("target/rebar-selected")), "Rebar benchmark corpus is not installed");
        for (String workload : new String[] {"LEIPZIG", "KEYWORDS", "URL", "NO_QUADRATIC"}) {
            BenchmarkRebarPairedTransitions benchmark = new BenchmarkRebarPairedTransitions();
            benchmark.workload = workload;
            benchmark.expectPairedTable = false;
            benchmark.setup();
            assertThat(benchmark.searchAll()).isEqualTo(benchmark.expectedResult());
        }
        for (String workload : new String[] {"WORD_BOUNDARY", "WORD_ENDING", "ANY_CODE_POINT", "BOUNDED_ENDING", "AROUND_HOLMES", "LINE_BOUNDARY"}) {
            BenchmarkRebarPairedTransitions benchmark = new BenchmarkRebarPairedTransitions();
            benchmark.workload = workload;
            benchmark.expectPairedTable = true;
            benchmark.setup();
            assertThat(benchmark.searchAll()).isEqualTo(benchmark.expectedResult());
        }
    }
}
