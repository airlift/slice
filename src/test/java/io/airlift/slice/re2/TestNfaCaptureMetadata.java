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

import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNfaCaptureMetadata
{
    @Test
    public void testCaptureStackSizeUsesFlattenedInstructionCounts()
    {
        Prog program = compileProg("((a)|b)(?:^|c)");

        int expectedSize = 2 * program.getInstCount(InstOp.CAPTURE) +
                program.getInstCount(InstOp.EMPTY_WIDTH) +
                program.getInstCount(InstOp.NOP) + 1;

        assertThat(Nfa.captureStackSize(program)).isEqualTo(Math.max(8, expectedSize));
    }
}
