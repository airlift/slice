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
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.re2.OnePass.CaptureFinalization.DIRECT_REBASE;
import static io.airlift.slice.re2.OnePass.CaptureFinalization.DIRECT_ZERO_ORIGIN;
import static io.airlift.slice.re2.OnePass.CaptureFinalization.SCRATCH_COPY;
import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOnePassCaptureFinalization
{
    @Test
    public void testCaptureFinalizationModes()
    {
        int[] oneCaptureGroup = new int[4];
        int[] threeCaptureGroups = new int[8];
        assertThat(OnePass.captureFinalization(oneCaptureGroup, Prog.MatchKind.FULL_MATCH, 0)).isEqualTo(DIRECT_REBASE);
        assertThat(OnePass.captureFinalization(threeCaptureGroups, Prog.MatchKind.FULL_MATCH, 0)).isEqualTo(DIRECT_ZERO_ORIGIN);
        assertThat(OnePass.captureFinalization(threeCaptureGroups, Prog.MatchKind.FULL_MATCH, 2)).isEqualTo(DIRECT_REBASE);
        assertThat(OnePass.captureFinalization(threeCaptureGroups, Prog.MatchKind.FIRST_MATCH, 0)).isEqualTo(SCRATCH_COPY);
        assertThat(OnePass.captureFinalization(null, Prog.MatchKind.FULL_MATCH, 0)).isEqualTo(SCRATCH_COPY);
    }

    @Test
    public void testDirectCaptureRebasesNonZeroSearchOrigin()
    {
        Prog program = compileProg("([0-9]+)");
        Slice context = utf8Slice("xx123yy");
        int[] groups = new int[4];
        assertThat(OnePass.search(program, context, 2, 5, true, Prog.MatchKind.FULL_MATCH, groups)).isTrue();
        assertThat(groups).containsExactly(0, 3, 0, 3);
    }
}
