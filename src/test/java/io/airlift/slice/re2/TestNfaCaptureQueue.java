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

import java.lang.reflect.Field;
import java.util.Arrays;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNfaCaptureQueue
{
    @Test
    public void testCaptureNfaUsesPrimitiveOrderedSparseQueue()
            throws ReflectiveOperationException
    {
        Class<?> implementationClass = Arrays.stream(Nfa.class.getDeclaredClasses())
                .filter(candidate -> candidate.getSimpleName().equals("NfaImpl"))
                .findFirst()
                .orElseThrow();
        Field firstQueue = implementationClass.getDeclaredField("q0");
        Field secondQueue = implementationClass.getDeclaredField("q1");
        assertThat(firstQueue.getType().getSimpleName()).isEqualTo("CaptureThreadQueue");
        assertThat(secondQueue.getType()).isEqualTo(firstQueue.getType());
        assertThat(firstQueue.getType().getDeclaredFields())
                .extracting(Field::getType)
                .allMatch(type -> type == int.class || type == int[].class);

        Prog program = compileProg("((a|aa)+)(a?)");
        Slice text = utf8Slice(".".repeat(70) + "aaa" + ".".repeat(10));
        int[] groups = new int[8];
        assertThat(Nfa.search(program, text, false, Prog.MatchKind.FIRST_MATCH, groups)).isTrue();
        assertThat(groups).containsExactly(70, 73, 70, 73, 72, 73, 73, 73);
    }
}
