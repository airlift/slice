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

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Ported from upstream RE2: re2/testing/compile_test.cc.
public class TestUpstreamCompileMemoryBudget
{
    @Test
    public void testInsufficientMemoryBudgetOneByteFails()
    {
        // From upstream re2/testing/compile_test.cc TestRegexpCompileToProg.Simple.
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer("a".getBytes(StandardCharsets.UTF_8)), Regexp.PERL_EXTENSIONS | Regexp.LATIN1);

        assertThatThrownBy(() -> Compiler.compile(parsed.regexp(), false, 1))
                .isInstanceOf(RegexpCompileOutOfMemoryException.class);
    }

    @Test
    public void testInsufficientMemoryFailsInsteadOfNoMatch()
    {
        // From upstream re2/testing/compile_test.cc TestCompile.InsufficientMemory.
        String pattern = "^(?P<name1>[^\\s]+)\\s+(?P<name2>[^\\s]+)\\s+(?P<name3>.+)$";
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8)), Regexp.LIKE_PERL);

        assertThatThrownBy(() -> Compiler.compile(parsed.regexp(), false, 850))
                .isInstanceOf(RegexpCompileOutOfMemoryException.class);
    }

    @Test
    public void testRemainingMemoryAssignedToDfa()
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer("a".getBytes(StandardCharsets.UTF_8)), Regexp.LIKE_PERL);
        int maxMemory = 4096;
        Prog program = Compiler.compile(parsed.regexp(), false, maxMemory);

        long programMemory = 512L + ((long) program.size() * 8);
        if (program.canBitState()) {
            programMemory += (long) program.size() * 2;
        }
        long onePassMemory = (long) program.onePassStateCount() *
                (Integer.BYTES + ((long) program.bytemapRange() * Integer.BYTES));
        assertThat(program.isOnePass()).isTrue();
        assertThat(program.dfaMemory()).isEqualTo(maxMemory - programMemory - onePassMemory);
    }

    @Test
    public void testOnePassRespectsDfaMemoryBudget()
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer("a".getBytes(StandardCharsets.UTF_8)), Regexp.LIKE_PERL);
        Prog reference = Compiler.compile(parsed.regexp(), false, 4096);

        long programMemory = 512L + ((long) reference.size() * 8);
        if (reference.canBitState()) {
            programMemory += (long) reference.size() * 2;
        }
        int maximumOnePassStates = 2 + 1;
        long onePassStateSize = Integer.BYTES + ((long) reference.bytemapRange() * Integer.BYTES);
        long insufficientDfaMemory = (4 * maximumOnePassStates * onePassStateSize) - 1;

        Prog program = Compiler.compile(parsed.regexp(), false, programMemory + insufficientDfaMemory);

        assertThat(program.isOnePass()).isFalse();
        assertThat(program.dfaMemory()).isEqualTo(insufficientDfaMemory);
    }

    @Test
    public void testUnboundedCompileUsesUpstreamDefaultDfaBudget()
    {
        ParseResult parsed = RegexpParser.parse(Slices.wrappedBuffer("a".getBytes(StandardCharsets.UTF_8)), Regexp.LIKE_PERL);
        Prog program = Compiler.compile(parsed.regexp(), false, 0);

        long onePassMemory = (long) program.onePassStateCount() *
                (Integer.BYTES + ((long) program.bytemapRange() * Integer.BYTES));
        assertThat(program.dfaMemory()).isEqualTo((1 << 20) - onePassMemory);
    }
}
