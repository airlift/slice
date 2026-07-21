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

import static io.airlift.slice.re2.Dfa.DfaInstance.Kind.LONGEST_MATCH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDfaRetainedState
{
    @Test
    public void testMemoryBudgetDoesNotChangeColdAllocation()
    {
        Dfa.DfaInstance smallBudget = compileDfa("0[01]{8}$", 1 << 20);
        Dfa.DfaInstance largeBudget = compileDfa("0[01]{8}$", 32 << 20);

        assertThat(largeBudget.retainedStateMemory())
                .isEqualTo(smallBudget.retainedStateMemory());
    }

    @Test
    public void testResetClearsStateRootsAndRetainsStableCapacity()
    {
        Prog program = compileProgram("0[01]{8}$", 1 << 20);
        Dfa.DfaInstance dfa = program.getCachedDfa(LONGEST_MATCH);

        Dfa.CacheSnapshot cold = dfa.cacheSnapshot();
        assertThat(cold.stateCount()).isZero();
        assertThat(cold.cacheEntries()).isZero();
        assertThat(cold.populatedStateData()).isZero();
        assertThat(cold.populatedStateReferences()).isZero();
        assertThat(cold.populatedStartStates()).isZero();
        assertThat(dfa.retainedStateMemory())
                .isEqualTo(dfa.stateMemorySnapshot().totalBytes());
        assertThat(cold.stateBudget() - cold.availableStateMemory())
                .isEqualTo(dfa.stateMemorySnapshot().totalBytes());

        Slice input = Slices.utf8Slice("0010101010");
        Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true);
        Dfa.CacheSnapshot warm = dfa.cacheSnapshot();
        assertThat(warm.stateCount()).isPositive();
        assertThat(warm.cacheEntries()).isEqualTo(warm.stateCount());
        assertThat(warm.populatedStateData()).isEqualTo(warm.stateCount());
        assertThat(warm.populatedStateReferences()).isEqualTo(warm.stateCount());
        assertThat(warm.availableStateMemory()).isLessThan(warm.stateBudget());
        assertThat(dfa.retainedStateMemory())
                .isEqualTo(dfa.stateMemorySnapshot().totalBytes());
        assertThat(warm.stateBudget() - warm.availableStateMemory())
                .isGreaterThanOrEqualTo(dfa.stateMemorySnapshot().totalBytes());

        dfa.resetCacheExternal();
        Dfa.CacheSnapshot reset = dfa.cacheSnapshot();
        assertThat(reset.stateCount()).isZero();
        assertThat(reset.cacheEntries()).isZero();
        assertThat(reset.populatedStateData()).isZero();
        assertThat(reset.populatedStateReferences()).isZero();
        assertThat(reset.populatedStartStates()).isZero();
        assertThat(reset.pairedRows()).isZero();
        assertThat(reset.pairedTransitionMemory()).isZero();
        assertThat(dfa.retainedStateMemory())
                .isEqualTo(dfa.stateMemorySnapshot().totalBytes());
        assertThat(reset.stateBudget() - reset.availableStateMemory())
                .isEqualTo(dfa.stateMemorySnapshot().totalBytes() + reset.fixedDistanceByteCandidateMemory());
        assertThat(reset.transitionCapacity()).isEqualTo(warm.transitionCapacity());
        assertThat(reset.stateCapacity()).isEqualTo(warm.stateCapacity());
        assertThat(reset.stateReferenceCapacity()).isEqualTo(warm.stateReferenceCapacity());

        Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true);
        Dfa.CacheSnapshot rebuilt = dfa.cacheSnapshot();
        assertThat(rebuilt.transitionCapacity()).isEqualTo(warm.transitionCapacity());
        assertThat(rebuilt.stateCapacity()).isEqualTo(warm.stateCapacity());
        assertThat(rebuilt.stateReferenceCapacity()).isEqualTo(warm.stateReferenceCapacity());
        assertThat(rebuilt.stateCount()).isEqualTo(warm.stateCount());
        assertThat(rebuilt.cacheEntries()).isEqualTo(warm.cacheEntries());
    }

    private static Dfa.DfaInstance compileDfa(String expression, long memoryBudget)
    {
        return compileProgram(expression, memoryBudget).getCachedDfa(LONGEST_MATCH);
    }

    private static Prog compileProgram(String expression, long memoryBudget)
    {
        return Compiler.compile(
                RegexpParser.parse(Slices.utf8Slice(expression), Regexp.LIKE_PERL).regexp(),
                false,
                memoryBudget);
    }
}
