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

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.re2.Dfa.DfaInstance.Kind.FIRST_MATCH;
import static io.airlift.slice.re2.Dfa.DfaInstance.Kind.LONGEST_MATCH;
import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;

public final class DfaAbsolutePointerProbe
{
    private DfaAbsolutePointerProbe() {}

    public static void main(String[] arguments)
    {
        boolean expectedNativeAccess = Boolean.parseBoolean(arguments[0]);
        check(Dfa.nativeAccessEnabled() == expectedNativeAccess, "unexpected native-access capability");

        String suffix = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        Prog program = compileProg("[ -~]*" + suffix + "$");
        program.setDfaMemory(64L << 20);
        Slice input = utf8Slice("x".repeat(4_096) + suffix);
        check(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true) == input.length(), "initial search failed");
        Dfa.DfaInstance dfa = program.getCachedDfa(LONGEST_MATCH);

        if (!expectedNativeAccess) {
            check(dfa.absolutePointerTransitionMemory() == 0, "disabled native access allocated a sidecar");
            System.out.println("OK disabled");
            return;
        }

        check(dfa.absolutePointerTransitionMemory() > 0, "enabled native access did not allocate a sidecar");
        check(dfa.absolutePointerTransitionMemory() < 1L << 20, "small DFA reserved a maximum-capacity sidecar");
        check(dfa.absolutePointerTransitionCount() > 0, "search did not populate pointer transitions");
        check(dfa.availableStateMemory() + dfa.absolutePointerTransitionMemory() < dfa.stateBudget(), "sidecar was not charged to the DFA budget");

        long originalAddress = dfa.absolutePointerTransitionBaseAddress();
        long originalMemory = dfa.absolutePointerTransitionMemory();
        dfa.resetCacheExternal();
        check(dfa.absolutePointerTransitionBaseAddress() == originalAddress, "reset changed the sidecar address");
        check(dfa.absolutePointerTransitionMemory() == originalMemory, "reset changed the permanent sidecar charge");
        check(dfa.absolutePointerTransitionCount() == 0, "reset retained pointer transitions");
        check(dfa.availableStateMemory() + dfa.retainedStateMemory() + originalMemory == dfa.stateBudget(), "reset returned the permanent sidecar charge");
        check(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true) == input.length(), "search after reset failed");
        check(dfa.absolutePointerTransitionCount() > 0, "search after reset did not rebuild pointer transitions");

        Prog shortProgram = compileProg("(\\s*)((?:# [Nn][Oo][Qq][Aa])(?::\\s?(([A-Z]+[0-9]+(?:[,\\s]+)?)+))?)");
        shortProgram.setDfaMemory(64L << 20);
        Slice shortMatch = utf8Slice("# noqa");
        check(Dfa.search(shortProgram, shortMatch, false, Prog.MatchKind.FIRST_MATCH, true) == shortMatch.length(), "short matching search failed");
        Slice shortInput = utf8Slice("int value = 123;");
        for (int iteration = 0; iteration < 20; iteration++) {
            check(Dfa.search(shortProgram, shortInput, false, Prog.MatchKind.FIRST_MATCH, true) == Dfa.SEARCH_NO_MATCH, "short search failed");
        }
        Dfa.DfaInstance shortDfa = shortProgram.getCachedDfa(FIRST_MATCH);
        check(shortDfa.absolutePointerTransitionMemory() > 0, "short search did not allocate a pointer sidecar");
        check(shortDfa.absolutePointerTransitionCount() > 0, "short search did not populate pointer transitions");

        Slice deadInput = utf8Slice("x".repeat(4_096) + suffix.substring(0, suffix.length() - 1) + "_");
        check(Dfa.search(program, deadInput, false, Prog.MatchKind.FIRST_MATCH, true) == Dfa.SEARCH_NO_MATCH, "dead transition changed the result");
        check(Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true) == input.length(), "match transition changed the boundary");

        String growingSuffix = suffix.repeat(4);
        Prog growingProgram = compileProg("[ -~]*" + growingSuffix + "$");
        growingProgram.setDfaMemory(64L << 20);
        Slice growthWarmupInput = utf8Slice("_".repeat(4_096));
        Dfa.DfaInstance growingDfa = growingProgram.getCachedDfa(LONGEST_MATCH);
        growingDfa.beginSearch(true);
        try {
            growingDfa.analyzeStart(
                    growthWarmupInput,
                    growthWarmupInput.byteArrayOffset(),
                    growthWarmupInput.byteArrayOffset() + growthWarmupInput.length(),
                    false,
                    true);
            growingDfa.requestAbsolutePointerTransitions();
        }
        finally {
            growingDfa.endSearch(true, null);
        }
        long initialGrowingPointerMemory = growingDfa.absolutePointerTransitionMemory();
        check(initialGrowingPointerMemory < 1L << 20, "growing DFA reserved a maximum-capacity sidecar before graph construction");
        Slice growingInput = utf8Slice("_".repeat(4_096) + growingSuffix);
        check(Dfa.search(growingProgram, growingInput, false, Prog.MatchKind.FIRST_MATCH, true) == growingInput.length(), "growing DFA search failed");
        long growingPointerMemory = growingDfa.absolutePointerTransitionMemory();
        check((long) growingDfa.stateCount * growingDfa.nextSize * Long.BYTES > 48L << 10, "growing DFA did not cross the former initial tier");
        check(growingPointerMemory > initialGrowingPointerMemory, "growing DFA did not enlarge the sidecar");
        check(growingDfa.absolutePointerTransitionsAvailable(), "growing DFA switched to object traversal");
        check(growingDfa.availableStateMemory() + growingDfa.retainedStateMemory() + growingPointerMemory == growingDfa.stateBudget(), "growing DFA accounting did not reconcile to its memory budget");

        long growingAddress = growingDfa.absolutePointerTransitionBaseAddress();
        growingDfa.resetCacheExternal();
        check(growingDfa.absolutePointerTransitionBaseAddress() == growingAddress, "reset changed the grown sidecar address");
        check(growingDfa.absolutePointerTransitionMemory() == growingPointerMemory, "reset changed the grown sidecar allocation");

        System.out.println("OK enabled");
    }

    private static void check(boolean condition, String message)
    {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
