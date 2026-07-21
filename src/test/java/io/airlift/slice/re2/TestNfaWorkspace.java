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

import java.lang.reflect.Field;
import java.util.Arrays;

import static io.airlift.slice.re2.Re2.Anchor.UNANCHORED;
import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestNfaWorkspace
{
    @Test
    public void testScratchStorageIsReusedAndReset()
            throws ReflectiveOperationException
    {
        Prog program = compileProg("(a+)(b)?");
        Nfa.Workspace workspace = new Nfa.Workspace();
        int[] groups = new int[6];

        Arrays.fill(groups, -1);
        assertThat(Nfa.search(
                program,
                Slices.utf8Slice("aaab"),
                0,
                4,
                true,
                Prog.MatchKind.FULL_MATCH,
                groups,
                workspace)).isTrue();
        assertThat(groups).containsExactly(0, 4, 0, 3, 3, 4);

        Object firstQueue = field("firstQueue").get(workspace);
        Object match = field("match").get(workspace);
        Object threadCaptures = field("threadCaptures").get(workspace);

        assertThat(Nfa.search(
                program,
                Slices.utf8Slice("a"),
                0,
                1,
                true,
                Prog.MatchKind.FULL_MATCH,
                groups,
                workspace)).isTrue();
        assertThat(groups).containsExactly(0, 1, 0, 1, -1, -1);
        assertThat(field("firstQueue").get(workspace)).isSameAs(firstQueue);
        assertThat(field("match").get(workspace)).isSameAs(match);
        assertThat(field("threadCaptures").get(workspace)).isSameAs(threadCaptures);

        Arrays.fill(groups, -1);
        assertThat(Nfa.search(
                program,
                Slices.utf8Slice("x"),
                0,
                1,
                true,
                Prog.MatchKind.FULL_MATCH,
                groups,
                workspace)).isFalse();
        assertThat(groups).containsExactly(-1, -1, -1, -1, -1, -1);
        assertThat((int[]) field("threadReferences").get(workspace)).containsOnly(0);
    }

    @Test
    public void testMatcherOwnsAndReusesWorkspace()
            throws ReflectiveOperationException
    {
        String patternText = "((?:a|aa){100})(b{100})(c{100})(d{100})(e{100})(f{100})";
        Slice input = Slices.utf8Slice(
                "a".repeat(100) +
                        "b".repeat(100) +
                        "c".repeat(100) +
                        "d".repeat(100) +
                        "e".repeat(100) +
                        "f".repeat(100));
        Re2 pattern = Re2.compile(Slices.utf8Slice(patternText));
        assertThat(pattern.forwardProgramForDiagnostics().isOnePass()).isFalse();
        assertThat(pattern.forwardProgramForDiagnostics().canBitState()).isFalse();

        Re2Matcher matcher = pattern.matcher(input);
        Field workspaceField = Re2Matcher.class.getDeclaredField("nfaWorkspace");
        workspaceField.setAccessible(true);
        Nfa.Workspace workspace = (Nfa.Workspace) workspaceField.get(matcher);
        assertThat(field("firstQueue").get(workspace)).isNull();

        assertThat(matcher.find()).isTrue();
        Object firstQueue = field("firstQueue").get(workspace);
        Object match = field("match").get(workspace);
        assertThat(firstQueue).isNotNull();

        matcher.reset(input);
        assertThat(matcher.find()).isTrue();
        assertThat(field("firstQueue").get(workspace)).isSameAs(firstQueue);
        assertThat(field("match").get(workspace)).isSameAs(match);
        assertThat(matcher.start()).isZero();
        assertThat(matcher.end()).isEqualTo(input.length());
    }

    @Test
    public void testDifferentBackingOffsets()
    {
        Prog program = compileProg("(ab)");
        Nfa.Workspace workspace = new Nfa.Workspace();
        int[] groups = new int[4];
        Slice input = Slices.utf8Slice("xxabyy").slice(2, 2);

        assertThat(Nfa.search(
                program,
                input,
                0,
                input.length(),
                true,
                Prog.MatchKind.FULL_MATCH,
                groups,
                workspace)).isTrue();
        assertThat(groups).containsExactly(0, 2, 0, 2);

        Re2 pattern = Re2.compile(Slices.utf8Slice("(ab)"));
        assertThat(pattern.matchInto(input, UNANCHORED, groups)).isTrue();
        assertThat(groups).containsExactly(0, 2, 0, 2);
    }

    @Test
    public void testExceptionInvalidatesWorkspaceBeforeReuse()
            throws ReflectiveOperationException
    {
        Prog program = compileProg("(a+)");
        Nfa.Workspace workspace = new Nfa.Workspace();
        int[] groups = new int[4];
        Slice input = Slices.utf8Slice("aaa");

        assertThat(Nfa.search(program, input, 0, input.length(), true, Prog.MatchKind.FULL_MATCH, groups, workspace)).isTrue();
        field("addInstructionIdStack").set(workspace, new int[0]);
        field("addRestoreThreadStack").set(workspace, new int[0]);

        assertThatThrownBy(() -> Nfa.search(program, input, 0, input.length(), true, Prog.MatchKind.FULL_MATCH, groups, workspace))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class);
        assertThat(field("program").get(workspace)).isNull();
        assertThat(field("firstQueue").get(workspace)).isNull();
        assertThat(field("threadCaptures").get(workspace)).isNull();

        assertThat(Nfa.search(program, input, 0, input.length(), true, Prog.MatchKind.FULL_MATCH, groups, workspace)).isTrue();
        assertThat(groups).containsExactly(0, 3, 0, 3);
    }

    private static Field field(String name)
            throws ReflectiveOperationException
    {
        Field field = Nfa.Workspace.class.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
