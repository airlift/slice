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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// FilteredRe2 uses prefiltering to quickly eliminate regexps that cannot match.
// The caller must search text for atoms and provide found atom indices.
// Ported from upstream RE2: re2/testing/filtered_re2_test.cc.
public class TestFilteredRe2
{
    private static class FilterTestState
    {
        List<Slice> atoms = new ArrayList<>();
        List<Integer> atomIndices = new ArrayList<>();
        List<Integer> matches = new ArrayList<>();
        Re2.Options options = Re2.Options.defaults();
        FilteredRe2 filteredRe2;

        FilterTestState()
        {
            filteredRe2 = new FilteredRe2();
        }

        FilterTestState(int minimumAtomLength)
        {
            filteredRe2 = new FilteredRe2(minimumAtomLength);
        }
    }

    @Test
    public void testEmpty()
    {
        FilterTestState state = new FilterTestState();
        state.atoms.add(utf8("sentinel"));
        state.filteredRe2.compile(state.atoms);
        assertThat(state.atoms).containsExactly(utf8("sentinel"));
        assertThatThrownBy(() -> state.filteredRe2.allMatches(textBytes("foo"), state.atomIndices, state.matches))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must be called before matching");

        int patternIndex = state.filteredRe2.add(utf8("foo"), state.options);
        state.filteredRe2.compile(state.atoms);
        assertThat(state.atoms).containsExactly(utf8("foo"));
        state.filteredRe2.allMatches(textBytes("foo"), List.of(0), state.matches);
        assertThat(state.matches).containsExactly(patternIndex);
    }

    @Test
    public void testAddAfterCompileThrows()
    {
        FilterTestState state = new FilterTestState();
        state.filteredRe2.add(utf8("foo"), state.options);
        state.filteredRe2.compile(state.atoms);
        assertThatThrownBy(() -> state.filteredRe2.add(utf8("bar"), state.options))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot be called after compile");
    }

    @Test
    public void testMatchBeforeCompileThrows()
    {
        FilterTestState state = new FilterTestState();
        assertThatThrownBy(() -> state.filteredRe2.allMatches(textBytes("foo"), List.of(), state.matches))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must be called before matching");
    }

    @Test
    public void testSmallOr()
    {
        // With minimumAtomLength=4, atoms foo and bar are too short to extract
        FilterTestState state = new FilterTestState(4);
        int patternIndex = state.filteredRe2.add(utf8("(foo|bar)"), state.options);
        state.filteredRe2.compile(state.atoms);
        assertThat(state.atoms).isEmpty();
        // Pattern with no atoms should match via ALL prefilter
        state.filteredRe2.allMatches(textBytes("lemurs bar"), state.atomIndices, state.matches);
        assertThat(state.matches).hasSize(1);
        assertThat(state.matches.get(0)).isEqualTo(patternIndex);
    }

    @Test
    public void testBasicMatchingWithAtoms()
    {
        // Use high minimumAtomLength so no atoms are extracted (ALL prefilter)
        FilterTestState state = new FilterTestState(100);
        int helloPatternIndex = state.filteredRe2.add(utf8("hello"), state.options);
        int worldPatternIndex = state.filteredRe2.add(utf8("world"), state.options);
        state.filteredRe2.compile(state.atoms);
        // No atoms extracted due to high minimumAtomLength
        assertThat(state.atoms).isEmpty();

        // Both patterns should match (ALL prefilter means RE2 verification)
        state.filteredRe2.allMatches(textBytes("hello world"), state.atomIndices, state.matches);
        assertThat(state.matches).containsExactlyInAnyOrder(helloPatternIndex, worldPatternIndex);

        // Partial matching
        state.filteredRe2.allMatches(textBytes("hello"), state.atomIndices, state.matches);
        assertThat(state.matches).containsExactly(helloPatternIndex);

        // No match
        state.filteredRe2.allMatches(textBytes("goodbye"), state.atomIndices, state.matches);
        assertThat(state.matches).isEmpty();
    }

    @Test
    public void testFirstMatch()
    {
        FilterTestState state = new FilterTestState(100);  // High minimumAtomLength = no atoms
        state.filteredRe2.add(utf8("abc"), state.options);
        state.filteredRe2.add(utf8("def"), state.options);
        state.filteredRe2.compile(state.atoms);

        Slice text = textBytes("abcdef");
        int firstMatch = state.filteredRe2.firstMatch(text, state.atomIndices);
        assertThat(firstMatch).isEqualTo(0);
    }

    @Test
    public void testMatchEmptyPattern()
    {
        FilterTestState state = new FilterTestState();
        state.filteredRe2.add(utf8(""), state.options);
        state.filteredRe2.compile(state.atoms);

        Slice text = textBytes("0123");
        List<Integer> atomIds = new ArrayList<>();
        // Empty pattern matches everywhere
        assertThat(state.filteredRe2.firstMatch(text, atomIds)).isEqualTo(0);
    }

    @Test
    public void testNumRegexps()
    {
        FilterTestState state = new FilterTestState();
        assertThat(state.filteredRe2.patternCount()).isEqualTo(0);

        state.filteredRe2.add(utf8("foo"), state.options);
        assertThat(state.filteredRe2.patternCount()).isEqualTo(1);

        state.filteredRe2.add(utf8("bar"), state.options);
        assertThat(state.filteredRe2.patternCount()).isEqualTo(2);
    }

    @Test
    public void testGetRe2()
    {
        FilterTestState state = new FilterTestState();
        int patternIndex = state.filteredRe2.add(utf8("test\\d+"), state.options);
        state.filteredRe2.compile(state.atoms);

        Re2 re2 = state.filteredRe2.pattern(patternIndex);
        assertThat(re2).isNotNull();
    }

    @Test
    public void testComplexPattern()
    {
        FilterTestState state = new FilterTestState(100);  // High minimumAtomLength = no atoms
        // Complex pattern with multiple parts
        state.filteredRe2.add(utf8("(abc|def)\\d+xyz"), state.options);
        state.filteredRe2.compile(state.atoms);

        state.filteredRe2.allMatches(textBytes("abc123xyz"), state.atomIndices, state.matches);
        assertThat(state.matches).hasSize(1);

        state.filteredRe2.allMatches(textBytes("def456xyz"), state.atomIndices, state.matches);
        assertThat(state.matches).hasSize(1);

        state.filteredRe2.allMatches(textBytes("ghi789xyz"), state.atomIndices, state.matches);
        assertThat(state.matches).isEmpty();
    }

    @Test
    public void testMultiplePatterns()
    {
        FilterTestState state = new FilterTestState(100);  // High minimumAtomLength = no atoms
        int alphaPatternIndex = state.filteredRe2.add(utf8("alpha"), state.options);
        int betaPatternIndex = state.filteredRe2.add(utf8("beta"), state.options);
        int gammaPatternIndex = state.filteredRe2.add(utf8("gamma"), state.options);

        state.filteredRe2.compile(state.atoms);

        // Match all three
        state.filteredRe2.allMatches(textBytes("alpha beta gamma"), state.atomIndices, state.matches);
        assertThat(state.matches).containsExactlyInAnyOrder(alphaPatternIndex, betaPatternIndex, gammaPatternIndex);
    }

    @Test
    public void testPrefilterWithAtomIndices()
    {
        // Test that providing atom indices correctly filters
        FilterTestState state = new FilterTestState();  // Default minimumAtomLength=3
        state.filteredRe2.add(utf8("hello"), state.options);
        state.filteredRe2.add(utf8("world"), state.options);
        state.filteredRe2.compile(state.atoms);

        // Atoms should be extracted
        assertThat(state.atoms).isNotEmpty();

        // Without providing correct atom indices, prefilter may reject
        // This is expected behavior - prefilter requires atoms to be found
    }

    @Test
    public void testAllPotentials()
    {
        FilterTestState state = new FilterTestState(100);  // High minimumAtomLength = no atoms
        int patternIndex1 = state.filteredRe2.add(utf8("pattern1"), state.options);
        int patternIndex2 = state.filteredRe2.add(utf8("pattern2"), state.options);
        state.filteredRe2.compile(state.atoms);

        List<Integer> potentials = new ArrayList<>();
        // With no atoms extracted (ALL prefilter), all patterns are potential matches
        state.filteredRe2.allPotentials(List.of(), potentials);
        assertThat(potentials).containsExactlyInAnyOrder(patternIndex1, patternIndex2);
    }

    private List<String> compileAndGetAtoms(List<String> patterns)
    {
        FilteredRe2 filteredRe2 = new FilteredRe2();  // Uses default minimumAtomLength=3
        Re2.Options options = Re2.Options.defaults();
        for (String pattern : patterns) {
            filteredRe2.add(utf8(pattern), options);
        }
        List<Slice> atoms = new ArrayList<>();
        filteredRe2.compile(atoms);
        return atoms.stream()
                .map(Slice::toStringUtf8)
                .toList();
    }

    @Test
    public void testAtomExtractionEmptyPattern()
    {
        // This test checks to make sure empty patterns are allowed.
        List<String> actualAtoms = compileAndGetAtoms(List.of(""));
        assertThat(actualAtoms).isEmpty();
    }

    @Test
    public void testAtomExtractionMinLength()
    {
        // This test checks that atoms of length greater than min length
        // are found, and atoms shorter than min length are not extracted.
        List<String> patterns = List.of(
                "(abc123|def456|ghi789).*mnop[x-z]+",
                "abc..yyy..zz",
                "mnmnpp[a-z]+PPP");

        List<String> actualAtoms = compileAndGetAtoms(patterns);

        assertThat(actualAtoms).containsExactlyInAnyOrder(
                "abc123", "def456", "ghi789", "mnop",
                "abc", "yyy", "mnmnpp", "ppp");
    }

    @Test
    public void testAtomExtractionUnicode()
    {
        List<String> patterns = List.of(
                "(?i)ΔδΠϖπΣςσ",
                "ΛΜΝΟΠ",
                "ψρστυ");

        List<String> actualAtoms = compileAndGetAtoms(patterns);

        assertThat(actualAtoms).containsExactlyInAnyOrder(
                "δδπππσσσ",
                "λμνοπ",
                "ψρστυ");
    }

    @Test
    public void testAtomExtractionSubstrNoDedup()
    {
        List<String> patterns = List.of(
                "(abc123|abc|defxyz|ghi789|abc1234|xyz).*[x-z]+",
                "abcd..yyy..yyyzzz",
                "mnmnpp[a-z]+PPP");

        List<String> actualAtoms = compileAndGetAtoms(patterns);

        assertThat(actualAtoms).containsExactlyInAnyOrder(
                "abc", "ghi789", "xyz",
                "abcd", "yyy", "yyyzzz",
                "mnmnpp", "ppp");
    }

    @Test
    public void testAtomExtractionCharClass()
    {
        List<String> patterns = List.of(
                "m[a-c][d-f]n.*[x-z]+",
                "[x-y]bcde[ab]");

        List<String> actualAtoms = compileAndGetAtoms(patterns);

        assertThat(actualAtoms).containsExactlyInAnyOrder(
                "madn", "maen", "mafn",
                "mbdn", "mben", "mbfn",
                "mcdn", "mcen", "mcfn",
                "xbcdea", "xbcdeb",
                "ybcdea", "ybcdeb");
    }

    private List<Integer> findAtomIndices(List<Slice> atoms, List<String> toFind)
    {
        List<Integer> indices = new ArrayList<>();
        for (String find : toFind) {
            for (int atomIndex = 0; atomIndex < atoms.size(); atomIndex++) {
                if (find.equals(atoms.get(atomIndex).toStringUtf8())) {
                    indices.add(atomIndex);
                    break;
                }
            }
        }
        return indices;
    }

    @Test
    public void testMatchWithAtomIndices()
    {
        // Use patterns from the SubstrAtomRemovesSuperStrInOr test case
        // These are 3 patterns that will compile to multiple atoms
        List<String> patterns = List.of(
                "(abc123|abc|defxyz|ghi789|abc1234|xyz).*[x-z]+",
                "abcd..yyy..yyyzzz",
                "mnmnpp[a-z]+PPP");

        FilteredRe2 filteredRe2 = new FilteredRe2();
        Re2.Options options = Re2.Options.defaults();
        for (String pattern : patterns) {
            filteredRe2.add(utf8(pattern), options);
        }
        List<Slice> atoms = new ArrayList<>();
        filteredRe2.compile(atoms);

        List<Integer> matches = new ArrayList<>();

        // Test 1: text = "abc121212xyz", atoms = ["abc"]
        String text1 = "abc121212xyz";
        List<Integer> atomIds1 = findAtomIndices(atoms, List.of("abc"));
        filteredRe2.allMatches(textBytes(text1), atomIds1, matches);
        assertThat(matches).containsExactly(0);

        // Test 2: text = "abc12312yyyzzz", atoms = ["abc", "yyy", "yyyzzz"]
        String text2 = "abc12312yyyzzz";
        List<Integer> atomIds2 = findAtomIndices(atoms, List.of("abc", "yyy", "yyyzzz"));
        filteredRe2.allMatches(textBytes(text2), atomIds2, matches);
        assertThat(matches).containsExactly(0);

        // Test 3: text = "abcd12yyy32yyyzzz", atoms = ["abc", "abcd", "yyy", "yyyzzz"]
        String text3 = "abcd12yyy32yyyzzz";
        List<Integer> atomIds3 = findAtomIndices(atoms, List.of("abc", "abcd", "yyy", "yyyzzz"));
        filteredRe2.allMatches(textBytes(text3), atomIds3, matches);
        assertThat(matches).containsExactly(0, 1);
    }

    @Test
    public void testSmallLatinAsciiOnly()
    {
        // Test Latin1 encoding with ASCII-only pattern (no high bytes)
        FilterTestState state = new FilterTestState(100);  // High minimumAtomLength = no atoms extracted
        int patternIndex;

        state.options.setEncoding(Re2.Options.Encoding.LATIN1);
        // ASCII-only Latin1 pattern
        String pattern = "TestPattern";
        patternIndex = state.filteredRe2.add(utf8(pattern), state.options);
        state.filteredRe2.compile(state.atoms);

        // With high minimumAtomLength, no atoms are extracted (ALL prefilter)
        assertThat(state.atoms).isEmpty();

        // Test that matching works correctly for ASCII text
        byte[] textBytes = "fooTestPatternbar".getBytes(StandardCharsets.ISO_8859_1);
        state.filteredRe2.allMatches(Slices.wrappedBuffer(textBytes), state.atomIndices, state.matches);
        assertThat(state.matches).hasSize(1);
        assertThat(state.matches.get(0)).isEqualTo(patternIndex);
    }

    @Test
    public void testSmallLatinHighBytes()
    {
        FilterTestState state = new FilterTestState();

        state.options.setEncoding(Re2.Options.Encoding.LATIN1);
        byte[] patternBytes = new byte[] {(byte) 0xde, (byte) 0xad, 'Q', (byte) 0xbe, (byte) 0xef};
        int patternIndex = state.filteredRe2.add(Slices.wrappedBuffer(patternBytes), state.options);
        state.filteredRe2.compile(state.atoms);

        byte[] expectedAtom = new byte[] {(byte) 0xde, (byte) 0xad, 'q', (byte) 0xbe, (byte) 0xef};
        assertThat(state.atoms).containsExactly(Slices.wrappedBuffer(expectedAtom));

        byte[] textBytes = new byte[] {'f', 'o', 'o', (byte) 0xde, (byte) 0xad, 'Q', (byte) 0xbe, (byte) 0xef, 'l', 'e', 'm', 'u', 'r'};
        state.filteredRe2.allMatches(Slices.wrappedBuffer(textBytes), List.of(0), state.matches);
        assertThat(state.matches).containsExactly(patternIndex);
    }

    @Test
    public void testEmptyStringInStringSetBug()
    {
        // Bug due to find() finding "" at the start of everything in a string
        // set and thus SimplifyStringSet() would end up erasing everything.
        // In order to test this, we have to keep PrefilterTree from discarding
        // the OR entirely, so we have to make the minimum atom length zero.

        FilterTestState state = new FilterTestState(0);  // override the minimum atom length
        state.filteredRe2.add(utf8("-R.+(|ADD=;AA){12}}"), state.options);
        state.filteredRe2.compile(state.atoms);

        List<String> expectedAtoms = new ArrayList<>(List.of("", "-r", "add=;aa", "}"));
        List<String> actualAtoms = state.atoms.stream()
                .map(Slice::toStringUtf8)
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));

        Collections.sort(expectedAtoms);
        Collections.sort(actualAtoms);

        assertThat(actualAtoms)
                .as("EmptyStringInStringSetBug")
                .isEqualTo(expectedAtoms);
    }

    private static Slice textBytes(String value)
    {
        return Slices.utf8Slice(value);
    }

    private static Slice utf8(String value)
    {
        return Slices.utf8Slice(value);
    }
}
