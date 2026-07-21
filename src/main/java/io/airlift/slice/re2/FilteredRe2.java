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

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * FilteredRe2 provides filtered regular expression matching.
 * <p>
 * It uses prefiltering to quickly eliminate regexps that cannot match,
 * then verifies potential matches with the actual RE2 engine.
 */
public final class FilteredRe2
{
    private static final int DEFAULT_MIN_ATOM_LENGTH = 3;

    private final List<Re2> re2List;
    private final PrefilterTree prefilterTree;
    private boolean compiled;

    public FilteredRe2()
    {
        this(DEFAULT_MIN_ATOM_LENGTH);
    }

    public FilteredRe2(int minimumAtomLength)
    {
        this.re2List = new ArrayList<>();
        this.prefilterTree = new PrefilterTree(minimumAtomLength);
        this.compiled = false;
    }

    /**
     * Add a pattern and return its index.
     *
     * @throws io.airlift.slice.re2.RegexpParseException if the pattern is invalid
     */
    public int add(Slice pattern, Re2.Options options)
    {
        ensureNotCompiled();
        requireNonNull(pattern, "pattern is null");
        Re2 compiledRegexp = Re2.compile(pattern, requireNonNull(options, "options is null"));

        int regexpIndex = re2List.size();
        re2List.add(compiledRegexp);

        Prefilter prefilter = Prefilter.fromRe2(compiledRegexp);
        prefilterTree.add(prefilter);
        return regexpIndex;
    }

    /**
     * Compile the FilteredRe2 set, populating the atoms list.
     * Must be called before matching.
     *
     * @param atoms output list to receive the required atoms
     */
    public void compile(List<Slice> atoms)
    {
        ensureNotCompiled();
        requireNonNull(atoms, "atoms is null");
        prefilterTree.compile(atoms);
        compiled = !re2List.isEmpty();
    }

    /**
     * Get the number of patterns in the set.
     */
    public int patternCount()
    {
        return re2List.size();
    }

    /**
     * Get the RE2 for a specific pattern index.
     */
    public Re2 pattern(int index)
    {
        return re2List.get(index);
    }

    /**
     * Find all potential matches given matched atom indices.
     * This only consults the prefilter tree, not the actual RE2s.
     *
     * @param atomIndices indices of atoms that were found in the text
     * @param potentialMatches output list of pattern indices that might match
     */
    public void allPotentials(List<Integer> atomIndices, List<Integer> potentialMatches)
    {
        ensureCompiled();
        requireNonNull(atomIndices, "atomIndices is null");
        requireNonNull(potentialMatches, "potentialMatches is null");
        prefilterTree.regexpMatches(atomIndices, potentialMatches);
    }

    /**
     * Find all patterns that match the text.
     * Uses prefiltering to reduce candidates, then verifies with RE2.
     *
     * @param text the text to match against
     * @param atomIndices indices of atoms found in the text
     * @param matches output list of pattern indices that match
     */
    public void allMatches(Slice text, List<Integer> atomIndices, List<Integer> matches)
    {
        ensureCompiled();
        requireNonNull(text, "text is null");
        requireNonNull(atomIndices, "atomIndices is null");
        requireNonNull(matches, "matches is null");
        matches.clear();

        // Get potential matches from prefilter
        List<Integer> potentials = new ArrayList<>();
        allPotentials(atomIndices, potentials);

        // Verify each potential match
        for (int regexpIndex : potentials) {
            if (re2List.get(regexpIndex).matchInto(text, Re2.Anchor.UNANCHORED, null)) {
                matches.add(regexpIndex);
            }
        }
    }

    /**
     * Find the first pattern that matches the text.
     *
     * @param text the text to match against
     * @param atomIndices indices of atoms found in the text
     * @return the index of the first matching pattern, or -1 if none match
     */
    public int firstMatch(Slice text, List<Integer> atomIndices)
    {
        ensureCompiled();
        requireNonNull(text, "text is null");
        requireNonNull(atomIndices, "atomIndices is null");

        // Get potential matches from prefilter
        List<Integer> potentials = new ArrayList<>();
        allPotentials(atomIndices, potentials);

        // Find first actual match
        for (int regexpIndex : potentials) {
            if (re2List.get(regexpIndex).matchInto(text, Re2.Anchor.UNANCHORED, null)) {
                return regexpIndex;
            }
        }
        return -1;
    }

    private void ensureCompiled()
    {
        if (!compiled) {
            throw new IllegalStateException("FilteredRe2.compile() must be called before matching");
        }
    }

    private void ensureNotCompiled()
    {
        if (compiled) {
            throw new IllegalStateException("FilteredRe2.add() cannot be called after compile()");
        }
    }
}
