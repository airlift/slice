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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.re2.Prefilter.PrefilterOp;

/**
 * Deduplicates prefilter nodes and determines which regular expressions may
 * match after their required atoms are found.
 */
final class PrefilterTree
{
    private record PrefilterKey(PrefilterOp operation, Slice atom, List<Integer> childEntryIndexes)
    {
        private PrefilterKey
        {
            childEntryIndexes = List.copyOf(childEntryIndexes);
        }
    }

    private static final class PrefilterEntry
    {
        private final PrefilterOp operation;
        private final List<Integer> childIndexes;
        private final List<Integer> parentIndexes = new ArrayList<>();

        private PrefilterEntry(PrefilterOp operation, List<Integer> childIndexes)
        {
            this.operation = operation;
            this.childIndexes = List.copyOf(childIndexes);
        }
    }

    private final List<PrefilterEntry> entries;
    private final Map<PrefilterKey, Integer> entriesByKey;
    private final List<Integer> regexpRootIndexes;
    private final List<Slice> atoms;
    private final Map<Slice, Integer> atomIndexes;
    private final List<List<Integer>> entryIndexesByAtom;
    private final int minimumAtomLength;
    private boolean compiled;

    PrefilterTree(int minimumAtomLength)
    {
        this.entries = new ArrayList<>();
        this.entriesByKey = new HashMap<>();
        this.regexpRootIndexes = new ArrayList<>();
        this.atoms = new ArrayList<>();
        this.atomIndexes = new HashMap<>();
        this.entryIndexesByAtom = new ArrayList<>();
        this.minimumAtomLength = minimumAtomLength;
    }

    /**
     * Adds a prefilter and returns its regular-expression index.
     */
    int add(Prefilter prefilter)
    {
        int regexpIndex = regexpRootIndexes.size();
        if (!keepNode(prefilter)) {
            regexpRootIndexes.add(-1);
        }
        else {
            int entryIndex = addInternal(prefilter);
            regexpRootIndexes.add(entryIndex);
        }
        return regexpIndex;
    }

    private boolean keepNode(Prefilter prefilter)
    {
        if (prefilter == null) {
            return false;
        }

        return switch (prefilter.op()) {
            case ALL, NONE -> false;
            case ATOM -> prefilter.atom().length() >= minimumAtomLength;
            case AND -> {
                prefilter.subs().removeIf(sub -> !keepNode(sub));
                yield !prefilter.subs().isEmpty();
            }
            case OR -> prefilter.subs().stream().allMatch(this::keepNode);
        };
    }

    private int addInternal(Prefilter prefilter)
    {
        if (prefilter.op() == PrefilterOp.ATOM) {
            return addAtomEntry(prefilter.atom());
        }

        List<Integer> childIndexes = new ArrayList<>();
        for (Prefilter subPrefilter : prefilter.subs()) {
            childIndexes.add(addInternal(subPrefilter));
        }

        PrefilterKey key = new PrefilterKey(prefilter.op(), null, childIndexes);
        Integer existingEntryIndex = entriesByKey.get(key);
        if (existingEntryIndex != null) {
            return existingEntryIndex;
        }

        int entryIndex = entries.size();
        PrefilterEntry entry = new PrefilterEntry(prefilter.op(), childIndexes);
        entries.add(entry);
        entriesByKey.put(key, entryIndex);

        for (int childIndex : childIndexes) {
            entries.get(childIndex).parentIndexes.add(entryIndex);
        }

        return entryIndex;
    }

    private int addAtomEntry(Slice atom)
    {
        PrefilterKey key = new PrefilterKey(PrefilterOp.ATOM, atom, List.of());
        Integer existingEntryIndex = entriesByKey.get(key);
        if (existingEntryIndex != null) {
            return existingEntryIndex;
        }

        int entryIndex = entries.size();
        PrefilterEntry entry = new PrefilterEntry(PrefilterOp.ATOM, List.of());
        entries.add(entry);
        entriesByKey.put(key, entryIndex);

        Integer atomListIndex = atomIndexes.get(atom);
        if (atomListIndex == null) {
            atomListIndex = atoms.size();
            atoms.add(atom);
            atomIndexes.put(atom, atomListIndex);
            entryIndexesByAtom.add(new ArrayList<>());
        }
        entryIndexesByAtom.get(atomListIndex).add(entryIndex);

        return entryIndex;
    }

    /**
     * Compiles the prefilter tree and populates the atom list.
     */
    void compile(List<Slice> outputAtoms)
    {
        if (regexpRootIndexes.isEmpty()) {
            return;
        }
        if (compiled) {
            return;
        }

        compiled = true;
        outputAtoms.clear();
        atoms.stream()
                .map(Slice::copy)
                .forEach(outputAtoms::add);
    }

    /**
     * Finds every regular expression whose prefilter accepts the matched atoms.
     */
    void regexpMatches(List<Integer> atomIndices, List<Integer> regexpIndices)
    {
        regexpIndices.clear();

        for (int regexpIndex = 0; regexpIndex < regexpRootIndexes.size(); regexpIndex++) {
            if (regexpRootIndexes.get(regexpIndex) == -1) {
                regexpIndices.add(regexpIndex);
            }
        }

        if (entries.isEmpty()) {
            return;
        }

        SparseIntArray matchedEntries = new SparseIntArray(entries.size());
        SparseIntArray remainingChildCounts = new SparseIntArray(entries.size());

        for (int entryIndex = 0; entryIndex < entries.size(); entryIndex++) {
            PrefilterEntry entry = entries.get(entryIndex);
            if (entry.operation == PrefilterOp.AND) {
                remainingChildCounts.set(entryIndex, entry.childIndexes.size());
            }
        }

        for (int matchedAtomIndex : atomIndices) {
            if (matchedAtomIndex < 0 || matchedAtomIndex >= entryIndexesByAtom.size()) {
                continue;
            }
            for (int entryIndex : entryIndexesByAtom.get(matchedAtomIndex)) {
                propagateMatch(entryIndex, matchedEntries, remainingChildCounts);
            }
        }

        for (int regexpIndex = 0; regexpIndex < regexpRootIndexes.size(); regexpIndex++) {
            int rootIndex = regexpRootIndexes.get(regexpIndex);
            if (rootIndex != -1 && matchedEntries.hasIndex(rootIndex)) {
                regexpIndices.add(regexpIndex);
            }
        }
    }

    private void propagateMatch(int entryIndex, SparseIntArray matchedEntries, SparseIntArray remainingChildCounts)
    {
        if (matchedEntries.hasIndex(entryIndex)) {
            return;
        }
        matchedEntries.set(entryIndex, 1);

        PrefilterEntry entry = entries.get(entryIndex);
        for (int parentIndex : entry.parentIndexes) {
            PrefilterEntry parent = entries.get(parentIndex);
            if (parent.operation == PrefilterOp.OR) {
                propagateMatch(parentIndex, matchedEntries, remainingChildCounts);
            }
            else if (parent.operation == PrefilterOp.AND) {
                int remainingChildren = remainingChildCounts.getExisting(parentIndex) - 1;
                remainingChildCounts.setExisting(parentIndex, remainingChildren);
                if (remainingChildren == 0) {
                    propagateMatch(parentIndex, matchedEntries, remainingChildCounts);
                }
            }
        }
    }
}
