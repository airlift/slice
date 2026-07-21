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
import io.airlift.slice.SliceUtf8;

import java.util.Map;

import static io.airlift.slice.re2.Re2.Anchor.ANCHOR_BOTH;
import static io.airlift.slice.re2.Re2.Anchor.ANCHOR_START;
import static io.airlift.slice.re2.Re2.Anchor.UNANCHORED;
import static java.util.Objects.requireNonNull;

/**
 * A reusable matcher for one compiled pattern.
 * <p>
 * The matcher owns one capture buffer and reuses it across operations. It is mutable and not
 * thread-safe. Group values are zero-copy views of the current input.
 */
public final class Re2Matcher
{
    private final Re2 pattern;
    private final int[] groups;
    private final Map<String, Integer> namedCapturingGroups;
    private final boolean latin1;
    private final SingleByteMatcher singleByteMatcher;
    private final Dfa.CandidateStartCursor candidateStartCursor;
    private final BitState.Workspace bitStateWorkspace = new BitState.Workspace();
    private final Nfa.Workspace nfaWorkspace = new Nfa.Workspace();

    private Slice input;
    private int regionStart;
    private int regionEnd;
    private int nextFindStart;
    private boolean hasMatch;

    Re2Matcher(Re2 pattern, Slice input)
    {
        this(pattern, input, null);
    }

    Re2Matcher(Re2 pattern, Slice input, SingleByteMatcher singleByteMatcher)
    {
        this(pattern, input, singleByteMatcher, pattern.capturingGroupCount());
    }

    Re2Matcher(Re2 pattern, Slice input, SingleByteMatcher singleByteMatcher, int capturingGroupCount)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        if (capturingGroupCount < 0 || capturingGroupCount > pattern.capturingGroupCount()) {
            throw new IllegalArgumentException("capturingGroupCount out of range: " + capturingGroupCount);
        }
        this.groups = new int[2 * (capturingGroupCount + 1)];
        this.namedCapturingGroups = capturingGroupCount == 0 ? Map.of() : pattern.namedCapturingGroups();
        this.latin1 = (pattern.flags() & Regexp.LATIN1) != 0;
        this.singleByteMatcher = groups.length == 2 ? singleByteMatcher : null;
        this.candidateStartCursor = groups.length == 2 && this.singleByteMatcher == null
                ? pattern.createCandidateStartCursor()
                : null;
        reset(input);
    }

    public Re2Matcher reset(Slice input)
    {
        requireNonNull(input, "input is null");
        return reset(input, 0, input.length());
    }

    /**
     * Resets this matcher to a logical input region without constructing a Slice view. Match and
     * group offsets are relative to the start of the region.
     */
    public Re2Matcher reset(Slice input, int start, int end)
    {
        requireNonNull(input, "input is null");
        if (start < 0 || end < start || end > input.length()) {
            throw new IndexOutOfBoundsException("region out of bounds: [" + start + ", " + end + ")");
        }
        this.input = input;
        regionStart = start;
        regionEnd = end;
        if (candidateStartCursor != null) {
            candidateStartCursor.reset(end - start);
        }
        nextFindStart = 0;
        invalidateMatch();
        return this;
    }

    public boolean find()
    {
        int regionLength = regionLength();
        if (nextFindStart > regionLength) {
            invalidateMatch();
            return false;
        }

        if (singleByteMatcher != null) {
            int matchStart = singleByteMatcher.find(input, regionStart + nextFindStart, regionEnd);
            if (matchStart < 0) {
                nextFindStart = regionLength + 1;
                invalidateMatch();
                return false;
            }
            matchStart -= regionStart;
            groups[0] = matchStart;
            groups[1] = matchStart + 1;
            nextFindStart = matchStart + 1;
            hasMatch = true;
            return true;
        }

        if (pattern.canReturnEmptyAtStart(input, regionStart, regionEnd, regionStart + nextFindStart)) {
            groups[0] = nextFindStart;
            groups[1] = nextFindStart;
            setMatchAndAdvance(nextFindStart, nextFindStart);
            return true;
        }

        if (candidateStartCursor != null && candidateStartCursor.enabled()) {
            long candidateBoundary = Dfa.searchCandidateBoundary(
                    candidateStartCursor,
                    input,
                    regionStart,
                    regionEnd,
                    regionStart + nextFindStart);
            if (candidateBoundary >= 0) {
                groups[0] = (int) (candidateBoundary >>> 32);
                groups[1] = (int) candidateBoundary;
                setMatchAndAdvance(groups[0], groups[1]);
                return true;
            }
            if (candidateBoundary == Dfa.SEARCH_NO_MATCH) {
                nextFindStart = regionLength + 1;
                invalidateMatch();
                return false;
            }
        }

        if (!pattern.matchRegionInto(
                input,
                regionStart,
                regionEnd,
                regionStart + nextFindStart,
                regionEnd,
                UNANCHORED,
                groups,
                bitStateWorkspace,
                nfaWorkspace)) {
            nextFindStart = regionLength + 1;
            invalidateMatch();
            return false;
        }

        setMatchAndAdvance(groups[0], groups[1]);
        return true;
    }

    private void setMatchAndAdvance(int matchStart, int matchEnd)
    {
        hasMatch = true;
        if (matchStart != matchEnd) {
            nextFindStart = matchEnd;
        }
        else if (matchEnd == regionLength()) {
            nextFindStart = regionLength() + 1;
        }
        else if (latin1) {
            nextFindStart = matchEnd + 1;
        }
        else {
            nextFindStart = matchEnd + SliceUtf8.lengthOfCodePointSafe(
                    input.byteArray(),
                    input.byteArrayOffset() + regionStart,
                    regionLength(),
                    matchEnd);
        }
    }

    /**
     * Finds the next match starting at the specified byte offset in the logical input Slice.
     */
    public boolean find(int start)
    {
        if (start < 0 || start > regionLength()) {
            throw new IndexOutOfBoundsException("start out of bounds: " + start);
        }
        nextFindStart = start;
        invalidateMatch();
        return find();
    }

    public boolean matches()
    {
        return match(ANCHOR_BOTH);
    }

    public boolean lookingAt()
    {
        return match(ANCHOR_START);
    }

    private boolean match(Re2.Anchor anchor)
    {
        invalidateMatch();
        if (!pattern.matchRegionInto(input, regionStart, regionEnd, regionStart, regionEnd, anchor, groups, bitStateWorkspace, nfaWorkspace)) {
            return false;
        }
        hasMatch = true;
        nextFindStart = end();
        return true;
    }

    public int groupCount()
    {
        return (groups.length / 2) - 1;
    }

    public boolean matched(int group)
    {
        checkMatch();
        checkGroup(group);
        return groups[group * 2] >= 0;
    }

    public int start()
    {
        return start(0);
    }

    public int start(int group)
    {
        checkMatch();
        checkGroup(group);
        return groups[group * 2];
    }

    public int end()
    {
        return end(0);
    }

    public int end(int group)
    {
        checkMatch();
        checkGroup(group);
        return groups[group * 2 + 1];
    }

    public Slice group()
    {
        return group(0);
    }

    public Slice group(int group)
    {
        int start = start(group);
        if (start < 0) {
            return null;
        }
        return input.slice(regionStart + start, end(group) - start);
    }

    public Slice group(String groupName)
    {
        requireNonNull(groupName, "groupName is null");
        Integer group = namedCapturingGroups.get(groupName);
        if (group == null) {
            throw new IllegalArgumentException("unknown named group: " + groupName);
        }
        return group(group);
    }

    public MatchResult toMatchResult()
    {
        checkMatch();
        Slice resultInput = regionStart == 0 && regionEnd == input.length()
                ? input
                : input.slice(regionStart, regionLength());
        return new MatchResult(resultInput, groups.clone(), namedCapturingGroups);
    }

    int groupOffsetForDiagnostics(int index)
    {
        return groups[index];
    }

    boolean candidateStartCursorEnabledForDiagnostics()
    {
        return candidateStartCursor != null && candidateStartCursor.enabled();
    }

    long candidateStartRouteCountForDiagnostics()
    {
        return candidateStartCursor == null ? 0 : candidateStartCursor.routeCount();
    }

    long candidateStartFallbackCountForDiagnostics()
    {
        return candidateStartCursor == null ? 0 : candidateStartCursor.fallbackCount();
    }

    private void invalidateMatch()
    {
        hasMatch = false;
    }

    private void checkMatch()
    {
        if (!hasMatch) {
            throw new IllegalStateException("no successful match");
        }
    }

    private void checkGroup(int group)
    {
        if (group < 0 || group > groupCount()) {
            throw new IllegalArgumentException("group index out of range: " + group);
        }
    }

    private int regionLength()
    {
        return regionEnd - regionStart;
    }
}
