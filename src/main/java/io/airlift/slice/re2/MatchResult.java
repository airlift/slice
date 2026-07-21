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

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * An immutable snapshot of one match.
 * <p>
 * Group offsets are byte offsets relative to the original logical Slice. Group slices are
 * zero-copy views and therefore retain the input's backing storage.
 */
public final class MatchResult
{
    private final Slice text;
    private final int[] groups;
    private final Map<String, Integer> namedCapturingGroups;

    MatchResult(Slice text, int[] groups, Map<String, Integer> namedCapturingGroups)
    {
        this.text = requireNonNull(text, "text is null");
        this.groups = requireNonNull(groups, "groups is null");
        if ((groups.length % 2) != 0) {
            throw new IllegalArgumentException("groups length must be even: " + groups.length);
        }
        this.namedCapturingGroups = requireNonNull(namedCapturingGroups, "namedCapturingGroups is null");
    }

    public int groupCount()
    {
        return Math.max(0, (groups.length / 2) - 1);
    }

    public boolean matched(int group)
    {
        checkGroup(group);
        int start = groups[group * 2];
        int end = groups[group * 2 + 1];
        return start >= 0 && end >= start;
    }

    public int start(int group)
    {
        checkGroup(group);
        if (!matched(group)) {
            return -1;
        }
        return groups[group * 2];
    }

    public int end(int group)
    {
        checkGroup(group);
        if (!matched(group)) {
            return -1;
        }
        return groups[group * 2 + 1];
    }

    public int length(int group)
    {
        int start = start(group);
        if (start < 0) {
            return -1;
        }
        return end(group) - start;
    }

    public Slice groupSlice(int group)
    {
        int start = start(group);
        if (start < 0) {
            return null;
        }
        int end = end(group);
        return text.slice(start, end - start);
    }

    public Slice groupSlice(String groupName)
    {
        return groupSlice(groupIndex(groupName));
    }

    public String groupUtf8(int group)
    {
        Slice slice = groupSlice(group);
        if (slice == null) {
            return null;
        }
        return slice.toStringUtf8();
    }

    public String groupUtf8(String groupName)
    {
        return groupUtf8(groupIndex(groupName));
    }

    public int parseInt(int group)
    {
        return parseInt(group, 10);
    }

    public int parseInt(int group, int radix)
    {
        long value = parseSignedLongGroup(group, radix);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new NumberFormatException("group " + group + " value out of int range");
        }
        return (int) value;
    }

    public long parseLong(int group)
    {
        return parseLong(group, 10);
    }

    public long parseLong(int group, int radix)
    {
        return parseSignedLongGroup(group, radix);
    }

    public long parseUnsignedInt(int group)
    {
        return parseUnsignedInt(group, 10);
    }

    public long parseUnsignedInt(int group, int radix)
    {
        long value = parseUnsignedLongGroup(group, radix);
        if (Long.compareUnsigned(value, 0xFFFF_FFFFL) > 0) {
            throw new NumberFormatException("group " + group + " value out of unsigned int range");
        }
        return value;
    }

    public long parseUnsignedLong(int group)
    {
        return parseUnsignedLong(group, 10);
    }

    public long parseUnsignedLong(int group, int radix)
    {
        return parseUnsignedLongGroup(group, radix);
    }

    private int groupIndex(String groupName)
    {
        requireNonNull(groupName, "groupName is null");
        Integer groupIndex = namedCapturingGroups.get(groupName);
        if (groupIndex == null) {
            throw new IllegalArgumentException("unknown named group: " + groupName);
        }
        return groupIndex;
    }

    private long parseSignedLongGroup(int group, int radix)
    {
        Slice slice = requireMatchedGroup(group);
        return NumericParsers.parseSignedLong(slice.byteArray(), slice.byteArrayOffset(), slice.length(), radix);
    }

    private long parseUnsignedLongGroup(int group, int radix)
    {
        Slice slice = requireMatchedGroup(group);
        return NumericParsers.parseUnsignedLong(slice.byteArray(), slice.byteArrayOffset(), slice.length(), radix);
    }

    private Slice requireMatchedGroup(int group)
    {
        Slice slice = groupSlice(group);
        if (slice == null) {
            throw new NumberFormatException("group " + group + " is unmatched");
        }
        return slice;
    }

    private void checkGroup(int group)
    {
        if (group < 0 || group > groupCount()) {
            throw new IllegalArgumentException("group index out of range: " + group);
        }
    }
}
