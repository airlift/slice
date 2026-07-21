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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static io.airlift.slice.re2.RegexpStatusCode.BAD_UTF8;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Trino SQL regular-expression operations implemented over RE2.
 * <p>
 * Function behavior follows Trino's shared regexp contract, including UTF-8 code-point positions,
 * empty-match iteration, trailing split fields, and Java-style replacement references. Pattern
 * syntax remains RE2 syntax; unsupported Java constructs and known syntax collisions are rejected.
 */
public final class TrinoRegexp
{
    private final Re2 pattern;
    private final boolean mayHaveSingleByteMatcher;
    private final boolean mayHaveSingleByteRepeatMatcher;
    private volatile SingleByteMatcher singleByteMatcher;
    private volatile SingleByteRepeatMatcher singleByteRepeatMatcher;

    private TrinoRegexp(Re2 pattern)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.mayHaveSingleByteMatcher = pattern.mayHaveSingleByteMatcher();
        this.mayHaveSingleByteRepeatMatcher = pattern.canMatchEmpty();
    }

    public static TrinoRegexp compile(Slice pattern)
    {
        requireNonNull(pattern, "pattern is null");
        validateNoSyntaxCollision(pattern);
        try {
            return new TrinoRegexp(Re2.compile(pattern));
        }
        catch (RegexpParseException exception) {
            if (exception.statusCode() != BAD_UTF8) {
                throw exception;
            }
            return new TrinoRegexp(Re2.compile(pattern, Re2.Options.latin1()));
        }
    }

    Re2 pattern()
    {
        return pattern;
    }

    boolean isSingleByteRepeatMatcherComputed()
    {
        return singleByteRepeatMatcher != null;
    }

    boolean isSingleByteMatcherComputed()
    {
        return singleByteMatcher != null;
    }

    public boolean contains(Slice source)
    {
        requireNonNull(source, "source is null");
        return pattern.partialMatch(source);
    }

    public long count(Slice source)
    {
        requireNonNull(source, "source is null");
        SingleByteMatcher byteMatcher = singleByteMatcher();
        if (byteMatcher != SingleByteMatcher.unsupported()) {
            return byteMatcher.count(source);
        }

        SingleByteRepeatMatcher repeatMatcher = singleByteRepeatMatcher();
        if (repeatMatcher != SingleByteRepeatMatcher.unsupported()) {
            return repeatMatcher.count(source);
        }

        long optimizedCount = pattern.countMatches(source);
        if (optimizedCount >= 0) {
            return optimizedCount;
        }

        Re2Matcher matcher = newGroupZeroMatcher(source);
        long count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    public long position(Slice source)
    {
        return position(source, 1, 1);
    }

    public long position(Slice source, long start)
    {
        return position(source, start, 1);
    }

    public long position(Slice source, long start, long occurrence)
    {
        requireNonNull(source, "source is null");
        if (start < 1) {
            throw new IllegalArgumentException("start position cannot be smaller than 1");
        }
        if (occurrence < 1) {
            throw new IllegalArgumentException("occurrence cannot be smaller than 1");
        }

        long codePointStart = start - 1;
        if (codePointStart > source.length()) {
            return -1;
        }
        int byteStart = SliceUtf8.offsetOfCodePoint(source, toIntExact(codePointStart));
        if (byteStart < 0) {
            return -1;
        }

        SingleByteRepeatMatcher.Cursor repeatCursor = newSingleByteRepeatCursor(source, false);
        if (repeatCursor != null) {
            repeatCursor.find(byteStart);
            for (long match = 1; match < occurrence; match++) {
                if (!repeatCursor.find()) {
                    return -1;
                }
            }
            return SliceUtf8.countCodePoints(source, 0, repeatCursor.start()) + 1L;
        }

        Re2Matcher matcher = newGroupZeroMatcher(source);
        if (!matcher.find(byteStart)) {
            return -1;
        }
        for (long match = 1; match < occurrence; match++) {
            if (!matcher.find()) {
                return -1;
            }
        }
        return SliceUtf8.countCodePoints(source, 0, matcher.start()) + 1L;
    }

    public Slice extract(Slice source)
    {
        return extract(source, 0);
    }

    public Slice extract(Slice source, int group)
    {
        validateGroup(group);
        requireNonNull(source, "source is null");
        Re2Matcher matcher = group == 0 ? newGroupZeroMatcher(source) : newMatcher(source);
        if (!matcher.find()) {
            return null;
        }
        return matcher.group(group);
    }

    public List<Slice> extractAll(Slice source)
    {
        return extractAll(source, 0);
    }

    public List<Slice> extractAll(Slice source, int group)
    {
        validateGroup(group);
        requireNonNull(source, "source is null");
        SingleByteRepeatMatcher.Cursor repeatCursor = newSingleByteRepeatCursor(source, group != 0);
        if (repeatCursor != null) {
            List<Slice> matches = new ArrayList<>();
            while (repeatCursor.find()) {
                matches.add(repeatCursor.group());
            }
            return Collections.unmodifiableList(matches);
        }

        Re2Matcher matcher = group == 0 ? newGroupZeroMatcher(source) : newMatcher(source);
        List<Slice> matches = new ArrayList<>();
        while (matcher.find()) {
            matches.add(matcher.group(group));
        }
        return Collections.unmodifiableList(matches);
    }

    public List<Slice> split(Slice source)
    {
        requireNonNull(source, "source is null");
        SingleByteRepeatMatcher.Cursor repeatCursor = newSingleByteRepeatCursor(source, false);
        if (repeatCursor != null) {
            List<Slice> parts = new ArrayList<>();
            int previousEnd = 0;
            while (repeatCursor.find()) {
                parts.add(source.slice(previousEnd, repeatCursor.start() - previousEnd));
                previousEnd = repeatCursor.end();
            }
            parts.add(source.slice(previousEnd, source.length() - previousEnd));
            return Collections.unmodifiableList(parts);
        }

        Re2Matcher matcher = newGroupZeroMatcher(source);
        List<Slice> parts = new ArrayList<>();
        int previousEnd = 0;
        while (matcher.find()) {
            parts.add(source.slice(previousEnd, matcher.start() - previousEnd));
            previousEnd = matcher.end();
        }
        parts.add(source.slice(previousEnd, source.length() - previousEnd));
        return Collections.unmodifiableList(parts);
    }

    public Slice replace(Slice source, Slice replacement)
    {
        requireNonNull(source, "source is null");
        requireNonNull(replacement, "replacement is null");

        boolean needsCapturingGroups = replacementNeedsCapturingGroups(replacement);
        SingleByteRepeatMatcher.Cursor repeatCursor = newSingleByteRepeatCursor(source, needsCapturingGroups);
        if (repeatCursor != null) {
            DynamicSliceOutput output = new DynamicSliceOutput(source.length() + replacement.length());
            int previousEnd = 0;
            while (repeatCursor.find()) {
                output.writeBytes(source, previousEnd, repeatCursor.start() - previousEnd);
                appendReplacement(output, replacement, source, repeatCursor.start(), repeatCursor.end());
                previousEnd = repeatCursor.end();
            }
            output.writeBytes(source, previousEnd, source.length() - previousEnd);
            return output.slice();
        }

        Re2Matcher matcher = needsCapturingGroups ? newMatcher(source) : newGroupZeroMatcher(source);
        DynamicSliceOutput output = new DynamicSliceOutput(source.length() + replacement.length());
        int previousEnd = 0;
        boolean matched = false;
        while (matcher.find()) {
            matched = true;
            output.writeBytes(source, previousEnd, matcher.start() - previousEnd);
            appendReplacement(output, replacement, matcher);
            previousEnd = matcher.end();
        }
        if (!matched) {
            return source;
        }
        output.writeBytes(source, previousEnd, source.length() - previousEnd);
        return output.slice();
    }

    boolean replacementNeedsCapturingGroups(Slice replacement)
    {
        for (int index = 0; index < replacement.length(); index++) {
            int current = replacement.getUnsignedByte(index);
            if (current == '\\') {
                if (++index == replacement.length()) {
                    return true;
                }
                continue;
            }
            if (current != '$') {
                continue;
            }
            if (++index == replacement.length()) {
                return true;
            }

            int next = replacement.getUnsignedByte(index);
            if (next == '{' || next < '0' || next > '9') {
                return true;
            }

            int group = next - '0';
            if (group > 0) {
                return true;
            }
            while (index + 1 < replacement.length()) {
                int digit = replacement.getUnsignedByte(index + 1) - '0';
                if (digit < 0 || digit > 9) {
                    break;
                }
                int candidate = group * 10 + digit;
                if (candidate > pattern.capturingGroupCount()) {
                    break;
                }
                if (candidate > 0) {
                    return true;
                }
                group = candidate;
                index++;
            }
        }
        return false;
    }

    public Slice replace(Slice source, Function<List<Slice>, Slice> replacement)
    {
        requireNonNull(source, "source is null");
        requireNonNull(replacement, "replacement is null");

        SingleByteRepeatMatcher.Cursor repeatCursor = newSingleByteRepeatCursor(source, true);
        if (repeatCursor != null) {
            DynamicSliceOutput output = new DynamicSliceOutput(source.length());
            int previousEnd = 0;
            List<Slice> groups = List.of();
            while (repeatCursor.find()) {
                output.writeBytes(source, previousEnd, repeatCursor.start() - previousEnd);
                Slice replacementValue = replacement.apply(groups);
                if (replacementValue == null) {
                    return null;
                }
                output.appendBytes(replacementValue);
                previousEnd = repeatCursor.end();
            }
            output.writeBytes(source, previousEnd, source.length() - previousEnd);
            return output.slice();
        }

        Re2Matcher matcher = newMatcher(source);
        DynamicSliceOutput output = new DynamicSliceOutput(source.length());
        int previousEnd = 0;
        boolean matched = false;
        while (matcher.find()) {
            matched = true;
            output.writeBytes(source, previousEnd, matcher.start() - previousEnd);
            List<Slice> groups = new ArrayList<>(matcher.groupCount());
            for (int group = 1; group <= matcher.groupCount(); group++) {
                groups.add(matcher.group(group));
            }
            Slice replacementValue = replacement.apply(Collections.unmodifiableList(groups));
            if (replacementValue == null) {
                return null;
            }
            output.appendBytes(replacementValue);
            previousEnd = matcher.end();
        }
        if (!matched) {
            return source;
        }
        output.writeBytes(source, previousEnd, source.length() - previousEnd);
        return output.slice();
    }

    private void appendReplacement(DynamicSliceOutput output, Slice replacement, Re2Matcher matcher)
    {
        appendReplacement(output, replacement, matcher, null, 0, 0);
    }

    private void appendReplacement(DynamicSliceOutput output, Slice replacement, Slice source, int matchStart, int matchEnd)
    {
        appendReplacement(output, replacement, null, source, matchStart, matchEnd);
    }

    private void appendReplacement(DynamicSliceOutput output, Slice replacement, Re2Matcher matcher, Slice source, int matchStart, int matchEnd)
    {
        for (int index = 0; index < replacement.length(); index++) {
            int current = replacement.getUnsignedByte(index);
            if (current == '\\') {
                if (++index == replacement.length()) {
                    throw new IllegalArgumentException("backslash cannot be last in replacement");
                }
                output.writeByte(replacement.getUnsignedByte(index));
                continue;
            }
            if (current != '$') {
                output.writeByte(current);
                continue;
            }

            if (++index == replacement.length()) {
                throw new IllegalArgumentException("dollar sign cannot be last in replacement");
            }
            int next = replacement.getUnsignedByte(index);
            if (next == '{') {
                int nameStart = ++index;
                while (index < replacement.length() && replacement.getUnsignedByte(index) != '}') {
                    index++;
                }
                if (index == replacement.length() || index == nameStart) {
                    throw new IllegalArgumentException("invalid named group in replacement");
                }
                String groupName = replacement.slice(nameStart, index - nameStart).toStringUtf8();
                if (matcher == null) {
                    throw new IllegalArgumentException("unknown named group: " + groupName);
                }
                Slice group = matcher.group(groupName);
                if (group != null) {
                    output.appendBytes(group);
                }
                continue;
            }

            if (next < '0' || next > '9') {
                throw new IllegalArgumentException("dollar sign must be followed by a digit or group name");
            }
            int group = next - '0';
            int groupCount = matcher == null ? 0 : matcher.groupCount();
            if (group > groupCount) {
                throw new IllegalArgumentException("unknown group: " + group);
            }
            while (index + 1 < replacement.length()) {
                int digit = replacement.getUnsignedByte(index + 1) - '0';
                if (digit < 0 || digit > 9) {
                    break;
                }
                int candidate = group * 10 + digit;
                if (candidate > groupCount) {
                    break;
                }
                group = candidate;
                index++;
            }
            Slice groupValue = matcher == null ? source.slice(matchStart, matchEnd - matchStart) : matcher.group(group);
            if (groupValue != null) {
                output.appendBytes(groupValue);
            }
        }
    }

    private void validateGroup(int group)
    {
        if (group < 0) {
            throw new IllegalArgumentException("group cannot be negative");
        }
        if (group > pattern.capturingGroupCount()) {
            throw new IllegalArgumentException("pattern has " + pattern.capturingGroupCount() +
                    " groups; cannot access group " + group);
        }
    }

    private Re2Matcher newMatcher(Slice source)
    {
        if (pattern.capturingGroupCount() != 0) {
            return pattern.matcher(source);
        }
        SingleByteMatcher byteMatcher = singleByteMatcher();
        return pattern.matcher(source, byteMatcher == SingleByteMatcher.unsupported() ? null : byteMatcher);
    }

    private Re2Matcher newGroupZeroMatcher(Slice source)
    {
        SingleByteMatcher byteMatcher = singleByteMatcher();
        return pattern.groupZeroMatcher(source, byteMatcher == SingleByteMatcher.unsupported() ? null : byteMatcher);
    }

    private SingleByteRepeatMatcher.Cursor newSingleByteRepeatCursor(Slice source, boolean capturingGroupsRequired)
    {
        if (!mayHaveSingleByteRepeatMatcher || (capturingGroupsRequired && pattern.capturingGroupCount() != 0)) {
            return null;
        }
        SingleByteRepeatMatcher repeatMatcher = singleByteRepeatMatcher();
        return repeatMatcher == SingleByteRepeatMatcher.unsupported() ? null : repeatMatcher.matcher(source);
    }

    private SingleByteMatcher singleByteMatcher()
    {
        if (!mayHaveSingleByteMatcher) {
            return SingleByteMatcher.unsupported();
        }
        return loadSingleByteMatcher();
    }

    final SingleByteMatcher loadSingleByteMatcher()
    {
        SingleByteMatcher byteMatcher = singleByteMatcher;
        if (byteMatcher == null) {
            byteMatcher = pattern.createSingleByteMatcher();
            if (byteMatcher == null) {
                byteMatcher = SingleByteMatcher.unsupported();
            }
            singleByteMatcher = byteMatcher;
        }
        return byteMatcher;
    }

    private SingleByteRepeatMatcher singleByteRepeatMatcher()
    {
        if (!mayHaveSingleByteRepeatMatcher) {
            return SingleByteRepeatMatcher.unsupported();
        }
        SingleByteRepeatMatcher repeatMatcher = singleByteRepeatMatcher;
        if (repeatMatcher == null) {
            repeatMatcher = pattern.createSingleByteRepeatMatcher();
            if (repeatMatcher == null) {
                repeatMatcher = SingleByteRepeatMatcher.unsupported();
            }
            singleByteRepeatMatcher = repeatMatcher;
        }
        return repeatMatcher;
    }

    private static void validateNoSyntaxCollision(Slice pattern)
    {
        boolean inCharacterClass = false;
        for (int index = 0; index < pattern.length(); index++) {
            int current = pattern.getUnsignedByte(index);
            if (current == '\\') {
                index++;
                continue;
            }
            if (current == '[') {
                inCharacterClass = true;
                continue;
            }
            if (current == ']' && inCharacterClass) {
                inCharacterClass = false;
                continue;
            }
            if (inCharacterClass && current == '&' && index + 1 < pattern.length() && pattern.getUnsignedByte(index + 1) == '&') {
                throw syntaxCollision("Java character-class intersection");
            }
            if (!inCharacterClass && current == '(' && index + 2 < pattern.length() && pattern.getUnsignedByte(index + 1) == '?') {
                for (int flagIndex = index + 2; flagIndex < pattern.length(); flagIndex++) {
                    int flag = pattern.getUnsignedByte(flagIndex);
                    if (flag == 'U') {
                        throw syntaxCollision("Java Unicode-character-class mode");
                    }
                    if (flag == ')' || flag == ':') {
                        break;
                    }
                    if (flag != 'i' && flag != 'm' && flag != 's' && flag != '-') {
                        break;
                    }
                }
            }
        }
    }

    private static IllegalArgumentException syntaxCollision(String syntax)
    {
        return new IllegalArgumentException(syntax + " conflicts with RE2 syntax");
    }
}
