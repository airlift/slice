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
import org.safere.Pattern;
import org.safere.Utf8Input;
import org.safere.Utf8Matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

final class TestingSafeReTrinoRegexp
{
    private final Pattern pattern;

    private TestingSafeReTrinoRegexp(Pattern pattern)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
    }

    public static TestingSafeReTrinoRegexp compile(Slice pattern)
    {
        requireNonNull(pattern, "pattern is null");
        return new TestingSafeReTrinoRegexp(Pattern.compile(pattern.toStringUtf8()));
    }

    public boolean contains(Slice source)
    {
        return pattern.find(input(source));
    }

    public long count(Slice source)
    {
        Utf8Matcher matcher = matcher(source);
        long count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    public long position(Slice source, long occurrence)
    {
        if (occurrence < 1) {
            throw new IllegalArgumentException("occurrence cannot be smaller than 1");
        }

        Utf8Matcher matcher = matcher(source);
        for (long match = 0; match < occurrence; match++) {
            if (!matcher.find()) {
                return -1;
            }
        }
        return SliceUtf8.countCodePoints(source, 0, matcher.start()) + 1L;
    }

    public Slice extract(Slice source)
    {
        Utf8Matcher matcher = matcher(source);
        if (!matcher.find()) {
            return null;
        }
        return source.slice(matcher.start(), matcher.end() - matcher.start());
    }

    public List<Slice> extractAll(Slice source)
    {
        Utf8Matcher matcher = matcher(source);
        List<Slice> matches = new ArrayList<>();
        while (matcher.find()) {
            matches.add(source.slice(matcher.start(), matcher.end() - matcher.start()));
        }
        return Collections.unmodifiableList(matches);
    }

    public List<Slice> split(Slice source)
    {
        Utf8Matcher matcher = matcher(source);
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
        requireNonNull(replacement, "replacement is null");
        Utf8Matcher matcher = matcher(source);
        if (!matcher.find()) {
            return source;
        }

        DynamicSliceOutput output = new DynamicSliceOutput(source.length() + replacement.length());
        Utf8Input replacementInput = input(replacement);
        do {
            matcher.appendReplacement(output::writeBytes, replacementInput);
        }
        while (matcher.find());
        matcher.appendTail(output::writeBytes);
        return output.slice();
    }

    public Slice replace(Slice source, Function<List<Slice>, Slice> replacement)
    {
        requireNonNull(replacement, "replacement is null");
        Utf8Matcher matcher = matcher(source);
        if (!matcher.find()) {
            return source;
        }

        DynamicSliceOutput output = new DynamicSliceOutput(source.length());
        int previousEnd = 0;
        do {
            output.writeBytes(source, previousEnd, matcher.start() - previousEnd);
            List<Slice> groups = new ArrayList<>(matcher.groupCount());
            for (int group = 1; group <= matcher.groupCount(); group++) {
                int groupStart = matcher.start(group);
                groups.add(groupStart < 0 ? null : source.slice(groupStart, matcher.end(group) - groupStart));
            }
            Slice replacementValue = replacement.apply(Collections.unmodifiableList(groups));
            if (replacementValue == null) {
                return null;
            }
            output.appendBytes(replacementValue);
            previousEnd = matcher.end();
        }
        while (matcher.find());
        output.writeBytes(source, previousEnd, source.length() - previousEnd);
        return output.slice();
    }

    private Utf8Matcher matcher(Slice source)
    {
        return pattern.matcher(input(source));
    }

    private static Utf8Input input(Slice value)
    {
        requireNonNull(value, "value is null");
        return Utf8Input.trusted(value.byteArray(), value.byteArrayOffset(), value.length());
    }
}
