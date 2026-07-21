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

import static io.airlift.slice.re2.Regexp.LATIN1;
import static io.airlift.slice.re2.Regexp.NON_GREEDY;
import static io.airlift.slice.re2.RegexpOp.CAPTURE;
import static io.airlift.slice.re2.RegexpOp.REPEAT;
import static io.airlift.slice.re2.RegexpOp.STAR;
import static java.util.Objects.requireNonNull;

final class SingleByteRepeatMatcher
{
    private static final SingleByteRepeatMatcher UNSUPPORTED = new SingleByteRepeatMatcher();

    private final byte[] matches;
    private final boolean latin1;

    private SingleByteRepeatMatcher()
    {
        matches = null;
        latin1 = false;
    }

    private SingleByteRepeatMatcher(SingleByteMatcher.Analysis analysis, boolean latin1)
    {
        matches = new byte[256];
        for (int value = 0; value < matches.length; value++) {
            if (analysis.matches(value)) {
                matches[value] = 1;
            }
        }
        this.latin1 = latin1;
    }

    static SingleByteRepeatMatcher analyze(Regexp regexp)
    {
        while (regexp.op() == CAPTURE) {
            regexp = regexp.sub(0);
        }
        boolean unboundedZeroOrMore = regexp.op() == STAR ||
                (regexp.op() == REPEAT && regexp.min() == 0 && regexp.max() == -1);
        if (!unboundedZeroOrMore || (regexp.parseFlags() & NON_GREEDY) != 0) {
            return null;
        }

        SingleByteMatcher.Analysis analysis = SingleByteMatcher.analyzePattern(regexp.sub(0));
        if (analysis.length() != 1 || analysis.isEmpty()) {
            return null;
        }
        return new SingleByteRepeatMatcher(analysis, (regexp.parseFlags() & LATIN1) != 0);
    }

    static SingleByteRepeatMatcher unsupported()
    {
        return UNSUPPORTED;
    }

    // PERFORMANCE-SENSITIVE HOT LOOPS: byte-table access, UTF-8 advancement, and branch
    // placement have measured effects. Do not apply readability-only changes without direct
    // path tests and focused Intel and Graviton benchmarks.

    long count(Slice input)
    {
        byte[] bytes = input.byteArray();
        int inputOffset = input.byteArrayOffset();
        int inputLength = input.length();
        int position = 0;
        // Every maximal matching run is one greedy match. Each unmatched code point is an
        // empty match, and the final input boundary contributes one more empty match.
        long count = 1;
        boolean insideMatch = false;

        while (position < inputLength) {
            if (matches[bytes[inputOffset + position] & 0xFF] != 0) {
                if (!insideMatch) {
                    count++;
                    insideMatch = true;
                }
                position++;
                continue;
            }

            count++;
            insideMatch = false;
            if (latin1) {
                position++;
            }
            else {
                position += SliceUtf8.lengthOfCodePointSafe(bytes, inputOffset, inputLength, position);
            }
        }
        return count;
    }

    Cursor matcher(Slice input)
    {
        return new Cursor(input);
    }

    final class Cursor
    {
        private final Slice input;
        private int nextFindStart;
        private int matchStart;
        private int matchEnd;

        private Cursor(Slice input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        // This is part of the performance-sensitive repeat-scanning loop described above.
        boolean find()
        {
            if (nextFindStart > input.length()) {
                return false;
            }

            byte[] bytes = input.byteArray();
            int inputOffset = input.byteArrayOffset();
            int inputEnd = inputOffset + input.length();
            matchStart = nextFindStart;
            int position = inputOffset + matchStart;
            while (position < inputEnd && matches[bytes[position] & 0xFF] != 0) {
                position++;
            }
            matchEnd = position - inputOffset;

            if (matchStart != matchEnd) {
                nextFindStart = matchEnd;
            }
            else if (matchEnd == input.length()) {
                nextFindStart = input.length() + 1;
            }
            else if (latin1) {
                nextFindStart = matchEnd + 1;
            }
            else {
                nextFindStart = matchEnd + SliceUtf8.lengthOfCodePointSafe(
                        bytes,
                        inputOffset,
                        input.length(),
                        matchEnd);
            }
            return true;
        }

        boolean find(int start)
        {
            if (start < 0 || start > input.length()) {
                throw new IndexOutOfBoundsException("start out of bounds: " + start);
            }
            nextFindStart = start;
            return find();
        }

        int start()
        {
            return matchStart;
        }

        int end()
        {
            return matchEnd;
        }

        Slice group()
        {
            return input.slice(matchStart, matchEnd - matchStart);
        }
    }
}
