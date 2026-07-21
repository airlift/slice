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

import static io.airlift.slice.re2.Regexp.LATIN1;
import static io.airlift.slice.re2.Regexp.NON_GREEDY;
import static io.airlift.slice.re2.RegexpOp.CAPTURE;
import static io.airlift.slice.re2.RegexpOp.CHAR_CLASS;
import static io.airlift.slice.re2.RegexpOp.REPEAT;

final class BoundedCharacterClassCounter
{
    private static final int CODE_POINT_WORD_COUNT = (CharClass.RUNEMAX + Long.SIZE) / Long.SIZE;
    private static final BoundedCharacterClassCounter UNSUPPORTED = new BoundedCharacterClassCounter();

    private final long[] matchingCodePoints;
    private final int minimum;
    private final int maximum;
    private final boolean latin1;

    private BoundedCharacterClassCounter()
    {
        matchingCodePoints = null;
        minimum = 0;
        maximum = 0;
        latin1 = false;
    }

    private BoundedCharacterClassCounter(RuneRange[] ranges, int minimum, int maximum, boolean latin1)
    {
        matchingCodePoints = new long[latin1 ? 4 : CODE_POINT_WORD_COUNT];
        for (RuneRange range : ranges) {
            int low = Math.max(0, range.low());
            int high = Math.min(latin1 ? 0xFF : CharClass.RUNEMAX, range.high());
            for (int codePoint = low; codePoint <= high; codePoint++) {
                matchingCodePoints[codePoint >>> 6] |= 1L << codePoint;
            }
        }
        this.minimum = minimum;
        this.maximum = maximum;
        this.latin1 = latin1;
    }

    static BoundedCharacterClassCounter analyze(Regexp regexp)
    {
        while (regexp.op() == CAPTURE) {
            regexp = regexp.sub(0);
        }
        if (regexp.op() != REPEAT ||
                regexp.min() <= 0 ||
                regexp.max() < regexp.min() ||
                (regexp.parseFlags() & NON_GREEDY) != 0) {
            return null;
        }

        Regexp atom = regexp.sub(0);
        while (atom.op() == CAPTURE) {
            atom = atom.sub(0);
        }
        if (atom.op() != CHAR_CLASS || atom.charClass().isEmpty()) {
            return null;
        }
        return new BoundedCharacterClassCounter(
                atom.charClass().ranges(),
                regexp.min(),
                regexp.max(),
                (regexp.parseFlags() & LATIN1) != 0);
    }

    static BoundedCharacterClassCounter unsupported()
    {
        return UNSUPPORTED;
    }

    // PERFORMANCE-SENSITIVE HOT LOOP: decoding and bitset membership replace one complete matcher
    // invocation per result. Changes require focused Intel and Graviton qualification.
    long count(Slice input)
    {
        byte[] bytes = input.byteArray();
        int position = input.byteArrayOffset();
        int end = position + input.length();
        int runLength = 0;
        long count = 0;

        while (position < end) {
            int codePoint;
            int width;
            if (latin1) {
                codePoint = bytes[position] & 0xFF;
                width = 1;
            }
            else {
                long decoded = Utf8.decode(bytes, position, end);
                codePoint = Utf8.decodedCodePoint(decoded);
                width = Utf8.decodedWidth(decoded);
                if (width == 1 && codePoint == Utf8.RUNE_ERROR && (bytes[position] & 0xFF) >= 0x80) {
                    codePoint = -1;
                }
            }

            if (matches(codePoint)) {
                runLength++;
            }
            else {
                count += countRun(runLength);
                runLength = 0;
            }
            position += width;
        }
        return count + countRun(runLength);
    }

    private long countRun(int runLength)
    {
        int completeMatches = runLength / maximum;
        int remainder = runLength - (completeMatches * maximum);
        return completeMatches + (remainder >= minimum ? 1 : 0);
    }

    private boolean matches(int codePoint)
    {
        return codePoint >= 0 &&
                codePoint < matchingCodePoints.length * Long.SIZE &&
                (matchingCodePoints[codePoint >>> 6] & (1L << codePoint)) != 0;
    }
}
