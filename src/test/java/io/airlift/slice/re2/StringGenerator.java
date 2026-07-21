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

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public final class StringGenerator
{
    private final int maxLength;
    private final List<Slice> alphabet;

    private final List<Integer> digits = new ArrayList<>();
    private boolean generateNull;
    private boolean exhausted;

    private boolean randomEnabled;
    private final Re2Random random = new Re2Random(0);
    private int randomRemaining;

    public StringGenerator(int maxLength, List<Slice> alphabet)
    {
        if (maxLength < 0) {
            throw new IllegalArgumentException("maxLength must be >= 0");
        }
        this.maxLength = maxLength;
        this.alphabet = List.copyOf(requireNonNull(alphabet, "alphabet is null"));
        reset();
    }

    public void reset()
    {
        digits.clear();
        generateNull = false;
        exhausted = false;
        randomEnabled = false;
        randomRemaining = 0;
    }

    public void generateNull()
    {
        generateNull = true;
    }

    public void random(int seed, int count)
    {
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        random.reset(seed);
        randomEnabled = true;
        randomRemaining = count;
    }

    public boolean hasNext()
    {
        if (generateNull) {
            return true;
        }
        if (randomEnabled) {
            return randomRemaining > 0;
        }
        return !exhausted;
    }

    public Slice next()
    {
        if (generateNull) {
            generateNull = false;
            return null;
        }

        if (randomEnabled) {
            if (randomRemaining <= 0) {
                throw new IllegalStateException("no next element");
            }
            randomRemaining--;
            return nextRandom();
        }

        Slice value = nextEnumerated();
        incrementDigits();
        return value;
    }

    private Slice nextRandom()
    {
        if (alphabet.isEmpty()) {
            return Slices.wrappedBuffer(new byte[0]);
        }

        // Length is uniform in [0..maxLength], matching RE2's intent.
        int length = (maxLength == 0) ? 0 : random.uniform(maxLength + 1);
        List<Slice> parts = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            parts.add(alphabet.get(random.uniform(alphabet.size())));
        }
        return Slices.wrappedBuffer(ByteArrays.concatAll(parts));
    }

    private Slice nextEnumerated()
    {
        if (exhausted) {
            throw new IllegalStateException("no next element");
        }

        if (alphabet.isEmpty()) {
            return Slices.wrappedBuffer(new byte[0]);
        }

        if (digits.isEmpty()) {
            // The first output is the empty string.
            return Slices.wrappedBuffer(new byte[0]);
        }

        List<Slice> parts = new ArrayList<>(digits.size());
        for (int digit : digits) {
            parts.add(alphabet.get(digit));
        }
        return Slices.wrappedBuffer(ByteArrays.concatAll(parts));
    }

    private void incrementDigits()
    {
        if (alphabet.isEmpty()) {
            // Only the empty string is representable.
            digits.clear();
            exhausted = true;
            return;
        }

        for (int i = digits.size() - 1; i >= 0; i--) {
            int nextDigit = digits.get(i) + 1;
            if (nextDigit < alphabet.size()) {
                digits.set(i, nextDigit);
                return;
            }
            digits.set(i, 0);
        }

        if (digits.size() < maxLength) {
            digits.add(0);
        }
        else {
            // No more strings.
            digits.clear();
            exhausted = true;
        }
    }

    public static List<Slice> explodeUtf8(Slice utf8)
    {
        requireNonNull(utf8, "utf8 is null");
        List<Slice> result = new ArrayList<>();
        int index = 0;
        byte[] bytes = utf8.byteArray();
        int start = utf8.byteArrayOffset();
        int end = start + utf8.length();

        while (start + index < end) {
            long decoded = Utf8.decode(bytes, start + index, end);
            int width = Utf8.decodedWidth(decoded);
            if (width == 0) {
                break;
            }
            checkFromIndexSize(start + index, width, bytes.length);
            result.add(Slices.wrappedBuffer(bytes, start + index, width));
            index += width;
        }
        return result;
    }
}
