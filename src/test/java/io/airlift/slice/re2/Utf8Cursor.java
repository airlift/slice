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

import static java.util.Objects.requireNonNull;

/**
 * A mutable cursor over a {@link Slice} for UTF-8 decoding.
 */
public final class Utf8Cursor
{
    private final byte[] bytes;
    private final int start;
    private final int end;

    // Absolute index into {@code bytes}.
    private int position;

    public Utf8Cursor(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        this.bytes = slice.byteArray();
        this.start = slice.byteArrayOffset();
        this.end = start + slice.length();
        this.position = start;
    }

    public int position()
    {
        return position - start;
    }

    public void setPosition(int position)
    {
        // Keep bounds checks simple and predictable for now.
        if (position < 0 || position > (end - start)) {
            throw new IndexOutOfBoundsException("position out of bounds: " + position);
        }
        this.position = start + position;
    }

    public int remaining()
    {
        return end - position;
    }

    public boolean hasRemaining()
    {
        return position < end;
    }

    public int peekByteUnsigned()
    {
        if (position >= end) {
            return -1;
        }
        return bytes[position] & 0xFF;
    }

    public int readCodePoint()
    {
        long decoded = Utf8.decode(bytes, position, end);
        int width = Utf8.decodedWidth(decoded);
        if (width == 0) {
            return -1;
        }
        position += width;
        return Utf8.decodedCodePoint(decoded);
    }
}
