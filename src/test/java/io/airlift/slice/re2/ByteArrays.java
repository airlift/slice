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

import java.util.List;

import static java.util.Objects.requireNonNull;

final class ByteArrays
{
    private ByteArrays() {}

    static byte[] concat(Slice first, Slice second)
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");

        byte[] out = new byte[first.length() + second.length()];
        System.arraycopy(first.byteArray(), first.byteArrayOffset(), out, 0, first.length());
        System.arraycopy(second.byteArray(), second.byteArrayOffset(), out, first.length(), second.length());
        return out;
    }

    static byte[] concat(Slice first, Slice second, Slice third)
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");
        requireNonNull(third, "third is null");

        byte[] out = new byte[first.length() + second.length() + third.length()];
        int position = 0;
        System.arraycopy(first.byteArray(), first.byteArrayOffset(), out, position, first.length());
        position += first.length();
        System.arraycopy(second.byteArray(), second.byteArrayOffset(), out, position, second.length());
        position += second.length();
        System.arraycopy(third.byteArray(), third.byteArrayOffset(), out, position, third.length());
        return out;
    }

    static byte[] concat(Slice prefix, byte[] middle, Slice suffix)
    {
        requireNonNull(prefix, "prefix is null");
        requireNonNull(middle, "middle is null");
        requireNonNull(suffix, "suffix is null");

        byte[] out = new byte[prefix.length() + middle.length + suffix.length()];
        int position = 0;
        System.arraycopy(prefix.byteArray(), prefix.byteArrayOffset(), out, position, prefix.length());
        position += prefix.length();
        System.arraycopy(middle, 0, out, position, middle.length);
        position += middle.length;
        System.arraycopy(suffix.byteArray(), suffix.byteArrayOffset(), out, position, suffix.length());
        return out;
    }

    static byte[] concatAll(List<Slice> parts)
    {
        requireNonNull(parts, "parts is null");

        int size = 0;
        for (Slice part : parts) {
            requireNonNull(part, "part is null");
            size += part.length();
        }

        byte[] out = new byte[size];
        int position = 0;
        for (Slice part : parts) {
            System.arraycopy(part.byteArray(), part.byteArrayOffset(), out, position, part.length());
            position += part.length();
        }
        return out;
    }
}
