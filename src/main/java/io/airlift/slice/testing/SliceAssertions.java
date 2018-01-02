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
package io.airlift.slice.testing;

import io.airlift.slice.Slice;

public final class SliceAssertions
{
    private SliceAssertions() {}

    public static void assertSlicesEqual(Slice actual, Slice expected)
    {
        if (actual == null && expected == null) {
            return;
        }
        else if (actual == null) {
            throw new AssertionError("Actual is null");
        }
        else if (expected == null) {
            throw new AssertionError("Expected actual to be null");
        }

        if (actual.length() != expected.length()) {
            throw new AssertionError(String.format("Slices differ in size. Actual: %s, expected: %s", actual.length(), expected.length()));
        }

        for (int i = 0; i < actual.length(); i++) {
            if (actual.getByte(i) != expected.getByte(i)) {
                throw new AssertionError(String.format("Slices differ at index %s. Actual: 0x%02x, expected: 0x%02x", i, actual.getUnsignedByte(i), expected.getUnsignedByte(i)));
            }
        }
    }
}
