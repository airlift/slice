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
package io.airlift.slice;

public final class UnsafeSlice
{
    private UnsafeSlice() {}

    public static byte getByteUnchecked(Slice slice, int index)
    {
        return slice.getByteUnchecked(index);
    }

    public static short getShortUnchecked(Slice slice, int index)
    {
        return slice.getShortUnchecked(index);
    }

    public static int getIntUnchecked(Slice slice, int index)
    {
        return slice.getIntUnchecked(index);
    }

    public static long getLongUnchecked(Slice slice, int index)
    {
        return slice.getLongUnchecked(index);
    }
}
