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
record RuneRange(int low, int high)
        implements Comparable<RuneRange>
{
    public RuneRange
    {
        if (low > high) {
            throw new IllegalArgumentException("low > high");
        }
    }

    @Override
    public int compareTo(RuneRange other)
    {
        int byLow = Integer.compare(low, other.low);
        if (byLow != 0) {
            return byLow;
        }
        return Integer.compare(high, other.high);
    }

    public boolean contains(int rune)
    {
        return rune >= low && rune <= high;
    }
}
