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

import java.lang.foreign.ValueLayout;

import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class MemoryLayout
{
    private MemoryLayout() {}

    public static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE.withOrder(LITTLE_ENDIAN);
    public static final ValueLayout.OfShort SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    public static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    public static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(LITTLE_ENDIAN);
    public static final ValueLayout.OfFloat FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    public static final ValueLayout.OfDouble DOUBLE = ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(LITTLE_ENDIAN);

    public static final int SIZE_OF_BYTE = toIntExact(BYTE.byteSize());
    public static final int SIZE_OF_SHORT = toIntExact(SHORT.byteSize());
    public static final int SIZE_OF_INT = toIntExact(INT.byteSize());
    public static final int SIZE_OF_LONG = toIntExact(LONG.byteSize());
    public static final int SIZE_OF_FLOAT = toIntExact(FLOAT.byteSize());
    public static final int SIZE_OF_DOUBLE = toIntExact(DOUBLE.byteSize());
}
