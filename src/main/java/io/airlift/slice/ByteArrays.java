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

import static io.airlift.slice.JvmUtils.unsafe;
import static io.airlift.slice.Preconditions.checkPositionIndexes;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class ByteArrays
{
    private ByteArrays() {}

    public static short getShort(byte[] bytes, int index)
    {
        checkIndexLength(bytes.length, index, SIZE_OF_SHORT);
        return unsafe.getShort(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index);
    }

    public static int getInt(byte[] bytes, int index)
    {
        checkIndexLength(bytes.length, index, SIZE_OF_INT);
        return unsafe.getInt(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index);
    }

    public static long getLong(byte[] bytes, int index)
    {
        checkIndexLength(bytes.length, index, SIZE_OF_LONG);
        return unsafe.getLong(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index);
    }

    public static float getFloat(byte[] bytes, int index)
    {
        checkIndexLength(bytes.length, index, SIZE_OF_FLOAT);
        return unsafe.getFloat(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index);
    }

    public static double getDouble(byte[] bytes, int index)
    {
        checkIndexLength(bytes.length, index, SIZE_OF_DOUBLE);
        return unsafe.getDouble(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index);
    }

    public static void setShort(byte[] bytes, int index, short value)
    {
        checkIndexLength(bytes.length, index, SIZE_OF_SHORT);
        unsafe.putShort(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index, value);
    }

    public static void setInt(byte[] bytes, int index, int value)
    {
        checkIndexLength(bytes.length, index, SIZE_OF_INT);
        unsafe.putInt(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index, value);
    }

    public static void setLong(byte[] bytes, int index, long value)
    {
        checkIndexLength(bytes.length, index, SIZE_OF_LONG);
        unsafe.putLong(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index, value);
    }

    public static void setFloat(byte[] bytes, int index, float value)
    {
        checkIndexLength(bytes.length, index, SIZE_OF_FLOAT);
        unsafe.putFloat(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index, value);
    }

    public static void setDouble(byte[] bytes, int index, double value)
    {
        checkIndexLength(bytes.length, index, SIZE_OF_DOUBLE);
        unsafe.putDouble(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index, value);
    }

    private static void checkIndexLength(int arrayLength, int index, int typeLength)
    {
        checkPositionIndexes(index, index + typeLength, arrayLength);
    }
}
