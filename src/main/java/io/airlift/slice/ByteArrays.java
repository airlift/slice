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
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static java.util.Objects.checkFromIndexSize;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class ByteArrays
{
    private ByteArrays() {}

    public static short getShort(byte[] bytes, int index)
    {
        checkFromIndexSize(index, SIZE_OF_SHORT, bytes.length);
        return unsafe.getShort(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index);
    }

    public static int getInt(byte[] bytes, int index)
    {
        checkFromIndexSize(index, SIZE_OF_INT, bytes.length);
        return unsafe.getInt(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index);
    }

    public static long getLong(byte[] bytes, int index)
    {
        checkFromIndexSize(index, SIZE_OF_LONG, bytes.length);
        return unsafe.getLong(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index);
    }

    public static float getFloat(byte[] bytes, int index)
    {
        checkFromIndexSize(index, SIZE_OF_FLOAT, bytes.length);
        return unsafe.getFloat(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index);
    }

    public static double getDouble(byte[] bytes, int index)
    {
        checkFromIndexSize(index, SIZE_OF_DOUBLE, bytes.length);
        return unsafe.getDouble(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index);
    }

    public static void setShort(byte[] bytes, int index, short value)
    {
        checkFromIndexSize(index, SIZE_OF_SHORT, bytes.length);
        unsafe.putShort(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index, value);
    }

    public static void setInt(byte[] bytes, int index, int value)
    {
        checkFromIndexSize(index, SIZE_OF_INT, bytes.length);
        unsafe.putInt(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index, value);
    }

    public static void setLong(byte[] bytes, int index, long value)
    {
        checkFromIndexSize(index, SIZE_OF_LONG, bytes.length);
        unsafe.putLong(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index, value);
    }

    public static void setFloat(byte[] bytes, int index, float value)
    {
        checkFromIndexSize(index, SIZE_OF_FLOAT, bytes.length);
        unsafe.putFloat(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index, value);
    }

    public static void setDouble(byte[] bytes, int index, double value)
    {
        checkFromIndexSize(index, SIZE_OF_DOUBLE, bytes.length);
        unsafe.putDouble(bytes, ((long) ARRAY_BYTE_BASE_OFFSET) + index, value);
    }
}
