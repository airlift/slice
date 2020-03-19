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

import org.openjdk.jol.info.ClassLayout;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.ToLongFunction;

import static sun.misc.Unsafe.ARRAY_BOOLEAN_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BOOLEAN_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_CHAR_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_DOUBLE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_DOUBLE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_FLOAT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_FLOAT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_OBJECT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_OBJECT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;

public final class SizeOf
{
    public static final byte SIZE_OF_BYTE = 1;
    public static final byte SIZE_OF_SHORT = 2;
    public static final byte SIZE_OF_INT = 4;
    public static final byte SIZE_OF_LONG = 8;
    public static final byte SIZE_OF_FLOAT = 4;
    public static final byte SIZE_OF_DOUBLE = 8;

    private static final int SIMPLE_ENTRY_INSTANCE_SIZE = ClassLayout.parseClass(AbstractMap.SimpleEntry.class).instanceSize();
    private static final int STRING_INSTANCE_SIZE = ClassLayout.parseClass(String.class).instanceSize();

    public static long sizeOf(boolean[] array)
    {
        return (array == null) ? 0 : sizeOfBooleanArray(array.length);
    }

    public static long sizeOf(byte[] array)
    {
        return (array == null) ? 0 : sizeOfByteArray(array.length);
    }

    public static long sizeOf(short[] array)
    {
        return (array == null) ? 0 : sizeOfShortArray(array.length);
    }

    public static long sizeOf(char[] array)
    {
        return (array == null) ? 0 : sizeOfCharArray(array.length);
    }

    public static long sizeOf(int[] array)
    {
        return (array == null) ? 0 : sizeOfIntArray(array.length);
    }

    public static long sizeOf(long[] array)
    {
        return (array == null) ? 0 : sizeOfLongArray(array.length);
    }

    public static long sizeOf(float[] array)
    {
        return (array == null) ? 0 : sizeOfFloatArray(array.length);
    }

    public static long sizeOf(double[] array)
    {
        return (array == null) ? 0 : sizeOfDoubleArray(array.length);
    }

    public static long sizeOf(Object[] array)
    {
        return (array == null) ? 0 : sizeOfObjectArray(array.length);
    }

    public static long estimatedSizeOf(String string)
    {
        return (string == null) ? 0 : (STRING_INSTANCE_SIZE + string.length() * Character.BYTES);
    }

    public static <T> long estimatedSizeOf(List<T> list, ToLongFunction<T> valueSize)
    {
        if (list == null) {
            return 0;
        }

        long result = sizeOfObjectArray(list.size());
        for (T value : list) {
            result += valueSize.applyAsLong(value);
        }
        return result;
    }

    public static <K, V> long estimatedSizeOf(Map<K, V> map, ToLongFunction<K> keySize, ToLongFunction<V> valueSize)
    {
        if (map == null) {
            return 0;
        }

        long result = sizeOfObjectArray(map.size());
        for (Entry<K, V> entry : map.entrySet()) {
            result += SIMPLE_ENTRY_INSTANCE_SIZE +
                    keySize.applyAsLong(entry.getKey()) +
                    valueSize.applyAsLong(entry.getValue());
        }
        return result;
    }

    public static long sizeOfBooleanArray(int length)
    {
        return ARRAY_BOOLEAN_BASE_OFFSET + (((long) ARRAY_BOOLEAN_INDEX_SCALE) * length);
    }

    public static long sizeOfByteArray(int length)
    {
        return ARRAY_BYTE_BASE_OFFSET + (((long) ARRAY_BYTE_INDEX_SCALE) * length);
    }

    public static long sizeOfShortArray(int length)
    {
        return ARRAY_SHORT_BASE_OFFSET + (((long) ARRAY_SHORT_INDEX_SCALE) * length);
    }

    public static long sizeOfCharArray(int length)
    {
        return ARRAY_CHAR_BASE_OFFSET + (((long) ARRAY_CHAR_INDEX_SCALE) * length);
    }

    public static long sizeOfIntArray(int length)
    {
        return ARRAY_INT_BASE_OFFSET + (((long) ARRAY_INT_INDEX_SCALE) * length);
    }

    public static long sizeOfLongArray(int length)
    {
        return ARRAY_LONG_BASE_OFFSET + (((long) ARRAY_LONG_INDEX_SCALE) * length);
    }

    public static long sizeOfFloatArray(int length)
    {
        return ARRAY_FLOAT_BASE_OFFSET + (((long) ARRAY_FLOAT_INDEX_SCALE) * length);
    }

    public static long sizeOfDoubleArray(int length)
    {
        return ARRAY_DOUBLE_BASE_OFFSET + (((long) ARRAY_DOUBLE_INDEX_SCALE) * length);
    }

    public static long sizeOfObjectArray(int length)
    {
        return ARRAY_OBJECT_BASE_OFFSET + (((long) ARRAY_OBJECT_INDEX_SCALE) * length);
    }

    private SizeOf()
    {
    }
}
