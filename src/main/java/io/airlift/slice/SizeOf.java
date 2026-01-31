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

import com.sun.management.HotSpotDiagnosticMXBean;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.function.ToLongFunction;

import static java.lang.Math.toIntExact;

public final class SizeOf
{
    private SizeOf() {}

    public static final byte SIZE_OF_BYTE = 1;
    public static final byte SIZE_OF_SHORT = 2;
    public static final byte SIZE_OF_INT = 4;
    public static final byte SIZE_OF_LONG = 8;
    public static final byte SIZE_OF_FLOAT = 4;
    public static final byte SIZE_OF_DOUBLE = 8;

    private static final int OBJECT_HEADER_SIZE;
    private static final int ARRAY_HEADER_SIZE;
    private static final int REFERENCE_SIZE;
    private static final int OBJECT_ALIGNMENT;

    static {
        HotSpotDiagnosticMXBean hotSpotBean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);

        // Determine reference size based on UseCompressedOops
        // Default is true for heaps < 32GB on 64-bit JVMs
        boolean compressedOops = getBooleanVmOption(hotSpotBean, "UseCompressedOops", true);
        REFERENCE_SIZE = compressedOops ? 4 : 8;

        // Determine object alignment (default is 8 bytes)
        OBJECT_ALIGNMENT = getIntVmOption(hotSpotBean, "ObjectAlignmentInBytes", 8);

        // Determine object header size
        // Check for compact object headers (Project Lilliput, JDK 24+)
        boolean compactHeaders = getBooleanVmOption(hotSpotBean, "UseCompactObjectHeaders", false);
        if (compactHeaders) {
            // Compact headers: 8 bytes (or potentially 4 bytes in future)
            OBJECT_HEADER_SIZE = 8;
        }
        else {
            // Standard headers: mark word (8 bytes) + class pointer (4 or 8 bytes)
            boolean compressedClassPointers = getBooleanVmOption(hotSpotBean, "UseCompressedClassPointers", true);
            OBJECT_HEADER_SIZE = 8 + (compressedClassPointers ? 4 : 8);
        }

        // Array header: object header + 4 bytes for length field, aligned
        // With compressed class pointers: 12 + 4 = 16 bytes
        // Without compressed class pointers: 16 + 4 = 20 bytes, aligned to 24
        // With compact headers: 8 + 4 = 12 bytes, potentially aligned to 16
        int rawArrayHeader = OBJECT_HEADER_SIZE + 4;
        ARRAY_HEADER_SIZE = (int) alignSize(rawArrayHeader);
    }

    private static boolean getBooleanVmOption(HotSpotDiagnosticMXBean bean, String option, boolean defaultValue)
    {
        try {
            return Boolean.parseBoolean(bean.getVMOption(option).getValue());
        }
        catch (IllegalArgumentException e) {
            // Option doesn't exist in this JVM version
            return defaultValue;
        }
    }

    private static int getIntVmOption(HotSpotDiagnosticMXBean bean, String option, int defaultValue)
    {
        try {
            return Integer.parseInt(bean.getVMOption(option).getValue());
        }
        catch (IllegalArgumentException e) {
            // Option doesn't exist in this JVM version
            return defaultValue;
        }
    }

    public static final int BOOLEAN_INSTANCE_SIZE = instanceSize(Boolean.class);
    public static final int BYTE_INSTANCE_SIZE = instanceSize(Byte.class);
    public static final int SHORT_INSTANCE_SIZE = instanceSize(Short.class);
    public static final int CHARACTER_INSTANCE_SIZE = instanceSize(Character.class);
    public static final int INTEGER_INSTANCE_SIZE = instanceSize(Integer.class);
    public static final int LONG_INSTANCE_SIZE = instanceSize(Long.class);
    public static final int FLOAT_INSTANCE_SIZE = instanceSize(Float.class);
    public static final int DOUBLE_INSTANCE_SIZE = instanceSize(Double.class);

    public static final int OPTIONAL_INSTANCE_SIZE = instanceSize(Optional.class);
    public static final int OPTIONAL_INT_INSTANCE_SIZE = instanceSize(OptionalInt.class);
    public static final int OPTIONAL_LONG_INSTANCE_SIZE = instanceSize(OptionalLong.class);
    public static final int OPTIONAL_DOUBLE_INSTANCE_SIZE = instanceSize(OptionalDouble.class);

    public static final int STRING_INSTANCE_SIZE = instanceSize(String.class);

    private static final int SIMPLE_ENTRY_INSTANCE_SIZE = instanceSize(AbstractMap.SimpleEntry.class);

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

    public static long sizeOf(Boolean value)
    {
        return value == null ? 0 : BOOLEAN_INSTANCE_SIZE;
    }

    public static long sizeOf(Byte value)
    {
        return value == null ? 0 : BYTE_INSTANCE_SIZE;
    }

    public static long sizeOf(Short value)
    {
        return value == null ? 0 : SHORT_INSTANCE_SIZE;
    }

    public static long sizeOf(Character value)
    {
        return value == null ? 0 : CHARACTER_INSTANCE_SIZE;
    }

    public static long sizeOf(Integer value)
    {
        return value == null ? 0 : INTEGER_INSTANCE_SIZE;
    }

    public static long sizeOf(Long value)
    {
        return value == null ? 0 : LONG_INSTANCE_SIZE;
    }

    public static long sizeOf(Float value)
    {
        return value == null ? 0 : FLOAT_INSTANCE_SIZE;
    }

    public static long sizeOf(Double value)
    {
        return value == null ? 0 : DOUBLE_INSTANCE_SIZE;
    }

    public static <T> long sizeOf(Optional<T> optional, ToLongFunction<T> valueSize)
    {
        return optional != null && optional.isPresent() ? OPTIONAL_INSTANCE_SIZE + valueSize.applyAsLong(optional.get()) : 0;
    }

    public static long sizeOf(OptionalInt optional)
    {
        return optional != null && optional.isPresent() ? OPTIONAL_INT_INSTANCE_SIZE : 0;
    }

    public static long sizeOf(OptionalLong optional)
    {
        return optional != null && optional.isPresent() ? OPTIONAL_LONG_INSTANCE_SIZE : 0;
    }

    public static long sizeOf(OptionalDouble optional)
    {
        return optional != null && optional.isPresent() ? OPTIONAL_DOUBLE_INSTANCE_SIZE : 0;
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

    public static <T> long estimatedSizeOf(Queue<T> queue, ToLongFunction<T> valueSize)
    {
        if (queue == null) {
            return 0;
        }

        long result = sizeOfObjectArray(queue.size());
        for (T value : queue) {
            result += valueSize.applyAsLong(value);
        }
        return result;
    }

    public static <T> long estimatedSizeOf(Set<T> set, ToLongFunction<T> valueSize)
    {
        if (set == null) {
            return 0;
        }

        long result = sizeOfObjectArray(set.size());
        for (T value : set) {
            result += SIMPLE_ENTRY_INSTANCE_SIZE + valueSize.applyAsLong(value);
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

    public static <K, V> long estimatedSizeOf(Map<K, V> map, long keySize, long valueSize)
    {
        if (map == null) {
            return 0;
        }

        long result = sizeOfObjectArray(map.size());
        result += map.size() * (SIMPLE_ENTRY_INSTANCE_SIZE + keySize + valueSize);
        return result;
    }

    public static long sizeOfBooleanArray(int length)
    {
        return alignSize(ARRAY_HEADER_SIZE + (long) length);
    }

    public static long sizeOfByteArray(int length)
    {
        return alignSize(ARRAY_HEADER_SIZE + (long) length);
    }

    public static long sizeOfShortArray(int length)
    {
        return alignSize(ARRAY_HEADER_SIZE + ((long) Short.BYTES * length));
    }

    public static long sizeOfCharArray(int length)
    {
        return alignSize(ARRAY_HEADER_SIZE + ((long) Character.BYTES * length));
    }

    public static long sizeOfIntArray(int length)
    {
        return alignSize(ARRAY_HEADER_SIZE + ((long) Integer.BYTES * length));
    }

    public static long sizeOfLongArray(int length)
    {
        return alignSize(ARRAY_HEADER_SIZE + ((long) Long.BYTES * length));
    }

    public static long sizeOfFloatArray(int length)
    {
        return alignSize(ARRAY_HEADER_SIZE + ((long) Float.BYTES * length));
    }

    public static long sizeOfDoubleArray(int length)
    {
        return alignSize(ARRAY_HEADER_SIZE + ((long) Double.BYTES * length));
    }

    public static long sizeOfObjectArray(int length)
    {
        return alignSize(ARRAY_HEADER_SIZE + ((long) REFERENCE_SIZE * length));
    }

    /**
     * Estimates the size of an instance of the specified class.
     */
    public static int instanceSize(Class<?> clazz)
    {
        long size = OBJECT_HEADER_SIZE;

        // Sum up all instance field sizes, including inherited fields
        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    size += sizeOfField(field.getType());
                }
            }
        }

        // Align to object alignment boundary
        return toIntExact(alignSize(size));
    }

    private static int sizeOfField(Class<?> type)
    {
        if (type == boolean.class || type == byte.class) {
            return 1;
        }
        if (type == short.class || type == char.class) {
            return 2;
        }
        if (type == int.class || type == float.class) {
            return 4;
        }
        if (type == long.class || type == double.class) {
            return 8;
        }
        // Object reference
        return REFERENCE_SIZE;
    }

    private static long alignSize(long size)
    {
        return Math.ceilDiv(size, OBJECT_ALIGNMENT) * OBJECT_ALIGNMENT;
    }
}
