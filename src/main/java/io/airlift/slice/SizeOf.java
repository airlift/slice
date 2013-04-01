package io.airlift.slice;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static sun.misc.Unsafe.ARRAY_BOOLEAN_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BOOLEAN_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
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

    private static final Unsafe UNSAFE;
    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
            if (UNSAFE == null) {
                throw new RuntimeException("Unsafe access not available");
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static long sizeOf(boolean[] array)
    {
        if (array == null) {
            return 0;
        }
        return ARRAY_BOOLEAN_BASE_OFFSET + ((long) ARRAY_BOOLEAN_INDEX_SCALE * array.length);
    }

    public static long sizeOf(byte[] array)
    {
        if (array == null) {
            return 0;
        }
        return ARRAY_BYTE_BASE_OFFSET + ((long) ARRAY_BYTE_INDEX_SCALE * array.length);
    }

    public static long sizeOf(short[] array)
    {
        if (array == null) {
            return 0;
        }
        return ARRAY_SHORT_BASE_OFFSET + ((long) ARRAY_SHORT_INDEX_SCALE * array.length);
    }

    public static long sizeOf(int[] array)
    {
        if (array == null) {
            return 0;
        }
        return ARRAY_INT_BASE_OFFSET + ((long) ARRAY_INT_INDEX_SCALE * array.length);
    }

    public static long sizeOf(long[] array)
    {
        if (array == null) {
            return 0;
        }
        return ARRAY_LONG_BASE_OFFSET + ((long) ARRAY_LONG_INDEX_SCALE * array.length);
    }

    public static long sizeOf(float[] array)
    {
        if (array == null) {
            return 0;
        }
        return ARRAY_FLOAT_BASE_OFFSET + ((long) ARRAY_FLOAT_INDEX_SCALE * array.length);
    }

    public static long sizeOf(double[] array)
    {
        if (array == null) {
            return 0;
        }
        return ARRAY_DOUBLE_BASE_OFFSET + ((long) ARRAY_DOUBLE_INDEX_SCALE * array.length);
    }

    public static long sizeOf(Object[] array)
    {
        if (array == null) {
            return 0;
        }
        return ARRAY_OBJECT_BASE_OFFSET + ((long) ARRAY_OBJECT_INDEX_SCALE * array.length);
    }

    private SizeOf()
    {
    }
}
