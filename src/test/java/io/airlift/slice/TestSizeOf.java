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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Array;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfBooleanArray;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfCharArray;
import static io.airlift.slice.SizeOf.sizeOfDoubleArray;
import static io.airlift.slice.SizeOf.sizeOfFloatArray;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.airlift.slice.SizeOf.sizeOfLongArray;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static io.airlift.slice.SizeOf.sizeOfShortArray;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSizeOf
{
    @Test
    public void testInstanceSize()
    {
        // instanceSize returns positive values
        assertThat(instanceSize(BasicClass.class)).isGreaterThan(0);
        assertThat(instanceSize(BasicRecord.class)).isGreaterThan(0);

        // Class and record with same fields should have the same instance size
        assertThat(instanceSize(BasicRecord.class)).isEqualTo(instanceSize(BasicClass.class));

        // Empty class should only have header size (aligned to 8 bytes)
        // With standard headers: 12 or 16 bytes, aligned to 16
        // With compact headers: 8 bytes
        assertThat(instanceSize(EmptyClass.class)).isIn(8, 16);

        // Class with single byte field should be header + 1, aligned to 8 bytes
        // With 12-byte header: 13 -> 16
        // With 16-byte header: 17 -> 24
        // With 8-byte compact header: 9 -> 16
        assertThat(instanceSize(SingleByteClass.class)).isIn(16, 24);

        // Adding a long field increases size by 8 bytes
        assertThat(instanceSize(ClassWithLong.class))
                .isEqualTo(instanceSize(EmptyClass.class) + 8);
    }

    @Test
    public void testArraySizes()
    {
        // Array sizes should be positive
        assertThat(sizeOfByteArray(0)).isGreaterThan(0);
        assertThat(sizeOfByteArray(100)).isGreaterThan(sizeOfByteArray(0));

        // Use larger arrays to avoid alignment effects absorbing the difference
        // Int array should grow by 4 bytes per element (test 8 elements = 32 bytes difference)
        assertThat(sizeOfIntArray(108) - sizeOfIntArray(100)).isEqualTo(8 * Integer.BYTES);

        // Long array should grow by 8 bytes per element (test 8 elements = 64 bytes difference)
        assertThat(sizeOfLongArray(108) - sizeOfLongArray(100)).isEqualTo(8 * Long.BYTES);

        // Object array element size depends on compressed oops (4 or 8 bytes per element)
        // Test 8 elements difference
        long elementSizeTotal = sizeOfObjectArray(108) - sizeOfObjectArray(100);
        assertThat(elementSizeTotal).isIn(8 * 4L, 8 * 8L);
    }

    @ParameterizedTest
    @MethodSource("arrayLengths")
    public void testArraySizesAgainstJvm(int length)
    {
        assertArraySize(new boolean[length], sizeOfBooleanArray(length), SizeOf::sizeOf);
        assertArraySize(new byte[length], sizeOfByteArray(length), SizeOf::sizeOf);
        assertArraySize(new short[length], sizeOfShortArray(length), SizeOf::sizeOf);
        assertArraySize(new char[length], sizeOfCharArray(length), SizeOf::sizeOf);
        assertArraySize(new int[length], sizeOfIntArray(length), SizeOf::sizeOf);
        assertArraySize(new float[length], sizeOfFloatArray(length), SizeOf::sizeOf);
        assertArraySize(new long[length], sizeOfLongArray(length), SizeOf::sizeOf);
        assertArraySize(new double[length], sizeOfDoubleArray(length), SizeOf::sizeOf);
        assertArraySize(new Object[length], sizeOfObjectArray(length), SizeOf::sizeOf);
    }

    private static IntStream arrayLengths()
    {
        return IntStream.concat(IntStream.rangeClosed(0, 257), IntStream.of(1023, 1024, 1025, 16383, 16384, 16385));
    }

    private static <T> void assertArraySize(T array, long sizeByLength, ToLongFunction<T> sizeOf)
    {
        long expectedSize = ObjectSizeAgent.getObjectSize(array);
        assertThat(sizeOf.applyAsLong(array))
                .as("sizeOf(%s), length %s", array.getClass().getSimpleName(), Array.getLength(array))
                .isEqualTo(expectedSize);
        assertThat(sizeByLength)
                .as("size by length of %s, length %s", array.getClass().getSimpleName(), Array.getLength(array))
                .isEqualTo(expectedSize);
    }

    @SuppressWarnings("unused")
    private static class EmptyClass {}

    @SuppressWarnings("unused")
    private static class SingleByteClass
    {
        private byte b;
    }

    @SuppressWarnings("unused")
    private static class ClassWithLong
    {
        private long l;
    }

    @SuppressWarnings("unused")
    private static class BasicClass
    {
        private String a;
        private int b;
        private long c;
        private boolean d;
    }

    @SuppressWarnings("unused")
    private record BasicRecord(String a, int b, long c, boolean d) {}
}
