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

import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.IntToLongFunction;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;

/**
 * Runs in a child JVM started by {@link TestSizeOfJvmConfigurations}. It is both the {@code -javaagent}
 * (to get {@link Instrumentation}) and the main class that compares {@link SizeOf} with the JVM.
 */
public final class ArraySizeProbe
{
    static final String FLAG_PREFIX = "flag ";
    static final String MISMATCH_PREFIX = "mismatch ";
    static final String CHECKED_PREFIX = "checked ";
    static final List<String> FLAGS = List.of("UseCompactObjectHeaders", "UseCompressedOops", "UseCompressedClassPointers", "ObjectAlignmentInBytes");

    private static final int MAX_REPORTED_MISMATCHES = 20;

    private static Instrumentation instrumentation;

    private ArraySizeProbe() {}

    public static void premain(String agentArguments, Instrumentation instrumentation)
    {
        ArraySizeProbe.instrumentation = instrumentation;
    }

    public static void main(String[] args)
    {
        if (instrumentation == null) {
            throw new IllegalStateException("Probe must be started with -javaagent");
        }

        HotSpotDiagnosticMXBean hotSpot = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
        for (String flag : FLAGS) {
            System.out.println(FLAG_PREFIX + flag + "=" + hotSpot.getVMOption(flag).getValue());
        }

        List<String> mismatches = new ArrayList<>();
        int checked = 0;
        for (int length : lengths().toArray()) {
            checked += check("boolean[]", length, boolean[]::new, SizeOf::sizeOf, SizeOf::sizeOfBooleanArray, mismatches);
            checked += check("byte[]", length, byte[]::new, SizeOf::sizeOf, SizeOf::sizeOfByteArray, mismatches);
            checked += check("short[]", length, short[]::new, SizeOf::sizeOf, SizeOf::sizeOfShortArray, mismatches);
            checked += check("char[]", length, char[]::new, SizeOf::sizeOf, SizeOf::sizeOfCharArray, mismatches);
            checked += check("int[]", length, int[]::new, SizeOf::sizeOf, SizeOf::sizeOfIntArray, mismatches);
            checked += check("float[]", length, float[]::new, SizeOf::sizeOf, SizeOf::sizeOfFloatArray, mismatches);
            checked += check("long[]", length, long[]::new, SizeOf::sizeOf, SizeOf::sizeOfLongArray, mismatches);
            checked += check("double[]", length, double[]::new, SizeOf::sizeOf, SizeOf::sizeOfDoubleArray, mismatches);
            checked += check("Object[]", length, Object[]::new, SizeOf::sizeOf, SizeOf::sizeOfObjectArray, mismatches);
        }

        System.out.println(CHECKED_PREFIX + checked);
        mismatches.stream()
                .limit(MAX_REPORTED_MISMATCHES)
                .forEach(mismatch -> System.out.println(MISMATCH_PREFIX + mismatch));
        System.exit(mismatches.isEmpty() ? 0 : 1);
    }

    private static IntStream lengths()
    {
        return IntStream.concat(IntStream.rangeClosed(0, 257), IntStream.of(1023, 1024, 1025, 16383, 16384, 16385));
    }

    private static <T> int check(
            String type,
            int length,
            IntFunction<T> allocator,
            ToLongFunction<T> sizeOfArray,
            IntToLongFunction sizeOfLength,
            List<String> mismatches)
    {
        T array = allocator.apply(length);
        // HotSpot delegates to JVMTI GetObjectSize, which returns the actual object size
        long expected = instrumentation.getObjectSize(array);
        long actualFromArray = sizeOfArray.applyAsLong(array);
        long actualFromLength = sizeOfLength.applyAsLong(length);
        if (actualFromArray != expected || actualFromLength != expected) {
            mismatches.add("%s length=%s expected=%s sizeOf(array)=%s sizeOf*Array(length)=%s".formatted(
                    type, length, expected, actualFromArray, actualFromLength));
        }
        return 1;
    }
}
