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
    static final String COMPRESSED_CLASS_POINTERS = "UseCompressedClassPointers";
    static final List<String> FLAGS = List.of("UseCompactObjectHeaders", "UseCompressedOops", COMPRESSED_CLASS_POINTERS, "ObjectAlignmentInBytes");

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
            System.out.println(FLAG_PREFIX + flag + "=" + flagValue(hotSpot, flag));
        }

        List<String> mismatches = new ArrayList<>();
        for (int length : lengths().toArray()) {
            check(length, boolean[]::new, SizeOf::sizeOf, SizeOf::sizeOfBooleanArray, mismatches);
            check(length, byte[]::new, SizeOf::sizeOf, SizeOf::sizeOfByteArray, mismatches);
            check(length, short[]::new, SizeOf::sizeOf, SizeOf::sizeOfShortArray, mismatches);
            check(length, char[]::new, SizeOf::sizeOf, SizeOf::sizeOfCharArray, mismatches);
            check(length, int[]::new, SizeOf::sizeOf, SizeOf::sizeOfIntArray, mismatches);
            check(length, float[]::new, SizeOf::sizeOf, SizeOf::sizeOfFloatArray, mismatches);
            check(length, long[]::new, SizeOf::sizeOf, SizeOf::sizeOfLongArray, mismatches);
            check(length, double[]::new, SizeOf::sizeOf, SizeOf::sizeOfDoubleArray, mismatches);
            check(length, Object[]::new, SizeOf::sizeOf, SizeOf::sizeOfObjectArray, mismatches);
        }

        System.out.println("mismatches " + mismatches.size());
        mismatches.stream()
                .limit(MAX_REPORTED_MISMATCHES)
                .forEach(mismatch -> System.out.println("mismatch " + mismatch));
        System.exit(mismatches.isEmpty() ? 0 : 1);
    }

    private static String flagValue(HotSpotDiagnosticMXBean hotSpot, String flag)
    {
        try {
            return hotSpot.getVMOption(flag).getValue();
        }
        catch (IllegalArgumentException e) {
            // Removed in JDK 27, where class pointers are always compressed. SizeOf also defaults it to true.
            if (flag.equals(COMPRESSED_CLASS_POINTERS)) {
                return "true";
            }
            throw e;
        }
    }

    private static IntStream lengths()
    {
        return IntStream.concat(IntStream.rangeClosed(0, 257), IntStream.of(1023, 1024, 1025, 16383, 16384, 16385));
    }

    private static <T> void check(
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
                    array.getClass().getSimpleName(), length, expected, actualFromArray, actualFromLength));
        }
    }
}
