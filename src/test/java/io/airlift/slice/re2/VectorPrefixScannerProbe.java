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

import io.airlift.slice.Slices;

import java.nio.charset.StandardCharsets;

public final class VectorPrefixScannerProbe
{
    private VectorPrefixScannerProbe() {}

    public static void main(String[] arguments)
    {
        boolean expectedVectorApiAvailable = Boolean.parseBoolean(arguments[0]);
        int iterations = arguments.length > 1 ? Integer.parseInt(arguments[1]) : 1;
        byte[] prefix = "Шерлок Холмс".getBytes(StandardCharsets.UTF_8);
        byte[] data = new byte[4_096 + prefix.length];
        System.arraycopy(prefix, 0, data, data.length - prefix.length, prefix.length);

        Prog program = new Prog();
        program.configurePrefixAccel(Slices.wrappedBuffer(prefix), false);

        Prog.PrefixAccelStrategy expectedStrategy = expectedVectorApiAvailable
                ? Prog.PrefixAccelStrategy.FUSED_VECTOR
                : Prog.PrefixAccelStrategy.FUSED_SWAR;
        check(program.prefixAccelStrategy(data.length) == expectedStrategy, "unexpected prefix strategy");
        int checksum = 0;
        for (int iteration = 0; iteration < iterations; iteration++) {
            checksum += program.prefixAccel(data, 0, data.length);
        }
        check(checksum == (data.length - prefix.length) * iterations, "prefix scan failed");

        System.out.printf("OK %s%n", expectedVectorApiAvailable ? "vector" : "swar");
    }

    private static void check(boolean condition, String message)
    {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
