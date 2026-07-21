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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static io.airlift.slice.re2.Re2BenchmarkRunner.randomText;

/**
 * Assembly dump driver for DFA searchForward inner loop.
 * <p>
 * Usage:
 * <pre>
 * ./mvnw test-compile -q
 * CP="target/test-classes:target/classes:$(./mvnw -q dependency:build-classpath -DincludeScope=test -Dmdep.outputFile=/dev/stdout 2>/dev/null)"
 * java -XX:+UnlockDiagnosticVMOptions -XX:+PrintAssembly \
 *      -XX:CompileCommand=print,*Dfa.searchForward \
 *      -cp "$CP" \
 *      io.airlift.slice.re2.DfaAssemblyDump 2>&1 | tee searchForward-assembly.txt
 * </pre>
 */
public class DfaAssemblyDump
{
    private DfaAssemblyDump() {}

    public static void main(String[] args)
    {
        // HARD pattern: [ -~]* matches all printable ASCII, so the DFA stays in
        // the inner loop for the entire input. No literal prefix means canPrefixAccel
        // is false, so searchForward (not searchForwardPrefixAccel) is called.
        String pattern = "[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$";
        Prog prog = compileProg(pattern);
        byte[] textBytes = randomText(16 * 1024 * 1024); // 16MB of random text (matches HARD benchmark)
        Slice text = Slices.wrappedBuffer(textBytes);

        // Warmup: run enough iterations to trigger C2 compilation of searchForward
        System.out.println("Warming up for 10 seconds...");
        long warmupEnd = System.nanoTime() + 10_000_000_000L;
        long iterations = 0;
        while (System.nanoTime() < warmupEnd) {
            long result = Dfa.search(prog, text, false, Prog.MatchKind.FIRST_MATCH, true);
            if (result >= 0) {
                throw new AssertionError("unexpected match");
            }
            iterations++;
        }
        System.out.println("Warmup complete: " + iterations + " iterations");

        // Measurement pass — keeps the method hot for PrintAssembly
        System.out.println("Running measurement...");
        long start = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            long result = Dfa.search(prog, text, false, Prog.MatchKind.FIRST_MATCH, true);
            if (result >= 0) {
                throw new AssertionError("unexpected match");
            }
        }
        long elapsed = System.nanoTime() - start;
        System.out.println("Average: " + (elapsed / 100) + " ns per search (" + textBytes.length + " bytes)");
    }
}
