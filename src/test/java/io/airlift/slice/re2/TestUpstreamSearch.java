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
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/search_test.cc.
public class TestUpstreamSearch
{
    private static final int UPSTREAM_TEST_CASE_COUNT = 236;

    @Test
    public void testUpstreamSearchTestCorpus()
            throws IOException
    {
        boolean debug = Boolean.getBoolean("re2.searchTest.debug");
        List<String> failures = new ArrayList<>();
        int testCaseCount = 0;

        try (InputStream in = TestUpstreamSearch.class.getResourceAsStream("/io/airlift/slice/re2/testing/upstream_search_test_cases.base64")) {
            if (in == null) {
                throw new IOException("missing resource: upstream_search_test_cases.base64");
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, US_ASCII))) {
                String line;
                int lineNumber = 0;
                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    if (line.isEmpty() || line.startsWith("#")) {
                        continue;
                    }
                    testCaseCount++;
                    if (debug && (lineNumber % 25) == 0) {
                        System.err.println("Progress: line=" + lineNumber);
                    }

                    int tab = line.indexOf('\t');
                    if (tab < 0) {
                        throw new IOException("bad line (missing tab) at " + lineNumber);
                    }

                    byte[] regexpBytes = Base64.getDecoder().decode(line.substring(0, tab));
                    byte[] textBytes = Base64.getDecoder().decode(line.substring(tab + 1));

                    Tester tester = new Tester(Slices.wrappedBuffer(regexpBytes), Tester.Config.fullMatrix());
                    long startNanos = System.nanoTime();
                    boolean ok = tester.testInput(Slices.wrappedBuffer(textBytes));
                    long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                    if (debug && elapsedMillis >= 1_000) {
                        System.err.println("Slow case: line=" + lineNumber + " elapsedMs=" + elapsedMillis);
                    }
                    if (!ok) {
                        failures.add("line=" + lineNumber + " " + tester.failureMessage());
                    }
                }
            }
        }

        assertThat(testCaseCount).isEqualTo(UPSTREAM_TEST_CASE_COUNT);
        assertThat(failures).isEmpty();
    }
}
