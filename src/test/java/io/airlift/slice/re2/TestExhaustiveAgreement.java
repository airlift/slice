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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

// Ported from upstream RE2: re2/testing/exhaustive_test.cc.
public class TestExhaustiveAgreement
{
    // Test very simple expressions.
    @Test
    public void testEgrepLiteralsLowercase()
    {
        runExhaustiveEgrepTest(
                List.of("a", "b", "c", "."),
                3, 2, "abc", 3, null);
    }

    // Test mixed-case expressions.
    @Test
    public void testEgrepLiteralsMixedCase()
    {
        runExhaustiveEgrepTest(
                List.of("A", "a", "B", "b", "."),
                3, 2, "AaBb", 2, null);
    }

    // Test mixed-case in case-insensitive mode.
    @Test
    public void testEgrepLiteralsFoldCase()
    {
        // The punctuation characters surround A-Z and a-z
        // in the ASCII table.  This looks for bugs in the
        // bytemap range code in the DFA.
        runExhaustiveEgrepTest(
                List.of("a", "b", "A", "B", "."),
                3, 2, "aBc@_~", 2, "(?i:%s)");
    }

    // Test very simple expressions with UTF-8.
    @Test
    public void testEgrepLiteralsUtf8()
    {
        runExhaustiveEgrepTest(
                List.of("a", "b", "."),
                3, 2, "a\u263A", 4, null);
    }

    private static void runExhaustiveEgrepTest(
            List<String> atomStrings,
            int maxAtoms,
            int maxOps,
            String strAlphabet,
            int maxStrLen,
            String topWrapper)
    {
        List<Slice> atoms = new ArrayList<>();
        for (String a : atomStrings) {
            atoms.add(Slices.wrappedBuffer(a.getBytes(UTF_8)));
        }

        CollectingGenerator gen = new CollectingGenerator(atoms, RegexpGenerator.egrepOps(), maxAtoms, maxOps);
        gen.generate();

        List<Slice> alphabet = StringGenerator.explodeUtf8(Slices.wrappedBuffer(strAlphabet.getBytes(UTF_8)));
        StringGenerator strings = new StringGenerator(maxStrLen, alphabet);

        Slice wrapperPrefix = null;
        Slice wrapperSuffix = null;
        if (topWrapper != null) {
            int placeholderIndex = topWrapper.indexOf("%s");
            if (placeholderIndex >= 0) {
                wrapperPrefix = Slices.wrappedBuffer(topWrapper.substring(0, placeholderIndex).getBytes(UTF_8));
                wrapperSuffix = Slices.wrappedBuffer(topWrapper.substring(placeholderIndex + 2).getBytes(UTF_8));
            }
        }

        List<String> failures = new ArrayList<>();
        for (Slice regexp : gen.regexps()) {
            Slice testRegexp = regexp;
            if (wrapperPrefix != null) {
                testRegexp = Slices.wrappedBuffer(ByteArrays.concat(wrapperPrefix, regexp, wrapperSuffix));
            }

            Tester tester = new Tester(testRegexp, Tester.Config.fullMatrix());
            if (tester.error()) {
                failures.add("regexp=" + toUtf8(testRegexp) + " error: " + tester.failureMessage());
                continue;
            }

            strings.reset();
            while (strings.hasNext()) {
                Slice text = strings.next();
                if (!tester.testInput(text)) {
                    failures.add("regexp=" + toUtf8(testRegexp) + " " + tester.failureMessage());
                    break;
                }
            }
        }

        assertThat(failures).isEmpty();
    }

    private static String toUtf8(Slice bytes)
    {
        return new String(bytes.byteArray(), bytes.byteArrayOffset(), bytes.length(), UTF_8);
    }

    private static final class CollectingGenerator
            extends RegexpGenerator
    {
        private final List<Slice> regexps = new ArrayList<>();

        CollectingGenerator(List<Slice> atoms, List<Slice> ops, int maxAtoms, int maxOps)
        {
            super(atoms, ops, maxAtoms, maxOps);
        }

        @Override
        protected void handleRegexp(Slice regexp)
        {
            regexps.add(regexp);
        }

        public List<Slice> regexps()
        {
            return regexps;
        }
    }
}
