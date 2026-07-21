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

// Ported from upstream RE2: re2/testing/exhaustive1_test.cc.
public class TestExhaustiveRepetition
{
    private static List<Slice> repetitionOps()
    {
        List<Slice> ops = new ArrayList<>();
        for (String op : List.of(
                "%s{0}", "%s{0,}", "%s{1}", "%s{1,}", "%s{0,1}", "%s{0,2}",
                "%s{1,2}", "%s{2}", "%s{2,}", "%s{3,4}", "%s{4,5}",
                "%s*", "%s+", "%s?", "%s*?", "%s+?", "%s??")) {
            ops.add(Slices.wrappedBuffer(op.getBytes(UTF_8)));
        }
        return ops;
    }

    // Test simple repetition operators.
    @Test
    public void testRepetitionSimpleShortStrings()
    {
        runExhaustiveTest(
                List.of("a", "b", "c", "."),
                repetitionOps(),
                3, 2, "ab", 6, "(?:%s)");
    }

    @Test
    public void testRepetitionSimpleLongStrings()
    {
        runExhaustiveTest(
                List.of("a", "b", "c", "."),
                repetitionOps(),
                3, 2, "a", 40, "(?:%s)");
    }

    // Test capturing parens -- (a) -- inside repetition operators.
    @Test
    public void testRepetitionCapturingShortStrings()
    {
        runExhaustiveTest(
                List.of("a", "(a)", "b"),
                repetitionOps(),
                3, 2, "ab", 7, "(?:%s)");
    }

    @Test
    public void testRepetitionCapturingLongStrings()
    {
        runExhaustiveTest(
                List.of("a", "(a)"),
                repetitionOps(),
                3, 2, "a", 50, "(?:%s)");
    }

    private static void runExhaustiveTest(
            List<String> atomStrings,
            List<Slice> ops,
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

        CollectingGenerator gen = new CollectingGenerator(atoms, ops, maxAtoms, maxOps);
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
