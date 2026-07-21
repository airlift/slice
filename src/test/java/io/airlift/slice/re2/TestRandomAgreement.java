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
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

// Ported from upstream RE2: re2/testing/random_test.cc.
public class TestRandomAgreement
{
    private static final int REGEXP_SEED = 404;
    private static final int REGEXP_CONSTRUCTION_COUNT = 100;
    private static final int WRAPPER_COUNT = 4;
    private static final int STRING_SEED = 200;
    private static final int STRING_COUNT = 100;
    private static final int SHARD_CASE_COUNT = REGEXP_CONSTRUCTION_COUNT * WRAPPER_COUNT * STRING_COUNT;
    private static final String[] WRAPPERS = {"none", "both", "start", "end"};
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private static final List<CorpusShard> SHARDS = List.of(
            new CorpusShard("small_egrep_literals", "random_agreement_small_egrep_literals.tsv.gzip", 398_250, "8e49e248bdd526e18c7524cf3447d1c4dde2576fb316ffbfde37d679c1a523ce"),
            new CorpusShard("big_egrep_literals", "random_agreement_big_egrep_literals.tsv.gzip", 414_251, "e50ebf75de71499c4915a80de4049e7bfefdb00c7323e1dc012bb2fdfa12c2d2"),
            new CorpusShard("small_egrep_captures", "random_agreement_small_egrep_captures.tsv.gzip", 407_383, "5d68f136ff7f2f402906369f18052d2049f4689e2628b7aa7e1a11ad8c67aef6"),
            new CorpusShard("big_egrep_captures", "random_agreement_big_egrep_captures.tsv.gzip", 429_183, "8ab93c3c7e9ea7bc11625693734d050fb2b82954a96ef5e720fccce90097c77c"),
            new CorpusShard("complicated", "random_agreement_complicated.tsv.gzip", 441_026, "91239c8281d3ee9e0b0785ceb2e5d29ae4ba9c2560efe5b069ff44b2bc5547f1"));

    @TestFactory
    public List<DynamicTest> testPinnedRandomCorpus()
    {
        return SHARDS.stream()
                .map(shard -> DynamicTest.dynamicTest(shard.group(), () -> assertShard(shard)))
                .toList();
    }

    private static void assertShard(CorpusShard shard)
            throws IOException, NoSuchAlgorithmException
    {
        // Upstream probes a null string before these 100 texts. Slice APIs reject null,
        // so the native corpus records that probe as Java-not-applicable and omits it.
        byte[] compressedCorpus = readResource(shard.resource());
        check(compressedCorpus.length == shard.compressedSize(),
                shard.group() + " compressed size: expected " + shard.compressedSize() + ", found " + compressedCorpus.length);
        String actualHash = HEX_FORMAT.formatHex(MessageDigest.getInstance("SHA-256").digest(compressedCorpus));
        check(actualHash.equals(shard.sha256()),
                shard.group() + " SHA-256: expected " + shard.sha256() + ", found " + actualHash);

        List<CorpusCase> cases = readCorpus(shard, compressedCorpus);
        check(cases.size() == SHARD_CASE_COUNT,
                shard.group() + " case count: expected " + SHARD_CASE_COUNT + ", found " + cases.size());

        NativeRegexpState regexpState = null;
        for (int caseIndex = 0; caseIndex < cases.size(); caseIndex++) {
            CorpusCase testCase = cases.get(caseIndex);
            assertCaseIdentity(shard, caseIndex, testCase);
            if (regexpState == null || regexpState.regexpCaseId() != testCase.regexpCaseId()) {
                regexpState = NativeRegexpState.create(testCase);
            }
            else {
                check(Arrays.equals(regexpState.regexpBytes(), testCase.regexpBytes()),
                        testCase.caseId() + " changed regexp bytes within a regexp case");
                check(regexpState.groupCount() == testCase.groupCount(),
                        testCase.caseId() + " changed group count within a regexp case");
            }
            assertNativeResult(regexpState, testCase);
        }

        int regexpCaseId = -1;
        Tester tester = null;
        for (CorpusCase testCase : cases) {
            if (regexpCaseId != testCase.regexpCaseId()) {
                regexpCaseId = testCase.regexpCaseId();
                tester = new Tester(Slices.wrappedBuffer(testCase.regexpBytes()), Tester.Config.fullMatrix());
                check(!tester.error(), testCase.caseId() + " failed to compile in Java: " + tester.failureMessage());
            }
            check(tester.testInput(testCase.text()),
                    testCase.caseId() + " Java engine disagreement: " + tester.failureMessage());
        }
    }

    private static List<CorpusCase> readCorpus(CorpusShard shard, byte[] compressedCorpus)
            throws IOException
    {
        List<CorpusCase> cases = new ArrayList<>(SHARD_CASE_COUNT);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new GZIPInputStream(new ByteArrayInputStream(compressedCorpus)), StandardCharsets.US_ASCII))) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                CorpusCase testCase = CorpusCase.parse(line, shard.resource(), lineNumber);
                cases.add(testCase);
            }
        }
        return cases;
    }

    private static void assertCaseIdentity(CorpusShard shard, int caseIndex, CorpusCase testCase)
    {
        int expectedRegexpCaseId = caseIndex / STRING_COUNT;
        int expectedConstructionId = expectedRegexpCaseId / WRAPPER_COUNT;
        int expectedWrapperId = expectedRegexpCaseId % WRAPPER_COUNT;
        int expectedTextCaseId = caseIndex % STRING_COUNT;
        String expectedCaseId = String.format(Locale.ROOT, "%s-r%03d-w%d-t%03d",
                shard.group(), expectedConstructionId, expectedWrapperId, expectedTextCaseId);

        check(testCase.group().equals(shard.group()), expectedCaseId + " has group " + testCase.group());
        check(testCase.regexpSeed() == REGEXP_SEED, expectedCaseId + " has regexp seed " + testCase.regexpSeed());
        check(testCase.constructionId() == expectedConstructionId, expectedCaseId + " has construction ID " + testCase.constructionId());
        check(testCase.regexpCaseId() == expectedRegexpCaseId, expectedCaseId + " has regexp case ID " + testCase.regexpCaseId());
        check(testCase.wrapper().equals(WRAPPERS[expectedWrapperId]), expectedCaseId + " has wrapper " + testCase.wrapper());
        check(testCase.stringSeed() == STRING_SEED, expectedCaseId + " has string seed " + testCase.stringSeed());
        check(testCase.textCaseId() == expectedTextCaseId, expectedCaseId + " has text case ID " + testCase.textCaseId());
        check(testCase.caseId().equals(expectedCaseId), expectedCaseId + " has case ID " + testCase.caseId());
    }

    private static void assertNativeResult(NativeRegexpState regexpState, CorpusCase testCase)
    {
        int[] actualGroups = new int[testCase.groupCount() * 2];
        boolean actualMatched = regexpState.regexp().matchInto(
                testCase.text(), 0, testCase.text().length(), Re2.Anchor.UNANCHORED, actualGroups);
        check(actualMatched == testCase.matched(),
                testCase.caseId() + " native matched=" + testCase.matched() + ", Java matched=" + actualMatched);
        if (actualMatched) {
            check(Arrays.equals(actualGroups, testCase.groups()),
                    testCase.caseId() + " native groups=" + Arrays.toString(testCase.groups()) +
                            ", Java groups=" + Arrays.toString(actualGroups));
        }
    }

    private static byte[] readResource(String resource)
            throws IOException
    {
        try (InputStream input = TestRandomAgreement.class.getResourceAsStream("/io/airlift/slice/re2/testing/" + resource)) {
            if (input == null) {
                throw new IllegalArgumentException("resource not found: " + resource);
            }
            return input.readAllBytes();
        }
    }

    private static void check(boolean condition, String message)
    {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private record CorpusShard(String group, String resource, int compressedSize, String sha256) {}

    private record CorpusCase(
            String group,
            int regexpSeed,
            int constructionId,
            int regexpCaseId,
            String wrapper,
            int stringSeed,
            int textCaseId,
            String caseId,
            byte[] regexpBytes,
            Slice text,
            boolean matched,
            int groupCount,
            int[] groups)
    {
        private static CorpusCase parse(String line, String resource, int lineNumber)
        {
            String[] fields = line.split("\\t", -1);
            if (fields.length != 13) {
                throw new IllegalArgumentException(resource + ":" + lineNumber + ": expected 13 fields, found " + fields.length);
            }

            int groupCount = parseInteger(fields[11], resource, lineNumber, "group count");
            boolean matched = parseBoolean(fields[10], resource, lineNumber);
            int[] groups = matched ? parseGroups(fields[12], groupCount, resource, lineNumber) : new int[0];
            if (!matched && !fields[12].equals("-")) {
                throw new IllegalArgumentException(resource + ":" + lineNumber + ": unmatched case has groups");
            }
            return new CorpusCase(
                    fields[0],
                    parseInteger(fields[1], resource, lineNumber, "regexp seed"),
                    parseInteger(fields[2], resource, lineNumber, "construction ID"),
                    parseInteger(fields[3], resource, lineNumber, "regexp case ID"),
                    fields[4],
                    parseInteger(fields[5], resource, lineNumber, "string seed"),
                    parseInteger(fields[6], resource, lineNumber, "text case ID"),
                    fields[7],
                    decodeHex(fields[8]),
                    Slices.wrappedBuffer(decodeHex(fields[9])),
                    matched,
                    groupCount,
                    groups);
        }

        private static int[] parseGroups(String value, int groupCount, String resource, int lineNumber)
        {
            String[] ranges = value.split(",", -1);
            if (ranges.length != groupCount) {
                throw new IllegalArgumentException(resource + ":" + lineNumber + ": expected " + groupCount + " groups, found " + ranges.length);
            }
            int[] groups = new int[groupCount * 2];
            for (int group = 0; group < groupCount; group++) {
                String[] offsets = ranges[group].split(":", -1);
                if (offsets.length != 2) {
                    throw new IllegalArgumentException(resource + ":" + lineNumber + ": invalid group range " + ranges[group]);
                }
                groups[group * 2] = parseInteger(offsets[0], resource, lineNumber, "group start");
                groups[(group * 2) + 1] = parseInteger(offsets[1], resource, lineNumber, "group end");
            }
            return groups;
        }

        private static int parseInteger(String value, String resource, int lineNumber, String name)
        {
            try {
                return Integer.parseInt(value);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException(resource + ":" + lineNumber + ": invalid " + name + " " + value, e);
            }
        }

        private static boolean parseBoolean(String value, String resource, int lineNumber)
        {
            return switch (value) {
                case "true" -> true;
                case "false" -> false;
                default -> throw new IllegalArgumentException(resource + ":" + lineNumber + ": invalid boolean " + value);
            };
        }

        private static byte[] decodeHex(String value)
        {
            return value.equals("-") ? new byte[0] : HEX_FORMAT.parseHex(value);
        }
    }

    private record NativeRegexpState(int regexpCaseId, byte[] regexpBytes, int groupCount, Re2 regexp)
    {
        private static NativeRegexpState create(CorpusCase testCase)
        {
            Slice regexpBytes = Slices.wrappedBuffer(testCase.regexpBytes());
            return new NativeRegexpState(
                    testCase.regexpCaseId(),
                    testCase.regexpBytes(),
                    testCase.groupCount(),
                    Re2.compile(regexpBytes));
        }
    }
}
